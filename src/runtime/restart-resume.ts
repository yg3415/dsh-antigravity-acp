/**
 * dsh 重启后接着做（需求 1020）。
 *
 * 一个会话经本插件（ACP 路由或代理路由）在跑主对话的一轮时 dsh 重启了，重启后
 * 给这个会话发一条续跑通知开一轮。三件事：
 *
 * - 记账：为会话 S 服务一次主对话请求（`purpose` 为空）时把 S 记进 `busy.json`；
 *   收到 S 的 `turn/end` 时删掉，除非这一轮是被 dsh 关停（aborted / disposed）或
 *   冷加载补上的 interrupted 结束的——这两种正是要续跑的轮。被杀时没有任何事件，
 *   自然保留。
 * - 启动：每个 dsh 进程第一次装载时读出并清空 `busy.json`，延时（等 dsh 装好会话
 *   和 agent）逐个发通知。热重载不在新进程里，不读、不发。
 * - 投递：照 omc `wake` 与 dsh-schedule 的写法，冷会话经 `sessionController.resolveAgent`
 *   取 Agent 后 `followup`；没加载的子代理会话经父会话投递。
 *
 * 状态以 busy 文件为准；它只存会话 id，不存对话内容。
 */
import { mkdir, readFile, rename, unlink, writeFile } from 'node:fs/promises'
import { randomUUID } from 'node:crypto'
import { basename, dirname, join } from 'node:path'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import type { AdapterRequestOptions } from '../adapter/request-options.js'

/** 续跑通知的正文；需求 1020 要求写明的三件事都在里面。 */
export const RESTART_TEXT =
  '[Antigravity] dsh 重启打断了上一轮。重启时还没结束的工具调用已被停止，结果未知（需要的话先确认再决定要不要重做）。请从被打断的地方接着做完。如果这个会话有进行中的目标，dsh 恢复时会把它停下：先调 get_goal，再用 update_goal 的 resume 恢复。'
/** 通知卡片上的一句话摘要。 */
export const RESTART_SUMMARY = 'dsh 重启后接着做'
/** 启动后等多久再发通知：dsh 要先装好会话与 agent（omc 实测用 10 秒）。 */
export const RESUME_DELAY_MS = 10_000
/** 本插件的两条路由；会话最后一次请求不是这两条的，不归本插件续跑。 */
export const OWN_PROVIDERS: readonly string[] = ['antigravity-acp', 'antigravity-proxy']
/** 插件自己的通知来源（v4 日志要求 producer-owned kind）。 */
export const PLUGIN_NAME = 'dsh-antigravity-acp'
export const NOTICE_KIND = `plugin:${PLUGIN_NAME}`
/** busy 文件名，放在插件缓存目录（按 DSH_HOME 区分）下。 */
export const BUSY_FILE = 'busy.json'
/** 进程级启动标记：dsh 启动时可能多次装载插件，只有第一次读 busy 文件。 */
export const BOOT_KEY = Symbol.for('dsh-antigravity-acp.restartResume')

type Log = { info?: (message: string) => void; warn?: (message: string) => void }

// ------------------------------------------------------------------ 记账

/** 同一文件的读改写串行；跨插件实例共用，所以放在模块级。 */
const chains = new Map<string, Promise<unknown>>()

function serial<T>(file: string, work: () => Promise<T>): Promise<T> {
  const next = (chains.get(file) ?? Promise.resolve()).then(work, work)
  chains.set(file, next.catch(() => undefined))
  return next
}

async function readIds(file: string): Promise<string[] | undefined> {
  let raw: string
  try { raw = await readFile(file, 'utf8') } catch { return undefined }
  try {
    const parsed: unknown = JSON.parse(raw)
    return Array.isArray(parsed) ? parsed.filter((id): id is string => typeof id === 'string' && id !== '') : []
  } catch {
    return []
  }
}

async function writeIds(file: string, ids: readonly string[]): Promise<void> {
  await mkdir(dirname(file), { recursive: true })
  const temporary = join(dirname(file), `.${basename(file)}.${randomUUID()}.tmp`)
  try {
    await writeFile(temporary, `${JSON.stringify(ids)}\n`, { encoding: 'utf8', mode: 0o600 })
    await rename(temporary, file)
  } catch (error) {
    await unlink(temporary).catch(() => undefined)
    throw error
  }
}

/** 哪些会话有一轮在跑；写失败只记 warn（这次不续跑）。 */
export interface BusyLedger {
  mark(sessionId: string): Promise<void>
  clear(sessionId: string): Promise<void>
  /** 读出并清空：上一个进程留下的、没正常结束的会话。 */
  take(): Promise<string[]>
}

export function createBusyLedger(file: string, log: Log = {}): BusyLedger {
  const update = (sessionId: string, on: boolean) => serial(file, async () => {
    const ids = new Set(await readIds(file) ?? [])
    if (ids.has(sessionId) === on) return
    if (on) ids.add(sessionId)
    else ids.delete(sessionId)
    await writeIds(file, [...ids])
  }).catch((error: unknown) => {
    log.warn?.(`antigravity-acp: 续跑记录写入失败，这一轮重启后不会自动接着做：${error instanceof Error ? error.message : String(error)}`)
  })
  return {
    mark: (sessionId) => update(sessionId, true),
    clear: (sessionId) => update(sessionId, false),
    take: () => serial(file, async () => {
      const ids = await readIds(file)
      if (ids === undefined) return []
      await writeIds(file, []).catch(() => undefined)
      return ids
    }),
  }
}

/** 主对话请求才记：标题生成、压缩等带 purpose 的辅助请求不算一轮。 */
export function noteConversationRequest(ledger: BusyLedger | undefined, options: Pick<AdapterRequestOptions, 'sessionId' | 'purpose'>): void {
  if (!ledger || options.purpose !== undefined || typeof options.sessionId !== 'string' || options.sessionId === '') return
  void ledger.mark(options.sessionId)
}

type StreamFn = (options: AdapterRequestOptions) => AsyncIterable<unknown>
interface TrackableAdapter {
  stream: StreamFn
  prepareCall?: (provider: string, model: string, signal?: AbortSignal) => Promise<{ stream: StreamFn }>
}

/**
 * 在 adapter 实例上包一层：每次生成请求先交给 `note`。直接流式与 prepareCall
 * 返回的流两条入口都包；同一请求被记两次无害（mark 幂等）。
 */
export function trackRequests<T extends object>(adapter: T, note: (options: AdapterRequestOptions) => void): T {
  const target = adapter as unknown as TrackableAdapter
  const stream = target.stream.bind(adapter)
  target.stream = (options) => { note(options); return stream(options) }
  const prepare = target.prepareCall?.bind(adapter)
  if (prepare) {
    target.prepareCall = async (provider, model, signal) => {
      const call = await prepare(provider, model, signal)
      const prepared = call.stream
      return { ...call, stream: (options: AdapterRequestOptions) => { note(options); return prepared(options) } }
    }
  }
  return adapter
}

/** 这一轮的结束是不是重启打断的（要续跑，busy 保留）。 */
export function interruptedByRestart(reason: unknown): boolean {
  if (typeof reason !== 'object' || reason === null) return false
  const { kind } = reason as { kind?: unknown }
  if (kind === 'interrupted') return true
  if (kind !== 'aborted') return false
  const inner = (reason as { reason?: unknown }).reason
  return typeof inner === 'object' && inner !== null && (inner as { kind?: unknown }).kind === 'disposed'
}

/** `session/event` 监听：正常结束、用户 Stop、出错结束的轮把会话从 busy 里删掉。 */
export function busyTurnListener(ledger: BusyLedger): (session: unknown, event: unknown) => void {
  return (session, event) => {
    if (typeof event !== 'object' || event === null || (event as { type?: unknown }).type !== 'turn/end') return
    const id = typeof session === 'object' && session !== null ? (session as { id?: unknown }).id : undefined
    if (typeof id !== 'string' || id === '') return
    const data = (event as { data?: { reason?: unknown } }).data
    if (interruptedByRestart(data?.reason)) return
    void ledger.clear(id)
  }
}

// ------------------------------------------------------------------ 判断

interface LooseEvent { type?: unknown; data?: unknown }

/** 会话最近一次请求用的 provider：最后一条 `request/header` 的 `header.config.provider`。 */
export function lastRequestProvider(events: Iterable<LooseEvent>): string | undefined {
  let provider: string | undefined
  for (const event of events) {
    if (event?.type !== 'request/header') continue
    const config = (event.data as { header?: { config?: { provider?: unknown } } } | undefined)?.header?.config
    if (typeof config?.provider === 'string') provider = config.provider
  }
  return provider
}

/**
 * inbox 里是否已经有本插件的续跑通知、还没被一轮取走（最后一个 `turn/start` 之后的
 * next-turn `agent/inbox/spliced`）。dsh 的 inbox 是持久的，再发会叠一条。
 * 代表用户发出的（user source）按正文认。
 */
export function hasPendingNotice(events: Iterable<LooseEvent>): boolean {
  let pending = false
  for (const event of events) {
    if (event?.type === 'turn/start') { pending = false; continue }
    if (event?.type !== 'agent/inbox/spliced') continue
    const data = event.data as { target?: unknown; inserted?: unknown } | undefined
    if (data?.target !== 'next-turn' || !Array.isArray(data.inserted)) continue
    for (const message of data.inserted as Array<{ source?: { kind?: unknown; plugin?: unknown }; content?: unknown }>) {
      const source = message?.source
      if (source?.kind === NOTICE_KIND || (source?.kind === 'plugin' && source.plugin === PLUGIN_NAME)) pending = true
      if (Array.isArray(message?.content) && message.content.some((block: { text?: unknown }) => block?.text === RESTART_TEXT)) pending = true
    }
  }
  return pending
}

/** 通知的来源：有进行中的目标时用 user（dsh 只接受人发起的轮恢复目标），否则插件 notice。 */
export function noticeSource(goalActive: boolean, logVersion: number): Record<string, unknown> {
  if (goalActive) return { kind: 'user' }
  const rest = { form: 'notice', summary: RESTART_SUMMARY }
  return logVersion >= 4 ? { kind: NOTICE_KIND, ...rest } : { kind: 'plugin', plugin: PLUGIN_NAME, ...rest }
}

// ------------------------------------------------------------------ 投递

interface AgentLike {
  followup(message: unknown): void
  readonly status?: unknown
}
type AgentResult = { agent: AgentLike } | { error: unknown }
interface Inspection {
  meta?: { version?: unknown; origin?: unknown; parentSession?: unknown }
  events?: readonly LooseEvent[]
}

/** 投递要用到的 dsh 服务，都是可选的：缺哪个，那一步就做不了、记日志跳过。 */
export interface ResumeServices {
  sessionController?: {
    resolveAgent(sessionId: string): Promise<AgentResult>
    inspect(sessionId: string, signal?: AbortSignal): Promise<Inspection>
  }
  agents?: { get?(sessionId: string): AgentLike | undefined }
  goals?: { get?(agent: AgentLike): { phase?: unknown } | undefined }
  subagents?: Record<symbol, unknown>
}

const DELIVER_SUBAGENT_PROMPT = Symbol.for('dsh.subagent.deliverPrompt')
type DeliverPrompt = (parent: AgentLike, childId: string, content: unknown, source: unknown, signal: AbortSignal, delivery: 'queue' | 'steer') => Promise<unknown>

const errorText = (error: unknown) => error instanceof Error ? error.message : String(error)

function resolved(result: AgentResult): AgentLike {
  if ('error' in result) throw result.error instanceof Error ? result.error : new Error(String(result.error))
  return result.agent
}

function isRunning(agent: AgentLike | undefined): boolean {
  try { return agent?.status === 'running' } catch { return false }
}

/** 给一个被重启打断的会话发续跑通知；返回做了什么（写日志、测试用）。 */
export async function wakeInterrupted(sessionId: string, services: ResumeServices, log: Log = {}): Promise<string> {
  const controller = services.sessionController
  if (!controller) return 'skip: no sessionController'
  let inspection: Inspection
  try {
    inspection = await controller.inspect(sessionId)
  } catch (error) {
    return `skip: session unreadable (${errorText(error)})`
  }
  const events = inspection.events ?? []
  const provider = lastRequestProvider(events)
  if (provider === undefined || !OWN_PROVIDERS.includes(provider)) return `skip: provider ${provider ?? 'none'}`
  if (hasPendingNotice(events)) return 'skip: notice already pending'
  const logVersion = typeof inspection.meta?.version === 'number' ? inspection.meta.version : 0

  let loaded: AgentLike | undefined
  try { loaded = services.agents?.get?.(sessionId) } catch { loaded = undefined }
  if (isRunning(loaded)) return 'skip: a turn is already running'

  const parentId = inspection.meta?.origin === 'subagent' && typeof inspection.meta.parentSession === 'string'
    ? inspection.meta.parentSession
    : undefined
  if (loaded === undefined && parentId !== undefined) {
    // 没加载的子代理会话不能直接 resolve（归子代理路由管），经直接父会话投递。
    const deliver = services.subagents?.[DELIVER_SUBAGENT_PROMPT] as DeliverPrompt | undefined
    if (typeof deliver !== 'function') return 'skip: no subagent delivery'
    const parent = resolved(await controller.resolveAgent(parentId))
    const message = createUserMessage({ content: [{ type: 'text', text: RESTART_TEXT }], source: noticeSource(false, logVersion) as never })
    await deliver.call(services.subagents, parent, sessionId, message.content, message.source, AbortSignal.timeout(60_000), 'queue')
    log.info?.(`antigravity-acp: 已经父会话 ${parentId} 给子会话 ${sessionId} 发重启续跑通知`)
    return `sent via parent ${parentId}`
  }

  const agent = loaded ?? resolved(await controller.resolveAgent(sessionId))
  if (isRunning(agent)) return 'skip: a turn is already running'
  let goalActive = false
  try { goalActive = services.goals?.get?.(agent)?.phase === 'active' } catch { goalActive = false }
  agent.followup(createUserMessage({ content: [{ type: 'text', text: RESTART_TEXT }], source: noticeSource(goalActive, logVersion) as never }))
  log.info?.(`antigravity-acp: 已给会话 ${sessionId} 发重启续跑通知`)
  return goalActive ? 'sent (user source, goal active)' : 'sent'
}

// ------------------------------------------------------------------ 启动

interface BootState { services: () => ResumeServices; log: Log }

/**
 * 每个 dsh 进程只在第一次装载时读 busy 文件；之后的装载（启动时重新实例化、
 * 热重载）只更新投递用的服务。定时器不挂在 cordis 作用域上：启动时第一次装载的
 * 作用域可能在定时器触发前就被销毁。返回这一轮投递做完的 Promise（之后的装载返回 undefined）。
 */
export function resumeOnBoot(ledger: BusyLedger, services: () => ResumeServices, log: Log = {}, delayMs = RESUME_DELAY_MS): Promise<void> | undefined {
  const g = globalThis as Record<symbol, BootState | undefined>
  const existing = g[BOOT_KEY]
  if (existing) { existing.services = services; existing.log = log; return undefined }
  const state: BootState = { services, log }
  g[BOOT_KEY] = state
  const taken = ledger.take()
  let done!: () => void
  const finished = new Promise<void>((resolve) => { done = resolve })
  const timer = setTimeout(() => {
    void (async () => {
      const ids = await taken
      if (ids.length > 0) state.log.info?.(`antigravity-acp: 重启前有一轮在跑的会话：${ids.join(', ')}`)
      for (const id of ids) {
        try {
          const outcome = await wakeInterrupted(id, state.services(), state.log)
          if (outcome.startsWith('skip')) state.log.info?.(`antigravity-acp: 会话 ${id} 不续跑（${outcome}）`)
        } catch (error) {
          state.log.warn?.(`antigravity-acp: 给会话 ${id} 发重启续跑通知失败：${errorText(error)}`)
        }
      }
    })().finally(done)
  }, delayMs)
  ;(timer as { unref?: () => void }).unref?.()
  return finished
}

/** busy 文件的位置。 */
export function busyFile(cacheDir: string): string {
  return join(cacheDir, BUSY_FILE)
}
