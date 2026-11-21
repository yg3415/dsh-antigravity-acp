import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from 'vitest'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import type { LlmResolvedModelInfo, StreamChunk } from '@deepseek-ai/dsh-llm'
import type { AdapterRequestOptions } from '../src/adapter/request-options.js'
import type { AntigravityAcpAdapterOptions } from '../src/adapter/antigravity-adapter.js'
import type { SettingsControl } from '../src/quota/control.js'
import { PooledAcpAdapter } from '../src/pool/pooled-adapter.js'
import { apply } from '../src/index.js'
import { isVolatile, updateVolatile } from '@deepseek-ai/cosmokit'
import { Config } from '../src/settings.js'
import type { ProxyModeBus } from '../src/proxy/mode-bus.js'

interface WorkerStub {
  options: AntigravityAcpAdapterOptions
  close: Mock
  prepareCall: Mock
  discoverModels: Mock
  finish?: () => void
}

const external = vi.hoisted(() => ({
  workers: [] as WorkerStub[],
  events: [] as string[],
  fetchRoutes: [] as Array<() => { url: string; bypass: string } | undefined>,
  fetch: vi.fn(async () => { throw new Error('Unexpected network request') }),
  fetchClose: vi.fn(async () => {}),
  cli: vi.fn(async () => { throw new Error('Unexpected CLI invocation') }),
  download: vi.fn(async () => { throw new Error('Unexpected binary startup') }),
  cacheDir: '/fake/cache',
}))

vi.mock('../src/runtime/binary.js', async (original) => ({
  ...await original<typeof import('../src/runtime/binary.js')>(),
  cleanupStaleRuntimeDirectories: vi.fn(async () => 0),
  cleanupStaleStagingDirs: vi.fn(async () => 0),
  resolveCacheDir: () => external.cacheDir,
  resolveInstallRoot: () => '/fake/install',
  ensureAgentBinary: external.download,
}))
// 代理解析保持真实实现：装载期对非法代理的容错本身是被测行为（见下方用例）。
vi.mock('../src/runtime/proxy-env.js', async (original) => ({
  ...await original<typeof import('../src/runtime/proxy-env.js')>(),
}))
vi.mock('../src/runtime/proxied-fetch.js', async (original) => ({
  ...await original<typeof import('../src/runtime/proxied-fetch.js')>(),
  createLiveProxiedFetch: (readProxy: () => { url: string; bypass: string } | undefined) => {
    external.fetchRoutes.push(readProxy)
    return Object.assign(external.fetch, { close: external.fetchClose })
  },
}))
vi.mock('../src/runtime/agy-cli.js', () => ({
  agyCliCandidates: () => [], createSubprocessCliRun: () => external.cli, fetchAgyModels: external.cli,
}))
vi.mock('../src/runtime/catalog-cache.js', () => ({
  readCachedCatalog: vi.fn(async () => undefined), writeCachedCatalog: vi.fn(async () => {}),
}))
vi.mock('../src/quota/quota-remote.js', () => ({
  AgYQuotaService: class {}, registerAgYQuotaRemote: vi.fn(),
}))
vi.mock('../src/adapter/antigravity-adapter.js', () => ({
  AGY_PROVIDER_ID: 'antigravity-acp',
  AntigravityAcpAdapter: class {
    busy = false
    finish?: () => void
    readonly index = external.workers.length
    constructor(readonly options: AntigravityAcpAdapterOptions) {
      external.workers.push(this)
      options.registerBridgeTeardown?.(this.close)
    }
    close = vi.fn(async () => {
      external.events.push(`worker-${this.index}`)
      this.finish?.()
      this.busy = false
      this.options.lifecycle?.stopped()
    })
    importMetadata = vi.fn()
    async resolveModel(provider: string, id: string): Promise<LlmResolvedModelInfo> {
      return { provider, id, name: 'Example', inputModalities: this.options.canReadImages?.() ? ['text', 'image'] : ['text'] }
    }
    async listModels(provider: string) { return [await this.resolveModel(provider, 'example')] }
    prepareCall = vi.fn(async (provider: string, model: string) => ({
      model: await this.resolveModel(provider, model), stream: (options: AdapterRequestOptions) => this.stream(options),
    }))
    discoverModels = vi.fn(async (_signal?: AbortSignal, _interactive = true) => this.listModels('antigravity-acp'))
    warmup = vi.fn(async () => {})
    async *stream(_options: AdapterRequestOptions): AsyncGenerator<StreamChunk> {
      this.busy = true
      this.options.lifecycle?.spawned()
      await new Promise<void>((resolve) => { this.finish = resolve })
      this.busy = false
      yield { type: 'finish', reason: { kind: 'stop' } }
    }
  },
}))

type Cleanup = () => void | Promise<void>
type AttachmentStub = { readImage: Mock }
const harnesses: Array<{ dispose(): Promise<void> }> = []

function harness(initialAttachment?: AttachmentStub) {
  const cleanup: Cleanup[] = []
  const services = new Map<string, unknown>()
  let attachmentCleanup: Cleanup[] = []
  let injectAttachment: ((scope: unknown) => void) | undefined
  const waiting: Array<{ names: string[]; callback: (scope: unknown) => void }> = []
  let registered: PooledAcpAdapter | undefined
  let control: SettingsControl | undefined
  const unregister = vi.fn(() => { external.events.push('unregister') })
  const attach = (store: AttachmentStub) => {
    injectAttachment?.({ attachments: store, effect: (callback: () => Cleanup) => { attachmentCleanup.push(callback()) } })
  }
  const detach = async () => {
    for (const release of attachmentCleanup.reverse()) await release()
    attachmentCleanup = []
  }
  const ctx = {
    subprocess: { spawn: vi.fn(() => { throw new Error('Unexpected subprocess') }) },
    llm: {
      registerAdapter: vi.fn((_providers: string[], adapter: PooledAcpAdapter) => {
        registered = adapter
        return Object.assign(unregister, { replace: vi.fn() })
      }),
    },
    logger: { info: vi.fn(), warn: vi.fn() }, emit: vi.fn(),
    get: (name: string) => services.get(name),
    provide: vi.fn((name: string, value: unknown) => { services.set(name, value); return () => { services.delete(name) } }),
    on: vi.fn((_event: string, _listener: (paths: string[][]) => void) => () => {}),
    inject(names: string[], callback: (scope: unknown) => void) {
      if (names.includes('attachments')) {
        injectAttachment = callback
        if (initialAttachment) attach(initialAttachment)
        return
      }
      waiting.push({ names, callback })
    },
    plugin(_service: unknown, options: { control?: SettingsControl }) { if (options?.control) control = options.control },
    effect(callback: () => Generator<Cleanup> | Cleanup) {
      const result = callback()
      if (typeof result === 'function') cleanup.push(result)
      else cleanup.push(...result)
    },
  }
  let disposed = false
  const result = {
    ctx: ctx as unknown as Context,
    registered: () => registered!, control: () => control!, attach, detach, unregister,
    emit: ctx.emit,
    /** dsh 的可选服务就绪：满足依赖的 inject 回调按 cordis 的样子收到 scope。 */
    offer(provided: Record<string, unknown>) {
      for (const [name, value] of Object.entries(provided)) services.set(name, value)
      for (const { names, callback } of waiting) {
        if (names.every(name => services.has(name))) callback({ ...Object.fromEntries(services), get: (name: string) => services.get(name), effect: () => {} })
      }
    },
    sessionEvent: (session: unknown, event: unknown) => {
      for (const [name, listener] of ctx.on.mock.calls as unknown as Array<[string, (a: unknown, b: unknown) => void]>) if (name === 'session/event') listener(session, event)
    },
    updateSettings: (paths: string[][]) => { for (const [event, listener] of ctx.on.mock.calls) if (event === 'loader/volatile-update') listener(paths) },
    async dispose() {
      if (disposed) return
      disposed = true
      for (const release of cleanup.reverse()) await release()
      await detach()
    },
  }
  harnesses.push(result)
  return result
}

async function flush() { for (let i = 0; i < 30; i++) await Promise.resolve() }
function request(id: string): AdapterRequestOptions {
  return { provider: 'antigravity-acp', model: 'example', messages: [], sessionId: id as AdapterRequestOptions['sessionId'] }
}

beforeEach(async () => {
  external.cacheDir = await mkdtemp(join(tmpdir(), 'agy-integration-'))
  delete (globalThis as Record<symbol, unknown>)[Symbol.for('dsh-antigravity-acp.restartResume')]
  external.workers.length = 0
  external.events.length = 0
  external.fetchRoutes.length = 0
  vi.clearAllMocks()
  external.fetchClose.mockImplementation(async () => { external.events.push('fetch-close') })
})
afterEach(async () => {
  for (const instance of harnesses.splice(0)) await instance.dispose()
  expect(external.fetch).not.toHaveBeenCalled()
  expect(external.cli).not.toHaveBeenCalled()
  expect(external.download).not.toHaveBeenCalled()
  vi.unstubAllEnvs()
  vi.useRealTimers()
  delete (globalThis as Record<symbol, unknown>)[Symbol.for('dsh-antigravity-acp.restartResume')]
  await rm(external.cacheDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 20 }) // 记账写入是后台的，可能还在写
})

describe('apply wiring with real pool and SettingsControl', () => {
  it('代理 OAuth 显式继承宿主环境代理，与原生请求使用同一解析规则', () => {
    vi.stubEnv('https_proxy', 'http://ambient:7890')
    vi.stubEnv('HTTPS_PROXY', 'http://ambient:7890')
    vi.stubEnv('no_proxy', '*.internal')
    const instance = harness()
    const config = { proxyMode: true, proxyUrl: '' }
    apply(instance.ctx, config)
    expect(external.fetchRoutes).toHaveLength(2)
    expect(external.fetchRoutes[0]!()).toBeUndefined()
    expect(external.fetchRoutes[1]!()).toMatchObject({ url: 'http://ambient:7890' })
    config.proxyUrl = 'http://explicit:8080'
    expect(external.fetchRoutes[1]!()?.url).toBe('http://explicit:8080')
    config.proxyUrl = ''
    expect(external.fetchRoutes[1]!()?.url).toBe('http://ambient:7890')
  })
  it('代理模式的设置页目录读取与刷新仅走代理目录，切回 ACP 后恢复原目录', async () => {
    const instance = harness()
    const parsed = Config({ proxyMode: true })
    apply(instance.ctx, parsed)
    const models = vi.fn(async () => [{ id: 'proxy-only', name: 'Proxy only' }])
    instance.ctx.provide('antigravityProxyCatalog', { models })
    ;(instance.ctx.get('antigravityProxyMode') as ProxyModeBus).setRouterReady(true)
    expect((await instance.control().models(true)).models).toEqual([{ id: 'proxy-only', name: 'Proxy only' }])
    expect(models).toHaveBeenCalledWith(true)
    expect(external.workers[0]!.discoverModels).not.toHaveBeenCalled()

    const flag = parsed.proxyMode
    const next = Config({ proxyMode: false }).proxyMode
    if (!isVolatile(flag) || !isVolatile(next)) throw new Error('Expected live mode flag')
    updateVolatile(flag, next)
    instance.updateSettings([['proxyMode']])
    expect((await instance.control().models()).models[0]?.id).toBe('example')
  })

  it('代理组件缺席时仍关闭 ACP 目录、登录与预热', async () => {
    const instance = harness()
    apply(instance.ctx, Config({ proxyMode: true, autoWarmup: false }))
    expect(await instance.control().models()).toMatchObject({ status: 'error', models: [] })
    expect(instance.control().account().generationMode).toEqual({ requested: 'proxy', active: 'proxy', proxyRouterReady: false })
    expect(() => instance.control().account('login')).toThrow('ACP 连接与预热功能已停用')
    expect(() => instance.control().account('warmup')).toThrow('ACP 连接与预热功能已停用')
    expect(external.workers[0]!.discoverModels).not.toHaveBeenCalled()
    expect(external.download).not.toHaveBeenCalled()
  })

  it('代理组件上线与卸载自动切换路由和目录，并共享实时模型设置', async () => {
    const instance = harness()
    const parsed = Config({ proxyMode: true, modelOverrides: { proxy: { name: '自定义' } } })
    apply(instance.ctx, parsed)
    const bus = instance.ctx.get('antigravityProxyMode') as ProxyModeBus
    const registration = (instance.ctx.llm.registerAdapter as Mock).mock.results[0]!.value as { replace: Mock }
    expect(registration.replace).toHaveBeenLastCalledWith([])
    instance.ctx.provide('antigravityProxyCatalog', { models: async () => [{ id: 'proxy', name: '自定义' }] })
    bus.setRouterReady(true)
    expect(registration.replace).toHaveBeenLastCalledWith([])
    expect(instance.control().account().generationMode?.active).toBe('proxy')
    expect(bus.modelSettings?.().modelOverrides?.proxy?.name).toBe('自定义')
    expect((await instance.control().models()).models[0]?.id).toBe('proxy')
    bus.setRouterReady(false)
    expect(registration.replace).toHaveBeenLastCalledWith([])
    expect(instance.control().account().generationMode?.active).toBe('proxy')
    const flag = parsed.proxyMode
    const next = Config({ proxyMode: false }).proxyMode
    if (!isVolatile(flag) || !isVolatile(next)) throw new Error('Expected live mode flag')
    updateVolatile(flag, next)
    instance.updateSettings([['proxyMode']])
    expect(registration.replace).toHaveBeenLastCalledWith(['antigravity-acp'])
    expect((await instance.control().models()).models[0]?.id).toBe('example')
  })

  it('passes live preferences to existing workers and notifies the host picker on saved model settings', () => {
    const instance = harness()
    const parsed = Config({ modelOverrides: { before: { enabled: false } } })
    apply(instance.ctx, parsed)
    const next = Config({ modelOverrides: { after: { effort: 'high' } }, reuseSessions: false })
    for (const key of ['modelOverrides', 'reuseSessions'] as const) {
      const before = parsed[key]
      const after = next[key]
      if (!isVolatile(before) || !isVolatile(after)) throw new Error('Expected live setting')
      updateVolatile(before, after)
    }
    expect(external.workers[0]!.options.readSettings?.()).toMatchObject({ modelOverrides: { after: { enabled: true, effort: 'high' } }, reuseSessions: false })
    const beforeRevision = instance.control().account().runtime!.modelCatalogRevision!
    instance.updateSettings([['modelOverrides']])
    expect(instance.control().account().runtime!.modelCatalogRevision).toBe(beforeRevision + 1)
    expect(instance.emit).toHaveBeenCalledWith('llm/adapters-updated')
    instance.updateSettings([['proxyUrl']])
    expect(instance.control().account().runtime!.modelCatalogRevision).toBe(beforeRevision + 1)
  })

  it('handles synchronously available attachments without initialization-order errors', async () => {
    const instance = harness({ readImage: vi.fn() })
    expect(() => apply(instance.ctx, {})).not.toThrow()
    expect(instance.registered()).toBeInstanceOf(PooledAcpAdapter)
    const prepared = await instance.registered().prepareCall('antigravity-acp', 'example')
    expect(prepared.model.inputModalities).toEqual(['text', 'image'])
    expect(instance.control().account().runtime?.modelCatalogRevision).toBeGreaterThan(0)
  })

  it('invalidates capability preparation as the attachment service appears and disappears', async () => {
    const instance = harness()
    apply(instance.ctx, {})
    const pool = instance.registered()
    expect((await pool.prepareCall('antigravity-acp', 'example')).model.inputModalities).toEqual(['text'])
    expect(external.workers[0]!.prepareCall).toHaveBeenCalledTimes(1)
    instance.attach({ readImage: vi.fn() })
    expect((await pool.prepareCall('antigravity-acp', 'example')).model.inputModalities).toEqual(['text', 'image'])
    expect(external.workers[0]!.prepareCall).toHaveBeenCalledTimes(2)
    await instance.detach()
    expect((await pool.prepareCall('antigravity-acp', 'example')).model.inputModalities).toEqual(['text'])
    expect(external.workers[0]!.prepareCall).toHaveBeenCalledTimes(3)
  })

  it('refreshes models without interactive login but permits it for an explicit account login', async () => {
    const instance = harness()
    apply(instance.ctx, {})
    const control = instance.control()
    expect((await control.models(true)).status).toBe('ok')
    expect(external.workers[0]!.discoverModels).toHaveBeenLastCalledWith(undefined, false)
    expect(control.account().state).toBe('idle')
    expect(control.account('login').state).toBe('connecting')
    await flush()
    expect(external.workers[0]!.discoverModels).toHaveBeenLastCalledWith(expect.any(AbortSignal), true)
    expect(control.account().state).toBe('connected')
  })

  it('registers the pool and releases all worker resources and the shared fetch on unload', async () => {
    const instance = harness()
    apply(instance.ctx, {})
    const pool = instance.registered()
    expect(pool).toBeInstanceOf(PooledAcpAdapter)
    const a = pool.stream(request('a'))
    const b = pool.stream(request('b'))
    const first = a.next()
    const second = b.next()
    await flush()
    expect(external.workers).toHaveLength(2)
    await instance.dispose()
    await Promise.all([first, second])
    await Promise.all([a.return(undefined), b.return(undefined)])
    expect(external.workers.every(worker => worker.close.mock.calls.length === 1)).toBe(true)
    expect(external.fetchClose).toHaveBeenCalledTimes(2)
    expect(instance.unregister).toHaveBeenCalledTimes(1)
    expect(external.events.indexOf('fetch-close')).toBeGreaterThan(external.events.indexOf('worker-1'))
    expect(() => instance.control().account()).toThrow('已卸载')
  })

  it('keeps the plugin loadable when the saved proxy is invalid, so the card can fix it', () => {
    const instance = harness()
    // 非法代理不能让 apply 抛穿：一旦装载失败，本插件贡献的设置卡片也跟着消失，
    // 用户只剩手改宿主配置文件这一条路。
    expect(() => apply(instance.ctx, { proxyUrl: 'ftp://127.0.0.1:21' })).not.toThrow()
    expect(instance.registered()).toBeInstanceOf(PooledAcpAdapter)
    expect(instance.control().account().state).toBe('idle')
    const warned = (instance.ctx.logger as unknown as { warn: Mock }).warn.mock.calls.map(([message]) => String(message))
    expect(warned.some((message) => message.includes('代理配置无效'))).toBe(true)
    // 告警只报原因，不回显地址里的认证信息。
    expect(warned.join('\n')).not.toContain('ftp://127.0.0.1:21')
  })
})

describe('需求 1020：dsh 重启后接着做（经 apply 与池入口）', () => {
  const RESTART = Symbol.for('dsh-antigravity-acp.restartResume')
  const busyPath = () => join(external.cacheDir, 'busy.json')
  const readBusy = async (): Promise<unknown> => JSON.parse(await readFile(busyPath(), 'utf8'))
  const busyBecomes = (ids: string[]) => vi.waitFor(async () => expect(await readBusy()).toEqual(ids))
  const header = (provider: string) => ({ type: 'request/header', data: { header: { config: { provider, model: 'gemini-4-argon' } } } })
  const turnEnd = (reason: unknown) => ({ type: 'turn/end', data: { turn: 1, reason } })
  type FakeAgent = { followup: Mock; status: 'idle' | 'running' }

  /** dsh 服务（sessionController、agents、goals、subagents）的假实现，形状照 dsh 0.2.1。 */
  function dsh(sessions: Record<string, { events: unknown[]; origin?: 'subagent'; parentSession?: string }>) {
    const agents = new Map<string, FakeAgent>()
    const agentFor = (id: string) => {
      let agent = agents.get(id)
      if (!agent) { agent = { followup: vi.fn(), status: 'idle' }; agents.set(id, agent) }
      return agent
    }
    const deliver = vi.fn(async () => 'message-id')
    const goals = new Map<FakeAgent, string>()
    const loaded = new Map<string, FakeAgent>()
    const services = {
      sessionController: {
        inspect: vi.fn(async (id: string) => {
          const row = sessions[id]
          if (!row) throw new Error(`session ${id} not found`)
          return { meta: { id, version: 4, origin: row.origin, parentSession: row.parentSession }, events: row.events }
        }),
        resolveAgent: vi.fn(async (id: string) => sessions[id]?.origin === 'subagent' ? { error: new Error('owned by subagent routing') } : { agent: agentFor(id) }),
      },
      agents: { get: (id: string) => loaded.get(id) },
      goals: { get: (agent: FakeAgent) => (goals.has(agent) ? { phase: goals.get(agent) } : undefined) },
      subagents: { [Symbol.for('dsh.subagent.deliverPrompt')]: deliver },
    }
    return { services, agentFor, deliver, goals, loaded }
  }

  /** 一次 dsh 启动：装载插件、dsh 服务就绪、过了启动延时，等插件把重启续跑检查做完。 */
  async function boot(fake: ReturnType<typeof dsh>, between?: (instance: ReturnType<typeof harness>) => Promise<void>) {
    vi.useFakeTimers({ toFake: ['setTimeout'] })
    const instance = harness()
    apply(instance.ctx, Config({}))
    instance.offer(fake.services)
    await vi.advanceTimersByTimeAsync(0)
    await between?.(instance)
    await vi.advanceTimersByTimeAsync(10_000)
    vi.useRealTimers()
    const info = (instance.ctx as unknown as { logger: { info: Mock } }).logger.info
    await vi.waitFor(() => expect(info.mock.calls.some(([line]) => String(line).includes('重启续跑检查完成'))).toBe(true))
    return instance
  }

  it('需求 1020：经 ACP 路由服务一次主对话请求后会话记进 busy，正常结束的 turn/end 把它删掉；标题请求不记', async () => {
    const instance = harness()
    apply(instance.ctx, Config({}))
    void instance.registered().stream(request('session-main')).next()
    void instance.registered().stream({ ...request('session-title-only'), purpose: 'session-title' } as AdapterRequestOptions).next()
    await busyBecomes(['session-main'])
    instance.sessionEvent({ id: 'session-main' }, turnEnd({ kind: 'completed' }))
    await busyBecomes([])
    for (const worker of external.workers) worker.finish?.()
  })

  it('需求 1020：prepareCall 返回的流同样记 busy；压缩请求不记', async () => {
    const instance = harness()
    apply(instance.ctx, Config({}))
    const call = await instance.registered().prepareCall('antigravity-acp', 'example')
    void call.stream({ ...request('session-compact'), purpose: 'compaction' } as AdapterRequestOptions)[Symbol.asyncIterator]().next()
    void call.stream(request('session-prepared'))[Symbol.asyncIterator]().next()
    await busyBecomes(['session-prepared'])
    for (const worker of external.workers) worker.finish?.()
  })

  it('需求 1020：用户 Stop 和出错结束的轮把会话从 busy 里删掉，dsh 关停（aborted / disposed）和冷加载补的 interrupted 结束的轮保留', async () => {
    const instance = harness()
    apply(instance.ctx, Config({}))
    for (const id of ['session-stop', 'session-error', 'session-disposed', 'session-interrupted']) void instance.registered().stream(request(id)).next()
    await vi.waitFor(async () => expect(new Set(await readBusy() as string[]).size).toBe(4))
    instance.sessionEvent({ id: 'session-stop' }, turnEnd({ kind: 'aborted', reason: { kind: 'user' } }))
    instance.sessionEvent({ id: 'session-error' }, turnEnd({ kind: 'error', error: { message: 'x' } }))
    instance.sessionEvent({ id: 'session-disposed' }, turnEnd({ kind: 'aborted', reason: { kind: 'disposed' } }))
    instance.sessionEvent({ id: 'session-interrupted' }, turnEnd({ kind: 'interrupted' }))
    await vi.waitFor(async () => expect(new Set(await readBusy() as string[])).toEqual(new Set(['session-disposed', 'session-interrupted'])))
    for (const worker of external.workers) worker.finish?.()
  })

  it('需求 1020：启动时 busy 文件里有会话：延时前文件不动，延时后给它发续跑通知开一轮，并把它从文件里删掉', async () => {
    await writeFile(busyPath(), JSON.stringify(['session-cut']))
    const fake = dsh({ 'session-cut': { events: [header('antigravity-acp'), { type: 'turn/start', data: { turn: 1 } }] } })
    await boot(fake, async () => {
      // 延时内再重启一次也不丢：文件里还记着。
      expect(await readBusy()).toEqual(['session-cut'])
      expect(fake.agentFor('session-cut').followup).not.toHaveBeenCalled()
    })
    const followup = fake.agentFor('session-cut').followup
    expect(followup).toHaveBeenCalledTimes(1)
    const message = followup.mock.calls[0]![0] as { role: string; content: Array<{ text: string }>; source: { kind: string } }
    expect(message.role).toBe('user')
    expect(message.source.kind).toBe('plugin:dsh-antigravity-acp')
    const text = message.content[0]!.text
    expect(text).toMatch(/dsh 重启打断了上一轮/)
    expect(text).toMatch(/还没结束的工具调用已被停止，结果未知/)
    expect(text).toMatch(/从被打断的地方接着做/)
    expect(await readBusy()).toEqual([])
  })

  it('需求 1020：代理路由（antigravity-proxy）的会话同样续跑', async () => {
    await writeFile(busyPath(), JSON.stringify(['session-proxy']))
    const fake = dsh({ 'session-proxy': { events: [header('antigravity-proxy')] } })
    await boot(fake)
    expect(fake.agentFor('session-proxy').followup).toHaveBeenCalledTimes(1)
  })

  it('需求 1020：busy 文件不存在或为空时不开轮', async () => {
    const fake = dsh({})
    await boot(fake)
    delete (globalThis as Record<symbol, unknown>)[RESTART]
    await writeFile(busyPath(), '[]')
    await boot(fake)
    expect(fake.services.sessionController.resolveAgent).not.toHaveBeenCalled()
  })

  it('需求 1020：延时内这一轮已经正常结束（用户手动继续并做完）的会话不再发通知', async () => {
    await writeFile(busyPath(), JSON.stringify(['session-done']))
    const fake = dsh({ 'session-done': { events: [header('antigravity-acp')] } })
    await boot(fake, async (instance) => {
      instance.sessionEvent({ id: 'session-done' }, turnEnd({ kind: 'completed' }))
      await busyBecomes([])
    })
    expect(fake.agentFor('session-done').followup).not.toHaveBeenCalled()
  })

  it('需求 1020：最后一条 request/header 的 provider 不是本插件的两条路由时不发，并从 busy 里删掉', async () => {
    await writeFile(busyPath(), JSON.stringify(['session-switched']))
    const fake = dsh({ 'session-switched': { events: [header('antigravity-acp'), header('claude-code')] } })
    await boot(fake)
    expect(fake.agentFor('session-switched').followup).not.toHaveBeenCalled()
    expect(await readBusy()).toEqual([])
  })

  it('需求 1020：inbox 里已有本插件还没被取走的续跑通知时不叠；已被一轮取走的照常发', async () => {
    await writeFile(busyPath(), JSON.stringify(['session-pending', 'session-consumed']))
    const spliced = { type: 'agent/inbox/spliced', data: { target: 'next-turn', start: 0, inserted: [{ role: 'user', content: [{ type: 'text', text: '旧通知' }], source: { kind: 'plugin:dsh-antigravity-acp', form: 'notice' } }] } }
    const fake = dsh({
      'session-pending': { events: [header('antigravity-acp'), { type: 'turn/start', data: { turn: 1 } }, spliced] },
      'session-consumed': { events: [header('antigravity-acp'), spliced, { type: 'turn/start', data: { turn: 2 } }] },
    })
    await boot(fake)
    expect(fake.agentFor('session-pending').followup).not.toHaveBeenCalled()
    expect(fake.agentFor('session-consumed').followup).toHaveBeenCalledTimes(1)
  })

  it('需求 1020：没加载的子代理会话经父会话投递', async () => {
    await writeFile(busyPath(), JSON.stringify(['session-child']))
    const fake = dsh({
      'session-child': { events: [header('antigravity-acp')], origin: 'subagent', parentSession: 'session-parent' },
      'session-parent': { events: [header('claude-code')] },
    })
    await boot(fake)
    expect(fake.deliver).toHaveBeenCalledTimes(1)
    const [parent, childId, content, source, , delivery] = fake.deliver.mock.calls[0] as unknown as [unknown, string, Array<{ text: string }>, { kind: string }, AbortSignal, string]
    expect(parent).toBe(fake.agentFor('session-parent'))
    expect(childId).toBe('session-child')
    expect(content[0]!.text).toMatch(/dsh 重启打断了上一轮/)
    expect(source.kind).toBe('plugin:dsh-antigravity-acp')
    expect(delivery).toBe('queue')
    expect(fake.agentFor('session-parent').followup).not.toHaveBeenCalled()
  })

  it('需求 1020：会话有进行中的目标时通知用 user source', async () => {
    await writeFile(busyPath(), JSON.stringify(['session-goal']))
    const fake = dsh({ 'session-goal': { events: [header('antigravity-acp')] } })
    fake.goals.set(fake.agentFor('session-goal'), 'active')
    await boot(fake)
    expect((fake.agentFor('session-goal').followup.mock.calls[0]![0] as { source: unknown }).source).toEqual({ kind: 'user' })
  })

  it('需求 1020：同一进程里再次装载插件（热重载）不读 busy 文件、不发通知', async () => {
    const fake = dsh({ 'session-s': { events: [header('antigravity-acp')] } })
    await boot(fake)
    await writeFile(busyPath(), JSON.stringify(['session-s']))
    const again = harness()
    apply(again.ctx, Config({}))
    again.offer(fake.services)
    await new Promise(resolve => setTimeout(resolve, 50))
    expect(fake.services.sessionController.inspect).not.toHaveBeenCalled()
    expect(await readBusy()).toEqual(['session-s'])
  })

  it('需求 1020：会话已经有一轮在跑时不发、留在 busy 里；找不到的会话跳过，不影响其余会话', async () => {
    await writeFile(busyPath(), JSON.stringify(['session-live', 'session-gone', 'session-s']))
    const fake = dsh({ 'session-live': { events: [header('antigravity-acp')] }, 'session-s': { events: [header('antigravity-acp')] } })
    const live = fake.agentFor('session-live')
    live.status = 'running'
    fake.loaded.set('session-live', live)
    await boot(fake)
    expect(live.followup).not.toHaveBeenCalled()
    expect(fake.agentFor('session-s').followup).toHaveBeenCalledTimes(1)
    expect(await readBusy()).toEqual(['session-live'])
  })
})
