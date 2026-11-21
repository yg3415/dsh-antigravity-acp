/**
 * 需求 1020：选了 Antigravity 模型的会话在 dsh 重启后接着做。
 *
 * 入口：busy 记账（生成请求包装、session/event 监听）与启动钩子 resumeOnBoot。
 * dsh 服务（sessionController、agents、goals、subagents）用假的，形状照 dsh 0.2.1。
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import {
  BOOT_KEY,
  RESTART_TEXT,
  busyFile,
  busyTurnListener,
  createBusyLedger,
  noteConversationRequest,
  resumeOnBoot,
  trackRequests,
} from '../src/runtime/restart-resume.js'
import type { ResumeServices } from '../src/runtime/restart-resume.js'

let dir: string
let file: string

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'agy-resume-'))
  file = busyFile(dir)
  delete (globalThis as Record<symbol, unknown>)[BOOT_KEY]
})
afterEach(async () => {
  vi.useRealTimers()
  delete (globalThis as Record<symbol, unknown>)[BOOT_KEY]
  await rm(dir, { recursive: true, force: true })
})

const readBusy = async (): Promise<unknown> => JSON.parse(await readFile(file, 'utf8'))
const settle = async () => { for (let i = 0; i < 20; i++) await new Promise(resolve => setImmediate(resolve)) }

function header(provider: string) {
  return { type: 'request/header', data: { header: { config: { provider, model: 'gemini-4-argon' } } } }
}

interface FakeAgent { followup: ReturnType<typeof vi.fn>; status: 'idle' | 'running' }

function fakeServices(sessions: Record<string, { events: unknown[]; origin?: 'subagent'; parentSession?: string; version?: number }>) {
  const agents = new Map<string, FakeAgent>()
  const agentFor = (id: string) => {
    let agent = agents.get(id)
    if (!agent) { agent = { followup: vi.fn(), status: 'idle' }; agents.set(id, agent) }
    return agent
  }
  const deliver = vi.fn(async () => 'message-id')
  const goals = new Map<FakeAgent, string>()
  const services: ResumeServices = {
    sessionController: {
      inspect: vi.fn(async (id: string) => {
        const row = sessions[id]
        if (!row) throw new Error(`session ${id} not found`)
        return { meta: { id, version: row.version ?? 4, origin: row.origin, parentSession: row.parentSession }, events: row.events } as never
      }),
      resolveAgent: vi.fn(async (id: string) => {
        if (sessions[id]?.origin === 'subagent') return { error: new Error('owned by subagent routing') }
        return { agent: agentFor(id) }
      }),
    },
    agents: { get: vi.fn(() => undefined) },
    goals: { get: vi.fn((agent: never) => (goals.has(agent) ? { phase: goals.get(agent) } : undefined)) },
    subagents: { [Symbol.for('dsh.subagent.deliverPrompt')]: deliver },
  }
  return { services, agentFor, deliver, goals }
}

async function boot(services: ResumeServices) {
  vi.useFakeTimers({ toFake: ['setTimeout'] })
  resumeOnBoot(createBusyLedger(file), () => services, {}, 10_000)
  await settle()
  await vi.advanceTimersByTimeAsync(10_000)
  vi.useRealTimers()
  await settle()
}

describe('busy 记账', () => {
  it('需求 1020：服务一次主对话请求后 busy 文件里有这个会话，收到它正常结束的 turn/end 后没有', async () => {
    const ledger = createBusyLedger(file)
    const adapter = trackRequests({ async *stream() { yield 1 } }, options => noteConversationRequest(ledger, options))
    for await (const _ of adapter.stream({ provider: 'antigravity-acp', model: 'm', messages: [], sessionId: 'session-a' } as never)) { /* drain */ }
    await settle()
    expect(await readBusy()).toEqual(['session-a'])
    busyTurnListener(ledger)({ id: 'session-a' }, { type: 'turn/end', data: { turn: 1, reason: { kind: 'completed' } } })
    await settle()
    expect(await readBusy()).toEqual([])
  })

  it('需求 1020：prepareCall 返回的流同样记 busy', async () => {
    const ledger = createBusyLedger(file)
    const stream = async function* () { yield 1 }
    const adapter = trackRequests({ stream, prepareCall: async () => ({ model: {}, stream }) }, options => noteConversationRequest(ledger, options))
    const call = await adapter.prepareCall('antigravity-proxy', 'm')
    for await (const _ of call.stream({ provider: 'antigravity-proxy', model: 'm', messages: [], sessionId: 'session-p' } as never)) { /* drain */ }
    await settle()
    expect(await readBusy()).toEqual(['session-p'])
  })

  it('需求 1020：用户 Stop 和出错结束的轮把会话从 busy 里删掉', async () => {
    const ledger = createBusyLedger(file)
    await ledger.mark('session-stop')
    await ledger.mark('session-error')
    const listen = busyTurnListener(ledger)
    listen({ id: 'session-stop' }, { type: 'turn/end', data: { reason: { kind: 'aborted', reason: { kind: 'user' } } } })
    listen({ id: 'session-error' }, { type: 'turn/end', data: { reason: { kind: 'error', error: { message: 'x' } } } })
    await settle()
    expect(await readBusy()).toEqual([])
  })

  it('需求 1020：dsh 关停（aborted / disposed）和冷加载补的 interrupted 结束的轮仍留在 busy 文件里', async () => {
    const ledger = createBusyLedger(file)
    await ledger.mark('session-disposed')
    await ledger.mark('session-interrupted')
    const listen = busyTurnListener(ledger)
    listen({ id: 'session-disposed' }, { type: 'turn/end', data: { reason: { kind: 'aborted', reason: { kind: 'disposed' } } } })
    listen({ id: 'session-interrupted' }, { type: 'turn/end', data: { reason: { kind: 'interrupted' } } })
    await settle()
    expect(await readBusy()).toEqual(['session-disposed', 'session-interrupted'])
  })

  it('需求 1020：标题生成、压缩等带 purpose 的请求和没有会话 id 的请求不记 busy', async () => {
    const ledger = createBusyLedger(file)
    noteConversationRequest(ledger, { sessionId: 'session-t' as never, purpose: 'session-title' as never })
    noteConversationRequest(ledger, { sessionId: 'session-c' as never, purpose: 'compaction' as never })
    noteConversationRequest(ledger, { sessionId: undefined })
    await settle()
    await expect(readFile(file, 'utf8')).rejects.toThrow()
  })
})

describe('重启后发续跑通知', () => {
  it('需求 1020：启动时 busy 文件里有 S：延时后给 S 发一条续跑通知开一轮，并清空文件', async () => {
    await writeFile(file, JSON.stringify(['session-s']))
    const { services, agentFor } = fakeServices({ 'session-s': { events: [header('antigravity-acp'), { type: 'turn/start', data: { turn: 1 } }] } })
    await boot(services)
    const followup = agentFor('session-s').followup
    expect(followup).toHaveBeenCalledTimes(1)
    const message = followup.mock.calls[0]![0] as { role: string; content: Array<{ text: string }>; source: { kind: string } }
    expect(message.role).toBe('user')
    expect(message.content[0]!.text).toBe(RESTART_TEXT)
    expect(message.source.kind).toBe('plugin:dsh-antigravity-acp')
    expect(RESTART_TEXT).toMatch(/dsh 重启打断了上一轮/)
    expect(RESTART_TEXT).toMatch(/还没结束的工具调用已被停止，结果未知/)
    expect(RESTART_TEXT).toMatch(/从被打断的地方接着做/)
    expect(await readBusy()).toEqual([])
  })

  it('需求 1020：代理路由（antigravity-proxy）的会话同样续跑', async () => {
    await writeFile(file, JSON.stringify(['session-proxy']))
    const { services, agentFor } = fakeServices({ 'session-proxy': { events: [header('antigravity-proxy')] } })
    await boot(services)
    expect(agentFor('session-proxy').followup).toHaveBeenCalledTimes(1)
  })

  it('需求 1020：busy 文件为空或不存在时不发', async () => {
    const { services } = fakeServices({})
    await boot(services)
    expect(services.sessionController!.resolveAgent).not.toHaveBeenCalled()
    delete (globalThis as Record<symbol, unknown>)[BOOT_KEY]
    await writeFile(file, '[]')
    await boot(services)
    expect(services.sessionController!.resolveAgent).not.toHaveBeenCalled()
  })

  it('需求 1020：S 最后一条 request/header 的 provider 不是本插件的两条路由时不发', async () => {
    await writeFile(file, JSON.stringify(['session-switched']))
    const { services, agentFor } = fakeServices({ 'session-switched': { events: [header('antigravity-acp'), header('claude-code')] } })
    await boot(services)
    expect(agentFor('session-switched').followup).not.toHaveBeenCalled()
  })

  it('需求 1020：inbox 里已有本插件还没被取走的续跑通知时不叠', async () => {
    await writeFile(file, JSON.stringify(['session-pending']))
    const pending = { type: 'agent/inbox/spliced', data: { target: 'next-turn', start: 0, inserted: [{ role: 'user', content: [{ type: 'text', text: RESTART_TEXT }], source: { kind: 'plugin:dsh-antigravity-acp', form: 'notice' } }] } }
    const { services, agentFor } = fakeServices({ 'session-pending': { events: [header('antigravity-acp'), { type: 'turn/start', data: { turn: 1 } }, pending] } })
    await boot(services)
    expect(agentFor('session-pending').followup).not.toHaveBeenCalled()
  })

  it('需求 1020：通知已被一轮取走（之后有 turn/start）时照常发', async () => {
    await writeFile(file, JSON.stringify(['session-consumed']))
    const spliced = { type: 'agent/inbox/spliced', data: { target: 'next-turn', start: 0, inserted: [{ role: 'user', content: [{ type: 'text', text: RESTART_TEXT }], source: { kind: 'plugin:dsh-antigravity-acp' } }] } }
    const { services, agentFor } = fakeServices({ 'session-consumed': { events: [header('antigravity-acp'), spliced, { type: 'turn/start', data: { turn: 2 } }] } })
    await boot(services)
    expect(agentFor('session-consumed').followup).toHaveBeenCalledTimes(1)
  })

  it('需求 1020：S 是没加载的子代理会话时经父会话投递', async () => {
    await writeFile(file, JSON.stringify(['session-child']))
    const { services, agentFor, deliver } = fakeServices({
      'session-child': { events: [header('antigravity-acp')], origin: 'subagent', parentSession: 'session-parent' },
      'session-parent': { events: [header('claude-code')] },
    })
    await boot(services)
    expect(deliver).toHaveBeenCalledTimes(1)
    const [parent, childId, content, source, , delivery] = deliver.mock.calls[0] as unknown as [unknown, string, Array<{ text: string }>, { kind: string }, AbortSignal, string]
    expect(parent).toBe(agentFor('session-parent'))
    expect(childId).toBe('session-child')
    expect(content[0]!.text).toBe(RESTART_TEXT)
    expect(source.kind).toBe('plugin:dsh-antigravity-acp')
    expect(delivery).toBe('queue')
    expect(agentFor('session-parent').followup).not.toHaveBeenCalled()
  })

  it('需求 1020：S 有进行中的目标时通知用 user source', async () => {
    await writeFile(file, JSON.stringify(['session-goal']))
    const { services, agentFor, goals } = fakeServices({ 'session-goal': { events: [header('antigravity-acp')] } })
    goals.set(agentFor('session-goal'), 'active')
    await boot(services)
    const message = agentFor('session-goal').followup.mock.calls[0]![0] as { source: { kind: string } }
    expect(message.source).toEqual({ kind: 'user' })
  })

  it('需求 1020：同一进程里再次装载插件（热重载）不再读 busy 文件、不发通知', async () => {
    const { services, agentFor } = fakeServices({ 'session-s': { events: [header('antigravity-acp')] } })
    await boot(services)
    await writeFile(file, JSON.stringify(['session-s']))
    await boot(services)
    expect(agentFor('session-s').followup).not.toHaveBeenCalled()
    expect(await readBusy()).toEqual(['session-s'])
  })

  it('需求 1020：会话已经有一轮在跑（用户在延时内手动继续了）时不发', async () => {
    await writeFile(file, JSON.stringify(['session-live']))
    const { services } = fakeServices({ 'session-live': { events: [header('antigravity-acp')] } })
    const live = { followup: vi.fn(), status: 'running' as const }
    services.agents = { get: vi.fn(() => live) }
    await boot(services)
    expect(live.followup).not.toHaveBeenCalled()
  })

  it('需求 1020：找不到会话（重启期间被删）时跳过，不影响其余会话', async () => {
    await writeFile(file, JSON.stringify(['session-gone', 'session-s']))
    const { services, agentFor } = fakeServices({ 'session-s': { events: [header('antigravity-acp')] } })
    await boot(services)
    expect(agentFor('session-s').followup).toHaveBeenCalledTimes(1)
  })
})
