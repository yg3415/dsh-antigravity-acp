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
  await rm(external.cacheDir, { recursive: true, force: true })
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

describe('需求 1020：apply 接线（重启后接着做）', () => {
  const busyPath = () => join(external.cacheDir, 'busy.json')
  const settle = async () => { for (let i = 0; i < 30; i++) await new Promise(resolve => setImmediate(resolve)) }

  it('需求 1020：经 ACP 路由服务一次主对话请求后会话记进 busy，正常结束的 turn/end 把它删掉；标题请求不记', async () => {
    const instance = harness()
    apply(instance.ctx, Config({}))
    const turn = instance.registered().stream(request('session-main'))
    void turn.next()
    const title = instance.registered().stream({ ...request('session-title-only'), purpose: 'session-title' } as AdapterRequestOptions)
    void title.next()
    await vi.waitFor(async () => expect(JSON.parse(await readFile(busyPath(), 'utf8'))).toEqual(['session-main']))
    instance.sessionEvent({ id: 'session-main' }, { type: 'turn/end', data: { turn: 1, reason: { kind: 'completed' } } })
    await vi.waitFor(async () => expect(JSON.parse(await readFile(busyPath(), 'utf8'))).toEqual([]))
    for (const worker of external.workers) worker.options.lifecycle?.stopped()
  })

  it('需求 1020：启动时 busy 文件里有会话：dsh 服务就绪、延时后给它发续跑通知并清空文件', async () => {
    await writeFile(busyPath(), JSON.stringify(['session-cut']))
    vi.useFakeTimers({ toFake: ['setTimeout'] })
    const instance = harness()
    apply(instance.ctx, Config({}))
    const followup = vi.fn()
    instance.offer({
      sessionController: {
        inspect: vi.fn(async () => ({ meta: { version: 4 }, events: [{ type: 'request/header', data: { header: { config: { provider: 'antigravity-acp' } } } }] })),
        resolveAgent: vi.fn(async () => ({ agent: { followup, status: 'idle' } })),
      },
      agents: { get: () => undefined },
      goals: { get: () => undefined },
    })
    await settle()
    expect(followup).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(10_000)
    vi.useRealTimers()
    await vi.waitFor(() => expect(followup).toHaveBeenCalledTimes(1))
    expect((followup.mock.calls[0]![0] as { content: Array<{ text: string }> }).content[0]!.text).toMatch(/dsh 重启打断了上一轮/)
    expect(JSON.parse(await readFile(busyPath(), 'utf8'))).toEqual([])
  })
})
