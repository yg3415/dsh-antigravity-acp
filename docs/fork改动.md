# 本 fork 相对上游的改动

读者：在本仓库改代码、改 `package.json` 的 `version` 或合并上游新版本的任务，动手前读一遍。本文写本 fork 的版本号怎么写，并列出相对上游 [KouzakiUmi/dsh-antigravity-acp](https://github.com/KouzakiUmi/dsh-antigravity-acp) 的改动。合并上游、改源码构建和在任务实例上验证的步骤见 my-superagent 的 [指令-修改antigravity-fork.md](/Users/lilithgames/my-superagent/docs/rules/指令-修改antigravity-fork.md)。

## 版本号

版本号写成 `<上游版本>-local.<序号>`，当前是 `0.1.46-local.1`（基于上游 0.1.46）。部署脚本 `ops/deploy-antigravity-fork.sh`（在 my-superagent 里）打出的 tarball 放在 `~/.dsh/plugins/`。

改 `package.json` 的 `version` 时，在同一个分支里改上面这句的版本号，并在下表加一行（或在已有的那一行补上版本号）。

## 相对上游的改动

| 改动 | 文件 | 原因 |
|---|---|---|
| worker 不设上限，不同对话不排队（0.1.46-local.1，PRD 需求 1019） | `src/pool/pooled-adapter.ts` 的 `select`、`acquire`；`src/settings.ts` 删掉 `maxConcurrentSessions`、`queueTimeoutMs`；`src/index.ts`；`src/runtime/lifecycle.ts` 的 `queue` 去掉 `capacity`；`src/client.js` 和 `src/client-i18n/en-ui.js` 的设置卡片、连接诊断 | 上游最多 4 个 worker（默认 2），队列最多 64 项、排队 120 秒超时，同时跑的会话多了会排队、报错。用户要求「去掉代码中的并发限制」（父任务 desk-9120ee84，本任务 desk-a310a30f）。改成没有空闲 worker 就新开一个；只有同一个对话的重叠请求还会等（ACP 会话状态在 worker 里，不能并行），可取消、不设超时。上游在容量没满时宁可新开也不接管别的对话正在复用的 worker；没有上限后这会让槽位跟着对话数一直涨，所以新对话依次挑：没服务过对话的空闲 worker → 官方进程已回收（`read().processRunning` 为 false）的空闲 worker → 新开。每个官方进程约 170MB，空闲 `idleRecycleMs`（默认 5 分钟）后回收。合并上游时核对：上游动了 `select` 的挑选顺序或 `capacity` 的，按这里的规则重写 |
| 解压目录里的每个文件都补执行位（0.1.46-local.1，PRD 需求 1018） | `src/runtime/binary.ts` 的 `ensureExtractedExecutable`，用在 `ensureAgentBinary` 的缓存、并发提交、下载三条路径 | 官方 server 1.4.0 的 macOS 包除 `agy_acp_server.par` 外还带 `localharness_external`；上游只给主程序 chmod，helper 留在 0644，运行时报 Permission denied（父任务在测试实例 3093 上实测）。命中缓存时也补，已有的坏缓存下次启动自己修好。只管插件自己解压的目录，`binaryPath` / `AGY_ACP_BIN` 指定的不动 |
| 官方进程环境固定带 `AGY_ACP_FORCE_FILE_STORAGE=1`（0.1.46-local.1，PRD 需求 1017） | `src/protocol/agent-link.ts` 的 `officialAgentEnv` | 官方 server 1.4.0 在 macOS 上不设这个变量就不读插件写的 `~/.gemini/antigravity-acp/acp_token.json`，session/new 时打出「Credentials missing or invalid. Launching browser login flow...」，150 秒超时；设了以后 3–4 秒成功（父任务在 3093 上实测）。官方进程的环境按白名单拼，宿主的环境变量传不进去，只能在这里加。所有平台都设：插件在所有平台都靠这个文件；Linux、Windows 上设了以后的行为没有实测。升级官方 server 版本后核对这个变量名仍然有效，失效时表现是 session/new 又要浏览器登录 |
| AGENTS.md 开头指向 my-superagent 的规则 | `AGENTS.md` 第一段 | 本仓库是工作台仓库，遵守 my-superagent 的全套规则。上游的项目说明留在下面不动，整份替换的话上游每次改 AGENTS.md 合并都会冲突 |
