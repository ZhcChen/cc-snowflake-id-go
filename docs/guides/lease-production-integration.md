# lease 生产接入指南

这个指南面向已经决定在多实例 Go 服务里接入 `lease` 包的调用方。重点不是“怎么生成一个 ID”，而是“怎么把租约发号器安全地接进服务生命周期”。

## 先看结论

- `Acquire` 成功只是起点，不代表后续运行期一直健康
- `Runtime.Done()` / `Runtime.Err()` 必须被宿主监听；refresh loop 异常退出后，宿主至少要立刻摘掉 `ready`，并停止当前 ID 组件实例
- `/healthz` 只表达进程存活，`/readyz` 必须绑定 `generator.Ready`
- 发布门禁不能只看启动成功，至少还要检查 `readyz` 和最近窗口内的运行态诊断

## 推荐接入链路

1. 准备租约表最终结构，直接使用带 `generation_fence_ms` 的 schema，参考 [examples/lease-runtime/schema.sql](../../examples/lease-runtime/schema.sql)。
2. 在应用启动阶段创建 `PGLeaseStore`、`owner_id`、`Telemetry` 和 `LeasedGenerator`。
3. 在对外提供写入能力前先执行 `Acquire`。
4. `Acquire` 成功后立即 `StartRuntime`，让后台 refresh loop 接管租约续约。
5. 额外启动 `RunReporter`，把 `Snapshot` 和 telemetry 事件接进 stdout JSON、日志平台或监控系统。
6. 用 `generator.Ready` 绑定 `/readyz`，而 `/healthz` 仍只表示进程活着。
7. 监听 `Runtime.Done()` / `Runtime.Err()`；只要 refresh loop 异常退出，先立刻把写入路径摘出 ready，并停止旧的 `Runtime` / `LeasedGenerator`。
8. 以组件级重建为默认恢复动作：重新生成 `owner_id`，重建 `LeasedGenerator`，重新 `Acquire -> StartRuntime`，成功后再恢复 ready。
9. 组件级重建时通常继续沿用同一个 `node_id`；只有 `node_id` 规划冲突、配置错误或业务迁移时才需要换号。
10. 如果连续多次组件级重建仍失败，再由宿主根据运维策略选择持续 `not ready`、告警，或作为保守 fallback 退出进程。

最小数据库链路可参考 [examples/lease-runtime/main.go](../../examples/lease-runtime/main.go)。完整宿主生命周期可参考 [examples/lease-service/main.go](../../examples/lease-service/main.go)。

## 默认推荐口径：轻量接入原则

本项目当前推荐的不是“重型进程内自愈”，而是“轻量接入 + 组件级重建 + 必要时外部拉起”。

- 保留当前类库已有的轻量自动处理能力：
  - 启动期 `Acquire` 重试
  - 小幅时钟回拨等待
  - 后台 `RunRefreshLoop` 续租
  - 发号前的预防性 refresh
- 不在业务请求路径里堆叠多轮重试、复杂退避或长时间阻塞等待。
- `Runtime.Err()` 命中终态错误时，默认先判死当前 ID 组件实例，而不是继续复用原来的 `Runtime` / `LeasedGenerator`。
- 组件级重建只重建 snowflake 相关对象和宿主挂接状态，不要求把整个业务进程打掉。
- 组件重建期间持续保持 `not ready`，并加轻量退避；只有连续失败时才交给更外层恢复。
- 默认不要把类库改造成复杂的多阶段自愈状态机；只有在真实运行数据表明瞬时抖动会频繁触发重建失败时，再考虑追加一层很轻的后台重试。

这样做的原因很直接：

- 能保证雪花 ID 的正确性优先，不带病继续发号。
- 不会明显增加数据库压力；数据库协调仍然主要来自租约获取、续租和释放，而不是 `readyz`、`Snapshot` 或 `RunReporter`。
- 不会把请求路径做重，避免把少量异常放大成整体尾延迟问题。
- 当存在持续性环境问题时，能够尽快暴露，而不是让服务长时间停留在“半活着”的状态。

## 轻量方案的边界

- 对于单实例服务，或者外部已经稳定保证 `node_id` 唯一的场景，优先使用 `generator`，不要为了统一口径强行上 `lease`。
- 对于 `lease` 场景，默认推荐“发生明确终态错误时做组件级重建”，不把“整个业务进程直接退出”当成唯一方案。
- 如果宿主侧暂时没有能力实现组件级重建，进程退出只能算保守 fallback，不是规范上的硬性唯一要求。
- 如果后续真实观测到的主要问题是数据库短抖动、网络瞬断而不是租约丢失或时钟异常，再评估是否需要补一层非常轻的后台重试。
- 在没有真实 telemetry 证明之前，不建议为了理论上的瞬时异常，把类库和宿主状态机做得过重。

## 组件终态与 `node_id` / `owner_id` 语义

- “ID 组件判死”指的是当前这一套 `LeasedGenerator + LeaseManager + Runtime` 实例不能继续发号，不代表 `node_id` 报废。
- `node_id` 是稳定的逻辑节点编号。组件级重建时通常继续沿用原来的 `node_id`，这样业务上的节点规划不会被运行期异常打乱。
- `owner_id` 是当前持租约实例的身份标识。宿主如果把组件级重建视为一次新的接管，通常应在重建时重新生成 `owner_id`。
- 只有在 `node_id` 分配冲突、跨服务误共用、编号治理调整这类配置问题下，才需要真正更换 `node_id`。

## 宿主生命周期硬约束

- 任何会写业务主键的服务，都不应在 `Acquire` 之前开始对外提供写入路径。
- `Runtime.Err()` 是运行时终态原因；如果它不是 `context.Canceled` 或 `context.DeadlineExceeded`，宿主不应继续留在 ready 状态。
- 一旦当前组件实例进入终态，宿主必须丢弃这套实例，不要试图在原来的 failed `Runtime` / `LeasedGenerator` 上继续补救。
- 组件级重建成功前，宿主必须持续保持 `not ready`，避免旧实例失效后继续接收写流量。
- `Next`、`Acquire`、`Ready`、`Runtime.Err()` 返回的错误都可以配合 `lease.ClassifyError` 做稳定分类，方便业务系统记录可检索日志。
- `runtime.Stop()` 返回值可能会合并 refresh loop 错误和 close 错误，宿主日志不要只保留其中一层。

## 健康检查与诊断建议

### `/healthz` 与 `/readyz`

- `/healthz`：只回答“进程活着没有”
- `/readyz`：回答“当前还能不能安全发号”

建议 `/readyz` 返回体至少包含：

- `status=ready/not_ready`
- `checks.id_generator=ok/error`
- 在失败时返回稳定的错误标识，例如 `error=id_generator_unavailable`

### 运行态状态与事件

`RunReporter` 输出的周期状态里，建议重点观察这些字段：

- `ready`
- `lifecycle`
- `lease_remaining_ms`
- `refresh_success_total`
- `refresh_failure_total`
- `last_error_class`
- `readiness_error_class`

telemetry 事件和业务方追加的使用异常日志，建议保持低基数字段：

- `diagnostic_scope`
- `event`
- `action`
- `role`
- `error_class`

`owner_id`、请求号、业务主键等高基数字段可以保留在日志正文里，但不要轻易升级成聚合标签。

### JSON 合约

如果雪花 ID 会穿过 HTTP/JSON 返回给前端或 JavaScript/TypeScript 消费方，建议按字符串输出，不要直接暴露为 JSON number。宿主型 demo 的 `/next` 已按这个口径返回。

## 配置参数关系

`LeasedGeneratorConfig` 本身已经对关键关系做了校验，接入时要重点理解这些约束：

- `LeaseRefreshInterval` 必须大于 `0`
- `LeaseWindow` 必须大于 `LeaseRefreshInterval`
- `FenceWindow` 默认等于 `LeaseWindow`，并且也必须大于 `LeaseRefreshInterval`
- `LeaseWindow` 必须大于 `LeaseRefreshInterval + LeaseOperationTimeout`
- `FenceWindow` 必须大于 `LeaseRefreshInterval + LeaseOperationTimeout`

调参时优先遵守下面的原则：

- 先根据数据库往返时延和网络抖动，为 `LeaseWindow`、`FenceWindow` 预留足够余量
- `LeaseRefreshInterval` 通常应明显小于租约窗口，给 refresh 失败后的恢复留出空间
- 如果环境偶发慢请求，不要只孤立增大 `LeaseOperationTimeout`，而要一起复核 `LeaseWindow` 和 `FenceWindow` 是否仍有足够余量
- `LeaseOperationTimeout` 更适合按数据库操作的 p99 往返时间加安全边界来定，而不是无限逼近租约窗口

## 首次接入检查清单

- 租约表使用的是最终结构，并包含 `generation_fence_ms`
- 所有写入角色的 `node_id` 稳定且唯一
- 多模块仓库里所有写路径都依赖同一个已发布 tag
- 业务主键如果会出现在 JSON 响应里，输出口径已经改为字符串或等价的精度安全格式
- 部署成功判定里包含 `readyz` 和运行态诊断，而不是只看 `healthz`

## 运行态验收清单

- 服务启动后能稳定 `Acquire`，并在启动窗口内进入 ready
- `/readyz` 持续返回 `checks.id_generator=ok`
- 最近两个观察窗口内能看到 `idgen_status ready=true`
- 最近观察窗口内没有新的 `idgen_event` 或 `idgen_usage_event` 失败事件
- 真实写入路径能端到端产生雪花 ID，而不是只有 demo 接口可用

## 常见误用

- 只在启动时检查一次 `Acquire`，后面完全不管 runtime 是否失效
- 把 `/readyz` 当成普通 `/healthz`，导致租约丢失后实例仍继续收流量
- 复制旧项目的过渡版租约表，没有带上 `generation_fence_ms`
- API、Worker、任务进程各自引用不同 tag，导致错误行为或返回口径不一致
- 业务日志只记录原始错误文本，不记录稳定的 `error_class`
- 把“当前组件实例终态”误解成“`node_id` 必须报废”
- 在 failed 组件上继续尝试 `Next`、`Refresh` 或补救，而不是完整丢弃后重建
- 在没有真实证据前，直接把宿主和类库改造成重型进程内自愈系统
