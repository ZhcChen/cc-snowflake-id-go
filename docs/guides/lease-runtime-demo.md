# lease Demo 说明

这个文档说明 [examples/lease-runtime/main.go](../../examples/lease-runtime/main.go) 的运行前提、演示流程和接入含义。

## 这个 demo 覆盖的内容

- 创建 PostgreSQL 连接池
- 初始化 `PGLeaseStore`
- 为当前进程生成 `owner_id`
- 创建 `LeasedGenerator`
- 获取节点租约
- 启动后台 refresh runtime
- 在租约保护下生成雪花 ID
- 启用有界时间戳虚拟前移（`OverCostCount: 2000`）
- 读取 `Snapshot` 检查当前状态

## 运行前提

- 有可访问的 PostgreSQL 实例
- 已创建租约表，表结构见 [examples/lease-runtime/schema.sql](../../examples/lease-runtime/schema.sql)
- 已设置环境变量 `IDGEN_DATABASE_URL`

## 运行方式

1. 先按 [schema.sql](../../examples/lease-runtime/schema.sql) 建表。
2. 设置 `IDGEN_DATABASE_URL`。
3. 在仓库根目录执行 `go run ./examples/lease-runtime`。

## 这个 demo 主要用来验证什么

- PostgreSQL 连接参数是否正确
- 租约表结构是否满足 `lease` 包要求
- `Acquire -> StartRuntime -> Next -> Snapshot -> Stop` 这一条最小生命周期链路是否成立
- over cost 配置可以正常通过 `LeasedGenerator` 接入，并由持久化 fence 兜底

## demo 的 lease 参数

- `LeaseWindow: 10s`：单次 PG 租约时长
- `FenceWindow: 10s`：generation fence 未来窗口
- `LeaseRefreshInterval: 3s`：后台续约间隔
- `LeaseOperationTimeout: 1s`：单次 PG 操作超时
- `OverCostCount: 2000`：seq 溢出时最多虚拟前移 2000ms

`OverCostCount` 小于 `FenceWindow`，因此虚拟时间触达 fence 时可以由
`LeasedGenerator` 按需刷新后继续，不会把 fence 窗口耗尽。

## 接入业务项目时通常要替换的部分

- 把 demo 里的 `demoServiceName` 替换成真实服务名
- 把固定的 `demoNodeID` 替换成业务配置项
- 把连接池生命周期接入业务项目现有的数据库初始化流程
- 把 `StartRuntime` / `Stop` 接入应用启动和退出流程
- 把 `Telemetry` / `Snapshot` 接入日志、监控或健康检查逻辑

## 相关说明

### 数据库 executor 与事务边界

常规 `LeaseManager` / `LeasedGenerator` 接入沿用 demo 的自动提交连接池；独立 `pgx.Conn` 也应在自动提交模式使用。`LeaseDB` 接受 `pgx.Tx`，但不接管调用方事务生命周期。

- 默认自动提交和显式 READ COMMITTED 下，竞争失败后的补读使用新语句快照，可以读取刚提交的胜者。返回状态只代表本次可见视图，不保证原子或返回时仍最新；无可见行时允许空 `OwnerID`，同时 `acquired=false`，manager 继续按边界重试。
- REPEATABLE READ / SERIALIZABLE 并发冲突可能返回 PostgreSQL 序列化错误（SQLSTATE `40001`）。store 原样返回；manager 包装存储错误后停止获取，不自动重试外层事务。调用方负责回滚并重试整个事务。
- 事务中 `acquired=true` 仅表示事务内写入成功，提交前不能用于持续发号；回滚会撤销租约写入，竞争方可能随即获取节点。长期事务也会阻塞竞争获取，不适合常规运行时租约续期。

`DatabaseNowMillis` 是首次裁决语句采样的数据库时间参考。补读只更新租约字段，保留该参考，以配合 manager 的整次请求耗时扣减；这些字段不保证同一时刻采样。该策略减少重复扣时产生的额外请求，数据库 owner、过期、时钟偏差和 generation fence 条件仍决定是否能获取。

- 如果只是单实例服务，不需要这个 demo，直接使用 `generator`
- 本 demo 是最小接入链路，不等于生产环境全部配置
- 本地与 CI 的测试入口见 [测试与 CI](../testing/ci-and-tests.md)
