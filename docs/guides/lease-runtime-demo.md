# lease 最小 demo 说明

这个文档说明 [examples/lease-runtime/main.go](../../examples/lease-runtime/main.go) 的运行前提、演示流程和接入含义。

## 这个 demo 覆盖的内容

- 创建 PostgreSQL 连接池
- 初始化 `PGLeaseStore`
- 为当前进程生成 `owner_id`
- 创建 `LeasedGenerator`
- 获取节点租约
- 启动后台 refresh runtime
- 在租约保护下生成雪花 ID
- 读取 `Snapshot` 检查当前状态

## 运行前提

- 有可访问的 PostgreSQL 实例
- 已创建租约表，表结构见 [schema.sql](../../examples/lease-runtime/schema.sql)
- 已设置环境变量 `IDGEN_DATABASE_URL`

## 运行方式

1. 先按 [schema.sql](../../examples/lease-runtime/schema.sql) 建表。
2. 设置 `IDGEN_DATABASE_URL`。
3. 在仓库根目录执行 `go run ./examples/lease-runtime`。

## 这个 demo 主要用来验证什么

- PostgreSQL 连接参数是否正确
- 租约表结构是否满足 `lease` 包要求
- `Acquire -> StartRuntime -> Next -> Snapshot -> Stop` 这一条最小生命周期链路是否成立

## 接入业务项目时通常要替换的部分

- 把 demo 里的 `demoServiceName` 替换成真实服务名
- 把固定的 `demoNodeID` 替换成业务配置项
- 把连接池生命周期接入业务项目现有的数据库初始化流程
- 把 `StartRuntime` / `Stop` 接入应用启动和退出流程
- 把 `Telemetry` / `Snapshot` 接入日志、监控或健康检查逻辑

## 这个 demo 不覆盖什么

- 不负责演示 `/healthz` 与 `/readyz` 的分工
- 不负责演示 `Runtime.Done()` / `Runtime.Err()` 异常退出后的宿主 fail-fast
- 不负责演示 `RunReporter` 周期状态日志和事件日志

如果你需要把这些能力接到真实服务生命周期，请继续看 [`lease` 生产接入指南](lease-production-integration.md) 和 [宿主型服务 demo 说明](lease-service-demo.md)。
