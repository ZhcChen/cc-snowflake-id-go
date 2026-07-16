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

## 接入业务项目时通常要替换的部分

- 把 demo 里的 `demoServiceName` 替换成真实服务名
- 把固定的 `demoNodeID` 替换成业务配置项
- 把连接池生命周期接入业务项目现有的数据库初始化流程
- 把 `StartRuntime` / `Stop` 接入应用启动和退出流程
- 把 `Telemetry` / `Snapshot` 接入日志、监控或健康检查逻辑

## 相关说明

- 如果只是单实例服务，不需要这个 demo，直接使用 `generator`
- 本 demo 是最小接入链路，不等于生产环境全部配置
- 本地与 CI 的测试入口见 [测试与 CI](../testing/ci-and-tests.md)
