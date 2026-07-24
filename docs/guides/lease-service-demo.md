# lease 宿主型服务 demo 说明

这个文档说明 [examples/lease-service/main.go](../../examples/lease-service/main.go) 这一份更接近真实业务宿主的接入示例。

## 这个 demo 覆盖的内容

- 应用启动时创建 `PGLeaseStore`、`Telemetry` 和 `LeasedGenerator`
- 启动时先 `Acquire`，再 `StartRuntime`
- 监听 `Runtime.Done()` / `Runtime.Err()`，在 runtime 终态后摘掉 `ready` 并重建组件
- 区分 `/healthz` 与 `/readyz`
- 用 `RunReporter` 周期输出 `idgen_status`，并输出离散 `idgen_event`
- 暴露 `/next` 与 `/snapshot`，方便本地验证完整链路

## 运行前提

- 有可访问的 PostgreSQL 实例
- 已按 [examples/lease-runtime/schema.sql](../../examples/lease-runtime/schema.sql) 创建租约表
- 已设置环境变量 `IDGEN_DATABASE_URL`

可选环境变量：

- `IDGEN_HTTP_ADDR`：HTTP 监听地址，默认 `:8080`
- `IDGEN_SERVICE_NAME`：构造 `owner_id` 的服务名，默认 `cc-snowflake-id-go-lease-service-demo`
- `IDGEN_NODE_ID`：发号节点编号，默认 `100`

## 运行方式

1. 先完成租约表建表。
2. 设置 `IDGEN_DATABASE_URL`。
3. 在仓库根目录执行 `go run ./examples/lease-service`。
4. 访问 `http://127.0.0.1:8080/healthz`、`/readyz`、`/next`、`/snapshot` 观察行为。

## 关键接口

- `/healthz`：只表达进程是否存活，不代表当前仍可安全发号
- `/readyz`：直接绑定 `generator.Ready`，当租约丢失、围栏耗尽、时钟异常或生成器关闭时返回不就绪
- `/next`：生成一个雪花 ID，并以字符串形式返回，避免前端或 JSON 消费方出现 64 位精度丢失
- `/snapshot`：返回当前快照，便于本地调试 readiness、生命周期和失败分类

## 这个 demo 适合拿来复用的部分

- HTTP 服务启动顺序
- runtime watcher 与日志打点的接线方式
- 旧组件摘除、重建新组件和轻量退避的宿主编排方式
- `RunReporter` 与结构化日志输出方式
- `readyz` 响应体中 `checks.id_generator` 的返回口径

## 这份 demo 的推荐定位

- 这份 demo 演示的是当前推荐的宿主接法：runtime 进入明确终态错误后，先摘掉 `ready`，停止旧组件，再完整重建一套新的 `owner_id`、`LeasedGenerator` 和 `Runtime`。
- 它适合演示完整链路、观测点，以及“组件级重建而不是整进程退出”的接入方式。
- 组件级重建时通常继续沿用同一个 `node_id`；真正需要判死的是当前 ID 组件实例，不是 `node_id` 本身。
- 如果宿主暂时做不到组件级重建，再退回到“进程退出 + 外部拉起”的保守 fallback。

## 相关文档

- 生产接入约束见 [lease-production-integration.md](lease-production-integration.md)
- 如果只想先确认最小数据库链路，可以先看 [lease-runtime-demo.md](lease-runtime-demo.md)
