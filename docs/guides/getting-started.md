# 快速接入

这个仓库提供两类能力：

- `generator`：单进程内雪花 ID 生成
- `lease`：基于 PostgreSQL 租约的多实例安全发号

## 选择哪个包

- 如果服务只有单实例，或者 `node_id` 的唯一性由部署系统保证，直接接入 `generator`
- 如果服务会多副本部署，并且需要数据库保证同一 `node_id` 不能被多个实例同时发号，接入 `lease`

## 安装

在调用方项目中执行 `go get github.com/ZhcChen/cc-snowflake-id-go@v0.2.0`。

## 建议的接入顺序

1. 先运行 [examples/generator-basic/main.go](../../examples/generator-basic/main.go)，确认当前 Go 环境和依赖拉取正常。
2. 如果目标场景是多实例部署，再继续运行 [examples/lease-runtime/main.go](../../examples/lease-runtime/main.go)。
3. 把 demo 中的配置项替换成业务项目自己的 `node_id`、数据库连接池和生命周期管理方式。
4. 接入完成后，按 [测试与 CI](../testing/ci-and-tests.md) 执行本地验证。

## 高并发发号参数

默认情况下，同一毫秒内 `sequence` 用满后会等待 wall clock 进入下一毫秒再继续发号。
如果峰值并发需要避免这个等待，可以设置 `Config.OverCostCount`（`lease` 场景对应
`LeasedGeneratorConfig.OverCostCount`）启用有界时间戳虚拟前移，思路与
yitter/IdGenerator 的漂移算法一致：

- 纯 `generator` 使用 over cost 时必须同时设置 `AllowInMemoryOverCost: true`，
  表示接受 over cost 状态仅保存在内存、跨重启安全由调用方自行负责；否则
  `NewGenerator` 会返回 `ErrInvalidGeneratorConfig`；
- `lease` 不需要额外声明，`LeasedGenerator` 内部已通过持久化 generation fence
  提供跨重启安全边界；
- `sequence` 溢出时时间戳虚拟前移 1ms，不等待 wall clock，最多累计前移
  `OverCostCount` ms；
- 超过上限后仍等待 wall clock 追平并推进，避免时间戳无限超前；
- 使用 `DecodeStrict` 校验 ID 时，`max_future_lead` 需要覆盖 `OverCostCount`
  对应的未来窗口，否则突发期生成的 ID 会被判定为超前；
- 多实例 `lease` 场景建议让 `OverCostCount` 小于 `FenceWindow`，避免高频触达
  generation fence 后频繁刷新租约。

## 下一步文档

- 单进程 demo 说明见 [generator-basic-demo.md](generator-basic-demo.md)
- 多实例 demo 说明见 [lease-runtime-demo.md](lease-runtime-demo.md)
