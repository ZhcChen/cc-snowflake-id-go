# 快速接入

这个仓库提供两类能力：

- `generator`：单进程内雪花 ID 生成
- `lease`：基于 PostgreSQL 租约的多实例安全发号

## 选择哪个包

- 如果服务只有单实例，或者 `node_id` 的唯一性由部署系统保证，直接接入 `generator`
- 如果服务会多副本部署，并且需要数据库保证同一 `node_id` 不能被多个实例同时发号，接入 `lease`

## 安装

在调用方项目中执行 `go get github.com/ZhcChen/cc-snowflake-id-go@v0.1.1`。

## 建议的接入顺序

1. 先运行 [examples/generator-basic/main.go](../../examples/generator-basic/main.go)，确认当前 Go 环境和依赖拉取正常。
2. 如果目标场景是多实例部署，再继续运行 [examples/lease-runtime/main.go](../../examples/lease-runtime/main.go)。
3. 把 demo 中的配置项替换成业务项目自己的 `node_id`、数据库连接池和生命周期管理方式。
4. 接入完成后，按 [测试与 CI](../testing/ci-and-tests.md) 执行本地验证。

## 下一步文档

- 单进程 demo 说明见 [generator-basic-demo.md](generator-basic-demo.md)
- 多实例 demo 说明见 [lease-runtime-demo.md](lease-runtime-demo.md)
