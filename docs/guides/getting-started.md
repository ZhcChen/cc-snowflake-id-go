# 快速接入

这个仓库提供两类能力：

- `generator`：单进程内雪花 ID 生成
- `lease`：基于 PostgreSQL 租约的多实例安全发号

## 先选模式

- 如果服务只有单实例，或者 `node_id` 的唯一性已经由部署系统保证，直接接入 `generator`
- 如果服务会多副本部署，并且需要数据库保证同一 `node_id` 不能被多个实例同时发号，接入 `lease`

## 安装

在调用方项目中执行 `go get github.com/ZhcChen/cc-snowflake-id-go@v0.1.1`。

## 首次接入前检查

- 租约表必须直接使用带 `generation_fence_ms` 的最终结构，参考 [examples/lease-runtime/schema.sql](../../examples/lease-runtime/schema.sql)
- 如果业务系统分为 API、Worker、定时任务等多个 Go module，它们应统一依赖同一个已发布 tag
- 每个写入角色都需要稳定且唯一的 `node_id`

## 建议的接入顺序

1. 先运行 [examples/generator-basic/main.go](../../examples/generator-basic/main.go)，确认当前 Go 环境和依赖拉取正常。
2. 如果目标场景是多实例部署，再继续运行 [examples/lease-runtime/main.go](../../examples/lease-runtime/main.go)，验证数据库连接、租约表结构和最小生命周期链路。
3. 准备接入真实业务宿主前，先阅读 [`lease` 生产接入指南](lease-production-integration.md)，明确 `readyz`、runtime 失效和运行态诊断的约束。
4. 需要把 HTTP 服务生命周期、`/healthz`、`/readyz`、`RunReporter` 和 fail-fast 串起来时，直接参考 [examples/lease-service/main.go](../../examples/lease-service/main.go)。
5. 接入完成后，按 [测试与 CI](../testing/ci-and-tests.md) 执行本地验证。

## 下一步文档

- 单进程 demo 说明见 [generator-basic-demo.md](generator-basic-demo.md)
- 最小租约 demo 说明见 [lease-runtime-demo.md](lease-runtime-demo.md)
- 宿主型服务 demo 说明见 [lease-service-demo.md](lease-service-demo.md)
