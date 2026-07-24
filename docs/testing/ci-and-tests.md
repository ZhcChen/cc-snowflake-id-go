# 测试与 CI

这个仓库的测试入口在 `scripts/test/`，CI 配置在 [.github/workflows/test.yml](../../.github/workflows/test.yml)。

## 本地测试入口

- `scripts/test/unit.ps1`
  默认单元测试入口，对应 `go test ./...`
- `scripts/test/race.ps1`
  并发数据竞争检测入口，对应 `go test -race ./...`
- `scripts/test/integration.ps1`
  PostgreSQL 集成测试入口，对应 `go test -tags=integration ./lease ./examples/lease-service`

## 推荐执行顺序

1. 先执行 `powershell -ExecutionPolicy Bypass -File .\scripts\test\unit.ps1`
2. 再执行 `powershell -ExecutionPolicy Bypass -File .\scripts\test\race.ps1`
3. 准备 PostgreSQL 测试库后，再执行 `powershell -ExecutionPolicy Bypass -File .\scripts\test\integration.ps1`

## race 说明

- `-race` 依赖 `CGO_ENABLED=1`
- 当前机器还需要存在可用的 C 编译器
- 如果本机不具备这两个条件，应改在 GitHub Actions 或 Linux Go 容器环境执行

## integration 说明

- `integration` 测试默认覆盖 `lease` 包，以及 `examples/lease-service` 的真实数据库接入场景
- 运行前必须设置 `IDGEN_TEST_DATABASE_URL`
- 测试库需要可创建和清理临时 schema
- 如果只想跑 PostgreSQL 集成测试，不验证网络故障注入，保持 `IDGEN_TEST_TOXIPROXY_URL` 为空即可；`examples/lease-service` 的故障注入测试会自动跳过
- 如果要验证宿主型示例在数据库短断连后的摘 `ready` 与组件级重建，需要额外准备一个可访问的 Toxiproxy API，并设置：
  - `IDGEN_TEST_TOXIPROXY_URL`
  - `IDGEN_TEST_TOXIPROXY_LISTEN`
  - `IDGEN_TEST_TOXIPROXY_UPSTREAM`
- `IDGEN_TEST_TOXIPROXY_UPSTREAM` 留空时，会默认复用 `IDGEN_TEST_DATABASE_URL` 的 host:port；如果 Toxiproxy 与 PostgreSQL 不在同一个网络视角下，例如 GitHub Actions service 容器场景，需要显式覆盖成容器内可达地址

## CI 当前做什么

当前 CI 会在 `push main` 和 `pull_request` 时自动执行三段作业：

- `unit`
- `race`
- `integration`

执行顺序是先 `unit`，再 `race`，最后 `integration`。

当前 `integration` 作业在 2026 年 7 月 24 日的仓库状态下，已经同时包含：

- `lease` 包的 PostgreSQL 集成测试
- `examples/lease-service` 基于 PostgreSQL + Toxiproxy 的数据库断连恢复测试
