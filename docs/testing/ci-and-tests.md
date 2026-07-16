# 测试与 CI

这个仓库的测试入口在 `scripts/test/`，CI 配置在 [.github/workflows/test.yml](../../.github/workflows/test.yml)。

## 本地测试入口

- `scripts/test/unit.ps1`
  默认单元测试入口，对应 `go test ./...`
- `scripts/test/race.ps1`
  并发数据竞争检测入口，对应 `go test -race ./...`
- `scripts/test/integration.ps1`
  PostgreSQL 集成测试入口，对应 `go test -tags=integration ./lease`

## 推荐执行顺序

1. 先执行 `powershell -ExecutionPolicy Bypass -File .\scripts\test\unit.ps1`
2. 再执行 `powershell -ExecutionPolicy Bypass -File .\scripts\test\race.ps1`
3. 准备 PostgreSQL 测试库后，再执行 `powershell -ExecutionPolicy Bypass -File .\scripts\test\integration.ps1`

## race 说明

- `-race` 依赖 `CGO_ENABLED=1`
- 当前机器还需要存在可用的 C 编译器
- 如果本机不具备这两个条件，应改在 GitHub Actions 或 Linux Go 容器环境执行

## integration 说明

- `integration` 测试只覆盖 `lease` 包
- 运行前必须设置 `IDGEN_TEST_DATABASE_URL`
- 测试库需要可创建和清理临时 schema

## CI 当前做什么

当前 CI 会在 `push main` 和 `pull_request` 时自动执行三段作业：

- `unit`
- `race`
- `integration`

执行顺序是先 `unit`，再 `race`，最后 `integration`。
