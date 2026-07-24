<p align="center">
  <a href="https://go.dev/">
    <img alt="Go Version" src="https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white">
  </a>
  <a href="./.github/workflows/test.yml">
    <img alt="Test Workflow" src="https://img.shields.io/github/actions/workflow/status/ZhcChen/cc-snowflake-id-go/test.yml?branch=main&label=test&logo=githubactions&logoColor=white">
  </a>
  <a href="https://github.com/ZhcChen/cc-snowflake-id-go/releases">
    <img alt="Latest Tag" src="https://img.shields.io/github/v/tag/ZhcChen/cc-snowflake-id-go?sort=semver&logo=git&logoColor=white">
  </a>
  <img alt="PostgreSQL Lease" src="https://img.shields.io/badge/PostgreSQL-lease-4169E1?logo=postgresql&logoColor=white">
  <img alt="Examples" src="https://img.shields.io/badge/examples-minimal%20%7C%20service-0A7EA4">
</p>

<h1 align="center">cc-snowflake-id-go</h1>

<p align="center">面向 Go 项目的可复用雪花 ID 类库，覆盖单进程发号与基于 PostgreSQL 租约的多实例安全发号。</p>

`cc-snowflake-id-go` 的目标很明确：把业务仓库里的雪花 ID 逻辑抽成独立 module，统一复用、统一测试、统一升级，而不是在每个服务里各自维护一份实现。

## 适用场景

- 单实例服务，或外部已经保证 `node_id` 唯一：使用 `generator`
- 多实例服务，需要数据库保证同一个 `node_id` 只能由一个实例安全发号：使用 `lease`

## 包入口

- `github.com/ZhcChen/cc-snowflake-id-go/generator`
  进程内雪花 ID 生成、解码与时钟回退保护
- `github.com/ZhcChen/cc-snowflake-id-go/lease`
  PostgreSQL 租约、后台 runtime、telemetry 与状态快照

根模块只保留说明文档，不导出兼容 facade。调用方直接依赖子包。

## 快速入口

- 安装：`go get github.com/ZhcChen/cc-snowflake-id-go@v0.1.1`
- 单进程 demo：[examples/generator-basic/main.go](examples/generator-basic/main.go)
- 最小租约 demo：[examples/lease-runtime/main.go](examples/lease-runtime/main.go)
- 宿主型服务 demo：[examples/lease-service/main.go](examples/lease-service/main.go)
- 租约表结构示例：[examples/lease-runtime/schema.sql](examples/lease-runtime/schema.sql)

## 文档导航

- 文档首页：[docs/README.md](docs/README.md)
- 快速接入：[docs/guides/getting-started.md](docs/guides/getting-started.md)
- `lease` 生产接入指南：[docs/guides/lease-production-integration.md](docs/guides/lease-production-integration.md)
- `generator` demo 说明：[docs/guides/generator-basic-demo.md](docs/guides/generator-basic-demo.md)
- `lease` 最小 demo 说明：[docs/guides/lease-runtime-demo.md](docs/guides/lease-runtime-demo.md)
- `lease` 宿主型服务 demo 说明：[docs/guides/lease-service-demo.md](docs/guides/lease-service-demo.md)
- 测试与 CI：[docs/testing/ci-and-tests.md](docs/testing/ci-and-tests.md)
- 注释规范：[docs/standards/code-commenting.md](docs/standards/code-commenting.md)

## 接入原则

- 多模块业务系统应统一依赖同一个已发布 tag，避免 API、Worker 或批处理任务之间的类库版本漂移
- `lease` 场景不要只看 `Acquire` 成功，必须把 `Ready`、`Runtime.Done()` / `Runtime.Err()` 和运行态诊断一起接入
- 首次建表时直接使用带 `generation_fence_ms` 的最终结构，不要照抄旧项目里的过渡版 schema

## 仓库结构

- `generator/`：进程内发号器实现与测试
- `lease/`：租约实现、runtime、telemetry 与测试
- `examples/`：面向调用方的可运行 demo
- `docs/`：接入说明、测试说明与规范文档
