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
  <img alt="Packages" src="https://img.shields.io/badge/packages-generator%20%7C%20lease-0A7EA4">
</p>

<h1 align="center">cc-snowflake-id-go</h1>

<p align="center">面向 Go 项目的可复用雪花 ID 类库，覆盖单进程生成与 PostgreSQL 租约多实例发号。</p>

`cc-snowflake-id-go` 的目标很明确：把业务仓库里的发号逻辑抽成独立 module，统一复用、统一测试、统一升级，而不是在每个服务里各自维护一份实现。

## 适用场景

- 单实例服务，或外部已经保证 `node_id` 唯一：使用 `generator`
- 多副本服务，需要数据库保证同一个 `node_id` 只能由一个实例安全发号：使用 `lease`

## 包入口

- `github.com/ZhcChen/cc-snowflake-id-go/generator`
  进程内雪花 ID 生成、解码与时钟回退保护
- `github.com/ZhcChen/cc-snowflake-id-go/lease`
  PostgreSQL 租约、后台续约 runtime、telemetry 与状态快照

根模块只保留说明文档，不导出兼容 facade。调用方直接依赖子包。

## 快速入口

- 安装：`go get github.com/ZhcChen/cc-snowflake-id-go@v0.1.1`
- 单进程 demo：[examples/generator-basic/main.go](examples/generator-basic/main.go)
- 多实例租约 demo：[examples/lease-runtime/main.go](examples/lease-runtime/main.go)
- 租约表示例：[examples/lease-runtime/schema.sql](examples/lease-runtime/schema.sql)

## 文档导航

- 文档首页：[docs/README.md](docs/README.md)
- 快速接入：[docs/guides/getting-started.md](docs/guides/getting-started.md)
- `generator` demo 说明：[docs/guides/generator-basic-demo.md](docs/guides/generator-basic-demo.md)
- `lease` demo 说明：[docs/guides/lease-runtime-demo.md](docs/guides/lease-runtime-demo.md)
- 测试与 CI：[docs/testing/ci-and-tests.md](docs/testing/ci-and-tests.md)
- 注释规范：[docs/standards/code-commenting.md](docs/standards/code-commenting.md)

## 仓库结构

- `generator/`：纯进程内生成器实现与测试
- `lease/`：租约实现、runtime、telemetry 与测试
- `examples/`：面向调用方的可运行 demo
- `docs/`：使用说明、测试说明与规范文档
