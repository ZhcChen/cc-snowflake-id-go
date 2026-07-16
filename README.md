# cc-snowflake-id-go

`cc-snowflake-id-go` 是一个面向 Go 项目的雪花 ID 类库，用来把业务仓库里的发号逻辑抽离成独立依赖。

这个仓库要解决的问题很直接：

- 业务项目不再复制一份雪花 ID 代码再各自维护。
- 发号规则、测试和缺陷修复只在一个仓库里演进。
- 下游项目通过 Go module tag 升级，而不是手工同步源码。

## 项目定位

根模块只保留文档说明，不提供兼容 facade。调用方应按职责直接引用子包：

- `github.com/ZhcChen/cc-snowflake-id-go/generator`
- `github.com/ZhcChen/cc-snowflake-id-go/lease`

两个子包的职责边界如下：

- `generator`：纯进程内雪花 ID 生成器，处理时间戳、节点号、序列号和时钟回退保护。
- `lease`：基于 PostgreSQL 租约的多实例安全发号器，处理租约抢占、续约、运行时、telemetry 和状态上报。

如果你的服务只有单实例或节点号由外部保证唯一，优先使用 `generator`。
如果你的服务会多副本部署，并且需要靠数据库保证同一个 `node_id` 只能被一个实例安全使用，使用 `lease`。

## 安装

```bash
go get github.com/ZhcChen/cc-snowflake-id-go@v0.1.0
```

## 快速开始

### 1. 单进程雪花 ID 生成

最小用法如下：

```go
package main

import (
	"context"
	"fmt"

	idgen "github.com/ZhcChen/cc-snowflake-id-go/generator"
)

func main() {
	generator, err := idgen.NewGenerator(idgen.Config{
		NodeID: 1,
	}, nil)
	if err != nil {
		panic(err)
	}

	value, err := generator.Next(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Println(value)
}
```

完整 demo 见 [examples/generator-basic/main.go](examples/generator-basic/main.go)。

本地运行：

```powershell
go run ./examples/generator-basic
```

### 2. PostgreSQL 租约发号

`lease` 适合多实例部署场景。一个实例在发号前先抢占 `node_id` 对应的数据库租约，拿到租约后再启动后台 refresh loop，持续续约并生成 ID。

使用前先准备租约表。示例 DDL 见 [examples/lease-runtime/schema.sql](examples/lease-runtime/schema.sql)。

完整 demo 见 [examples/lease-runtime/main.go](examples/lease-runtime/main.go)。这个 demo 演示了以下步骤：

- 创建 `pgxpool.Pool`
- 初始化 `PGLeaseStore`
- 创建 `LeasedGenerator`
- `Acquire` 获取租约
- `StartRuntime` 启动后台 refresh loop
- `Next` 生成雪花 ID
- `Snapshot` 读取当前状态

本地运行前先设置数据库连接串：

```powershell
$env:IDGEN_DATABASE_URL = "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"
go run ./examples/lease-runtime
```

## Demo 目录

- `examples/generator-basic/`：最小单进程 demo，适合先确认 ID 生成与解码结果。
- `examples/lease-runtime/`：带 PostgreSQL 租约和 runtime 的完整 demo，适合多实例接入前做最小验证。

## 目录结构

- `doc.go`：模块根路径的包级说明。
- `generator/`：纯进程内生成器实现、基准和测试。
- `lease/`：租约生成器、运行时、telemetry、基准和测试。
- `examples/`：面向调用方的可运行 demo。
- `scripts/test/`：仓库测试脚本入口。
- `docs/standards/code-commenting.md`：代码注释规范。

测试代码仍跟随各自包放置，而不是集中放在单独的总测试目录。这是 Go 类库更常见、也更利于包内行为测试和公开 API 测试收敛的组织方式。

## 测试与 CI

默认单元测试：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test\unit.ps1
```

竞争检测：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test\race.ps1
```

说明：

- `-race` 依赖 `CGO_ENABLED=1` 和可用的 C 编译器。
- GitHub Actions 会在 `ubuntu-latest` 上自动执行这一步。

PostgreSQL 集成测试：

```powershell
$env:IDGEN_TEST_DATABASE_URL = "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"
powershell -ExecutionPolicy Bypass -File .\scripts\test\integration.ps1
```

仓库内置了 [.github/workflows/test.yml](.github/workflows/test.yml)，当前 CI 会自动执行：

- `unit`：默认单元测试
- `race`：并发数据竞争检测
- `integration`：带 `integration` build tag 的 PostgreSQL 集成测试
