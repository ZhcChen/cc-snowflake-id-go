# cc-snowflake-id-go

`cc-snowflake-id-go` 是一个可复用的 Go 雪花 ID 类库，目标是把发号逻辑从业务仓库里独立出来，统一复用、统一升级。

当前仓库按职责拆成了两个子包：

- `generator/`：纯进程内雪花 ID 生成能力，只处理时间戳、节点号、序列号与时钟保护。
- `lease/`：基于 PostgreSQL 租约的多实例安全发号能力，包含租约协调、运行时、telemetry 和状态上报。

根目录现在只保留包级文档，真正的 API 只从子包导出：

- `github.com/ZhcChen/cc-snowflake-id-go/generator`
- `github.com/ZhcChen/cc-snowflake-id-go/lease`

## 快速使用

纯生成器：

```go
generator, err := generator.NewGenerator(generator.Config{
	NodeID: 1,
}, nil)
if err != nil {
	return err
}

value, err := generator.Next(context.Background())
if err != nil {
	return err
}
```

## 目录约定

- `generator/*.go`：纯生成器实现与测试
- `lease/*.go`：租约相关实现与测试
- `docs/standards/code-commenting.md`：注释规范

测试仍然跟随各自包放置，而不是单独放到一个总测试目录。这是 Go 类库的标准组织方式，也更利于包内行为测试和公开 API 测试按职责收敛。

## 测试

默认单元测试：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\test\unit.ps1
```

PostgreSQL 集成测试：

```powershell
$env:IDGEN_TEST_DATABASE_URL = "postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable"
powershell -ExecutionPolicy Bypass -File .\scripts\test\integration.ps1
```

仓库内置了 `.github/workflows/test.yml`，会分别执行默认单元测试和带 `integration` build tag 的 PostgreSQL 集成测试。
