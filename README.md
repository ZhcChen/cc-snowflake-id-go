# cc-snowflake-id-go

[![test](https://github.com/ZhcChen/cc-snowflake-id-go/actions/workflows/test.yml/badge.svg)](https://github.com/ZhcChen/cc-snowflake-id-go/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ZhcChen/cc-snowflake-id-go.svg)](https://pkg.go.dev/github.com/ZhcChen/cc-snowflake-id-go)

可复用的 Go 雪花 ID 库：单进程生成，以及基于 PostgreSQL 租约的多实例安全发号。

```sh
go get github.com/ZhcChen/cc-snowflake-id-go@latest
```

| 包 | 用途 |
| --- | --- |
| `generator` | 进程内雪花 ID 生成、解码与时钟回退保护（单实例，或外部已保证节点号唯一） |
| `lease` | PostgreSQL 租约与后台续约运行时：多副本下保证同一节点号同一时刻只有一个实例发号 |

示例见 [`examples/`](examples)（租约表参考结构：[`examples/lease-runtime/schema.sql`](examples/lease-runtime/schema.sql)），API 说明见 [pkg.go.dev](https://pkg.go.dev/github.com/ZhcChen/cc-snowflake-id-go)。

许可：[MIT](LICENSE)
