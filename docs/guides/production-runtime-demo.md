# 生产级接入参考

`examples/production-runtime` 是长期运行服务的完整接入参考。它参考业务项目中已经验证的宿主管理方式，但只使用标准库和本模块 API，不绑定 Gin、Loki 或具体业务仓储。

## 覆盖范围

- 环境变量配置与租约参数校验
- PostgreSQL ping、租约获取和有限启动重试
- refresh runtime 监督
- runtime 失败后的 fail-closed 与组件重建
- `/healthz`、`/readyz` 和 `/id`
- `slog` 结构化状态与事件日志
- SIGINT/SIGTERM 有界优雅退出
- 以字符串返回雪花 ID，避免前端 `number` 精度丢失

## 运行

先创建租约表：

```bash
psql "$IDGEN_DATABASE_URL" -f examples/production-runtime/schema.sql
```

设置连接和节点配置：

```bash
export IDGEN_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable'
export IDGEN_NODE_ID=1
export IDGEN_SERVICE_NAME=my-service
go run ./examples/production-runtime
```

多个实例必须使用不同的 `IDGEN_NODE_ID`。租约冲突会阻止实例安全发号，不能降级到没有租约保护的 `generator`。

## 配置

常用配置及默认值：

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `IDGEN_DATABASE_URL` | 必填 | PostgreSQL DSN |
| `IDGEN_NODE_ID` | `1` | 固定逻辑节点，范围 `1-1024` |
| `IDGEN_SERVICE_NAME` | `cc-snowflake-id-production-demo` | owner 前缀 |
| `IDGEN_LISTEN_ADDRESS` | `:8080` | HTTP 监听地址 |
| `IDGEN_LEASE_WINDOW` | `10s` | owner lease 窗口 |
| `IDGEN_FENCE_WINDOW` | `10s` | generation fence 窗口 |
| `IDGEN_LEASE_REFRESH_INTERVAL` | `3s` | refresh 周期 |
| `IDGEN_LEASE_OPERATION_TIMEOUT` | `1s` | 单次租约操作超时 |

`lease window` 和 `fence window` 必须大于 `refresh interval + operation timeout`。

## HTTP 契约

- `GET /healthz`：只表示 HTTP 进程存活。
- `GET /readyz`：当前 generator 可安全发号时返回 `200`，重建或租约失效时返回 `503`。
- `GET /id`：成功返回 `{"id":"..."}`；发号不可用时返回 `503`。

runtime 失败后，旧组件先被摘除，`/readyz` 保持失败；新 owner 获取租约并启动 runtime 成功后才恢复 ready。

## 生产接入边界

这个 demo 仍使用显式固定节点。动态 node allocator 目前只是后续计划，不应假设已经提供。复制到业务项目时，需要把 HTTP server、日志 sink、配置来源和关闭信号接入项目自己的生命周期，并保留 `/readyz` 的流量摘除语义。
