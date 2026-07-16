# generator Demo 说明

这个文档说明 [examples/generator-basic/main.go](../../examples/generator-basic/main.go) 演示了什么，以及它适合用来验证哪些接入点。

## 这个 demo 覆盖的内容

- 创建一个 `node_id=1` 的进程内雪花 ID 生成器
- 生成一个新的雪花 ID
- 使用 `Decode` 把 ID 解析回时间戳、节点号和序列号
- 以可读方式输出解析结果

## 适用目的

- 验证本地 Go 环境可以正常编译和运行这个类库
- 验证 `generator` 包的最小接入链路
- 帮调用方快速理解雪花 ID 的组成字段

## 运行方式

在仓库根目录执行 `go run ./examples/generator-basic`。

## 输出字段说明

- `snowflake_id`：本次生成的雪花 ID 原始值
- `node_id`：从 ID 中解码出来的节点编号
- `sequence`：同一毫秒内的序列号
- `timestamp_utc`：从 ID 中解码出来的 UTC 时间

## 接入业务项目时通常要替换的部分

- 把 demo 里的固定 `node_id` 替换成业务配置项
- 在应用启动阶段初始化生成器，而不是每次请求临时创建
- 只在排障、日志或离线分析场景使用 `Decode`
