---
title: 新增生产级雪花 ID 接入参考 demo
date: 2026-09-22
artifact_contract: ce-unified-plan/v1
product_contract_source: ce-brainstorm
execution: code
status: draft
---

# Goal Capsule

保留现有 `generator-basic` 与 `lease-runtime` 最小 demo，新增一个不绑定具体业务项目的生产级接入参考。参考 `qfy-voucher-hub` 已验证的宿主方案，补齐长期运行服务所需的配置校验、runtime 监督、fail-closed、组件重建、readiness、结构化诊断、信号退出、幂等 schema 和服务级测试。

# Scope

## In scope

- 新增 `examples/production-runtime` 可运行 HTTP demo。
- 仅支持显式固定 `NodeID`；节点动态分配不属于本次范围。
- 使用标准库 `net/http` 与 `log/slog`，不引入业务框架或观测平台依赖。
- 提供稳定代理，使服务在底层 generator 重建后无需重新构造调用方。
- 运行期 runtime 失败时立即停止发号并进入后台重建；重建期间 readiness 失败。
- 提供有界优雅退出、结构化状态/事件日志和迁移示例。
- 增加宿主单元测试、HTTP 探针测试、生命周期测试和文档。

## Out of scope

- 不修改现有两个 demo 的最小定位。
- 不实现 node allocator。
- 不复制 qfy 的业务 repository wiring、API/Worker 节点角色、Loki、Gin 或部署脚本。
- 不把完整宿主状态机直接并入 `lease` 包；本期先以可复制 demo 形式验证方案。

# Requirements

- R1. 通过环境变量加载 DSN、service name、node ID 和租约时间参数，非本地缺少必填项时启动失败。
- R2. 启动路径为 `PG Ping -> Acquire -> StartRuntime -> HTTP serve`，Acquire 具备有限指数退避。
- R3. 对外仅暴露稳定 `Next`/`Ready`/`Snapshot` 代理；底层组件可以原子替换。
- R4. runtime 终止后，代理 fail-closed，旧组件停止，使用新 owner 重建并在成功后恢复 ready。
- R5. `/healthz` 仅表示进程存活；`/readyz` 表示当前可安全发号，重建期间返回 503。
- R6. ID 接口以十进制字符串返回，避免 JavaScript `number` 精度丢失。
- R7. SIGINT/SIGTERM 使用 bounded shutdown，runtime、reporter、HTTP server 和 generator 都能收敛退出。
- R8. 日志包含配置、状态、runtime failure、rebuild 生命周期和错误分类，owner ID 脱敏。
- R9. schema 可重复执行，包含租约与 generation fence 所需约束。
- R10. 测试覆盖正常启动、同节点竞争、runtime 故障、重建恢复、readiness、JSON 字符串 ID 和关闭顺序。

# Implementation Units

## U1. 生产 demo 宿主与配置

新增 `examples/production-runtime` 的配置、宿主代理、组件构建/重建、HTTP 服务和信号生命周期。

## U2. 生产 schema、文档与运行入口

新增幂等 schema、完整 demo 指南，并更新 README/docs 导航，明确固定节点和运行前提。

## U3. 宿主级测试与验证

覆盖代理、重建、readiness、HTTP JSON、竞争失败和优雅关闭；执行 unit、race、vet，并在可用 PostgreSQL 时执行 integration。

# Verification Contract

- `go test ./...`
- `go test -race ./...`
- `go vet ./...`
- 生产 demo 可编译并通过静态 HTTP handler 测试。
- 没有数据库时，demo 在 DSN 缺失或配置非法时快速返回诊断错误。
- 文档与实际环境变量、端口、schema 和生命周期一致。

# Definition of Done

- 两个原有 demo 未被删除且行为不变。
- `examples/production-runtime` 能作为其他 Go 项目的接入参考直接复制。
- R1-R10 均有源码或测试证据；未实现的动态 allocator 明确标为后续能力。
