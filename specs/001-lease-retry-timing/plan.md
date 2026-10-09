# 实施方案：租约补读重试时间与事务契约

来源规格：`specs/001-lease-retry-timing/spec.md`

## Technical Context

- Go module 1.26.0、工具链 1.26.8、pgx v5.7.6。
- 入口：`lease/lease.go`、`lease/lease_test.go`、`lease/lease_integration_test.go`、`lease/doc.go`、`docs/guides/lease-runtime-demo.md`。
- PostgreSQL 16 隔离临时容器，`IDGEN_TEST_DATABASE_URL`。
- 单元、race、vet、完整 integration、integration race、关键用例重复。
- 未知项：无。

## Constitution Check

- 原则 I–IV 均适用，遵守当前 AGENTS 和 runbook。
- 公共签名、schema、依赖不变，获取 SQL owner/过期/fence/skew 条件不变。
- 设计前后复核无偏离，仅隔离数据库验证，不改调用方或 module cache。

## 现状与约束

`116e5be` 已修复首条快照不可见冲突行的空 owner，但补读更新时间参考后 manager 又扣完整 RTT。原普通 RTT 测试必须保留，历史诊断原件不重写。

## 设计

补读只查询并映射四个租约字段，保留首次 CTE 时间参考；manager 不变。无行保留重试状态，错误传播。时间参考与补读字段不保证原子采样，注释明确。

矩阵增加显式 RC、RR、Serializable 和胜者 rollback，复用 pool 现有用例；高隔离错误后检查胜者持久化状态。锁观察改独立直连 observer，避免 pool_max_conns=1 耗尽；每轮独立 schema 并清理。

## 实施单元

1. 先写等待/映射回归，观察 red（600ms vs 800ms）。
2. 最小实现修复和事务契约说明。
3. 正式集成矩阵和 observer 硬化。
4. 全验证、独立审查、converge 与记录。

## 验证方案

| 层级 | 命令或场景 | 通过条件 |
| --- | --- | --- |
| 单元 | `go test -count=1 ./...` | 新800ms等待、原60ms RTT、映射/无行/错误全部通过 |
| race/vet | `go test -race -count=1 ./...`、`go vet ./...` | 全通过 |
| PG | `go test -tags=integration -count=1 -timeout=90s ./lease` | 全通过，另加-race复跑 |
| 重复 | winner状态100轮、事务矩阵10轮 | 无失败，独立schema |
| 池边界 | 关键并发用例 URL 参数 pool_max_conns=1 | observer 无耗尽 |

记录真实版本、命令、red/green 和清理结果。

## 辅助产物

`research.md`、`contracts/lease-store.md`、`quickstart.md`。无新实体或schema，不生成重复数据模型。

## 文档、兼容与回退

更新 API 注释和 runtime 指南；无迁移，撤销本轮diff可回退。发布和调用方升级另行处理。

## 风险与未决决策

- 既有全 RTT 扣减包含首次采样前排队，仍可能轻微偏早；SQL安全条件继续校验，本轮不重新设计时间协议。
- 未决：无。
