# 修复与验证记录

日期：2026-10-09。源码起点：`116e5bedb3b383c2bff962685032b01eb3de6be3`，分支 `fix/pg-lease-concurrent-owner-state`；验证完成时改动尚未提交或发布。既有 Spec Kit 迁移工作区保持保留。

## 根因与修复

原并发空 owner 是首次 CTE 快照看不到刚提交的冲突行；`116e5be` 已补新语句读取。此次重分析发现补读刷新数据库时间后，manager 再扣完整请求耗时，重复扣掉补读前冲突等待。

补读现在只返回四个租约字段，保留首次数据库时间参考。请求参考2000ms、冲突等待200ms、期限3000ms时，等待为 `(3000-2000)-200=800ms`。manager 原扣时算法及获取 CTE 安全条件不变。字段时间并非原子采样，既有全 RTT 包含采样前排队的局限仍在，不承诺所有延迟环境绝不会提前重试。

公开签名、schema、依赖不变；没有把 false 改成 true，没有放宽 owner、自然过期、fence 或时钟偏差条件。无证据表明已造成重复 ID。

## 环境

- Go：`go1.26.8 darwin/arm64`；module Go 1.26.0；pgx/v5 v5.7.6；完整依赖锁定见 `go.mod`、`go.sum`，本轮未改。
- Docker：29.4.0；镜像 postgres:16，ID `sha256:c319f2a8182bcdcbb3297d568e2a9cc9e7da3a624438e14b63921c719817e1b6`。
- PostgreSQL：16.15（Debian 16.15-1.pgdg13+2），aarch64，默认 read committed。
- 临时容器：`idgen-lease-retry-20261009`，`--rm`、tmpfs数据目录、动态绑定 `127.0.0.1:32911`，不挂载共享数据。
- 测试 URL：`postgres://idgen_test:idgen_test@127.0.0.1:32911/idgen_test?sslmode=disable`，仅本轮自建实例的测试账号；下表 PG 命令均设置 `IDGEN_TEST_DATABASE_URL` 为此 URL。单池测试追加 `&pool_max_conns=1`。

## Red / Green

先添加正式回归，再运行：

```bash
go test ./lease -run 'Test(PGLeaseStoreTryAcquireFallbackState|LeaseManagerFallbackDoesNotDeductConflictWaitTwice|LeaseManagerDeductsAcquireRoundTripFromRetryWait)$' -count=1
```

旧实现退出1：映射 success 的数据库时间为2200而非2000；等待实际 `[600ms]` 而非 `[800ms]`。普通 RTT 断言无失败。最小修复后相同命令及加入 `PGLeaseStoreTryAcquirePropagatesFallbackReadError` 的聚焦命令通过；完整状态、无行、错误与原60ms等待均通过。

## 完整验证

| 命令 | 结果 |
| --- | --- |
| `go test -count=1 ./...` | 通过；包含示例 package 编译与现有测试 |
| `go test -race -count=1 ./...` | 通过 |
| `go vet ./...` | 通过 |
| `go test -tags=integration -count=1 -timeout=90s ./lease` | 通过，6.983s |
| `go test -race -tags=integration -count=1 -timeout=90s ./lease` | 通过，8.260s |
| `go test -tags=integration -run '^TestPGLeaseStoreConcurrentAcquireReturnsWinnerStateIntegration$' -count=100 -timeout=120s ./lease` | 100/100通过，2.030s |
| `go test -tags=integration -run '^TestPGLeaseStoreConcurrentAcquireTransactionIntegration$' -count=10 -timeout=120s ./lease` | 10轮、每轮4种场景通过，0.846s |
| `go test -tags=integration -run '^TestPGLeaseStoreConcurrentAcquire(ReturnsWinnerState|Transaction)Integration$' -count=5 -timeout=90s ./lease`（单连接池 URL） | 全通过，0.597s |
| `git diff --check` | 通过 |

事务矩阵验证：RC竞争失败返回胜者完整租约；RR/Serializable返回40001且false，赢家持久化owner/期限/fence未变；胜者rollback后自动提交竞争方获取并持久化自己的租约。锁等待由独立observer连接确认，不使用固定sleep猜测交错。

每轮 helper 新建唯一 schema 并清理。最终 `SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'idgen_integration_%'` 返回0。容器已stop并由--rm删除，按名字查询无残留。100轮强制交错通过不是部署故障率估计；原v0.2.1对照复现保留在此前重分析报告，本轮没有重复该已完成实验。

## 流程与审查

- specify/plan/tasks：唯一需求目录，需求质量检查6/6，无未决事项；未配置 extensions/hooks。
- analyze：4 FR、3 SC、6 tasks，覆盖100%；无冲突、歧义、重复、未映射任务或原则偏离。FR001→T002/T003，FR002→T002/T003，FR003→T004/T005，FR004→T003/T004/T006；SC001→T002/T003，SC002/003→T006。
- 独立审查：`final_lease_review` 只读核对源码、正式测试、指南与规格；findings=[]；另行运行聚焦单测通过。完整结论见 `docs/reviews/2026-10-09-lease-retry-timing-review.md`。
- converge：核对4 FR、3 SC、两个使用场景、方案决策和4项原则，未发现 missing/partial/contradicts/unrequested，未追加任务。

## 调用方影响

普通自动提交调用无需改API或schema，补读失败状态的时间参考更一致，减少额外提前重试。事务调用方须按已明确契约负责提交与整事务重试，manager不代管。依赖v0.2.1不会自动得到本地改动；本轮未发布版本、未更新调用方或module cache。历史其他测试审计条目仍待分诊。

## 版本登记

用户随后授权创建 `v0.2.2` tag。版本包含已提交的并发空 owner 修复及本轮时间口径修复、正式回归和事务说明；Spec Kit 工作流迁移另行保留。创建本地 annotated tag 后，远程推送与调用方升级另行执行。
