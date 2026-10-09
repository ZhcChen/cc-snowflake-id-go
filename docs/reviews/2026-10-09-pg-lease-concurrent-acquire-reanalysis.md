---
title: PostgreSQL 租约并发获取修复重分析
status: reviewed
date: 2026-10-09
---

# PostgreSQL 租约并发获取修复重分析

## 结论

`116e5bedb3b383c2bff962685032b01eb3de6be3` 的修复正确处理了 READ COMMITTED 下首次并发插入后失败方返回空 owner 的缺陷。确定性对照中，v0.2.1 原实现失败 20/20 次，修复后通过 100/100 次。原始随机并发用例在旧实现上也通过 20/20 次，不能以随机测试通过反证竞态不存在。

补读不修改获取结果，也不修改租约裁决或数据，未发现冲突方误报成功或削弱 fencing 的证据。没有证据表明本次缺陷已导致重复 ID。

仍需处理两类边界：隔离级别/外层事务的公开使用契约尚未明确；补读时间与 manager 的耗时扣减口径不一致，会导致偏早重试。本次完成分析和诊断验证，没有修改生产 Go 代码、正式测试套件、调用方或 Go module cache。

## 根因与返回契约

`lease/lease.go:779-854` 的 CTE 在一个 SQL 语句内执行 UPSERT 和 existing 查询。竞争方语句开始时，胜者行尚未提交；唯一索引冲突处理会等待胜者事务，提交后可以识别冲突行，但 existing 查询仍使用原语句快照，因而看不到那一行。UPSERT 的更新条件拒绝接管时，最终返回 `acquired=false`、空 owner 和零租约字段。

PostgreSQL 官方文档明确说明 READ COMMITTED 逐语句使用新快照，ON CONFLICT 可处理常规查询快照不可见的冲突行；RR/Serializable 的冲突则可能要求重试整个事务。参见 [PostgreSQL 16 Transaction Isolation](https://www.postgresql.org/docs/16/transaction-iso.html)。

修复在 `lease/lease.go:879-898` 对失败且空 owner 的结果补充下一条 SELECT。READ COMMITTED 下，第二条语句能读到此前已提交的胜者。`acquired` 保持 false；原 SQL 的同 owner、数据库时间过期和 generation fence 条件完全未变，时钟偏差失败仍在补读之前返回。

`LeaseStore.TryAcquire` 应返回是否实际获取成功，以及本次尝试可见的租约视图或无可见行时的重试状态；它不是返回时刻的全局最新视图承诺。测试中胜者租约仍有效且没有再次变更，要求失败方看到胜者 owner 合理。若两条语句之间发生合法 takeover，补读可能看到下一任 owner；若外部删除了行，补读可能无行。库自身 Release 不删除行或提前取消有效期。

## 发现与边界

### P3：补读后的 retry wait 重复扣除前段耗时

`lease/lease.go:886` 在补读中更新 `DatabaseNowMillis`，manager 在 `lease/lease.go:223-226` 先据此计算剩余租约时长，再扣除整次 TryAcquire 的耗时。补读前的等待因此被重复扣除。

诊断用例模拟：请求开始时间 2000ms，冲突等待 200ms；补读返回数据库时间 2200ms、租约到期 3000ms，剩余租约 800ms。manager 实际 sleep 600ms，在租约到期前 200ms 再次请求。该现象连续 5 次复现。

影响是偏早重试和额外数据库请求。下一次 UPSERT 仍执行过期与 fence 裁决，不能据此推断提前取得租约。该等待计算本身早于本次修复存在，但补读路径改变了数据库时间的采样点，尚未统一两层时间口径。建议后续明确状态时间和请求耗时的对应关系，再补正式回归，不能直接取消其他路径必需的延迟扣减。

### 使用契约：外层事务与高隔离级别

`LeaseDB` 接口可接受 pgx.Tx。诊断实测得到：

| 竞争方 executor | 胜者提交后的结果 | 每项重复次数 |
| --- | --- | --- |
| pool / autocommit | false，owner-a，租约期限与 fence 匹配 | 5/5 |
| 显式 READ COMMITTED tx | false，owner-a，租约期限与 fence 匹配 | 5/5 |
| REPEATABLE READ tx | false，SQLSTATE 40001 | 5/5 |
| SERIALIZABLE tx | false，SQLSTATE 40001 | 5/5 |
| pool，胜者 rollback | true，owner-b；持久化行属于 owner-b | 5/5 |

高隔离级别在首条 UPSERT 直接产生序列化错误，并非补读返回空 owner。store 原样返回该错误；manager 将其作为 store 错误终止，而不自动重试整个事务。此行为早于本次修复存在，应明确由调用方处理整个事务的回滚/重试，或明确常规租约运行只支持 autocommit/READ COMMITTED。

外层事务中的 acquired=true 尚不意味着已经提交持久化；随后 rollback 会撤销租约。这也是既有使用边界，不能把新测试中未提交的赢家事务示例当作 generator 的常规使用方式。

### 正式测试尚未覆盖的边界

- 单元层补读完整字段映射（含 DatabaseNowMillis）、node 参数与 `acquired=false`，以及补读 ErrNoRows。正式集成回归已有 owner、期限和 fence 断言；本轮诊断补齐上述单元边界验证，尚未纳入正式测试。
- 本轮隔离级别矩阵和 winner rollback 的持久化结果。
- 上述 manager 等待时间口径。诊断是现状刻画，断言 600ms 并通过不意味着该等待行为理想。
- 确定性测试的 observer 复用 winner pool，要求至少有第二条可用连接；URL 设置 `pool_max_conns=1` 时 helper 会等不到连接。默认测试池下不影响本次复现。

补读 ErrNoRows 实测保留 NodeID、原 DatabaseNowMillis 和零租约字段，manager 会将其规范化为短重试；与第一条查询 ErrNoRows 返回请求 fence 的状态字段不同。建议在正式契约与测试中明确这种无可见行状态，不把它描述成已造成安全问题。

## 环境与验证

- 当前源码：`116e5bedb3b383c2bff962685032b01eb3de6be3`。
- 基线：tag v0.2.1，commit `3133ac64fa53`。`v0.2.1:lease/lease.go` 与 `116e5be^:lease/lease.go` blob 都为 `a0c604617d8c93d408e2c72b4efcc9b9c590b00b`。
- Go：`go1.26.8 darwin/arm64`；go.mod 声明 1.26.0；pgx/v5 为 v5.7.6。
- PostgreSQL：16.15，默认 READ COMMITTED；Docker postgres:16，镜像 ID `sha256:c319f2a8182bcdcbb3297d568e2a9cc9e7da3a624438e14b63921c719817e1b6`。
- 临时容器：`cc-snowflake-lease-reanalysis-1791531259399`，tmpfs 数据目录，仅绑定动态 loopback 端口 32888，没有挂载共享数据库数据。
- 每轮使用现有 helper 创建唯一 schema 并清理；所有测试结束后 schema 计数为 0，容器已停止并由 --rm 删除。

| 验证 | 命令核心参数 | 结果 |
| --- | --- | --- |
| 全部单元测试 | `go test -count=1 ./...` | 通过 |
| 全部 race | `go test -race -count=1 ./...` | 通过 |
| vet | `go vet ./...` | 通过 |
| 原实现，确定性回归 | baseline overlay，`-tags=integration -run '^TestPGLeaseStoreConcurrentAcquireReturnsWinnerStateIntegration$' -count=20 -json -timeout=90s ./lease` | 0 通过，20 失败；空 owner |
| 修复，确定性回归 | `-tags=integration -run '^TestPGLeaseStoreConcurrentAcquireReturnsWinnerStateIntegration$' -count=100 -timeout=90s ./lease` | 100 通过，0 失败 |
| 原实现，原始随机用例 | baseline overlay，`-tags=integration -run '^TestPGLeaseStoreConcurrentAcquireAndFenceTakeoverIntegration$' -count=20 -json -timeout=120s ./lease` | 20 通过，0 失败 |
| 修复，原始随机用例 | `-tags=integration -run '^TestPGLeaseStoreConcurrentAcquireAndFenceTakeoverIntegration$' -count=20 -timeout=120s ./lease` | 20 通过，0 失败 |
| 完整 PG integration | `go test -tags=integration -count=1 -timeout=90s ./lease` | 通过 |
| 诊断矩阵、映射、等待刻画 | probe overlay，`-tags=integration -run '^TestLeaseReanalysis' -count=5 -v -timeout=90s ./lease` | 全部预期成立 |
| 完整 PG integration + 诊断 race | probe overlay，`go test -race -tags=integration -count=1 -timeout=90s ./lease` | 通过 |

所有 PG 命令均显式设置 `IDGEN_TEST_DATABASE_URL` 为上述隔离容器。基线失败是预期复现，不是当前修复后的测试失败；强制时序下的 100% 不能视为实际部署的故障频率，随机样本 0/20 也不能证明竞态不存在。

## 复现材料

诊断源码保存为 `docs/reviews/artifacts/2026-10-09-pg-lease-reanalysis/probe_test.go.txt`，不会进入正常 Go package 编译。通过 overlay 映射为 `lease/lease_reanalysis_probe_test.go`，复用本库现有测试 helper，未改正式源码。

在仓库根目录准备隔离临时 PG，并设置只指向该临时实例的 `IDGEN_TEST_DATABASE_URL` 后，可生成 overlay：

```bash
reanalysis_tmp=$(mktemp -d)
git show v0.2.1:lease/lease.go > "$reanalysis_tmp/lease-baseline.go"
jq -n --arg source "$PWD/lease/lease.go" \
  --arg baseline "$reanalysis_tmp/lease-baseline.go" \
  '{Replace:{($source):$baseline}}' > "$reanalysis_tmp/baseline-overlay.json"
jq -n --arg source "$PWD/lease/lease_reanalysis_probe_test.go" \
  --arg probe "$PWD/docs/reviews/artifacts/2026-10-09-pg-lease-reanalysis/probe_test.go.txt" \
  '{Replace:{($source):$probe}}' > "$reanalysis_tmp/probe-overlay.json"

go test -overlay "$reanalysis_tmp/baseline-overlay.json" -tags=integration \
  -run '^TestPGLeaseStoreConcurrentAcquireReturnsWinnerStateIntegration$' \
  -count=20 -timeout=90s ./lease
# 上述基线命令预期失败；当前实现和诊断分别独立执行。
go test -tags=integration \
  -run '^TestPGLeaseStoreConcurrentAcquireReturnsWinnerStateIntegration$' \
  -count=100 -timeout=90s ./lease
go test -overlay "$reanalysis_tmp/probe-overlay.json" -tags=integration \
  -run '^TestLeaseReanalysis' -count=5 -v -timeout=90s ./lease
```

## 调用方影响

修复只改变失败方状态读取；公共签名、schema、owner 获取条件和 fencing 语义不变。正常 READ COMMITTED 并发不可见冲突会增加一次 SELECT，失败方可能因此更准确地计算重试状态；补读错误会正常向上返回。

调用方仍依赖 v0.2.1 时不会自动获得本地 `116e5be` 的改动，本轮没有发布新版本或改调用方依赖。后续需要明确事务支持边界，并将已验证的诊断场景纳入正式回归；等待口径问题另行修正。

## 后续修复登记

用户已授权实施，本次选定范围进入 `specs/001-lease-retry-timing/`。补读保留首次数据库时间参考，800ms等待回归先red后green；正式测试加入映射、无行、事务隔离与rollback，锁观察改独立连接。完整验证、独立审查和清理均完成，见 `specs/001-lease-retry-timing/validation.md`。上面的分析和诊断原件保留为修复前事实；其他历史测试审计条目未批量关闭。
