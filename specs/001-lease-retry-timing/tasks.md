# 实施任务：租约补读重试时间与事务契约

来源：本目录 `spec.md`、`plan.md`。

## 进度约定

保留任务 ID 与状态，完成后追加证据。共享文件修改串行，只读探索/审查可并行。

## 依赖与实施顺序

T001 -> T002 -> T003 -> T004 -> T005 -> T006。US1 先 red 再 green；US2 用真实 PG 验证。最小交付为 US1，但本次完成全部。无并行写入任务；时间策略与测试范围已由两个只读子任务并行确认。

## Phase 1: Setup / Foundations

- [x] T001 核对规范、复用诊断与记录规格质量于 specs/001-lease-retry-timing/
  - 验证：analyze 覆盖 FR-001–004、SC-001–003，无高风险遗漏。
  - 证据：analyze覆盖100%，需求检查6/6，见validation.md。

## Phase 2: User Story 1

- [x] T002 [US1] 在 lease/lease_test.go 增加800ms等待、完整映射和补读无行回归
  - 依赖：T001；验证：旧实现新增等待/时间参考断言失败，既有普通 RTT 仍通过。
  - 证据：旧实现实际600ms、时间2200导致退出1；修后聚焦通过，见validation.md。
- [x] T003 [US1] 在 lease/lease.go 保留初次数据库时间参考并同步字段注释
  - 依赖：T002；验证：聚焦全部通过，SQL安全条件不变（FR-001、002、004）。
  - 证据：新800ms、原60ms断言通过；独立review确认CTE安全条件不变。

## Phase 3: User Story 2

- [x] T004 [US2] 在 lease/lease_integration_test.go 补事务矩阵并使用独立观察连接
  - 依赖：T003；验证：RC胜者、RR/Serializable 40001与持久化保护、rollback竞争方成功；单连接池可运行（FR-003、004）。
  - 证据：事务矩阵10轮，单池关键用例5轮全部通过。
- [x] T005 [US2] 在 lease/lease.go、lease/doc.go、docs/guides/lease-runtime-demo.md 明确事务/executor契约
  - 依赖：T004；验证：与正式测试一致，明确提交和调用方重试责任（FR-003）。
  - 证据：独立review确认事务文档与矩阵一致。

## Phase 4: Polish / Cross-Cutting

- [x] T006 完成真实验证、独立审查、converge并记录 specs/001-lease-retry-timing/validation.md
  - 依赖：T002–005；验证：完整单元/race/vet/PG/PG race，winner100轮、事务10轮、单池连接边界；schema/容器清理，diff check（SC-001–003）。
  - 证据：全部通过，schema计数0，临时容器删除；review无发现，converge无追加。

## 收敛检查

按当前代码核对所有 FR/SC；历史其他审计缺口保持待分诊，不扩大本需求。
