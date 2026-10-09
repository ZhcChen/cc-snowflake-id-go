# 租约补读时间与事务回归复核

日期：2026-10-09。范围：`specs/001-lease-retry-timing/` 及其列出的租约代码、测试和指南；已有工作流迁移不在本轮代码审查范围。

独立只读 reviewer `final_lease_review` 未发现需修复问题，并独立运行聚焦单测通过。

- FR-001：首次数据库时间参考保留，800ms新回归与60ms原RTT回归覆盖两条路径。
- FR-002：字段映射、node参数、无行与错误正式回归覆盖。
- FR-003：既有自动提交用例与新增RC/RR/Serializable/rollback矩阵及文档一致。
- FR-004：获取CTE条件未变，补读仍不能改变acquired=false；签名/schema/依赖不变。

主agent完成全部验证与资源清理，详见 `specs/001-lease-retry-timing/validation.md`。本轮没有证据支持重复发号结论。

残余边界：全RTT仍包含采样前排队，可能轻微偏早；字段与首次时间参考不保证原子采样。规格和指南均说明，SQL继续承担安全裁决。
