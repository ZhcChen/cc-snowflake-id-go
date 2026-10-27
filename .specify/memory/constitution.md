# cc-snowflake-id-go Constitution

## 核心原则

### I. 维护可复用库契约

公开 Go API、错误语义和 PostgreSQL schema 是调用方契约。新增行为优先复用现有 package 边界；破坏性变化必须明确影响范围、升级路径和版本策略，不为单个调用方引入隐式兼容层。

### II. 正确性优先于便利性

ID 生成必须保留时间戳、序列、时钟回拨和并发语义。租约变更必须保留 owner 校验、租约自然过期、generation fence 与时钟偏差保护；不能以减少重试或放宽 fencing 换取表面成功。

### III. 验证与风险匹配

行为变更用聚焦单测覆盖边界；并发共享状态变化运行 race 测试；PostgreSQL SQL、事务、锁和快照语义用隔离的 PG integration 验证。测试数据应具备轮次隔离和清理，测试通过须记录实际命令与数据库版本。

### IV. 依赖和所有权清晰

优先沿用模块已有 Go 版本、依赖和测试入口。不得直接修改 Go module cache 或调用方项目来规避本库问题；新依赖应说明必要性和维护成本。示例、schema、文档与当前 API 行为保持一致。

## 约束

- module 版本、依赖和测试入口以 `go.mod`、`go.sum`、`scripts/test/` 与 `.github/workflows/test.yml` 为准。
- PostgreSQL 集成测试只连接隔离的临时数据库或明确授权的测试库，不使用共享或生产数据库。
- 时间、网络和数据库等待应按现有设计提供可控边界；不要通过未约束的 sleep 或真实服务依赖掩盖竞态。
- 面向用户的文档及维护说明使用简体中文；代码领域标识和命令原文保持英文。

## 工作流

- 中型及以上变更按项目 `AGENTS.md` 和 `docs/runbooks/spec-kit-workflow.md` 使用 Spec Kit；明确的小修复可直接实施并聚焦验证。
- Spec 与 plan 记录意图和设计，tasks 记录可验证执行单元。规格结构检查、`analyze` 或 `converge` 不能替代代码审查和测试。
- 新需求不批量重写历史计划；恢复旧工作时核对实际代码，仅迁移仍未完成且已选定的范围。

## 治理

本文件是项目原则摘要，不能覆盖用户当前指令、`AGENTS.md`、测试规范和 runbook。修改原则时先检查相关权威文档，再同步可能受影响的活动规格；有冲突时记录差异并按 `docs/standards/document-authority.md` 判断。

**Version**: 1.0.0 | **Ratified**: 2026-10-09 | **Last Amended**: 2026-10-09
