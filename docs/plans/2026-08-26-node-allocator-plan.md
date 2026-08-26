---
title: 雪花 ID 节点动态分配（node allocator）能力计划
date: 2026-08-26
artifact_contract: ce-unified-plan/v1
artifact_readiness: draft
product_contract_source: ce-plan-bootstrap
execution: code
status: draft
---

# 雪花 ID 节点动态分配（node allocator）能力计划

## Goal Capsule

为 `lease` 包增加“从数据库租约表动态分配 node_id”的能力：调用方不再必须把
`node_id` 写死在部署配置里，而是给定一个候选范围，由库在启动时从
`id_generator_node_leases` 中挑选空闲节点并以乐观锁抢占，随后沿用现有
`LeasedGenerator` 的 acquire / refresh / release / generation fence 语义。

## Problem Frame

当前 `lease` 的使用方式是“外部已经确定 node_id，数据库只保证同一个
node_id 不能被多个实例同时发号”：

```go
store, _ := lease.NewPGLeaseStore(db)
generator, _ := lease.NewLeasedGenerator(store, nil, lease.LeasedGeneratorConfig{
    NodeID: 1,
    ...
})
state, _ := generator.Acquire(ctx)
```

这对“每个部署节点需要独立配置 node_id”的运维模型不友好：

- 节点数量变化时需要人工分配 node_id；
- Deploy Go 等平台会把应用级 Env 分发给所有目标节点，无法按目标下发不同
  node_id；
- 同一个机器可能承载多个应用，node_id 不应成为节点级系统环境变量。

目标形态是调用方只提供候选范围（例如 API `1-99`），实例启动时自己从数据库
领号：

```go
generator, _ := lease.NewAllocatingLeasedGenerator(ctx, store, nil, cfg)
state, _ := generator.Acquire(ctx) // state.NodeID 为本次分配到的节点
```

## Requirements

- R1. 支持从 `[NodeRangeStart, NodeRangeEnd]` 范围内挑选空闲 node_id 并原子
  抢占，两个并发实例不能同时占用同一个节点。
- R2. 分配结果必须继续遵守现有 generation fence：即使旧租约已过期，只要旧
  fence 未过，新实例不能接管同一节点发号。
- R3. 支持 `PreferredNodeIDs`：实例重启时优先续用自己上一次持有的节点，减少
  ID 归属漂移；首选节点不可用时回退到范围内其他节点。
- R4. 候选范围与首选节点都使用现有 `generator.ValidateNodeID` 边界
  （`1-1024`），非法范围在分配前 fail fast。
- R5. 分配能力不依赖调用方自建 `role` / `status` 列；库只依赖
  `id_generator_node_leases` 的现有契约。业务侧预置 1-1024、固定不可分配等
  运维守门留在调用方 migration 中，通过传入范围表达。
- R6. 保持现有 `LeasedGenerator`、`LeaseStore`、`LeaseManager` API 完全兼容；
  allocator 作为新增能力存在。
- R7. 分配失败（范围耗尽、租约被占、fence 未过、存储错误）返回明确错误，
  不静默回退到未受租约保护的本地生成。

## Key Technical Decisions

- KTD1. 不修改现有 `LeaseStore` 接口，避免破坏调用方自定义 store；新增可选
  接口 `AnyNodeLeaseStore`，`PGLeaseStore` 实现该接口。
- KTD2. `TryAcquireAny` 采用“候选查询 + 现有 `TryAcquire` 原子 UPSERT”两段式：
  候选查询只负责缩小范围，最终排他性仍由现有 CAS SQL 保证；并发竞争时输家
  在下一轮重选候选，正确性不依赖候选查询本身加锁。
- KTD3. 候选顺序默认按 node_id 升序，保证行为确定、便于排查；调用方可通过
  `PreferredNodeIDs` 覆盖重启续用场景。
- KTD4. 分配器不感知 `role` / `status` 等业务扩展列；`100` / `1024` 等固定
  节点由调用方通过范围参数排除。
- KTD5. 高层入口 `NewAllocatingLeasedGenerator` 内部先分配并保留租约，再构造
  现有 `LeasedGenerator`；调用方继续走既有 `Acquire` 流程，保持生命周期一致。
- KTD6. 新增错误 `ErrNoAvailableNode` 与低基数错误分类
  `ErrorClassNoAvailableNode`，便于调用方判断“范围耗尽”而非“存储故障”。

## API 设计草案

```go
// NodeRange 表示可分配的节点闭区间，边界复用 generator.ValidateNodeID。
type NodeRange struct {
    Start int
    End   int
}

// ErrNoAvailableNode 表示给定范围内当前没有可占用的节点。
var ErrNoAvailableNode = errors.New("idgen: no available node in range")

// AnyNodeLeaseStore 扩展 LeaseStore，支持从节点范围分配租约。
type AnyNodeLeaseStore interface {
    LeaseStore
    TryAcquireAny(
        ctx context.Context,
        ownerID string,
        rng NodeRange,
        localNowMillis int64,
        leaseWindowMillis int64,
        generationFenceMillis int64,
        maxClockSkewMillis int64,
    ) (LeaseState, bool, error)
}

// AllocatingLeasedGeneratorConfig 在 LeasedGeneratorConfig 基础上增加分配范围。
// NodeID 字段在分配模式下不参与输入，由分配结果覆盖。
type AllocatingLeasedGeneratorConfig struct {
    LeasedGeneratorConfig
    NodeRange        NodeRange
    PreferredNodeIDs []int
}

// NewAllocatingLeasedGenerator 从 NodeRange 中分配节点并返回可复用的
// LeasedGenerator；调用方仍按现有流程调用 Acquire / StartRuntime。
func NewAllocatingLeasedGenerator(
    ctx context.Context,
    store AnyNodeLeaseStore,
    clock sf.Clock,
    cfg AllocatingLeasedGeneratorConfig,
) (*LeasedGenerator, error)
```

候选查询草案（`PGLeaseStore.TryAcquireAny` 内部使用）：

```sql
WITH db_clock AS (
    SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT AS now_ms
)
SELECT COALESCE((
    SELECT n.node_id
    FROM generate_series($1::INTEGER, $2::INTEGER) AS n(node_id)
    WHERE NOT EXISTS (
        SELECT 1
        FROM id_generator_node_leases l
        WHERE l.node_id = n.node_id
          AND l.reserved_until_ms > (SELECT now_ms FROM db_clock)
          AND l.owner_id <> $3::TEXT
    )
    ORDER BY n.node_id
    LIMIT 1
), 0)
```

候选为 0 时返回 `ErrNoAvailableNode`；否则调用现有 `TryAcquire` 抢占该节点。
`NewAllocatingLeasedGenerator` 会先尝试 `PreferredNodeIDs`，再按升序尝试
范围候选，直到 `LeaseAcquireTimeout` 耗尽。

## Implementation Units

### U1. Store 层 `TryAcquireAny`

- Files:
  - `lease/lease.go`
  - `lease/lease_test.go`
  - `lease/lease_integration_test.go`
- 实现 `NodeRange`、`AnyNodeLeaseStore`、`ErrNoAvailableNode` 和
  `PGLeaseStore.TryAcquireAny`。
- 范围校验复用 `generator.ValidateNodeID`，并校验 `Start <= End`。
- 补充集成测试：范围分配、并发抢占互斥、范围耗尽、释放后可再分配、预置空闲
  行（`reserved_until_ms = 0`）可被抢占。

### U2. 高层 allocator

- Files:
  - `lease/allocator.go`（新增）
  - `lease/allocator_test.go`（新增）
  - `lease/reliability_test.go`（如涉及超时/取消路径）
- 实现 `AllocatingLeasedGeneratorConfig` 与
  `NewAllocatingLeasedGenerator`：首选节点优先、范围升序候选、超时与上下文
  取消、失败时明确返回 `ErrNoAvailableNode` 或存储错误。
- 保持与现有 `Acquire / StartRuntime / Close` 生命周期一致；文档说明
  `NodeID` 在分配模式下由结果覆盖。

### U3. telemetry 与错误分类

- Files:
  - `lease/telemetry.go`
  - `lease/telemetry_test.go`
- 新增 `ErrorClassNoAvailableNode` 并接入 `ClassifyError`。
- 分配器失败日志/事件不提升 owner_id、node_id 为高基数标签，遵循现有
  telemetry 边界。

### U4. 示例与文档

- Files:
  - `examples/lease-runtime/main.go`
  - `examples/lease-runtime/schema.sql`（如需要补充说明）
  - `docs/guides/lease-runtime-demo.md`
  - `docs/guides/getting-started.md`
  - `docs/README.md`
  - `README.md`
- 增加 allocator 最小示例：范围分配、首选节点、`Acquire` 后读取
  `state.NodeID`。
- README 更新适用场景与安装版本（计划发布为 `v0.3.0`）。

### U5. 发布

- 跑通 `scripts/test/unit.ps1`、`scripts/test/race.ps1`、
  `scripts/test/integration.ps1`（或等价本地命令）。
- 导出 API 注释按 `docs/standards/code-commenting.md` 全量补齐。
- 合并 main 并打 tag `v0.3.0`；qfy-finance 升级依赖后接入。

## Verification Contract

| Gate | 验证 |
| --- | --- |
| 兼容性 | 现有 `LeaseStore` / `LeasedGenerator` / runtime 测试全部通过 |
| 分配互斥 | 两个 store 并发 `TryAcquireAny` 不重复占用同一节点 |
| 首选节点 | 首选节点可占用时优先返回，被占时回退范围 |
| 范围耗尽 | 全部节点被占或范围为空时返回 `ErrNoAvailableNode` |
| fence | 旧租约过期但 fence 未过时，allocator 不能接管同一节点 |
| 取消/超时 | ctx 取消与 `LeaseAcquireTimeout` 路径正确终止且状态可诊断 |
| 文档 | README / guide 与导出 API 注释一致 |

## Definition of Done

- `lease` 包提供 `TryAcquireAny` 与 `NewAllocatingLeasedGenerator`。
- 现有固定 node_id 用法零改动可用。
- 集成、race、单元测试通过，README 与示例更新。
- 发布 `v0.3.0`，qfy-finance 升级依赖并完成接线。

## Scope Boundaries

- 不引入 `role` / `status` / 1-1024 预置行等业务运维语义；这些由调用方在
  migration 中实现。
- 不修改 `generator` 包发号算法。
- 不改变 Worker 固定节点（100）或本地 seed 工具（1024）的调用方式。

## 待确认

- `NewAllocatingLeasedGenerator` 是否保留“内部先占用再让调用方 Acquire”的
  两次租约写入，还是重构 `LeasedGenerator` 支持注入已分配 state。
- 候选顺序固定升序是否足够，是否需要随机化避免热点。
- `PreferredNodeIDs` 由调用方持久化，还是库内提供按 owner 前缀找回上一节点的
  查询能力。
