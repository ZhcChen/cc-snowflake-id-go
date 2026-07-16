// Package idgen 只保留模块根路径的概览说明。
//
// 这个仓库面向 Go 项目提供可复用的雪花 ID 能力，并按职责拆分为两个子包：
// 1. generator：纯进程内雪花 ID 生成能力。
// 2. lease：基于 PostgreSQL 租约的多实例安全发号、运行时与 telemetry 能力。
//
// 根包不导出兼容 facade。调用方应直接依赖子包，而不是继续引用模块根路径。
//
// 入门示例见：
// 1. examples/generator-basic
// 2. examples/lease-runtime
//
// 注释规范见 docs/standards/code-commenting.md。
package idgen
