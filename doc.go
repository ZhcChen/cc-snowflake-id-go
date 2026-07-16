// Package idgen 只保留模块根路径的包级说明。
//
// 这个仓库已经按职责拆分为两个子包：
// 1. generator：纯进程内雪花 ID 生成能力。
// 2. lease：基于租约的多实例安全发号、运行时与 telemetry 能力。
//
// 根包不再导出兼容 facade。调用方应按职责直接依赖子包，而不是继续引用
// 模块根路径。
//
// 注释规范见 docs/standards/code-commenting.md。
package idgen
