// Package lease 基于 generator 包提供分布式租约发号能力。
//
// 这个包负责数据库租约协调、带租约保护的发号器、运行时生命周期管理、
// telemetry 与状态上报，适合多实例部署场景。
package lease
