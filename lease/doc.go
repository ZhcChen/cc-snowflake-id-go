// Package lease 基于 generator 包提供分布式租约发号能力。
//
// 这个包负责数据库租约协调、带租约保护的发号器、运行时生命周期管理、
// telemetry 与状态上报，适合多实例部署场景。
//
// PostgreSQL 租约管理器通常使用自动提交的 pool/conn。若直接将 pgx.Tx
// 传入 PGLeaseStore，调用方负责提交、回滚与高隔离级别的整事务重试；
// 获取成功不等于事务已提交，不应使用未提交的租约持续发号。
package lease
