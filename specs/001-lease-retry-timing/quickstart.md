# 验证入口

准备本机 Go/CGO 与 Docker。仅启动一次性 postgres:16，loopback 动态端口、tmpfs，`IDGEN_TEST_DATABASE_URL` 只指向自建实例，结束后删除容器。

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go test -tags=integration -count=1 -timeout=90s ./lease
go test -race -tags=integration -count=1 -timeout=90s ./lease
go test -tags=integration -run '^TestPGLeaseStoreConcurrentAcquireReturnsWinnerStateIntegration$' -count=100 -timeout=120s ./lease
go test -tags=integration -run '^TestPGLeaseStoreConcurrentAcquireTransactionIntegration$' -count=10 -timeout=120s ./lease
```

期望全部通过，schema无残留；关键并发加 URL 参数 pool_max_conns=1 验证独立观察连接。版本和结果见本目录 `validation.md`。
