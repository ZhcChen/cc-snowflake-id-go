//go:build integration

package lease

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPGLeaseStoreConcurrentAcquireAndFenceTakeoverIntegration(t *testing.T) {
	ctx := context.Background()
	poolA, poolB := newIntegrationLeasePools(t)
	storeA := mustPGLeaseStore(t, poolA)
	storeB := mustPGLeaseStore(t, poolB)

	// 这个集成测试覆盖两个真实数据库语义：
	// 1. 并发 acquire 时只能有一个 winner；
	// 2. 即使旧租约已过期，只要旧 fence 未过，新 owner 仍不能接管。
	databaseAlignedNow := time.Now().UnixMilli()
	fastOwnerNow := databaseAlignedNow + 300
	oldFence := fastOwnerNow + 500
	const (
		leaseWindowMillis  = int64(1_000)
		maxClockSkewMillis = int64(1_000)
	)

	type acquireResult struct {
		owner    string
		state    LeaseState
		acquired bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan acquireResult, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	acquire := func(store *PGLeaseStore, owner string) {
		ready.Done()
		<-start
		state, acquired, err := store.TryAcquire(
			ctx,
			7,
			owner,
			fastOwnerNow,
			leaseWindowMillis,
			oldFence,
			maxClockSkewMillis,
		)
		results <- acquireResult{owner: owner, state: state, acquired: acquired, err: err}
	}
	go acquire(storeA, "owner-a")
	go acquire(storeB, "owner-b")
	ready.Wait()
	close(start)

	first := <-results
	second := <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent acquire errors: first=%v second=%v", first.err, second.err)
	}
	if first.acquired == second.acquired {
		t.Fatalf("acquired results = %v/%v, want exactly one owner", first.acquired, second.acquired)
	}
	winner := first
	loser := second
	loserStore := storeB
	if second.acquired {
		winner, loser = second, first
		loserStore = storeA
	}
	if loser.state.OwnerID != winner.owner {
		t.Fatalf("loser observed owner = %q, want %q", loser.state.OwnerID, winner.owner)
	}
	state, acquired, err := loserStore.TryAcquire(
		ctx,
		7,
		loser.owner,
		oldFence+20,
		leaseWindowMillis,
		oldFence+520,
		maxClockSkewMillis,
	)
	if err != nil {
		t.Fatalf("fast-clock contender error = %v", err)
	}
	if acquired || state.OwnerID != winner.owner {
		t.Fatalf("fast-clock contender bypassed active owner: acquired=%v state=%+v", acquired, state)
	}

	takeoverNow := time.Now().UnixMilli() + 300
	takeoverFence := takeoverNow + 600
	state, acquired, err = storeA.TryAcquire(
		ctx,
		10,
		"old-fast-owner",
		takeoverNow,
		100,
		takeoverFence,
		maxClockSkewMillis,
	)
	if err != nil || !acquired {
		t.Fatalf("takeover setup state=%+v acquired=%v error=%v", state, acquired, err)
	}

	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()
	<-timer.C

	normalNow := time.Now().UnixMilli()
	state, acquired, err = storeB.TryAcquire(
		ctx,
		10,
		"normal-owner",
		normalNow,
		100,
		normalNow+500,
		maxClockSkewMillis,
	)
	if err != nil {
		t.Fatalf("takeover before old fence error = %v", err)
	}
	if acquired {
		t.Fatalf("takeover acquired before old fence: state=%+v", state)
	}
	if state.GenerationFenceMillis != takeoverFence || normalNow >= takeoverFence {
		t.Fatalf("persisted fence proof invalid: now=%d old_fence=%d state=%+v", normalNow, takeoverFence, state)
	}

	waitForWallClock(t, takeoverFence)
	normalNow = time.Now().UnixMilli()
	state, acquired, err = storeB.TryAcquire(
		ctx,
		10,
		"normal-owner",
		normalNow,
		100,
		normalNow+500,
		maxClockSkewMillis,
	)
	if err != nil {
		t.Fatalf("takeover after old fence error = %v", err)
	}
	if !acquired || state.OwnerID != "normal-owner" || state.GenerationFenceMillis <= takeoverFence {
		t.Fatalf("takeover state = %+v acquired=%v, want new non-overlapping owner fence", state, acquired)
	}
}

func TestPGLeaseStoreRefreshPreservesFenceAndRejectsClockSkewIntegration(t *testing.T) {
	ctx := context.Background()
	poolA, _ := newIntegrationLeasePools(t)
	store := mustPGLeaseStore(t, poolA)

	// refresh 只能把 fence 向前推进，不能回退；同时数据库时钟校验失败时不应落库。
	nowMillis := time.Now().UnixMilli()
	initialFence := nowMillis + 800
	state, acquired, err := store.TryAcquire(ctx, 8, "owner-a", nowMillis, 500, initialFence, 1_000)
	if err != nil || !acquired {
		t.Fatalf("initial acquire state=%+v acquired=%v error=%v", state, acquired, err)
	}

	refreshNow := time.Now().UnixMilli()
	state, err = store.Refresh(ctx, 8, "owner-a", refreshNow, 500, refreshNow+100, 1_000)
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if state.GenerationFenceMillis != initialFence {
		t.Fatalf("refresh shortened fence: got=%d want=%d", state.GenerationFenceMillis, initialFence)
	}

	badRefreshNow := time.Now().UnixMilli() + 2_000
	_, err = store.Refresh(ctx, 8, "owner-a", badRefreshNow, 500, badRefreshNow+500, 1_000)
	if !errors.Is(err, ErrClockSkew) {
		t.Fatalf("skewed Refresh() error = %v, want ErrClockSkew", err)
	}
	var persistedFence int64
	if err := poolA.QueryRow(ctx, "SELECT generation_fence_ms FROM id_generator_node_leases WHERE node_id = 8").Scan(&persistedFence); err != nil {
		t.Fatalf("read fence after skewed refresh: %v", err)
	}
	if persistedFence != initialFence {
		t.Fatalf("skewed refresh changed fence: got=%d want=%d", persistedFence, initialFence)
	}

	badLocalNow := time.Now().UnixMilli() + 2_000
	_, acquired, err = store.TryAcquire(ctx, 9, "skewed-owner", badLocalNow, 500, badLocalNow+500, 1_000)
	if !errors.Is(err, ErrClockSkew) || acquired {
		t.Fatalf("skewed acquire acquired=%v error=%v, want ErrClockSkew", acquired, err)
	}
	var rowCount int
	if err := poolA.QueryRow(ctx, "SELECT COUNT(*) FROM id_generator_node_leases WHERE node_id = 9").Scan(&rowCount); err != nil {
		t.Fatalf("count skewed lease row: %v", err)
	}
	if rowCount != 0 {
		t.Fatalf("skewed acquire persisted %d row(s), want 0", rowCount)
	}
}

func TestPGLeaseStoreOwnerMismatchAndReleaseWaitForNaturalExpiryIntegration(t *testing.T) {
	ctx := context.Background()
	poolA, poolB := newIntegrationLeasePools(t)
	storeA := mustPGLeaseStore(t, poolA)
	storeB := mustPGLeaseStore(t, poolB)

	// Release 只是停止当前 owner 的后续操作，不会提前缩短已经写入数据库的租约窗口。
	// 因此其他 owner 仍然必须等到 reserved_until 和 fence 自然过去之后才能接管。
	nowMillis := time.Now().UnixMilli()
	initialFence := nowMillis + 5_500
	state, acquired, err := storeA.TryAcquire(ctx, 11, "owner-a", nowMillis, 5_000, initialFence, 1_000)
	if err != nil || !acquired {
		t.Fatalf("initial acquire state=%+v acquired=%v error=%v", state, acquired, err)
	}
	initialReservedUntil := state.ReservedUntilMillis

	_, err = storeB.Refresh(ctx, 11, "owner-b", time.Now().UnixMilli(), 200, time.Now().UnixMilli()+300, 1_000)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("owner mismatch Refresh() error = %v, want ErrLeaseLost", err)
	}
	assertPersistedLeaseOwnerAndBounds(t, poolA, 11, "owner-a", initialReservedUntil, initialFence)

	if err := storeB.Release(ctx, 11, "owner-b"); err != nil {
		t.Fatalf("mismatched Release() error = %v", err)
	}
	assertPersistedLeaseOwnerAndBounds(t, poolA, 11, "owner-a", initialReservedUntil, initialFence)
	if err := storeA.Release(ctx, 11, "owner-a"); err != nil {
		t.Fatalf("owner Release() error = %v", err)
	}
	assertPersistedLeaseOwnerAndBounds(t, poolA, 11, "owner-a", initialReservedUntil, initialFence)

	contenderNow := time.Now().UnixMilli()
	if contenderNow >= initialReservedUntil || contenderNow >= initialFence {
		t.Fatalf(
			"pre-expiry contender setup exceeded owner window: now=%d reserved_until=%d fence=%d",
			contenderNow,
			initialReservedUntil,
			initialFence,
		)
	}
	state, acquired, err = storeB.TryAcquire(ctx, 11, "owner-b", contenderNow, 200, contenderNow+300, 1_000)
	if err != nil {
		t.Fatalf("contender before natural expiry error = %v", err)
	}
	if acquired || state.OwnerID != "owner-a" {
		t.Fatalf("release shortened ownership: state=%+v acquired=%v", state, acquired)
	}

	waitUntil := initialFence
	if initialReservedUntil > waitUntil {
		waitUntil = initialReservedUntil
	}
	waitForWallClock(t, waitUntil+2)
	contenderNow = time.Now().UnixMilli()
	state, acquired, err = storeB.TryAcquire(ctx, 11, "owner-b", contenderNow, 200, contenderNow+300, 1_000)
	if err != nil {
		t.Fatalf("contender after natural expiry error = %v", err)
	}
	if !acquired || state.OwnerID != "owner-b" || state.GenerationFenceMillis <= initialFence {
		t.Fatalf("takeover state=%+v acquired=%v, want owner-b with a later fence", state, acquired)
	}
}

func newIntegrationLeasePools(t *testing.T) (*pgxpool.Pool, *pgxpool.Pool) {
	t.Helper()
	databaseURL := os.Getenv("IDGEN_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("IDGEN_TEST_DATABASE_URL is required for integration tests")
	}

	ctx := context.Background()
	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect integration database: %v", err)
	}
	t.Cleanup(adminPool.Close)

	schemaName := fmt.Sprintf("idgen_integration_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatalf("create integration schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quotedSchema+" CASCADE")
	})

	newPool := func() *pgxpool.Pool {
		config, err := pgxpool.ParseConfig(databaseURL)
		if err != nil {
			t.Fatalf("parse integration database URL: %v", err)
		}
		config.ConnConfig.RuntimeParams["search_path"] = schemaName
		pool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			t.Fatalf("create integration pool: %v", err)
		}
		t.Cleanup(pool.Close)
		return pool
	}

	poolA := newPool()
	poolB := newPool()
	if _, err := poolA.Exec(ctx, `
CREATE TABLE id_generator_node_leases (
    node_id INTEGER PRIMARY KEY,
    owner_id TEXT NOT NULL,
    reserved_until_ms BIGINT NOT NULL CHECK (reserved_until_ms > 0),
    generation_fence_ms BIGINT NOT NULL CHECK (generation_fence_ms > 0),
    acquired_at_ms BIGINT NOT NULL,
    refreshed_at_ms BIGINT NOT NULL,
    heartbeat_at_ms BIGINT NOT NULL,
    lease_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`); err != nil {
		t.Fatalf("create integration lease table: %v", err)
	}
	return poolA, poolB
}

func mustPGLeaseStore(t *testing.T, pool *pgxpool.Pool) *PGLeaseStore {
	t.Helper()
	store, err := NewPGLeaseStore(pool)
	if err != nil {
		t.Fatalf("NewPGLeaseStore() error = %v", err)
	}
	return store
}

func assertPersistedLeaseOwnerAndBounds(
	t *testing.T,
	pool *pgxpool.Pool,
	nodeID int,
	wantOwner string,
	wantReservedUntil int64,
	wantFence int64,
) {
	t.Helper()
	var (
		ownerID       string
		reservedUntil int64
		fence         int64
	)
	if err := pool.QueryRow(
		context.Background(),
		"SELECT owner_id, reserved_until_ms, generation_fence_ms FROM id_generator_node_leases WHERE node_id = $1",
		nodeID,
	).Scan(&ownerID, &reservedUntil, &fence); err != nil {
		t.Fatalf("read persisted lease: %v", err)
	}
	if ownerID != wantOwner || reservedUntil != wantReservedUntil || fence != wantFence {
		t.Fatalf(
			"persisted lease = owner:%q reserved:%d fence:%d, want owner:%q reserved:%d fence:%d",
			ownerID,
			reservedUntil,
			fence,
			wantOwner,
			wantReservedUntil,
			wantFence,
		)
	}
}

func waitForWallClock(t *testing.T, targetMillis int64) {
	t.Helper()
	duration := time.Until(time.UnixMilli(targetMillis))
	if duration <= 0 {
		return
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	<-timer.C
}
