package lease

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sf "github.com/ZhcChen/cc-snowflake-id-go/generator"
)

var benchmarkLeaseID int64

func BenchmarkLeasedGeneratorNext(b *testing.B) {
	benchmarkLeasedGeneratorNext(b, nil)
}

func BenchmarkLeasedGeneratorNextWithTelemetry(b *testing.B) {
	telemetry := NewTelemetry()
	benchmarkLeasedGeneratorNext(b, telemetry)
	assertBenchmarkTelemetry(b, telemetry)
}

func benchmarkLeasedGeneratorNext(b *testing.B, observer Observer) {
	b.Helper()
	generator, store := newBenchmarkLeasedGenerator(b, observer)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	var (
		id  int64
		err error
	)
	for i := 0; i < b.N; i++ {
		id, err = generator.Next(ctx)
		if err != nil {
			break
		}
	}
	b.StopTimer()

	if err != nil {
		b.Fatalf("LeasedGenerator.Next() error = %v", err)
	}
	assertBenchmarkID(b, id)
	assertBenchmarkStoreHotPath(b, store)
	benchmarkLeaseID = id
}

func BenchmarkLeasedGeneratorNextParallel(b *testing.B) {
	benchmarkLeasedGeneratorNextParallel(b, nil)
}

func BenchmarkLeasedGeneratorNextParallelWithTelemetry(b *testing.B) {
	telemetry := NewTelemetry()
	benchmarkLeasedGeneratorNextParallel(b, telemetry)
	assertBenchmarkTelemetry(b, telemetry)
}

func benchmarkLeasedGeneratorNextParallel(b *testing.B, observer Observer) {
	b.Helper()
	generator, store := newBenchmarkLeasedGenerator(b, observer)
	ctx := context.Background()
	var (
		firstErr error
		errOnce  sync.Once
		lastID   atomic.Int64
	)

	b.ReportAllocs()
	b.SetParallelism(1)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var id int64
		for pb.Next() {
			generated, err := generator.Next(ctx)
			if err != nil {
				errOnce.Do(func() { firstErr = err })
				break
			}
			id = generated
		}
		if id > 0 {
			lastID.Store(id)
		}
	})
	b.StopTimer()

	if firstErr != nil {
		b.Fatalf("LeasedGenerator.Next() error = %v", firstErr)
	}
	id := lastID.Load()
	assertBenchmarkID(b, id)
	assertBenchmarkStoreHotPath(b, store)
	benchmarkLeaseID = id
}

func newBenchmarkLeasedGenerator(b *testing.B, observer Observer) (*LeasedGenerator, *benchmarkLeaseStore) {
	b.Helper()
	store := &benchmarkLeaseStore{}
	generator, err := NewLeasedGenerator(store, nil, LeasedGeneratorConfig{
		NodeID:               sf.MinNodeID,
		OwnerID:              "benchmark-owner",
		LeaseWindow:          24 * time.Hour,
		FenceWindow:          24 * time.Hour,
		MaxClockSkew:         time.Second,
		LeaseRefreshInterval: time.Hour,
		Observer:             observer,
	})
	if err != nil {
		b.Fatalf("NewLeasedGenerator() error = %v", err)
	}
	if _, err := generator.Acquire(context.Background()); err != nil {
		b.Fatalf("Acquire() error = %v", err)
	}
	b.Cleanup(func() {
		if err := generator.Close(context.Background()); err != nil {
			b.Errorf("Close() error = %v", err)
		}
	})
	return generator, store
}

func assertBenchmarkTelemetry(b *testing.B, telemetry *Telemetry) {
	b.Helper()
	if generated := telemetry.counterSnapshot().generatedTotal; generated != uint64(b.N) {
		b.Fatalf("telemetry generated total = %d, want %d", generated, b.N)
	}
}

func assertBenchmarkStoreHotPath(b *testing.B, store *benchmarkLeaseStore) {
	b.Helper()
	if calls := store.acquireCalls.Load(); calls != 1 {
		b.Fatalf("lease acquire calls = %d, want 1", calls)
	}
	if calls := store.refreshCalls.Load(); calls != 0 {
		b.Fatalf("lease refresh calls = %d, want 0 on ID hot path", calls)
	}
	if calls := store.releaseCalls.Load(); calls != 0 {
		b.Fatalf("lease release calls = %d before cleanup, want 0", calls)
	}
}

func assertBenchmarkID(b *testing.B, id int64) {
	b.Helper()
	if id <= 0 {
		b.Fatalf("generated id = %d, want positive", id)
	}
	parts, err := sf.Decode(id, sf.DefaultEpochMillis)
	if err != nil {
		b.Fatalf("Decode() error = %v", err)
	}
	if parts.NodeID != sf.MinNodeID {
		b.Fatalf("decoded node_id = %d, want %d", parts.NodeID, sf.MinNodeID)
	}
}

type benchmarkLeaseStore struct {
	acquireCalls atomic.Int64
	refreshCalls atomic.Int64
	releaseCalls atomic.Int64
}

func (s *benchmarkLeaseStore) TryAcquire(
	_ context.Context,
	nodeID int,
	ownerID string,
	localNowMillis int64,
	leaseWindowMillis int64,
	generationFenceMillis int64,
	_ int64,
) (LeaseState, bool, error) {
	s.acquireCalls.Add(1)
	return LeaseState{
		NodeID:                nodeID,
		OwnerID:               ownerID,
		ReservedUntilMillis:   localNowMillis + leaseWindowMillis,
		DatabaseNowMillis:     localNowMillis,
		GenerationFenceMillis: generationFenceMillis,
	}, true, nil
}

func (s *benchmarkLeaseStore) Refresh(
	_ context.Context,
	nodeID int,
	ownerID string,
	localNowMillis int64,
	leaseWindowMillis int64,
	generationFenceMillis int64,
	_ int64,
) (LeaseState, error) {
	s.refreshCalls.Add(1)
	return LeaseState{
		NodeID:                nodeID,
		OwnerID:               ownerID,
		ReservedUntilMillis:   localNowMillis + leaseWindowMillis,
		DatabaseNowMillis:     localNowMillis,
		GenerationFenceMillis: generationFenceMillis,
	}, nil
}

func (s *benchmarkLeaseStore) Release(context.Context, int, string) error {
	s.releaseCalls.Add(1)
	return nil
}
