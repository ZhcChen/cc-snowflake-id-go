package generator

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

var benchmarkID int64

func BenchmarkGeneratorNext(b *testing.B) {
	generator := newBenchmarkGenerator(b)
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
		b.Fatalf("Generator.Next() error = %v", err)
	}
	assertBenchmarkID(b, id)
	benchmarkID = id
}

func BenchmarkGeneratorNextParallel(b *testing.B) {
	generator := newBenchmarkGenerator(b)
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
		b.Fatalf("Generator.Next() error = %v", firstErr)
	}
	id := lastID.Load()
	assertBenchmarkID(b, id)
	benchmarkID = id
}

func newBenchmarkGenerator(b *testing.B) *Generator {
	b.Helper()
	generator, err := NewGenerator(Config{NodeID: MinNodeID}, nil)
	if err != nil {
		b.Fatalf("NewGenerator() error = %v", err)
	}
	return generator
}

func assertBenchmarkID(b *testing.B, id int64) {
	b.Helper()
	if id <= 0 {
		b.Fatalf("generated id = %d, want positive", id)
	}
	parts, err := Decode(id, DefaultEpochMillis)
	if err != nil {
		b.Fatalf("Decode() error = %v", err)
	}
	if parts.NodeID != MinNodeID {
		b.Fatalf("decoded node_id = %d, want %d", parts.NodeID, MinNodeID)
	}
}
