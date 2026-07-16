package generator

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"testing"
)

func TestGeneratorConcurrentMillionIDsAreUniqueAndDecodable(t *testing.T) {
	const (
		total  = 1_000_000
		nodeID = 321
	)
	workers := runtime.GOMAXPROCS(0) * 2
	if workers < 8 {
		workers = 8
	}
	if workers > 32 {
		workers = 32
	}

	generator, err := NewGenerator(Config{NodeID: nodeID}, nil)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}
	ids := make([]int64, total)
	ctx := context.Background()
	var (
		wg       sync.WaitGroup
		firstErr error
		errOnce  sync.Once
	)
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(offset int) {
			defer wg.Done()
			for index := offset; index < total; index += workers {
				id, generateErr := generator.Next(ctx)
				if generateErr != nil {
					errOnce.Do(func() { firstErr = generateErr })
					return
				}
				ids[index] = id
			}
		}(worker)
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatalf("concurrent Next() error = %v", firstErr)
	}

	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	for index, id := range ids {
		if id <= 0 {
			t.Fatalf("ids[%d] = %d, want positive", index, id)
		}
		if index > 0 && id == ids[index-1] {
			t.Fatalf("duplicate id at sorted indexes %d/%d: %d", index-1, index, id)
		}
		parts, decodeErr := Decode(id, DefaultEpochMillis)
		if decodeErr != nil {
			t.Fatalf("Decode(ids[%d]) error = %v", index, decodeErr)
		}
		if parts.NodeID != nodeID {
			t.Fatalf("Decode(ids[%d]).NodeID = %d, want %d", index, parts.NodeID, nodeID)
		}
	}
}
