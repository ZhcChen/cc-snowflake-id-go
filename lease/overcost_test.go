package lease

import (
	"context"
	"testing"
	"time"

	sf "github.com/ZhcChen/cc-snowflake-id-go/generator"
)

func TestLeasedGeneratorOverCostAdvancesWithoutSleeping(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state: LeaseState{
				NodeID:                7,
				OwnerID:               "api:host:1:abc",
				ReservedUntilMillis:   5_000,
				DatabaseNowMillis:     2_000,
				GenerationFenceMillis: 4_000,
			},
			acquired: true,
		}},
		refreshState: LeaseState{
			NodeID:                7,
			OwnerID:               "api:host:1:abc",
			ReservedUntilMillis:   5_000,
			DatabaseNowMillis:     2_000,
			GenerationFenceMillis: 4_000,
		},
	}
	clock := &fakeClock{now: 2_000}
	generator, err := NewLeasedGenerator(store, clock, LeasedGeneratorConfig{
		NodeID:                7,
		OwnerID:               "api:host:1:abc",
		EpochMillis:           1_000,
		OverCostCount:         10,
		LeaseWindow:           10 * time.Second,
		FenceWindow:           10 * time.Second,
		MaxClockSkew:          time.Second,
		LeaseAcquireTimeout:   10 * time.Millisecond,
		LeaseOperationTimeout: time.Millisecond,
		LeaseRefreshInterval:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeasedGenerator() error = %v", err)
	}
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	for i := int64(0); i <= sf.MaxSequence; i++ {
		if _, err := generator.Next(context.Background()); err != nil {
			t.Fatalf("warmup Next(%d) error = %v", i, err)
		}
	}
	id, err := generator.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	parts, err := sf.Decode(id, 1_000)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if parts.TimestampMillis != 2_001 || parts.Sequence != 0 {
		t.Fatalf("parts = %+v, want timestamp=2001 sequence=0", parts)
	}
	if len(clock.sleeps) != 0 {
		t.Fatalf("sleeps = %#v, want no wall clock wait", clock.sleeps)
	}
	if store.refreshCalls != 0 {
		t.Fatalf("refreshCalls = %d, want 0", store.refreshCalls)
	}
}
