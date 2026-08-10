package generator

import (
	"context"
	"errors"
	"testing"
)

func TestGeneratorOverCostAdvancesWithoutSleeping(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	g, err := NewGenerator(Config{
		NodeID:                1,
		EpochMillis:           1_000,
		OverCostCount:         3,
		AllowInMemoryOverCost: true,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	fillSequence(t, g, 2_000, 0)
	id, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after overflow error = %v", err)
	}
	assertParts(t, id, 2_001, 1, 0)
	if len(clock.sleeps) != 0 {
		t.Fatalf("sleeps = %#v, want no wall clock wait", clock.sleeps)
	}

	id, err = g.Next(context.Background())
	if err != nil {
		t.Fatalf("Next while wall clock behind error = %v", err)
	}
	assertParts(t, id, 2_001, 1, 1)
	if len(clock.sleeps) != 0 {
		t.Fatalf("sleeps = %#v, want no wall clock wait", clock.sleeps)
	}
}

func TestGeneratorOverCostConsumesLimitThenWaits(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	g, err := NewGenerator(Config{
		NodeID:                1,
		EpochMillis:           1_000,
		OverCostCount:         2,
		AllowInMemoryOverCost: true,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	fillSequence(t, g, 2_000, 0)
	id, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("first over cost error = %v", err)
	}
	assertParts(t, id, 2_001, 1, 0)

	fillSequence(t, g, 2_001, 1)
	id, err = g.Next(context.Background())
	if err != nil {
		t.Fatalf("second over cost error = %v", err)
	}
	assertParts(t, id, 2_002, 1, 0)

	fillSequence(t, g, 2_002, 1)
	id, err = g.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after over cost exhausted error = %v", err)
	}
	assertParts(t, id, 2_003, 1, 0)
	if len(clock.sleeps) != 1 || clock.sleeps[0] != 2_003 {
		t.Fatalf("sleeps = %#v, want [2003]", clock.sleeps)
	}
}

func TestGeneratorOverCostKeepsWorkingWhenWallClockIsBehind(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	g, err := NewGenerator(Config{
		NodeID:                1,
		EpochMillis:           1_000,
		OverCostCount:         5,
		AllowInMemoryOverCost: true,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	fillSequence(t, g, 2_000, 0)
	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("Next over cost error = %v", err)
	}

	clock.now = 1_995
	id, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("Next while wall clock behind error = %v", err)
	}
	assertParts(t, id, 2_001, 1, 1)
	if len(clock.sleeps) != 0 {
		t.Fatalf("sleeps = %#v, want no rollback wait", clock.sleeps)
	}
}

func TestGeneratorOverCostResetsWhenWallClockCatchesUp(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	g, err := NewGenerator(Config{
		NodeID:                1,
		EpochMillis:           1_000,
		OverCostCount:         3,
		AllowInMemoryOverCost: true,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	fillSequence(t, g, 2_000, 0)
	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("Next over cost error = %v", err)
	}
	if !g.overCostActive || g.overCostRemaining != 2 {
		t.Fatalf("over cost state before catch-up: active=%v remaining=%d", g.overCostActive, g.overCostRemaining)
	}

	clock.now = 2_001
	id, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after catch-up error = %v", err)
	}
	assertParts(t, id, 2_001, 1, 1)
	if g.overCostActive || g.overCostRemaining != 3 {
		t.Fatalf("over cost state after catch-up: active=%v remaining=%d", g.overCostActive, g.overCostRemaining)
	}

	fillSequence(t, g, 2_001, 2)
	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("Next over cost after reset error = %v", err)
	}
	if !g.overCostActive || g.overCostRemaining != 2 {
		t.Fatalf("over cost state after restart: active=%v remaining=%d", g.overCostActive, g.overCostRemaining)
	}
	if len(clock.sleeps) != 0 {
		t.Fatalf("sleeps = %#v, want no wall clock wait", clock.sleeps)
	}
}

func TestGeneratorOverCostWaitPropagatesClockFailure(t *testing.T) {
	sleepErr := errors.New("clock unavailable")
	clock := &fakeClock{now: 2_000, err: sleepErr}
	g, err := NewGenerator(Config{
		NodeID:                1,
		EpochMillis:           1_000,
		OverCostCount:         1,
		AllowInMemoryOverCost: true,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	fillSequence(t, g, 2_000, 0)
	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("Next over cost error = %v", err)
	}
	fillSequence(t, g, 2_001, 1)

	_, err = g.Next(context.Background())
	if !errors.Is(err, sleepErr) {
		t.Fatalf("Next() error = %v, want %v", err, sleepErr)
	}
	if g.lastMillis != 2_001 || g.sequence != MaxSequence {
		t.Fatalf("generator state changed after wait failure: last=%d sequence=%d", g.lastMillis, g.sequence)
	}
	if !g.overCostActive || g.overCostRemaining != 0 {
		t.Fatalf("over cost state changed after wait failure: active=%v remaining=%d", g.overCostActive, g.overCostRemaining)
	}
}

func TestGeneratorOverCostWaitRejectsStalledClock(t *testing.T) {
	clock := &stalledRollbackClock{now: 2_000}
	g, err := NewGenerator(Config{
		NodeID:                1,
		EpochMillis:           1_000,
		OverCostCount:         1,
		AllowInMemoryOverCost: true,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	fillSequence(t, g, 2_000, 0)
	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("Next over cost error = %v", err)
	}
	fillSequence(t, g, 2_001, 1)

	_, err = g.Next(context.Background())
	if !errors.Is(err, ErrClockRollback) {
		t.Fatalf("Next() error = %v, want ErrClockRollback", err)
	}
	if len(clock.sleeps) != 1 || clock.sleeps[0] != 2_002 {
		t.Fatalf("sleeps = %#v, want [2002]", clock.sleeps)
	}
}

func TestGeneratorDefaultOverflowWaitPropagatesClockFailure(t *testing.T) {
	sleepErr := errors.New("clock unavailable")
	clock := &fakeClock{now: 2_000, err: sleepErr}
	g, err := NewGenerator(Config{
		NodeID:      1,
		EpochMillis: 1_000,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	fillSequence(t, g, 2_000, 0)
	_, err = g.Next(context.Background())
	if !errors.Is(err, sleepErr) {
		t.Fatalf("Next() error = %v, want %v", err, sleepErr)
	}
	if len(clock.sleeps) != 1 || clock.sleeps[0] != 2_001 {
		t.Fatalf("sleeps = %#v, want [2001]", clock.sleeps)
	}
}

func TestGeneratorDefaultOverflowWaitRejectsStalledClock(t *testing.T) {
	clock := &stalledRollbackClock{now: 2_000}
	g, err := NewGenerator(Config{
		NodeID:      1,
		EpochMillis: 1_000,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	fillSequence(t, g, 2_000, 0)
	_, err = g.Next(context.Background())
	if !errors.Is(err, ErrClockRollback) {
		t.Fatalf("Next() error = %v, want ErrClockRollback", err)
	}
	if len(clock.sleeps) != 1 || clock.sleeps[0] != 2_001 {
		t.Fatalf("sleeps = %#v, want [2001]", clock.sleeps)
	}
}

func TestGeneratorOverCostNextWithinStopsAtFence(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	g, err := NewGenerator(Config{
		NodeID:                1,
		EpochMillis:           1_000,
		OverCostCount:         3,
		AllowInMemoryOverCost: true,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	fillSequenceWithin(t, g, 2_000, 2_002, 0)
	if _, err := g.NextWithin(context.Background(), 2_000, 2_002); err != nil {
		t.Fatalf("NextWithin over cost error = %v", err)
	}
	fillSequenceWithin(t, g, 2_001, 2_002, 1)

	_, err = g.NextWithin(context.Background(), 2_000, 2_002)
	if !errors.Is(err, ErrGenerationFenceReached) {
		t.Fatalf("NextWithin() error = %v, want ErrGenerationFenceReached", err)
	}
	if len(clock.sleeps) != 0 {
		t.Fatalf("sleeps = %#v, want no wall clock wait before fence error", clock.sleeps)
	}
	if g.lastMillis != 2_001 || g.sequence != MaxSequence {
		t.Fatalf("generator state changed after fence error: last=%d sequence=%d", g.lastMillis, g.sequence)
	}
	if !g.overCostActive || g.overCostRemaining != 2 {
		t.Fatalf("over cost state changed after fence error: active=%v remaining=%d", g.overCostActive, g.overCostRemaining)
	}
}

func TestGeneratorRejectsNegativeOverCostCount(t *testing.T) {
	_, err := NewGenerator(Config{
		NodeID:        1,
		OverCostCount: -1,
	}, nil)
	if !errors.Is(err, ErrInvalidGeneratorConfig) {
		t.Fatalf("NewGenerator() error = %v, want ErrInvalidGeneratorConfig", err)
	}
}

func TestGeneratorRejectsOverCostWithoutExplicitInMemoryOptIn(t *testing.T) {
	_, err := NewGenerator(Config{
		NodeID:        1,
		OverCostCount: 1,
	}, nil)
	if !errors.Is(err, ErrInvalidGeneratorConfig) {
		t.Fatalf("NewGenerator() error = %v, want ErrInvalidGeneratorConfig", err)
	}
}

func fillSequence(t *testing.T, g *Generator, millis int64, wantSequence int64) {
	t.Helper()
	for sequence := wantSequence; sequence <= MaxSequence; sequence++ {
		id, err := g.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		assertParts(t, id, millis, 1, sequence)
	}
}

func fillSequenceWithin(t *testing.T, g *Generator, millis int64, fenceMillis int64, wantSequence int64) {
	t.Helper()
	for sequence := wantSequence; sequence <= MaxSequence; sequence++ {
		id, err := g.NextWithin(context.Background(), 2_000, fenceMillis)
		if err != nil {
			t.Fatalf("NextWithin() error = %v", err)
		}
		assertParts(t, id, millis, 1, sequence)
	}
}

func assertParts(t *testing.T, id int64, timestampMillis int64, nodeID int, sequence int64) {
	t.Helper()
	parts, err := Decode(id, 1_000)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if parts.TimestampMillis != timestampMillis || parts.NodeID != nodeID || parts.Sequence != sequence {
		t.Fatalf("parts = %+v, want timestamp=%d node=%d sequence=%d", parts, timestampMillis, nodeID, sequence)
	}
}
