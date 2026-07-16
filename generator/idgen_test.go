package generator

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeClock struct {
	now            int64
	monotonic      int64
	sleeps         []int64
	durationSleeps []time.Duration
	err            error
}

func (c *fakeClock) NowMillis() int64 {
	return c.now
}

func (c *fakeClock) MonotonicMillis() int64 {
	return c.monotonic
}

func (c *fakeClock) SleepUntil(ctx context.Context, millis int64) error {
	c.sleeps = append(c.sleeps, millis)
	if c.err != nil {
		return c.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.now < millis {
		c.monotonic += millis - c.now
		c.now = millis
	}
	return nil
}

func (c *fakeClock) Sleep(ctx context.Context, duration time.Duration) error {
	c.durationSleeps = append(c.durationSleeps, duration)
	if c.err != nil {
		return c.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	millis := duration.Milliseconds()
	if millis > 0 {
		c.monotonic += millis
		c.now += millis
	}
	return nil
}

func TestValidateNodeID(t *testing.T) {
	for _, nodeID := range []int{1, 100, 1024} {
		if err := ValidateNodeID(nodeID); err != nil {
			t.Fatalf("ValidateNodeID(%d) error = %v", nodeID, err)
		}
	}
	for _, nodeID := range []int{0, -1, 1025} {
		if err := ValidateNodeID(nodeID); !errors.Is(err, ErrInvalidNodeID) {
			t.Fatalf("ValidateNodeID(%d) error = %v, want ErrInvalidNodeID", nodeID, err)
		}
	}
}

func TestGeneratorProducesUniqueIDsWithinSameMillisecond(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	g, err := NewGenerator(Config{NodeID: 7, EpochMillis: 1_000}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	first, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("Next first error = %v", err)
	}
	second, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("Next second error = %v", err)
	}
	if first == second {
		t.Fatalf("ids should be unique: %d", first)
	}

	firstParts, err := Decode(first, 1_000)
	if err != nil {
		t.Fatalf("Decode first error = %v", err)
	}
	secondParts, err := Decode(second, 1_000)
	if err != nil {
		t.Fatalf("Decode second error = %v", err)
	}
	if firstParts.NodeID != 7 || firstParts.TimestampMillis != 2_000 || firstParts.Sequence != 0 {
		t.Fatalf("first parts = %+v", firstParts)
	}
	if secondParts.NodeID != 7 || secondParts.TimestampMillis != 2_000 || secondParts.Sequence != 1 {
		t.Fatalf("second parts = %+v", secondParts)
	}
}

func TestGeneratorSkipsZeroIDAtEpochBoundary(t *testing.T) {
	clock := &fakeClock{now: 1_000}
	g, err := NewGenerator(Config{NodeID: 1, EpochMillis: 1_000}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	id, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if id == 0 {
		t.Fatalf("id should not be zero")
	}

	parts, err := Decode(id, 1_000)
	if err != nil {
		t.Fatalf("Decode error = %v", err)
	}
	if parts.TimestampMillis != 1_000 || parts.NodeID != 1 || parts.Sequence != 1 {
		t.Fatalf("parts = %+v, want timestamp=1000 node_id=1 sequence=1", parts)
	}
}

func TestGeneratorWaitsWhenSequenceOverflows(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	g, err := NewGenerator(Config{NodeID: 1, EpochMillis: 1_000}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	for i := int64(0); i <= MaxSequence; i++ {
		if _, err := g.Next(context.Background()); err != nil {
			t.Fatalf("Next(%d) error = %v", i, err)
		}
	}
	id, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after overflow error = %v", err)
	}
	parts, err := Decode(id, 1_000)
	if err != nil {
		t.Fatalf("Decode error = %v", err)
	}
	if parts.TimestampMillis != 2_001 || parts.Sequence != 0 {
		t.Fatalf("overflow parts = %+v", parts)
	}
	if len(clock.sleeps) != 1 || clock.sleeps[0] != 2_001 {
		t.Fatalf("sleeps = %#v, want [2001]", clock.sleeps)
	}
}

func TestGeneratorNextWithinRejectsTimestampBelowFloor(t *testing.T) {
	clock := &fakeClock{now: 1_999}
	g, err := NewGenerator(Config{NodeID: 1, EpochMillis: 1_000}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}

	_, err = g.NextWithin(context.Background(), 2_000, 2_005)
	if !errors.Is(err, ErrTimestampBelowGenerationFloor) {
		t.Fatalf("NextWithin() error = %v, want ErrTimestampBelowGenerationFloor", err)
	}
	if g.lastMillis != 0 || g.sequence != 0 {
		t.Fatalf("generator state changed after rejected timestamp: last=%d sequence=%d", g.lastMillis, g.sequence)
	}
}

func TestGeneratorNextWithinRejectsFenceReachedAfterSequenceWait(t *testing.T) {
	clock := &fakeClock{now: 2_004}
	g, err := NewGenerator(Config{NodeID: 1, EpochMillis: 1_000}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}
	g.lastMillis = 2_004
	g.sequence = MaxSequence

	_, err = g.NextWithin(context.Background(), 2_000, 2_005)
	if !errors.Is(err, ErrGenerationFenceReached) {
		t.Fatalf("NextWithin() error = %v, want ErrGenerationFenceReached", err)
	}
	if len(clock.sleeps) != 1 || clock.sleeps[0] != 2_005 {
		t.Fatalf("sleeps = %#v, want [2005]", clock.sleeps)
	}
	if g.lastMillis != 2_004 || g.sequence != MaxSequence {
		t.Fatalf("generator state changed after rejected timestamp: last=%d sequence=%d", g.lastMillis, g.sequence)
	}
}

func TestGeneratorWaitsForSmallClockRollback(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	g, err := NewGenerator(Config{
		NodeID:            1,
		EpochMillis:       1_000,
		SmallRollbackWait: 10 * time.Millisecond,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}
	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("Next first error = %v", err)
	}

	clock.now = 1_995
	id, err := g.Next(context.Background())
	if err != nil {
		t.Fatalf("Next rollback error = %v", err)
	}
	parts, err := Decode(id, 1_000)
	if err != nil {
		t.Fatalf("Decode error = %v", err)
	}
	if parts.TimestampMillis != 2_000 || parts.Sequence != 1 {
		t.Fatalf("parts after rollback = %+v", parts)
	}
	if len(clock.sleeps) != 1 || clock.sleeps[0] != 2_000 {
		t.Fatalf("sleeps = %#v, want [2000]", clock.sleeps)
	}
}

func TestGeneratorRejectsLargeClockRollback(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	g, err := NewGenerator(Config{
		NodeID:            1,
		EpochMillis:       1_000,
		SmallRollbackWait: time.Millisecond,
	}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}
	if _, err := g.Next(context.Background()); err != nil {
		t.Fatalf("Next first error = %v", err)
	}

	clock.now = 1_990
	_, err = g.Next(context.Background())
	if !errors.Is(err, ErrClockRollback) {
		t.Fatalf("Next rollback error = %v, want ErrClockRollback", err)
	}
}

func TestComposeRejectsZeroID(t *testing.T) {
	_, err := Compose(1_000, 1_000, 0, 0)
	if !errors.Is(err, ErrZeroID) {
		t.Fatalf("Compose error = %v, want ErrZeroID", err)
	}
}

func TestGeneratorRejectsTimestampBeforeEpoch(t *testing.T) {
	clock := &fakeClock{now: 999}
	g, err := NewGenerator(Config{NodeID: 1, EpochMillis: 1_000}, clock)
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}
	_, err = g.Next(context.Background())
	if !errors.Is(err, ErrTimestampBeforeEpoch) {
		t.Fatalf("Next error = %v, want ErrTimestampBeforeEpoch", err)
	}
}
