package generator

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestSystemClockUsesWallAndMonotonicTimeAndHonorsCancellation(t *testing.T) {
	clock := SystemClock{}
	before := time.Now().UnixMilli()
	now := clock.NowMillis()
	after := time.Now().UnixMilli()
	if now < before || now > after {
		t.Fatalf("NowMillis() = %d, want between %d and %d", now, before, after)
	}
	firstMonotonic := clock.MonotonicMillis()
	if err := clock.Sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("Sleep() error = %v", err)
	}
	if secondMonotonic := clock.MonotonicMillis(); secondMonotonic < firstMonotonic {
		t.Fatalf("MonotonicMillis() moved backwards: first=%d second=%d", firstMonotonic, secondMonotonic)
	}
	if err := clock.Sleep(context.Background(), 0); err != nil {
		t.Fatalf("Sleep(0) error = %v", err)
	}
	if err := clock.SleepUntil(context.Background(), time.Now().Add(-time.Millisecond).UnixMilli()); err != nil {
		t.Fatalf("SleepUntil(past) error = %v", err)
	}
	if err := clock.SleepUntil(context.Background(), time.Now().Add(2*time.Millisecond).UnixMilli()); err != nil {
		t.Fatalf("SleepUntil(future) error = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := clock.Sleep(canceled, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep(canceled) error = %v, want context.Canceled", err)
	}
	if err := clock.SleepUntil(canceled, time.Now().Add(time.Second).UnixMilli()); !errors.Is(err, context.Canceled) {
		t.Fatalf("SleepUntil(canceled) error = %v, want context.Canceled", err)
	}
}

func TestGeneratorHelpersValidateRangesAndPreserveExplicitIDs(t *testing.T) {
	if _, err := NewGenerator(Config{NodeID: 1, SmallRollbackWait: -time.Millisecond}, nil); !errors.Is(err, ErrInvalidGeneratorConfig) {
		t.Fatalf("NewGenerator() error = %v, want ErrInvalidGeneratorConfig", err)
	}
	generator, err := NewGenerator(Config{NodeID: 7, EpochMillis: 1_000}, &fakeClock{now: 2_000})
	if err != nil {
		t.Fatalf("NewGenerator() error = %v", err)
	}
	if generator.NodeID() != 7 {
		t.Fatalf("NodeID() = %d, want 7", generator.NodeID())
	}
	var nilGenerator *Generator
	if nilGenerator.NodeID() != 0 {
		t.Fatalf("nil NodeID() = %d, want 0", nilGenerator.NodeID())
	}
	if _, err := NodeCode(0); !errors.Is(err, ErrInvalidNodeID) {
		t.Fatalf("NodeCode(0) error = %v, want ErrInvalidNodeID", err)
	}
	if nodeCode, err := NodeCode(7); err != nil || nodeCode != 6 {
		t.Fatalf("NodeCode(7) = %d, %v; want 6, nil", nodeCode, err)
	}
	for _, input := range []struct {
		floor int64
		fence int64
	}{{0, 2_000}, {2_000, 2_000}, {2_001, 2_000}} {
		if _, err := generator.NextWithin(context.Background(), input.floor, input.fence); !errors.Is(err, ErrInvalidGenerationRange) {
			t.Fatalf("NextWithin(%d, %d) error = %v, want ErrInvalidGenerationRange", input.floor, input.fence, err)
		}
	}

	const existingID = int64(42)
	if id, err := EnsureID(context.Background(), nil, existingID); err != nil || id != existingID {
		t.Fatalf("EnsureID(existing) = %d, %v; want %d, nil", id, err, existingID)
	}
	if _, err := EnsureID(context.Background(), nil, 0); !errors.Is(err, ErrGeneratorMissing) {
		t.Fatalf("EnsureID(nil) error = %v, want ErrGeneratorMissing", err)
	}
	generatedID, err := EnsureID(context.Background(), nextGeneratorFunc(func(context.Context) (int64, error) {
		return 99, nil
	}), 0)
	if err != nil || generatedID != 99 {
		t.Fatalf("EnsureID(generator) = %d, %v; want 99, nil", generatedID, err)
	}

	if _, err := nilGenerator.Next(context.Background()); !errors.Is(err, ErrNilGenerator) {
		t.Fatalf("nil Next() error = %v, want ErrNilGenerator", err)
	}
}

func TestComposeAndDecodeRejectInvalidSnowflakeParts(t *testing.T) {
	tests := []struct {
		name      string
		timestamp int64
		epoch     int64
		nodeCode  int64
		sequence  int64
		wantErr   error
	}{
		{name: "timestamp before epoch", timestamp: 999, epoch: 1_000, wantErr: ErrTimestampBeforeEpoch},
		{name: "negative node", timestamp: 1_001, epoch: 1_000, nodeCode: -1, wantErr: ErrInvalidNodeID},
		{name: "node overflow", timestamp: 1_001, epoch: 1_000, nodeCode: MaxNodeID, wantErr: ErrInvalidNodeID},
		{name: "negative sequence", timestamp: 1_001, epoch: 1_000, sequence: -1, wantErr: ErrInvalidSequence},
		{name: "sequence overflow", timestamp: 1_001, epoch: 1_000, sequence: MaxSequence + 1, wantErr: ErrInvalidSequence},
		{name: "timestamp overflow", timestamp: 1_000 + maxTimestamp + 1, epoch: 1_000, wantErr: ErrTimestampOverflow},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Compose(test.timestamp, test.epoch, test.nodeCode, test.sequence)
			if err == nil {
				t.Fatal("Compose() succeeded, want error")
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("Compose() error = %v, want %v", err, test.wantErr)
			}
		})
	}
	if _, err := Decode(0, 0); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("Decode(0) error = %v, want ErrInvalidID", err)
	}
	if _, err := Decode(1<<timeShift, math.MaxInt64); !errors.Is(err, ErrTimestampOverflow) {
		t.Fatalf("Decode(timestamp overflow) error = %v, want ErrTimestampOverflow", err)
	}
	id, err := Compose(DefaultEpochMillis+1, 0, 2, 3)
	if err != nil {
		t.Fatalf("Compose(default epoch) error = %v", err)
	}
	parts, err := Decode(id, 0)
	if err != nil || parts.TimestampMillis != DefaultEpochMillis+1 || parts.NodeID != 3 || parts.Sequence != 3 {
		t.Fatalf("Decode(default epoch) = %+v, %v", parts, err)
	}
}

func TestDecodeStrictRejectsUnboundedFutureIDs(t *testing.T) {
	id, err := Compose(2_500, 1_000, 2, 3)
	if err != nil {
		t.Fatalf("Compose() error = %v", err)
	}
	if _, err := DecodeStrict(id, 1_000, 2_000, 499*time.Millisecond); !errors.Is(err, ErrTimestampInFuture) {
		t.Fatalf("DecodeStrict(future) error = %v, want ErrTimestampInFuture", err)
	}
	parts, err := DecodeStrict(id, 1_000, 2_000, 500*time.Millisecond)
	if err != nil || parts.TimestampMillis != 2_500 || parts.NodeID != 3 || parts.Sequence != 3 {
		t.Fatalf("DecodeStrict(within lead) = %+v, %v", parts, err)
	}
	if _, err := DecodeStrict(id, 1_000, 0, time.Second); !errors.Is(err, ErrInvalidGenerationRange) {
		t.Fatalf("DecodeStrict(non-positive now) error = %v, want ErrInvalidGenerationRange", err)
	}
	if _, err := DecodeStrict(id, 1_000, 2_000, -time.Millisecond); !errors.Is(err, ErrInvalidGenerationRange) {
		t.Fatalf("DecodeStrict(negative lead) error = %v, want ErrInvalidGenerationRange", err)
	}
}

type nextGeneratorFunc func(context.Context) (int64, error)

func (f nextGeneratorFunc) Next(ctx context.Context) (int64, error) {
	return f(ctx)
}
