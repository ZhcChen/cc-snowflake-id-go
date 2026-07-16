package generator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

const (
	// NodeBits 表示节点编号占用的 bit 数。
	NodeBits = 10
	// SequenceBits 表示同一毫秒内序列号占用的 bit 数。
	SequenceBits = 12

	// MinNodeID 是允许配置的最小节点编号。
	MinNodeID = 1
	// MaxNodeID 是允许配置的最大节点编号。
	MaxNodeID = 1 << NodeBits

	// MaxSequence 是单毫秒内允许生成的最大序列号。
	MaxSequence = (1 << SequenceBits) - 1

	// DefaultEpochMillis 是 2026-01-01T00:00:00Z 对应的 Unix 毫秒时间戳。
	DefaultEpochMillis int64 = 1767225600000
)

const (
	timestampBits = 41
	nodeShift     = SequenceBits
	timeShift     = NodeBits + SequenceBits
	maxTimestamp  = (int64(1) << timestampBits) - 1
)

var (
	// ErrInvalidNodeID 表示节点编号超出允许范围。
	ErrInvalidNodeID = errors.New("idgen: invalid node_id")
	// ErrClockRollback 表示当前时钟落后于已发号时间。
	ErrClockRollback = errors.New("idgen: clock moved backwards")
	// ErrTimestampBeforeEpoch 表示发号时间早于纪元起点。
	ErrTimestampBeforeEpoch = errors.New("idgen: timestamp is before epoch")
	// ErrTimestampOverflow 表示时间戳超出雪花 ID 可编码范围。
	ErrTimestampOverflow = errors.New("idgen: timestamp exceeds snowflake range")
	// ErrTimestampInFuture 表示解码后的时间戳超出允许的未来窗口。
	ErrTimestampInFuture = errors.New("idgen: timestamp is beyond the allowed future lead")
	// ErrTimestampBelowGenerationFloor 表示时间戳落在当前允许发号下界之前。
	ErrTimestampBelowGenerationFloor = errors.New("idgen: timestamp is below generation floor")
	// ErrGenerationFenceReached 表示时间戳已经触达当前允许发号上界。
	ErrGenerationFenceReached = errors.New("idgen: generation fence reached")
	// ErrInvalidGenerationRange 表示 floor 或 fence 参数非法。
	ErrInvalidGenerationRange = errors.New("idgen: invalid generation range")
	// ErrZeroID 表示本次组合出的雪花 ID 为保留值 0。
	ErrZeroID = errors.New("idgen: generated id is zero")
	// ErrGeneratorMissing 表示调用方要求补发 ID，但没有可用生成器。
	ErrGeneratorMissing = errors.New("idgen: generator is not configured")
)

// NextGenerator 定义最小可用的发号接口。
type NextGenerator interface {
	// Next 生成一个新的雪花 ID。
	Next(context.Context) (int64, error)
}

// Clock 抽象发号与租约逻辑使用的时钟行为。
type Clock interface {
	// NowMillis 返回当前 wall clock 的 Unix 毫秒值。
	NowMillis() int64
	// MonotonicMillis 返回单调时钟的相对毫秒值。
	MonotonicMillis() int64
	// SleepUntil 阻塞到目标 wall clock 毫秒时间点。
	SleepUntil(ctx context.Context, millis int64) error
	// Sleep 按给定时长阻塞。
	Sleep(ctx context.Context, duration time.Duration) error
}

// SystemClock 基于系统 wall clock 和 monotonic clock 提供默认实现。
type SystemClock struct{}

var systemMonotonicOrigin = time.Now()

// NowMillis 返回当前 wall clock 的 Unix 毫秒时间。
func (SystemClock) NowMillis() int64 {
	return time.Now().UnixMilli()
}

// MonotonicMillis 返回进程内 monotonic clock 的相对毫秒值。
func (SystemClock) MonotonicMillis() int64 {
	return time.Since(systemMonotonicOrigin).Milliseconds()
}

// SleepUntil 阻塞到目标毫秒时间，或在上下文取消时提前返回。
func (SystemClock) SleepUntil(ctx context.Context, millis int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	delay := time.Until(time.UnixMilli(millis))
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Sleep 按给定时长休眠，或在上下文取消时提前返回。
func (SystemClock) Sleep(ctx context.Context, duration time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Config 描述进程内雪花生成器的基础配置。
type Config struct {
	// NodeID 是当前进程使用的逻辑节点编号。
	NodeID int
	// EpochMillis 是雪花 ID 使用的纪元起点；为 0 时使用 DefaultEpochMillis。
	EpochMillis int64
	// SmallRollbackWait 定义可容忍的小幅时钟回退等待时间；为 0 时使用默认值。
	SmallRollbackWait time.Duration
}

// Generator 是单进程内使用的雪花 ID 生成器。
type Generator struct {
	mu                sync.Mutex
	clock             Clock
	nodeID            int
	nodeCode          int64
	epochMillis       int64
	smallRollbackWait time.Duration
	lastMillis        int64
	sequence          int64
}

// Parts 表示一个雪花 ID 解码后的组成部分。
type Parts struct {
	// TimestampMillis 是解码后的绝对毫秒时间戳。
	TimestampMillis int64
	// NodeID 是解码后的节点编号。
	NodeID int
	// Sequence 是解码后的同毫秒序列号。
	Sequence int64
}

// NewGenerator 根据配置创建一个进程内雪花生成器。
func NewGenerator(cfg Config, clock Clock) (*Generator, error) {
	nodeCode, err := NodeCode(cfg.NodeID)
	if err != nil {
		return nil, err
	}
	epochMillis := cfg.EpochMillis
	if epochMillis == 0 {
		epochMillis = DefaultEpochMillis
	}
	if cfg.SmallRollbackWait < 0 {
		return nil, fmt.Errorf("idgen: small rollback wait must be non-negative")
	}
	smallRollbackWait := cfg.SmallRollbackWait
	if smallRollbackWait == 0 {
		smallRollbackWait = 10 * time.Millisecond
	}
	if clock == nil {
		clock = SystemClock{}
	}
	return &Generator{
		clock:             clock,
		nodeID:            cfg.NodeID,
		nodeCode:          nodeCode,
		epochMillis:       epochMillis,
		smallRollbackWait: smallRollbackWait,
	}, nil
}

// ValidateNodeID 校验节点编号是否落在允许范围内。
func ValidateNodeID(nodeID int) error {
	if nodeID < MinNodeID || nodeID > MaxNodeID {
		return fmt.Errorf("%w: node_id must be between %d and %d", ErrInvalidNodeID, MinNodeID, MaxNodeID)
	}
	return nil
}

// NodeCode 把对外节点编号转换成雪花编码中使用的零基节点码。
func NodeCode(nodeID int) (int64, error) {
	if err := ValidateNodeID(nodeID); err != nil {
		return 0, err
	}
	return int64(nodeID - 1), nil
}

// NodeID 返回生成器绑定的节点编号。
func (g *Generator) NodeID() int {
	if g == nil {
		return 0
	}
	return g.nodeID
}

// Next 生成一个新的雪花 ID。
func (g *Generator) Next(ctx context.Context) (int64, error) {
	return g.next(ctx, 0, 0)
}

// NextWithin 生成一个时间戳落在 [floorMillis, fenceMillis) 内的雪花 ID。
func (g *Generator) NextWithin(ctx context.Context, floorMillis int64, fenceMillis int64) (int64, error) {
	if floorMillis <= 0 || fenceMillis <= floorMillis {
		return 0, fmt.Errorf("%w: floor_ms=%d fence_ms=%d", ErrInvalidGenerationRange, floorMillis, fenceMillis)
	}
	return g.next(ctx, floorMillis, fenceMillis)
}

func (g *Generator) next(ctx context.Context, floorMillis int64, fenceMillis int64) (int64, error) {
	if g == nil {
		return 0, errors.New("idgen: nil generator")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.clock.NowMillis()
	if err := g.waitForRollback(ctx, &now); err != nil {
		return 0, err
	}
	if now < g.epochMillis {
		return 0, fmt.Errorf("%w: now=%d epoch=%d", ErrTimestampBeforeEpoch, now, g.epochMillis)
	}

	lastMillis := g.lastMillis
	sequence := g.sequence
	switch {
	case now == g.lastMillis:
		if g.sequence >= MaxSequence {
			nextMillis := g.lastMillis + 1
			if err := g.clock.SleepUntil(ctx, nextMillis); err != nil {
				return 0, err
			}
			now = g.clock.NowMillis()
			if now <= g.lastMillis {
				return 0, fmt.Errorf("%w: now=%d last=%d", ErrClockRollback, now, g.lastMillis)
			}
			lastMillis = now
			sequence = 0
		} else {
			sequence++
		}
	case now > g.lastMillis:
		lastMillis = now
		sequence = 0
	default:
		return 0, fmt.Errorf("%w: now=%d last=%d", ErrClockRollback, now, g.lastMillis)
	}
	if lastMillis == g.epochMillis && g.nodeCode == 0 && sequence == 0 {
		sequence = 1
	}
	if floorMillis > 0 && lastMillis < floorMillis {
		return 0, fmt.Errorf("%w: timestamp_ms=%d floor_ms=%d", ErrTimestampBelowGenerationFloor, lastMillis, floorMillis)
	}
	if fenceMillis > 0 && lastMillis >= fenceMillis {
		return 0, fmt.Errorf("%w: timestamp_ms=%d fence_ms=%d", ErrGenerationFenceReached, lastMillis, fenceMillis)
	}

	id, err := Compose(lastMillis, g.epochMillis, g.nodeCode, sequence)
	if err != nil {
		return 0, err
	}
	g.lastMillis = lastMillis
	g.sequence = sequence
	return id, nil
}

// EnsureID 在 current 已有值时直接返回；否则通过生成器补发一个 ID。
func EnsureID(ctx context.Context, generator NextGenerator, current int64) (int64, error) {
	if current != 0 {
		return current, nil
	}
	if generator == nil {
		return 0, ErrGeneratorMissing
	}
	return generator.Next(ctx)
}

func (g *Generator) waitForRollback(ctx context.Context, now *int64) error {
	if g.lastMillis == 0 || *now >= g.lastMillis {
		return nil
	}
	rollback := time.Duration(g.lastMillis-*now) * time.Millisecond
	if rollback > g.smallRollbackWait {
		return fmt.Errorf("%w: now=%d last=%d rollback=%s", ErrClockRollback, *now, g.lastMillis, rollback)
	}
	if err := g.clock.SleepUntil(ctx, g.lastMillis); err != nil {
		return err
	}
	*now = g.clock.NowMillis()
	if *now < g.lastMillis {
		return fmt.Errorf("%w: now=%d last=%d", ErrClockRollback, *now, g.lastMillis)
	}
	return nil
}

// Compose 按时间戳、节点码和序列号组合出一个雪花 ID。
func Compose(timestampMillis int64, epochMillis int64, nodeCode int64, sequence int64) (int64, error) {
	if epochMillis == 0 {
		epochMillis = DefaultEpochMillis
	}
	if timestampMillis < epochMillis {
		return 0, fmt.Errorf("%w: timestamp=%d epoch=%d", ErrTimestampBeforeEpoch, timestampMillis, epochMillis)
	}
	if nodeCode < 0 || nodeCode >= int64(MaxNodeID) {
		return 0, fmt.Errorf("%w: node_code=%d", ErrInvalidNodeID, nodeCode)
	}
	if sequence < 0 || sequence > MaxSequence {
		return 0, fmt.Errorf("idgen: sequence out of range: %d", sequence)
	}
	relativeMillis := timestampMillis - epochMillis
	if relativeMillis > maxTimestamp {
		return 0, fmt.Errorf("%w: relative_millis=%d", ErrTimestampOverflow, relativeMillis)
	}
	id := (relativeMillis << timeShift) | (nodeCode << nodeShift) | sequence
	if id == 0 {
		return 0, fmt.Errorf("%w: timestamp=%d epoch=%d node_code=%d sequence=%d", ErrZeroID, timestampMillis, epochMillis, nodeCode, sequence)
	}
	return id, nil
}

// Decode 把雪花 ID 还原为时间戳、节点编号和序列号。
func Decode(id int64, epochMillis int64) (Parts, error) {
	if id <= 0 {
		return Parts{}, fmt.Errorf("idgen: id must be positive")
	}
	if epochMillis == 0 {
		epochMillis = DefaultEpochMillis
	}
	relativeMillis := id >> timeShift
	if relativeMillis > maxTimestamp || epochMillis > math.MaxInt64-relativeMillis {
		return Parts{}, fmt.Errorf("%w: relative_millis=%d epoch=%d", ErrTimestampOverflow, relativeMillis, epochMillis)
	}
	nodeCode := (id >> nodeShift) & int64(MaxNodeID-1)
	sequence := id & int64(MaxSequence)
	return Parts{
		TimestampMillis: relativeMillis + epochMillis,
		NodeID:          int(nodeCode) + 1,
		Sequence:        sequence,
	}, nil
}

// DecodeStrict 解码雪花 ID，并拒绝明显超前于调用方时钟的未来时间戳。
func DecodeStrict(id int64, epochMillis int64, nowMillis int64, maxFutureLead time.Duration) (Parts, error) {
	if nowMillis <= 0 {
		return Parts{}, fmt.Errorf("%w: now_ms must be positive", ErrInvalidGenerationRange)
	}
	if maxFutureLead < 0 {
		return Parts{}, fmt.Errorf("%w: max_future_lead must be non-negative", ErrInvalidGenerationRange)
	}
	parts, err := Decode(id, epochMillis)
	if err != nil {
		return Parts{}, err
	}
	maxFutureLeadMillis := maxFutureLead.Milliseconds()
	if nowMillis > math.MaxInt64-maxFutureLeadMillis {
		return Parts{}, fmt.Errorf("%w: now_ms=%d max_future_lead_ms=%d", ErrTimestampOverflow, nowMillis, maxFutureLeadMillis)
	}
	latestAllowedMillis := nowMillis + maxFutureLeadMillis
	if parts.TimestampMillis > latestAllowedMillis {
		return Parts{}, fmt.Errorf(
			"%w: timestamp_ms=%d now_ms=%d max_future_lead_ms=%d",
			ErrTimestampInFuture,
			parts.TimestampMillis,
			nowMillis,
			maxFutureLeadMillis,
		)
	}
	return parts, nil
}
