package lease

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	sf "github.com/ZhcChen/cc-snowflake-id-go/generator"
)

const (
	// DefaultStatusReportInterval 是周期状态上报的默认间隔。
	DefaultStatusReportInterval = 30 * time.Second
	// DefaultTelemetryEventBuffer 是 telemetry 事件通道的默认缓冲区大小。
	DefaultTelemetryEventBuffer = 16
)

// Lifecycle 表示租约生成器的生命周期状态。
type Lifecycle string

const (
	// LifecycleNew 表示对象已创建但尚未 acquire。
	LifecycleNew Lifecycle = "new"
	// LifecycleActive 表示当前处于可发号状态。
	LifecycleActive Lifecycle = "active"
	// LifecycleFailed 表示对象进入终止失败状态。
	LifecycleFailed Lifecycle = "failed"
	// LifecycleClosed 表示对象已关闭。
	LifecycleClosed Lifecycle = "closed"
)

// ErrorClass 表示对外暴露的低基数错误分类。
type ErrorClass string

const (
	// ErrorClassNone 表示没有错误。
	ErrorClassNone ErrorClass = "none"
	// ErrorClassClockRollback 表示本地时间或发号下界相关的时钟回退问题。
	ErrorClassClockRollback ErrorClass = "clock_rollback"
	// ErrorClassClockSkew 表示应用与数据库时钟偏差过大。
	ErrorClassClockSkew ErrorClass = "clock_skew"
	// ErrorClassFenceAhead 表示 generation fence 相关失败。
	ErrorClassFenceAhead ErrorClass = "fence_ahead"
	// ErrorClassLeaseBusy 表示租约被占用或重复 acquire。
	ErrorClassLeaseBusy ErrorClass = "lease_busy"
	// ErrorClassLeaseLost 表示租约丢失或未获取。
	ErrorClassLeaseLost ErrorClass = "lease_lost"
	// ErrorClassClosed 表示对象已关闭。
	ErrorClassClosed ErrorClass = "closed"
	// ErrorClassCanceled 表示上下文取消或超时。
	ErrorClassCanceled ErrorClass = "canceled"
	// ErrorClassStoreFailure 表示底层租约存储失败。
	ErrorClassStoreFailure ErrorClass = "store_failure"
	// ErrorClassUnknown 表示无法归类到稳定维度的错误。
	ErrorClassUnknown ErrorClass = "unknown"
)

// EventKind 表示 telemetry 事件类型。
type EventKind string

const (
	// EventAcquired 表示成功获取租约。
	EventAcquired EventKind = "acquired"
	// EventFailed 表示进入失败状态。
	EventFailed EventKind = "failed"
	// EventClosed 表示成功关闭。
	EventClosed EventKind = "closed"
	// EventCloseFailed 表示关闭过程中出现失败。
	EventCloseFailed EventKind = "close_failed"
)

// Event 表示一次离散的租约或运行时事件。
type Event struct {
	// Kind 表示事件类型。
	Kind EventKind
	// NodeID 表示事件对应的节点编号。
	NodeID int
	// OwnerID 表示事件对应的 owner 标识。
	OwnerID string
	// ErrorClass 表示该事件归类后的低基数错误类别。
	ErrorClass ErrorClass
	// Err 保留原始错误，便于调用方做更细粒度诊断。
	Err error
}

// Observer 定义租约发号器向外报告统计与事件的接口。
type Observer interface {
	// ObserveGenerated 记录一次成功发号。
	ObserveGenerated()
	// ObserveAcquired 记录一次成功获取租约。
	ObserveAcquired(LeaseState)
	// ObserveRefresh 记录一次租约刷新结果。
	ObserveRefresh(success bool)
	// ObserveFailed 记录一次失败状态迁移。
	ObserveFailed(LeaseState, error)
	// ObserveClosed 记录一次关闭结果。
	ObserveClosed(LeaseState, error)
}

// FailureCounts 汇总不同错误类别的累计次数。
type FailureCounts struct {
	// ClockRollback 是时钟回退类失败累计次数。
	ClockRollback uint64
	// ClockSkew 是时钟偏差类失败累计次数。
	ClockSkew uint64
	// FenceAhead 是 generation fence 类失败累计次数。
	FenceAhead uint64
	// LeaseBusy 是租约占用类失败累计次数。
	LeaseBusy uint64
	// LeaseLost 是租约丢失类失败累计次数。
	LeaseLost uint64
	// Closed 是关闭后调用类失败累计次数。
	Closed uint64
	// Canceled 是上下文取消或超时类失败累计次数。
	Canceled uint64
	// StoreFailure 是底层存储失败累计次数。
	StoreFailure uint64
	// Unknown 是无法归类失败累计次数。
	Unknown uint64
}

// Snapshot 表示租约生成器某一时刻的状态快照。
type Snapshot struct {
	// CapturedAtMillis 是抓取快照时的本地毫秒时间。
	CapturedAtMillis int64
	// NodeID 是快照对应的节点编号。
	NodeID int
	// OwnerID 是快照对应的 owner 标识。
	OwnerID string
	// Lifecycle 是当前生命周期状态。
	Lifecycle Lifecycle
	// Ready 表示当前是否通过发号前安全检查。
	Ready bool
	// ReadinessErrorClass 是最近一次 readiness 失败的稳定分类。
	ReadinessErrorClass ErrorClass
	// LeaseOwned 表示当前是否仍视为持有租约。
	LeaseOwned bool
	// LeaseRemainingMillis 是基于 monotonic clock 计算出的剩余租约时长。
	LeaseRemainingMillis int64
	// GenerationFloorMillis 是当前允许发号的时间下界。
	GenerationFloorMillis int64
	// GenerationFenceMillis 是当前允许发号的时间上界。
	GenerationFenceMillis int64
	// FenceLeadMillis 是上界相对当前时间的领先量。
	FenceLeadMillis int64
	// GeneratedTotal 是累计成功发号次数。
	GeneratedTotal uint64
	// AcquireSuccessTotal 是累计成功 acquire 次数。
	AcquireSuccessTotal uint64
	// RefreshSuccessTotal 是累计成功 refresh 次数。
	RefreshSuccessTotal uint64
	// RefreshFailureTotal 是累计 refresh 失败次数。
	RefreshFailureTotal uint64
	// FailureTotal 是累计失败次数。
	FailureTotal uint64
	// CloseTotal 是累计关闭次数。
	CloseTotal uint64
	// DroppedEventsTotal 是因缓冲区满而丢弃的事件数。
	DroppedEventsTotal uint64
	// LastErrorClass 是最近一次失败归类。
	LastErrorClass ErrorClass
	// Failures 是按类别聚合的失败计数。
	Failures FailureCounts
}

// StatusReport 表示带窗口增量统计的周期状态报告。
type StatusReport struct {
	Snapshot
	// WindowMillis 是本次增量统计窗口大小。
	WindowMillis int64
	// GeneratedDelta 是窗口内新增的成功发号次数。
	GeneratedDelta uint64
	// GenerationRatePerSecond 是窗口内平均发号速率。
	GenerationRatePerSecond float64
}

// TelemetryConfig 描述 Telemetry 的可调参数。
type TelemetryConfig struct {
	// EventBufferSize 是事件通道缓冲区大小。
	EventBufferSize int
}

// SnapshotSource 定义可被 reporter 轮询的状态来源。
type SnapshotSource interface {
	// Snapshot 返回当前状态快照。
	Snapshot() Snapshot
}

// Telemetry 负责聚合发号统计、失败分类和离散事件。
type Telemetry struct {
	generatedTotal      atomic.Uint64
	acquireSuccessTotal atomic.Uint64
	refreshSuccessTotal atomic.Uint64
	refreshFailureTotal atomic.Uint64
	failureTotal        atomic.Uint64
	closeTotal          atomic.Uint64
	droppedEventsTotal  atomic.Uint64
	lastErrorClass      atomic.Value

	clockRollbackFailures atomic.Uint64
	clockSkewFailures     atomic.Uint64
	fenceAheadFailures    atomic.Uint64
	leaseBusyFailures     atomic.Uint64
	leaseLostFailures     atomic.Uint64
	closedFailures        atomic.Uint64
	canceledFailures      atomic.Uint64
	storeFailures         atomic.Uint64
	unknownFailures       atomic.Uint64

	events chan Event
}

// NewTelemetry 用默认配置创建一个 Telemetry。
func NewTelemetry() *Telemetry {
	return NewTelemetryWithConfig(TelemetryConfig{})
}

// NewTelemetryWithConfig 用给定配置创建一个 Telemetry。
func NewTelemetryWithConfig(cfg TelemetryConfig) *Telemetry {
	eventBufferSize := cfg.EventBufferSize
	if eventBufferSize <= 0 {
		eventBufferSize = DefaultTelemetryEventBuffer
	}
	return &Telemetry{events: make(chan Event, eventBufferSize)}
}

// Events 返回 telemetry 的事件流。
func (t *Telemetry) Events() <-chan Event {
	if t == nil {
		return nil
	}
	return t.events
}

// ObserveGenerated 记录一次成功发号。
func (t *Telemetry) ObserveGenerated() {
	if t != nil {
		t.generatedTotal.Add(1)
	}
}

// ObserveAcquired 记录一次成功 acquire。
func (t *Telemetry) ObserveAcquired(state LeaseState) {
	if t == nil {
		return
	}
	t.acquireSuccessTotal.Add(1)
	t.emit(Event{
		Kind:       EventAcquired,
		NodeID:     state.NodeID,
		OwnerID:    state.OwnerID,
		ErrorClass: ErrorClassNone,
	})
}

// ObserveRefresh 记录一次 refresh 成功或失败。
func (t *Telemetry) ObserveRefresh(success bool) {
	if t == nil {
		return
	}
	if success {
		t.refreshSuccessTotal.Add(1)
		return
	}
	t.refreshFailureTotal.Add(1)
}

// ObserveFailed 记录一次失败事件并累计错误分类。
func (t *Telemetry) ObserveFailed(state LeaseState, err error) {
	if t == nil {
		return
	}
	errorClass := ClassifyError(err)
	t.recordFailure(errorClass)
	t.emit(Event{
		Kind:       EventFailed,
		NodeID:     state.NodeID,
		OwnerID:    state.OwnerID,
		ErrorClass: errorClass,
		Err:        err,
	})
}

// ObserveClosed 记录一次关闭结果。
func (t *Telemetry) ObserveClosed(state LeaseState, err error) {
	if t == nil {
		return
	}
	t.closeTotal.Add(1)
	if err == nil {
		t.emit(Event{
			Kind:       EventClosed,
			NodeID:     state.NodeID,
			OwnerID:    state.OwnerID,
			ErrorClass: ErrorClassClosed,
		})
		return
	}
	errorClass := ClassifyError(err)
	t.recordFailure(errorClass)
	t.emit(Event{
		Kind:       EventCloseFailed,
		NodeID:     state.NodeID,
		OwnerID:    state.OwnerID,
		ErrorClass: errorClass,
		Err:        err,
	})
}

func (t *Telemetry) counterSnapshot() telemetryCounters {
	if t == nil {
		return telemetryCounters{lastErrorClass: ErrorClassNone}
	}
	return telemetryCounters{
		generatedTotal:      t.generatedTotal.Load(),
		acquireSuccessTotal: t.acquireSuccessTotal.Load(),
		refreshSuccessTotal: t.refreshSuccessTotal.Load(),
		refreshFailureTotal: t.refreshFailureTotal.Load(),
		failureTotal:        t.failureTotal.Load(),
		closeTotal:          t.closeTotal.Load(),
		droppedEventsTotal:  t.droppedEventsTotal.Load(),
		lastErrorClass:      t.loadLastErrorClass(),
		failures: FailureCounts{
			ClockRollback: t.clockRollbackFailures.Load(),
			ClockSkew:     t.clockSkewFailures.Load(),
			FenceAhead:    t.fenceAheadFailures.Load(),
			LeaseBusy:     t.leaseBusyFailures.Load(),
			LeaseLost:     t.leaseLostFailures.Load(),
			Closed:        t.closedFailures.Load(),
			Canceled:      t.canceledFailures.Load(),
			StoreFailure:  t.storeFailures.Load(),
			Unknown:       t.unknownFailures.Load(),
		},
	}
}

func (t *Telemetry) recordFailure(errorClass ErrorClass) {
	t.failureTotal.Add(1)
	t.lastErrorClass.Store(errorClass)
	switch errorClass {
	case ErrorClassClockRollback:
		t.clockRollbackFailures.Add(1)
	case ErrorClassClockSkew:
		t.clockSkewFailures.Add(1)
	case ErrorClassFenceAhead:
		t.fenceAheadFailures.Add(1)
	case ErrorClassLeaseBusy:
		t.leaseBusyFailures.Add(1)
	case ErrorClassLeaseLost:
		t.leaseLostFailures.Add(1)
	case ErrorClassClosed:
		t.closedFailures.Add(1)
	case ErrorClassCanceled:
		t.canceledFailures.Add(1)
	case ErrorClassStoreFailure:
		t.storeFailures.Add(1)
	default:
		t.unknownFailures.Add(1)
	}
}

func (t *Telemetry) loadLastErrorClass() ErrorClass {
	value := t.lastErrorClass.Load()
	if value == nil {
		return ErrorClassNone
	}
	return value.(ErrorClass)
}

func (t *Telemetry) emit(event Event) {
	if t.events == nil {
		return
	}
	select {
	case t.events <- event:
	default:
		t.droppedEventsTotal.Add(1)
	}
}

type telemetryCounters struct {
	generatedTotal      uint64
	acquireSuccessTotal uint64
	refreshSuccessTotal uint64
	refreshFailureTotal uint64
	failureTotal        uint64
	closeTotal          uint64
	droppedEventsTotal  uint64
	lastErrorClass      ErrorClass
	failures            FailureCounts
}

// ClassifyError 把底层错误折叠为稳定、低基数的对外分类。
func ClassifyError(err error) ErrorClass {
	switch {
	case err == nil:
		return ErrorClassNone
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ErrorClassCanceled
	case errors.Is(err, sf.ErrClockRollback), errors.Is(err, sf.ErrTimestampBelowGenerationFloor), errors.Is(err, sf.ErrTimestampBeforeEpoch):
		return ErrorClassClockRollback
	case errors.Is(err, ErrClockSkew):
		return ErrorClassClockSkew
	case errors.Is(err, ErrGenerationFenceAhead), errors.Is(err, sf.ErrGenerationFenceReached):
		return ErrorClassFenceAhead
	case errors.Is(err, ErrLeaseUnavailable), errors.Is(err, ErrLeaseAlreadyAcquired):
		return ErrorClassLeaseBusy
	case errors.Is(err, ErrLeaseLost), errors.Is(err, ErrLeaseNotAcquired):
		return ErrorClassLeaseLost
	case errors.Is(err, ErrGeneratorClosed):
		return ErrorClassClosed
	case errors.Is(err, ErrLeaseStore):
		return ErrorClassStoreFailure
	default:
		return ErrorClassUnknown
	}
}

// Ready 检查租约生成器当前是否具备安全发号条件。
func (g *LeasedGenerator) Ready(_ context.Context) error {
	if g == nil {
		return fmt.Errorf("%w: nil leased generator", ErrInvalidLeaseConfig)
	}
	m := g.leaseManager
	m.mu.Lock()
	defer m.mu.Unlock()
	return g.readyAtLocked(g.clock.NowMillis())
}

func (g *LeasedGenerator) readyAtLocked(nowMillis int64) error {
	m := g.leaseManager
	if err := m.ensureActiveLocked(); err != nil {
		return err
	}
	if nowMillis < m.state.GenerationFloorMillis {
		err := fmt.Errorf(
			"%w: node_id=%d now_ms=%d floor_ms=%d",
			sf.ErrTimestampBelowGenerationFloor,
			m.state.NodeID,
			nowMillis,
			m.state.GenerationFloorMillis,
		)
		return m.failLocked(err)
	}
	if nowMillis >= m.state.GenerationFenceMillis {
		return fmt.Errorf(
			"%w: node_id=%d now_ms=%d fence_ms=%d",
			sf.ErrGenerationFenceReached,
			m.state.NodeID,
			nowMillis,
			m.state.GenerationFenceMillis,
		)
	}
	return nil
}

// Snapshot 返回当前状态快照，并复用 Ready 的关键安全检查。
func (g *LeasedGenerator) Snapshot() Snapshot {
	if g == nil {
		return Snapshot{
			Lifecycle:           LifecycleClosed,
			ReadinessErrorClass: ErrorClassUnknown,
			LastErrorClass:      ErrorClassUnknown,
		}
	}
	m := g.leaseManager
	m.mu.Lock()
	nowMillis := g.clock.NowMillis()
	readinessErr := g.readyAtLocked(nowMillis)
	leaseRemainingMillis := m.leaseRemainingMillisLocked()
	lifecycle := lifecycleLabel(m.lifecycle)
	state := m.state
	if state.NodeID == 0 {
		state.NodeID = m.config.NodeID
		state.OwnerID = m.config.OwnerID
	}
	terminalErr := m.terminalErr
	leaseOwned := m.lifecycle == leaseLifecycleActive && leaseRemainingMillis > 0
	counters := telemetryCounters{lastErrorClass: ErrorClassNone}
	if telemetry, ok := g.observer.(*Telemetry); ok {
		counters = telemetry.counterSnapshot()
	}
	m.mu.Unlock()
	lastErrorClass := counters.lastErrorClass
	if lastErrorClass == ErrorClassNone && terminalErr != nil {
		lastErrorClass = ClassifyError(terminalErr)
	}
	return Snapshot{
		CapturedAtMillis:      nowMillis,
		NodeID:                state.NodeID,
		OwnerID:               state.OwnerID,
		Lifecycle:             lifecycle,
		Ready:                 readinessErr == nil,
		ReadinessErrorClass:   ClassifyError(readinessErr),
		LeaseOwned:            leaseOwned,
		LeaseRemainingMillis:  leaseRemainingMillis,
		GenerationFloorMillis: state.GenerationFloorMillis,
		GenerationFenceMillis: state.GenerationFenceMillis,
		FenceLeadMillis:       state.GenerationFenceMillis - nowMillis,
		GeneratedTotal:        counters.generatedTotal,
		AcquireSuccessTotal:   counters.acquireSuccessTotal,
		RefreshSuccessTotal:   counters.refreshSuccessTotal,
		RefreshFailureTotal:   counters.refreshFailureTotal,
		FailureTotal:          counters.failureTotal,
		CloseTotal:            counters.closeTotal,
		DroppedEventsTotal:    counters.droppedEventsTotal,
		LastErrorClass:        lastErrorClass,
		Failures:              counters.failures,
	}
}

// RunReporter 周期读取快照并转发 telemetry 事件。
func RunReporter(
	ctx context.Context,
	interval time.Duration,
	source SnapshotSource,
	telemetry *Telemetry,
	onStatus func(StatusReport),
	onEvent func(Event),
) error {
	if source == nil {
		return fmt.Errorf("%w: reporter snapshot source is required", ErrInvalidLeaseConfig)
	}
	if interval <= 0 {
		return fmt.Errorf("%w: reporter interval must be positive", ErrInvalidLeaseConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	previous := source.Snapshot()
	previousAt := time.Now()
	if onStatus != nil {
		onStatus(StatusReport{Snapshot: previous})
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var events <-chan Event
	if telemetry != nil {
		events = telemetry.Events()
	}
	for {
		select {
		case <-ctx.Done():
			drainEvents(events, onEvent)
			return ctx.Err()
		// telemetry 未配置时，nil channel 会自然禁用这个 select 分支。
		case event := <-events:
			if onEvent != nil {
				onEvent(event)
			}
		case now := <-ticker.C:
			current := source.Snapshot()
			report := buildStatusReport(previous, current, now.Sub(previousAt))
			if onStatus != nil {
				onStatus(report)
			}
			previous = current
			previousAt = now
		}
	}
}

func drainEvents(events <-chan Event, onEvent func(Event)) {
	for {
		select {
		case event := <-events:
			if onEvent != nil {
				onEvent(event)
			}
		default:
			return
		}
	}
}

func buildStatusReport(previous Snapshot, current Snapshot, window time.Duration) StatusReport {
	delta := current.GeneratedTotal
	if current.GeneratedTotal >= previous.GeneratedTotal {
		delta = current.GeneratedTotal - previous.GeneratedTotal
	}
	rate := float64(0)
	if window > 0 {
		rate = float64(delta) / window.Seconds()
	}
	return StatusReport{
		Snapshot:                current,
		WindowMillis:            window.Milliseconds(),
		GeneratedDelta:          delta,
		GenerationRatePerSecond: rate,
	}
}

func lifecycleLabel(lifecycle leaseLifecycle) Lifecycle {
	switch lifecycle {
	case leaseLifecycleActive:
		return LifecycleActive
	case leaseLifecycleFailed:
		return LifecycleFailed
	case leaseLifecycleClosed:
		return LifecycleClosed
	default:
		return LifecycleNew
	}
}
