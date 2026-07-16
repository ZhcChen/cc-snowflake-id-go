package lease

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	sf "github.com/ZhcChen/cc-snowflake-id-go/generator"
)

func TestClassifyErrorUsesStableLowCardinalityClasses(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ErrorClass
	}{
		{name: "none", want: ErrorClassNone},
		{name: "clock rollback", err: sf.ErrClockRollback, want: ErrorClassClockRollback},
		{name: "below acquire floor", err: sf.ErrTimestampBelowGenerationFloor, want: ErrorClassClockRollback},
		{name: "before epoch", err: sf.ErrTimestampBeforeEpoch, want: ErrorClassClockRollback},
		{name: "clock skew", err: ErrClockSkew, want: ErrorClassClockSkew},
		{name: "fence ahead", err: ErrGenerationFenceAhead, want: ErrorClassFenceAhead},
		{name: "fence reached", err: sf.ErrGenerationFenceReached, want: ErrorClassFenceAhead},
		{name: "lease busy", err: ErrLeaseUnavailable, want: ErrorClassLeaseBusy},
		{name: "lease lost", err: ErrLeaseLost, want: ErrorClassLeaseLost},
		{name: "closed", err: ErrGeneratorClosed, want: ErrorClassClosed},
		{name: "canceled", err: context.Canceled, want: ErrorClassCanceled},
		{name: "deadline", err: context.DeadlineExceeded, want: ErrorClassCanceled},
		{name: "store", err: fmt.Errorf("%w: unavailable", ErrLeaseStore), want: ErrorClassStoreFailure},
		{name: "unknown", err: errors.New("unexpected"), want: ErrorClassUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := ClassifyError(test.err); got != test.want {
				t.Fatalf("ClassifyError(%v) = %q, want %q", test.err, got, test.want)
			}
		})
	}
}

func TestTelemetryCountsConcurrentGenerationAndFixedFailureClasses(t *testing.T) {
	telemetry := NewTelemetry()
	const (
		workers   = 16
		perWorker = 10_000
	)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range perWorker {
				telemetry.ObserveGenerated()
			}
			telemetry.ObserveRefresh(true)
		}()
	}
	wg.Wait()
	// ObserveGenerated 只累加计数，不应该为每个 ID 发送事件，否则高吞吐路径会被 telemetry 反压。
	select {
	case event := <-telemetry.Events():
		t.Fatalf("generation emitted per-ID event: %+v", event)
	default:
	}
	telemetry.ObserveRefresh(false)
	telemetry.ObserveFailed(LeaseState{}, ErrClockSkew)
	telemetry.ObserveFailed(LeaseState{}, ErrLeaseLost)

	snapshot := telemetry.counterSnapshot()
	if snapshot.generatedTotal != workers*perWorker {
		t.Fatalf("generated total = %d, want %d", snapshot.generatedTotal, workers*perWorker)
	}
	if snapshot.refreshSuccessTotal != workers || snapshot.refreshFailureTotal != 1 {
		t.Fatalf("refresh totals = %d/%d, want %d/1", snapshot.refreshSuccessTotal, snapshot.refreshFailureTotal, workers)
	}
	if snapshot.failureTotal != 2 || snapshot.failures.ClockSkew != 1 || snapshot.failures.LeaseLost != 1 {
		t.Fatalf("failure counters = %+v total=%d", snapshot.failures, snapshot.failureTotal)
	}
	if snapshot.lastErrorClass != ErrorClassLeaseLost {
		t.Fatalf("last error class = %q, want %q", snapshot.lastErrorClass, ErrorClassLeaseLost)
	}
}

func TestTelemetryNilReceiverAndNilEventSinkAreSafe(t *testing.T) {
	var nilTelemetry *Telemetry
	if nilTelemetry.Events() != nil {
		t.Fatal("nil telemetry Events() should return nil")
	}
	nilTelemetry.ObserveGenerated()
	nilTelemetry.ObserveAcquired(LeaseState{NodeID: 7, OwnerID: "owner-a"})
	nilTelemetry.ObserveRefresh(true)
	nilTelemetry.ObserveFailed(LeaseState{NodeID: 7, OwnerID: "owner-a"}, ErrLeaseLost)
	nilTelemetry.ObserveClosed(LeaseState{NodeID: 7, OwnerID: "owner-a"}, ErrLeaseLost)
	nilSnapshot := nilTelemetry.counterSnapshot()
	if nilSnapshot.failureTotal != 0 || nilSnapshot.closeTotal != 0 || nilSnapshot.lastErrorClass != ErrorClassNone {
		t.Fatalf("nil telemetry snapshot = %+v, want zero counters", nilSnapshot)
	}

	sinkless := &Telemetry{}
	if sinkless.Events() != nil {
		t.Fatal("sinkless telemetry Events() should return nil")
	}
	sinkless.ObserveAcquired(LeaseState{NodeID: 7, OwnerID: "owner-a"})
	sinkless.ObserveRefresh(true)
	sinkless.ObserveRefresh(false)
	sinkless.ObserveFailed(LeaseState{NodeID: 7, OwnerID: "owner-a"}, ErrLeaseUnavailable)
	sinkless.ObserveClosed(LeaseState{NodeID: 7, OwnerID: "owner-a"}, nil)

	snapshot := sinkless.counterSnapshot()
	if snapshot.acquireSuccessTotal != 1 || snapshot.refreshSuccessTotal != 1 || snapshot.refreshFailureTotal != 1 {
		t.Fatalf("sinkless refresh/acquire totals = %+v", snapshot)
	}
	if snapshot.failureTotal != 1 || snapshot.failures.LeaseBusy != 1 {
		t.Fatalf("sinkless failure totals = %+v", snapshot)
	}
	if snapshot.closeTotal != 1 || snapshot.droppedEventsTotal != 0 || snapshot.lastErrorClass != ErrorClassLeaseBusy {
		t.Fatalf("sinkless close/event totals = %+v", snapshot)
	}
}

func TestTelemetryFailureCountersCoverEveryErrorClass(t *testing.T) {
	telemetry := NewTelemetryWithConfig(TelemetryConfig{EventBufferSize: 32})
	state := LeaseState{NodeID: 7, OwnerID: "owner-a"}
	cases := []struct {
		name string
		err  error
		want ErrorClass
	}{
		{name: "clock rollback", err: sf.ErrClockRollback, want: ErrorClassClockRollback},
		{name: "clock skew", err: ErrClockSkew, want: ErrorClassClockSkew},
		{name: "fence ahead", err: ErrGenerationFenceAhead, want: ErrorClassFenceAhead},
		{name: "lease busy", err: ErrLeaseUnavailable, want: ErrorClassLeaseBusy},
		{name: "lease lost", err: ErrLeaseLost, want: ErrorClassLeaseLost},
		{name: "closed", err: ErrGeneratorClosed, want: ErrorClassClosed},
		{name: "canceled", err: context.DeadlineExceeded, want: ErrorClassCanceled},
		{name: "store failure", err: fmt.Errorf("%w: unavailable", ErrLeaseStore), want: ErrorClassStoreFailure},
		{name: "unknown", err: errors.New("unexpected"), want: ErrorClassUnknown},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			telemetry.ObserveFailed(state, test.err)
			select {
			case event := <-telemetry.Events():
				if event.Kind != EventFailed || event.ErrorClass != test.want || event.NodeID != 7 || event.OwnerID != "owner-a" {
					t.Fatalf("event = %+v, want kind=failed class=%q node=7 owner=owner-a", event, test.want)
				}
			default:
				t.Fatalf("missing event for %s", test.name)
			}
		})
	}

	snapshot := telemetry.counterSnapshot()
	if snapshot.failureTotal != uint64(len(cases)) {
		t.Fatalf("failure total = %d, want %d", snapshot.failureTotal, len(cases))
	}
	if snapshot.failures.ClockRollback != 1 || snapshot.failures.ClockSkew != 1 || snapshot.failures.FenceAhead != 1 ||
		snapshot.failures.LeaseBusy != 1 || snapshot.failures.LeaseLost != 1 || snapshot.failures.Closed != 1 ||
		snapshot.failures.Canceled != 1 || snapshot.failures.StoreFailure != 1 || snapshot.failures.Unknown != 1 {
		t.Fatalf("failure counters = %+v, want one count per class", snapshot.failures)
	}
	if snapshot.lastErrorClass != ErrorClassUnknown {
		t.Fatalf("last error class = %q, want %q", snapshot.lastErrorClass, ErrorClassUnknown)
	}
}

func TestTelemetryEventBufferSizeIsConfigurable(t *testing.T) {
	telemetry := NewTelemetryWithConfig(TelemetryConfig{EventBufferSize: 1})
	if got := cap(telemetry.Events()); got != 1 {
		t.Fatalf("event buffer cap = %d, want 1", got)
	}
	telemetry.ObserveAcquired(LeaseState{NodeID: 7, OwnerID: "owner-a"})
	telemetry.ObserveClosed(LeaseState{NodeID: 7, OwnerID: "owner-a"}, nil)
	if snapshot := telemetry.counterSnapshot(); snapshot.droppedEventsTotal != 1 {
		t.Fatalf("dropped events = %d, want 1", snapshot.droppedEventsTotal)
	}

	defaultTelemetry := NewTelemetryWithConfig(TelemetryConfig{})
	if got := cap(defaultTelemetry.Events()); got != DefaultTelemetryEventBuffer {
		t.Fatalf("default event buffer cap = %d, want %d", got, DefaultTelemetryEventBuffer)
	}
}

func TestLeasedGeneratorSnapshotTracksReadinessAndRefreshHealth(t *testing.T) {
	telemetry := NewTelemetry()
	clock := &fakeClock{now: 2_000}
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state: LeaseState{
				NodeID:                7,
				OwnerID:               "owner-a",
				ReservedUntilMillis:   3_000,
				DatabaseNowMillis:     2_000,
				GenerationFenceMillis: 3_000,
			},
			acquired: true,
		}},
		refreshState: LeaseState{
			NodeID:                7,
			OwnerID:               "owner-a",
			ReservedUntilMillis:   4_000,
			DatabaseNowMillis:     2_000,
			GenerationFenceMillis: 4_000,
		},
	}
	generator := newTelemetryTestGenerator(t, store, clock, telemetry)
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	for range 2 {
		if _, err := generator.Next(context.Background()); err != nil {
			t.Fatalf("Next() error = %v", err)
		}
	}
	if _, err := generator.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}

	snapshot := generator.Snapshot()
	if !snapshot.Ready || snapshot.Lifecycle != LifecycleActive || !snapshot.LeaseOwned {
		t.Fatalf("active snapshot = %+v", snapshot)
	}
	if snapshot.GeneratedTotal != 2 || snapshot.AcquireSuccessTotal != 1 || snapshot.RefreshSuccessTotal != 1 || snapshot.RefreshFailureTotal != 0 {
		t.Fatalf("telemetry totals = %+v", snapshot)
	}
	if snapshot.LeaseRemainingMillis != 2_000 || snapshot.FenceLeadMillis != 2_000 {
		t.Fatalf("lease/fence lead = %d/%d, want 2000/2000", snapshot.LeaseRemainingMillis, snapshot.FenceLeadMillis)
	}
	// acquire 事件应该离散上报，而不是只体现在快照计数里。
	select {
	case event := <-telemetry.Events():
		if event.Kind != EventAcquired || event.NodeID != 7 || event.OwnerID != "owner-a" {
			t.Fatalf("acquire event = %+v", event)
		}
	default:
		t.Fatal("missing acquire event")
	}

	clock.monotonic = 2_001
	snapshot = generator.Snapshot()
	if snapshot.Ready || snapshot.Lifecycle != LifecycleFailed || snapshot.ReadinessErrorClass != ErrorClassLeaseLost {
		t.Fatalf("expired snapshot = %+v", snapshot)
	}
	if snapshot.FailureTotal != 1 || snapshot.Failures.LeaseLost != 1 || snapshot.LastErrorClass != ErrorClassLeaseLost {
		t.Fatalf("expired failure telemetry = %+v", snapshot)
	}
	select {
	case event := <-telemetry.Events():
		if event.Kind != EventFailed || event.NodeID != 7 || event.OwnerID != "owner-a" || event.ErrorClass != ErrorClassLeaseLost {
			t.Fatalf("failure event = %+v", event)
		}
	default:
		t.Fatal("missing lease-lost event")
	}
	if err := generator.Close(context.Background()); err != nil {
		t.Fatalf("Close() after failure error = %v", err)
	}
	snapshot = generator.Snapshot()
	// Close 可以切到 closed 生命周期，但不应该抹掉此前已经确定的终态失败原因。
	if snapshot.Lifecycle != LifecycleClosed || snapshot.LastErrorClass != ErrorClassLeaseLost {
		t.Fatalf("close overwrote terminal failure snapshot = %+v", snapshot)
	}
}

func TestLeasedGeneratorSnapshotWithoutObserverIsNoop(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	store := &fakeLeaseStore{acquireResults: []fakeAcquireResult{{
		state: LeaseState{
			NodeID:                7,
			OwnerID:               "owner-a",
			ReservedUntilMillis:   3_000,
			DatabaseNowMillis:     2_000,
			GenerationFenceMillis: 3_000,
		},
		acquired: true,
	}}}
	generator := newTelemetryTestGenerator(t, store, clock, nil)
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if _, err := generator.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	snapshot := generator.Snapshot()
	if !snapshot.Ready || snapshot.GeneratedTotal != 0 || snapshot.AcquireSuccessTotal != 0 || snapshot.LastErrorClass != ErrorClassNone {
		t.Fatalf("no-op observer snapshot = %+v", snapshot)
	}
}

func TestAcquireTelemetryDistinguishesLocalRangeAndStoreFailures(t *testing.T) {
	t.Run("local range failure", func(t *testing.T) {
		telemetry := NewTelemetry()
		store := &fakeLeaseStore{}
		generator := newTelemetryTestGenerator(t, store, &fakeClock{now: math.MaxInt64}, telemetry)
		// 本地时间窗口溢出属于本地配置/计算错误，不应该被误归类成租约存储失败。
		_, err := generator.Acquire(context.Background())
		if !errors.Is(err, ErrInvalidLeaseConfig) || errors.Is(err, ErrLeaseStore) {
			t.Fatalf("Acquire() error = %v, want local ErrInvalidLeaseConfig without ErrLeaseStore", err)
		}
		if store.acquireCalls != 0 {
			t.Fatalf("store acquire calls = %d, want 0", store.acquireCalls)
		}
		if snapshot := generator.Snapshot(); snapshot.LastErrorClass != ErrorClassUnknown || snapshot.Failures.Unknown != 1 {
			t.Fatalf("local range snapshot = %+v", snapshot)
		}
	})

	t.Run("store failure", func(t *testing.T) {
		storeErr := errors.New("database unavailable")
		telemetry := NewTelemetry()
		store := &fakeLeaseStore{acquireResults: []fakeAcquireResult{{err: storeErr}}}
		generator := newTelemetryTestGenerator(t, store, &fakeClock{now: 2_000}, telemetry)
		_, err := generator.Acquire(context.Background())
		if !errors.Is(err, ErrLeaseStore) || !errors.Is(err, storeErr) {
			t.Fatalf("Acquire() error = %v, want wrapped ErrLeaseStore and root cause", err)
		}
		if snapshot := generator.Snapshot(); snapshot.LastErrorClass != ErrorClassStoreFailure || snapshot.Failures.StoreFailure != 1 {
			t.Fatalf("store failure snapshot = %+v", snapshot)
		}
	})
}

func TestLeasedGeneratorReadinessRejectsFloorFenceAndClosedStates(t *testing.T) {
	t.Run("below acquire floor is terminal", func(t *testing.T) {
		telemetry := NewTelemetry()
		clock := &fakeClock{now: 2_000}
		store := &fakeLeaseStore{acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "owner-a", ReservedUntilMillis: 3_000, DatabaseNowMillis: 2_000, GenerationFenceMillis: 3_000},
			acquired: true,
		}}}
		generator := newTelemetryTestGenerator(t, store, clock, telemetry)
		if _, err := generator.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire() error = %v", err)
		}
		clock.now = 1_999
		if err := generator.Ready(context.Background()); !errors.Is(err, sf.ErrTimestampBelowGenerationFloor) {
			t.Fatalf("Ready() error = %v, want ErrTimestampBelowGenerationFloor", err)
		}
		if snapshot := generator.Snapshot(); snapshot.Lifecycle != LifecycleFailed || snapshot.ReadinessErrorClass != ErrorClassClockRollback {
			t.Fatalf("rollback snapshot = %+v", snapshot)
		}
	})

	t.Run("exhausted fence can recover", func(t *testing.T) {
		clock := &fakeClock{now: 2_000}
		store := &fakeLeaseStore{acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "owner-a", ReservedUntilMillis: 3_000, DatabaseNowMillis: 2_000, GenerationFenceMillis: 2_000},
			acquired: true,
		}}}
		generator := newTelemetryTestGenerator(t, store, clock, nil)
		if _, err := generator.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire() error = %v", err)
		}
		if err := generator.Ready(context.Background()); !errors.Is(err, sf.ErrGenerationFenceReached) {
			t.Fatalf("Ready() error = %v, want ErrGenerationFenceReached", err)
		}
		if snapshot := generator.Snapshot(); snapshot.Lifecycle != LifecycleActive || snapshot.ReadinessErrorClass != ErrorClassFenceAhead {
			t.Fatalf("fence snapshot = %+v", snapshot)
		}
	})

	t.Run("closed", func(t *testing.T) {
		telemetry := NewTelemetry()
		clock := &fakeClock{now: 2_000}
		store := &fakeLeaseStore{acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "owner-a", ReservedUntilMillis: 3_000, DatabaseNowMillis: 2_000, GenerationFenceMillis: 3_000},
			acquired: true,
		}}, releaseErr: errors.New("database unavailable")}
		generator := newTelemetryTestGenerator(t, store, clock, telemetry)
		if _, err := generator.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire() error = %v", err)
		}
		if err := generator.Close(context.Background()); !errors.Is(err, ErrLeaseStore) {
			t.Fatalf("Close() error = %v, want ErrLeaseStore", err)
		}
		snapshot := generator.Snapshot()
		if snapshot.Ready || snapshot.Lifecycle != LifecycleClosed || snapshot.LeaseOwned || snapshot.CloseTotal != 1 {
			t.Fatalf("closed snapshot = %+v", snapshot)
		}
		if snapshot.FailureTotal != 1 || snapshot.Failures.StoreFailure != 1 || snapshot.LastErrorClass != ErrorClassStoreFailure {
			t.Fatalf("close failure snapshot = %+v", snapshot)
		}
	})
}

func TestBuildStatusReportCalculatesStableWindowRate(t *testing.T) {
	report := buildStatusReport(
		Snapshot{GeneratedTotal: 10},
		Snapshot{GeneratedTotal: 40},
		30*time.Second,
	)
	if report.GeneratedDelta != 30 || report.WindowMillis != 30_000 || report.GenerationRatePerSecond != 1 {
		t.Fatalf("report = %+v, want delta=30 window=30000 rate=1", report)
	}
	zero := buildStatusReport(Snapshot{GeneratedTotal: 40}, Snapshot{GeneratedTotal: 40}, 30*time.Second)
	if zero.GeneratedDelta != 0 || zero.GenerationRatePerSecond != 0 {
		t.Fatalf("zero report = %+v", zero)
	}
	restarted := buildStatusReport(Snapshot{GeneratedTotal: 40}, Snapshot{GeneratedTotal: 5}, 30*time.Second)
	if restarted.GeneratedDelta != 5 {
		t.Fatalf("restarted report delta = %d, want 5", restarted.GeneratedDelta)
	}
}

func TestRunReporterEmitsInitialStatusPeriodicDeltaAndImmediateEvents(t *testing.T) {
	telemetry := NewTelemetry()
	source := &mutableSnapshotSource{snapshot: Snapshot{GeneratedTotal: 10, Ready: true}}
	telemetry.ObserveAcquired(LeaseState{NodeID: 7, OwnerID: "owner-a"})
	statuses := make(chan StatusReport, 4)
	events := make(chan Event, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunReporter(ctx, 10*time.Millisecond, source, telemetry, func(report StatusReport) {
			statuses <- report
		}, func(event Event) {
			events <- event
		})
	}()

	initial := receiveStatusReport(t, statuses)
	if initial.GeneratedTotal != 10 || initial.GeneratedDelta != 0 || initial.GenerationRatePerSecond != 0 {
		t.Fatalf("initial report = %+v", initial)
	}
	source.set(Snapshot{GeneratedTotal: 20, Ready: true})
	periodic := receiveStatusReport(t, statuses)
	if periodic.GeneratedDelta != 10 || periodic.WindowMillis <= 0 || periodic.GenerationRatePerSecond <= 0 {
		t.Fatalf("periodic report = %+v", periodic)
	}
	select {
	case event := <-events:
		if event.Kind != EventAcquired || event.ErrorClass != ErrorClassNone {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reporter event")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunReporter() error = %v, want context.Canceled", err)
	}

	if err := RunReporter(context.Background(), time.Second, nil, nil, nil, nil); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("RunReporter(nil source) error = %v", err)
	}
	if err := RunReporter(context.Background(), 0, source, nil, nil, nil); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("RunReporter(zero interval) error = %v", err)
	}
}

func TestRunReporterDrainsTerminalEventDuringShutdown(t *testing.T) {
	telemetry := NewTelemetry()
	source := &mutableSnapshotSource{snapshot: Snapshot{Ready: false, Lifecycle: LifecycleClosed}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	telemetry.ObserveClosed(LeaseState{NodeID: 7, OwnerID: "owner-a"}, errors.New("release failed"))
	events := make(chan Event, 1)
	// Reporter 在退出前需要把缓冲区里最后一个终态事件排空，否则 close 失败这类关键诊断会丢失。
	err := RunReporter(ctx, time.Second, source, telemetry, nil, func(event Event) {
		events <- event
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunReporter() error = %v, want context.Canceled", err)
	}
	select {
	case event := <-events:
		if event.Kind != EventCloseFailed || event.NodeID != 7 {
			t.Fatalf("drained event = %+v", event)
		}
	default:
		t.Fatal("terminal event was dropped during reporter shutdown")
	}
}

func newTelemetryTestGenerator(t *testing.T, store LeaseStore, clock sf.Clock, observer Observer) *LeasedGenerator {
	t.Helper()
	generator, err := NewLeasedGenerator(store, clock, LeasedGeneratorConfig{
		NodeID:               7,
		OwnerID:              "owner-a",
		EpochMillis:          1_000,
		LeaseWindow:          2 * time.Second,
		FenceWindow:          2 * time.Second,
		MaxClockSkew:         time.Second,
		LeaseAcquireTimeout:  10 * time.Millisecond,
		LeaseRefreshInterval: 100 * time.Millisecond,
		Observer:             observer,
	})
	if err != nil {
		t.Fatalf("NewLeasedGenerator() error = %v", err)
	}
	return generator
}

type mutableSnapshotSource struct {
	mu       sync.Mutex
	snapshot Snapshot
}

func (s *mutableSnapshotSource) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func (s *mutableSnapshotSource) set(snapshot Snapshot) {
	s.mu.Lock()
	s.snapshot = snapshot
	s.mu.Unlock()
}

func receiveStatusReport(t *testing.T, reports <-chan StatusReport) StatusReport {
	t.Helper()
	select {
	case report := <-reports:
		return report
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for status report")
		return StatusReport{}
	}
}
