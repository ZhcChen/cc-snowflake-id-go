package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLeasedGeneratorCloseSerializesWithRefreshAndStaysClosedAfterReleaseFailure(t *testing.T) {
	releaseErr := errors.New("release failed")
	store := &controlledLeaseStore{
		acquireState:   activeReliabilityLeaseState(),
		acquired:       true,
		refreshState:   activeReliabilityLeaseState(),
		refreshStarted: make(chan struct{}),
		allowRefresh:   make(chan struct{}),
		releaseStarted: make(chan struct{}),
		releaseErr:     releaseErr,
	}
	generator := newReliabilityGenerator(t, store)
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	refreshDone := make(chan error, 1)
	go func() {
		_, err := generator.Refresh(context.Background())
		refreshDone <- err
	}()
	<-store.refreshStarted

	closeAttempted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeAttempted)
		closeDone <- generator.Close(context.Background())
	}()
	<-closeAttempted
	// Close 不能和 Refresh 并发地同时进入底层 store，否则 release/refresh 顺序会被打乱。
	assertNoSignal(t, store.releaseStarted, "release started while refresh was blocked")

	close(store.allowRefresh)
	if err := <-refreshDone; err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if err := <-closeDone; !errors.Is(err, releaseErr) {
		t.Fatalf("Close() error = %v, want release error", err)
	}

	operations := store.operationSnapshot()
	wantOperations := []string{"refresh-start", "refresh-end", "release-start", "release-end"}
	if len(operations) != len(wantOperations) {
		t.Fatalf("operations = %#v, want %#v", operations, wantOperations)
	}
	for index := range wantOperations {
		if operations[index] != wantOperations[index] {
			t.Fatalf("operations = %#v, want %#v", operations, wantOperations)
		}
	}

	assertClosedOperations(t, generator)
	if err := generator.Close(context.Background()); !errors.Is(err, releaseErr) {
		t.Fatalf("second Close() error = %v, want cached release error", err)
	}
	if calls := store.releaseCallCount(); calls != 1 {
		t.Fatalf("release calls = %d, want 1", calls)
	}
}

func TestLeasedGeneratorRefreshFailureIsTerminalForConcurrentNext(t *testing.T) {
	store := &controlledLeaseStore{
		acquireState:   activeReliabilityLeaseState(),
		acquired:       true,
		refreshErr:     ErrLeaseLost,
		refreshStarted: make(chan struct{}),
		allowRefresh:   make(chan struct{}),
	}
	generator := newReliabilityGenerator(t, store)
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	refreshDone := make(chan error, 1)
	go func() {
		_, err := generator.Refresh(context.Background())
		refreshDone <- err
	}()
	<-store.refreshStarted

	const callers = 8
	nextErrors := make(chan error, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	// 一旦 refresh 失败并进入终态，所有并发 Next 都应该观察到同一个终态错误，
	// 不能有人继续成功发号，也不能出现不同的失败原因。
	for range callers {
		go func() {
			ready.Done()
			_, err := generator.Next(context.Background())
			nextErrors <- err
		}()
	}
	ready.Wait()
	close(store.allowRefresh)

	refreshErr := <-refreshDone
	if !errors.Is(refreshErr, ErrLeaseLost) {
		t.Fatalf("Refresh() error = %v, want ErrLeaseLost", refreshErr)
	}
	for range callers {
		err := <-nextErrors
		if !errors.Is(err, ErrLeaseLost) || err.Error() != refreshErr.Error() {
			t.Fatalf("Next() error = %v, want terminal error %v", err, refreshErr)
		}
	}
}

func TestLeaseLifecycleCancellationIsTerminalAndNeverRestoresActive(t *testing.T) {
	// 这组三个子场景覆盖 acquire、refresh、close 三条生命周期路径，
	// 核心约束是“取消一旦被采纳为终态，就不能再恢复到 active”。
	t.Run("acquire wait", func(t *testing.T) {
		store := &controlledLeaseStore{
			acquireState: LeaseState{
				NodeID:                7,
				OwnerID:               "other-owner",
				ReservedUntilMillis:   2_010,
				DatabaseNowMillis:     2_000,
				GenerationFenceMillis: 2_000,
			},
		}
		generator := newReliabilityGenerator(t, store)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := generator.Acquire(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Acquire() error = %v, want context.Canceled", err)
		}
		_, secondErr := generator.Acquire(context.Background())
		if !errors.Is(secondErr, context.Canceled) || secondErr.Error() != err.Error() {
			t.Fatalf("second Acquire() error = %v, want terminal error %v", secondErr, err)
		}
		if calls := store.releaseCallCount(); calls != 0 {
			t.Fatalf("release calls before close = %d, want 0", calls)
		}
		if err := generator.Close(context.Background()); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		assertClosedOperations(t, generator)
	})

	t.Run("refresh", func(t *testing.T) {
		store := &controlledLeaseStore{
			acquireState:   activeReliabilityLeaseState(),
			acquired:       true,
			refreshStarted: make(chan struct{}),
			allowRefresh:   make(chan struct{}),
		}
		generator := newReliabilityGenerator(t, store)
		if _, err := generator.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire() error = %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		refreshDone := make(chan error, 1)
		go func() {
			_, err := generator.Refresh(ctx)
			refreshDone <- err
		}()
		<-store.refreshStarted
		cancel()

		refreshErr := <-refreshDone
		if !errors.Is(refreshErr, context.Canceled) {
			t.Fatalf("Refresh() error = %v, want context.Canceled", refreshErr)
		}
		if _, err := generator.Next(context.Background()); !errors.Is(err, context.Canceled) || err.Error() != refreshErr.Error() {
			t.Fatalf("Next() error = %v, want terminal error %v", err, refreshErr)
		}
	})

	t.Run("close database release", func(t *testing.T) {
		store := &controlledLeaseStore{
			acquireState:   activeReliabilityLeaseState(),
			acquired:       true,
			releaseStarted: make(chan struct{}),
			allowRelease:   make(chan struct{}),
		}
		generator := newReliabilityGenerator(t, store)
		if _, err := generator.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire() error = %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		closeDone := make(chan error, 1)
		go func() { closeDone <- generator.Close(ctx) }()
		<-store.releaseStarted
		assertClosedOperations(t, generator)
		cancel()

		closeErr := <-closeDone
		if !errors.Is(closeErr, context.Canceled) {
			t.Fatalf("Close() error = %v, want context.Canceled", closeErr)
		}
		assertClosedOperations(t, generator)
		if err := generator.Close(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatalf("second Close() error = %v, want cached context.Canceled", err)
		}
		if calls := store.releaseCallCount(); calls != 1 {
			t.Fatalf("release calls = %d, want 1", calls)
		}
	})
}

func TestLeaseManagerLeaseRemainingUsesOnlyMonotonicClock(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	store := &controlledLeaseStore{acquireState: activeReliabilityLeaseState(), acquired: true}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "owner-a",
		LeaseWindow:    time.Second,
		FenceWindow:    time.Second,
		MaxClockSkew:   time.Second,
		AcquireTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}
	if _, err := manager.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	for _, wallNow := range []int64{9_000_000, 1_000} {
		clock.now = wallNow
		manager.mu.Lock()
		remaining := manager.leaseRemainingMillisLocked()
		manager.mu.Unlock()
		if remaining != 1_000 {
			t.Fatalf("wall now=%d changed remaining lease to %dms, want 1000ms", wallNow, remaining)
		}
	}
	clock.monotonic = 250
	manager.mu.Lock()
	remaining := manager.leaseRemainingMillisLocked()
	manager.mu.Unlock()
	if remaining != 750 {
		t.Fatalf("monotonic advance changed remaining lease to %dms, want 750ms", remaining)
	}
}

func TestLeasedGeneratorConcurrentNextStateRefreshLoopAndClose(t *testing.T) {
	releaseErr := errors.New("release failed")
	store := &controlledLeaseStore{
		acquireState:   activeReliabilityLeaseState(),
		acquired:       true,
		refreshState:   activeReliabilityLeaseState(),
		refreshStarted: make(chan struct{}),
		releaseStarted: make(chan struct{}),
		allowRelease:   make(chan struct{}),
		releaseErr:     releaseErr,
	}
	generator := newReliabilityGenerator(t, store)
	generator.refreshInterval = time.Millisecond
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refreshLoopDone := make(chan error, 1)
	go func() { refreshLoopDone <- generator.RunRefreshLoop(ctx) }()
	<-store.refreshStarted

	// 这里故意把 Next、Snapshot、RefreshLoop 和 Close 摆到一起跑，
	// 验证关闭过程中不会留下悬挂 goroutine 或把对象重新带回可用状态。
	stop := make(chan struct{})
	workerErrors := make(chan error, 8)
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				if _, err := generator.Next(context.Background()); err != nil {
					if !errors.Is(err, ErrGeneratorClosed) {
						workerErrors <- err
					}
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = generator.Snapshot()
			}
		}
	}()
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if _, err := generator.Refresh(context.Background()); err != nil {
					if !errors.Is(err, ErrGeneratorClosed) {
						workerErrors <- err
					}
					return
				}
			}
		}
	}()

	closeDone := make(chan error, 1)
	go func() { closeDone <- generator.Close(context.Background()) }()
	<-store.releaseStarted
	assertClosedOperations(t, generator)
	close(stop)
	cancel()
	close(store.allowRelease)
	if err := <-closeDone; !errors.Is(err, releaseErr) {
		t.Fatalf("Close() error = %v, want release error", err)
	}

	workersDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(workersDone)
	}()
	select {
	case <-workersDone:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent generator workers did not stop")
	}
	close(workerErrors)
	for err := range workerErrors {
		t.Fatalf("concurrent operation error = %v", err)
	}
	if err := <-refreshLoopDone; !errors.Is(err, context.Canceled) && !errors.Is(err, ErrGeneratorClosed) {
		t.Fatalf("RunRefreshLoop() error = %v, want cancellation or closed state", err)
	}
}

func assertClosedOperations(t *testing.T, generator *LeasedGenerator) {
	t.Helper()
	if _, err := generator.Next(context.Background()); !errors.Is(err, ErrGeneratorClosed) {
		t.Fatalf("Next() error = %v, want ErrGeneratorClosed", err)
	}
	if _, err := generator.Refresh(context.Background()); !errors.Is(err, ErrGeneratorClosed) {
		t.Fatalf("Refresh() error = %v, want ErrGeneratorClosed", err)
	}
	if _, err := generator.Acquire(context.Background()); !errors.Is(err, ErrGeneratorClosed) {
		t.Fatalf("Acquire() error = %v, want ErrGeneratorClosed", err)
	}
}

func assertNoSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-signal:
		t.Fatal(message)
	case <-timer.C:
	}
}

func activeReliabilityLeaseState() LeaseState {
	return LeaseState{
		NodeID:                7,
		OwnerID:               "owner-a",
		ReservedUntilMillis:   3_000,
		DatabaseNowMillis:     2_000,
		GenerationFenceMillis: 3_000,
	}
}

func newReliabilityGenerator(t *testing.T, store LeaseStore) *LeasedGenerator {
	t.Helper()
	generator, err := NewLeasedGenerator(store, &fakeClock{now: 2_000}, LeasedGeneratorConfig{
		NodeID:               7,
		OwnerID:              "owner-a",
		EpochMillis:          1_000,
		LeaseWindow:          2 * time.Second,
		FenceWindow:          2 * time.Second,
		MaxClockSkew:         time.Second,
		LeaseAcquireTimeout:  10 * time.Millisecond,
		LeaseRefreshInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeasedGenerator() error = %v", err)
	}
	return generator
}

type controlledLeaseStore struct {
	mu sync.Mutex

	acquireState LeaseState
	acquired     bool
	acquireErr   error
	refreshState LeaseState
	refreshErr   error
	releaseErr   error

	refreshStarted chan struct{}
	allowRefresh   chan struct{}
	releaseStarted chan struct{}
	allowRelease   chan struct{}

	refreshStartedOnce sync.Once
	releaseStartedOnce sync.Once
	releaseCalls       int
	operations         []string
}

func (s *controlledLeaseStore) TryAcquire(context.Context, int, string, int64, int64, int64, int64) (LeaseState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquireState, s.acquired, s.acquireErr
}

func (s *controlledLeaseStore) Refresh(ctx context.Context, _ int, _ string, _ int64, _ int64, _ int64, _ int64) (LeaseState, error) {
	s.recordOperation("refresh-start")
	if s.refreshStarted != nil {
		s.refreshStartedOnce.Do(func() { close(s.refreshStarted) })
	}
	if s.allowRefresh != nil {
		select {
		case <-s.allowRefresh:
		case <-ctx.Done():
			s.recordOperation("refresh-end")
			return LeaseState{}, ctx.Err()
		}
	}
	s.recordOperation("refresh-end")
	return s.refreshState, s.refreshErr
}

func (s *controlledLeaseStore) Release(ctx context.Context, _ int, _ string) error {
	s.mu.Lock()
	s.releaseCalls++
	s.operations = append(s.operations, "release-start")
	s.mu.Unlock()
	if s.releaseStarted != nil {
		s.releaseStartedOnce.Do(func() { close(s.releaseStarted) })
	}
	if s.allowRelease != nil {
		select {
		case <-s.allowRelease:
		case <-ctx.Done():
			s.recordOperation("release-end")
			return ctx.Err()
		}
	}
	s.recordOperation("release-end")
	return s.releaseErr
}

func (s *controlledLeaseStore) recordOperation(operation string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.operations = append(s.operations, operation)
}

func (s *controlledLeaseStore) operationSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.operations...)
}

func (s *controlledLeaseStore) releaseCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseCalls
}
