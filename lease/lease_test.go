package lease

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	sf "github.com/ZhcChen/cc-snowflake-id-go/generator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type fakeLeaseStore struct {
	acquireResults []fakeAcquireResult
	refreshState   LeaseState
	refreshErr     error
	releaseErr     error
	acquireHook    func()
	refreshHook    func(context.Context)
	acquireCalls   int
	refreshCalls   int
	releaseCalls   int
}

type fakeAcquireResult struct {
	state    LeaseState
	acquired bool
	err      error
}

func (s *fakeLeaseStore) TryAcquire(context.Context, int, string, int64, int64, int64, int64) (LeaseState, bool, error) {
	s.acquireCalls++
	if len(s.acquireResults) == 0 {
		return LeaseState{}, false, errors.New("unexpected acquire")
	}
	result := s.acquireResults[0]
	s.acquireResults = s.acquireResults[1:]
	if s.acquireHook != nil {
		s.acquireHook()
	}
	return result.state, result.acquired, result.err
}

func (s *fakeLeaseStore) Refresh(ctx context.Context, _ int, _ string, _ int64, _ int64, _ int64, _ int64) (LeaseState, error) {
	s.refreshCalls++
	if s.refreshHook != nil {
		s.refreshHook(ctx)
	}
	return s.refreshState, s.refreshErr
}

func (s *fakeLeaseStore) Release(context.Context, int, string) error {
	s.releaseCalls++
	return s.releaseErr
}

func TestLeaseManagerAcquireSuccess(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 2_005},
			acquired: true,
		}},
	}
	clock := &fakeClock{now: 2_000}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "api:host:1:abc",
		LeaseWindow:    5 * time.Millisecond,
		AcquireTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}

	state, err := manager.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if state.NodeID != 7 || state.OwnerID != "api:host:1:abc" || state.ReservedUntilMillis != 2_005 {
		t.Fatalf("state = %+v", state)
	}
	if store.acquireCalls != 1 {
		t.Fatalf("acquireCalls = %d, want 1", store.acquireCalls)
	}
}

func TestLeaseManagerWaitsForShortBusyLease(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{
			{
				state:    LeaseState{NodeID: 7, OwnerID: "other", ReservedUntilMillis: 2_003},
				acquired: false,
			},
			{
				state:    LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 2_008},
				acquired: true,
			},
		},
	}
	clock := &fakeClock{now: 2_000}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "api:host:1:abc",
		LeaseWindow:    5 * time.Millisecond,
		AcquireTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}

	state, err := manager.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if state.ReservedUntilMillis != 2_008 {
		t.Fatalf("state = %+v", state)
	}
	if len(clock.durationSleeps) != 1 || clock.durationSleeps[0] != 3*time.Millisecond {
		t.Fatalf("duration sleeps = %#v, want [3ms]", clock.durationSleeps)
	}
	if store.acquireCalls != 2 {
		t.Fatalf("acquireCalls = %d, want 2", store.acquireCalls)
	}
}

func TestLeaseManagerDeductsAcquireRoundTripFromRetryWait(t *testing.T) {
	clock := &fakeClock{now: 2_000}
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{
			{
				state: LeaseState{
					NodeID:              7,
					OwnerID:             "other",
					ReservedUntilMillis: 2_100,
					DatabaseNowMillis:   2_000,
				},
				acquired: false,
			},
			{
				state: LeaseState{
					NodeID:                7,
					OwnerID:               "api:host:1:abc",
					ReservedUntilMillis:   2_200,
					DatabaseNowMillis:     2_100,
					GenerationFenceMillis: 2_200,
				},
				acquired: true,
			},
		},
	}
	// 第一次 acquire 人为引入 40ms 往返延迟，验证重试等待会扣除已经花掉的时间，
	// 避免把“数据库返回的剩余租约时长”再完整等待一遍。
	store.acquireHook = func() {
		if store.acquireCalls == 1 {
			clock.now += 40
			clock.monotonic += 40
		}
	}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "api:host:1:abc",
		LeaseWindow:    100 * time.Millisecond,
		FenceWindow:    100 * time.Millisecond,
		MaxClockSkew:   time.Second,
		AcquireTimeout: 120 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}

	if _, err := manager.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if len(clock.durationSleeps) != 1 || clock.durationSleeps[0] != 60*time.Millisecond {
		t.Fatalf("duration sleeps = %#v, want [60ms] after 40ms database round trip", clock.durationSleeps)
	}
	if store.acquireCalls != 2 {
		t.Fatalf("acquireCalls = %d, want 2", store.acquireCalls)
	}
}

func TestLeaseManagerBacksOffBeforeRetryingExpiredLeaseMarker(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{
			{
				state:    LeaseState{NodeID: 7, OwnerID: "other", ReservedUntilMillis: 2_000},
				acquired: false,
			},
			{
				state:    LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 2_006},
				acquired: true,
			},
		},
	}
	clock := &fakeClock{now: 2_000}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "api:host:1:abc",
		LeaseWindow:    5 * time.Millisecond,
		AcquireTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}

	state, err := manager.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if state.ReservedUntilMillis != 2_006 {
		t.Fatalf("state = %+v", state)
	}
	if len(clock.durationSleeps) != 1 || clock.durationSleeps[0] != time.Millisecond {
		t.Fatalf("duration sleeps = %#v, want [1ms]", clock.durationSleeps)
	}
	if store.acquireCalls != 2 {
		t.Fatalf("acquireCalls = %d, want 2", store.acquireCalls)
	}
}

func TestLeaseManagerRejectsBusyLeasePastTimeout(t *testing.T) {
	busyOwnerID := "other:raw-host:pid-abc:nonce-secret"
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: busyOwnerID, ReservedUntilMillis: 2_011},
			acquired: false,
		}},
	}
	clock := &fakeClock{now: 2_000}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "api:host:1:abc",
		LeaseWindow:    5 * time.Millisecond,
		AcquireTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}

	_, err = manager.Acquire(context.Background())
	if !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("Acquire() error = %v, want ErrLeaseUnavailable", err)
	}
	// 抢租失败时错误里应该保留诊断能力，但不能泄露原始 owner 标识。
	errText := err.Error()
	if !strings.Contains(errText, RedactOwnerID(busyOwnerID)) {
		t.Fatalf("Acquire() error = %v, want redacted owner %q", err, RedactOwnerID(busyOwnerID))
	}
	for _, leaked := range []string{busyOwnerID, "raw-host", "pid-abc", "nonce-secret"} {
		if strings.Contains(errText, leaked) {
			t.Fatalf("Acquire() error leaked raw owner component %q: %v", leaked, err)
		}
	}
	if len(clock.sleeps) != 0 {
		t.Fatalf("sleeps = %#v, want none", clock.sleeps)
	}
}

func TestLeaseManagerRejectsExpiredLeaseMarkerWhenTimeoutElapsed(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "other", ReservedUntilMillis: 2_000},
			acquired: false,
		}},
	}
	clock := &fakeClock{now: 2_000}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "api:host:1:abc",
		LeaseWindow:    5 * time.Millisecond,
		AcquireTimeout: 0,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}

	_, err = manager.Acquire(context.Background())
	if !errors.Is(err, ErrLeaseUnavailable) {
		t.Fatalf("Acquire() error = %v, want ErrLeaseUnavailable", err)
	}
	if len(clock.sleeps) != 0 {
		t.Fatalf("sleeps = %#v, want none", clock.sleeps)
	}
	if store.acquireCalls != 1 {
		t.Fatalf("acquireCalls = %d, want 1", store.acquireCalls)
	}
}

func TestLeaseManagerRefreshAndRelease(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 2_005},
			acquired: true,
		}},
		refreshState: LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 2_005},
	}
	clock := &fakeClock{now: 2_000}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "api:host:1:abc",
		LeaseWindow:    5 * time.Millisecond,
		AcquireTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}

	if _, err := manager.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	state, err := manager.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if state.ReservedUntilMillis != 2_005 || store.refreshCalls != 1 {
		t.Fatalf("refresh state=%+v calls=%d", state, store.refreshCalls)
	}
	if err := manager.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if store.releaseCalls != 1 {
		t.Fatalf("releaseCalls = %d, want 1", store.releaseCalls)
	}
}

func TestLeaseManagerDeductsStoreLatencyFromLeaseRemaining(t *testing.T) {
	newManager := func(t *testing.T, store *fakeLeaseStore, clock *fakeClock) *LeaseManager {
		t.Helper()
		manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
			NodeID:         7,
			OwnerID:        "api:host:1:abc",
			LeaseWindow:    100 * time.Millisecond,
			FenceWindow:    100 * time.Millisecond,
			MaxClockSkew:   time.Second,
			AcquireTimeout: 200 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewLeaseManager() error = %v", err)
		}
		return manager
	}
	remaining := func(manager *LeaseManager) int64 {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.leaseRemainingMillisLocked()
	}

	t.Run("acquire", func(t *testing.T) {
		clock := &fakeClock{now: 2_000}
		store := &fakeLeaseStore{
			acquireResults: []fakeAcquireResult{{
				state: LeaseState{
					NodeID:                7,
					OwnerID:               "api:host:1:abc",
					ReservedUntilMillis:   2_100,
					DatabaseNowMillis:     2_000,
					GenerationFenceMillis: 2_100,
				},
				acquired: true,
			}},
			acquireHook: func() { clock.monotonic += 40 },
		}
		manager := newManager(t, store, clock)
		if _, err := manager.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire() error = %v", err)
		}
		if got := remaining(manager); got != 60 {
			t.Fatalf("lease remaining after 40ms acquire latency = %dms, want 60ms", got)
		}
	})

	t.Run("refresh", func(t *testing.T) {
		clock := &fakeClock{now: 2_000}
		store := &fakeLeaseStore{
			acquireResults: []fakeAcquireResult{{
				state: LeaseState{
					NodeID:                7,
					OwnerID:               "api:host:1:abc",
					ReservedUntilMillis:   2_100,
					DatabaseNowMillis:     2_000,
					GenerationFenceMillis: 2_100,
				},
				acquired: true,
			}},
			refreshState: LeaseState{
				NodeID:                7,
				OwnerID:               "api:host:1:abc",
				ReservedUntilMillis:   2_120,
				DatabaseNowMillis:     2_020,
				GenerationFenceMillis: 2_120,
			},
		}
		manager := newManager(t, store, clock)
		if _, err := manager.Acquire(context.Background()); err != nil {
			t.Fatalf("Acquire() error = %v", err)
		}

		clock.monotonic = 20
		store.refreshHook = func(context.Context) { clock.monotonic += 30 }
		if _, err := manager.Refresh(context.Background()); err != nil {
			t.Fatalf("Refresh() error = %v", err)
		}
		if got := remaining(manager); got != 70 {
			t.Fatalf("lease remaining after 30ms refresh latency = %dms, want 70ms", got)
		}
	})
}

func TestLeaseManagerRefreshRequiresAcquire(t *testing.T) {
	store := &fakeLeaseStore{}
	clock := &fakeClock{now: 2_000}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "api:host:1:abc",
		LeaseWindow:    5 * time.Millisecond,
		AcquireTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}

	_, err = manager.Refresh(context.Background())
	if !errors.Is(err, ErrLeaseNotAcquired) {
		t.Fatalf("Refresh() error = %v, want ErrLeaseNotAcquired", err)
	}
	if store.refreshCalls != 0 {
		t.Fatalf("refreshCalls = %d, want 0", store.refreshCalls)
	}
}

func TestLeaseManagerRefreshRejectsExpiredMonotonicLease(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 2_005},
			acquired: true,
		}},
		refreshState: LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 2_010},
	}
	clock := &fakeClock{now: 2_000}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "api:host:1:abc",
		LeaseWindow:    5 * time.Millisecond,
		AcquireTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}
	if _, err := manager.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	clock.now = 1_500
	clock.monotonic = 5
	_, err = manager.Refresh(context.Background())
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Refresh() error = %v, want ErrLeaseLost", err)
	}
	if store.refreshCalls != 0 {
		t.Fatalf("refreshCalls = %d, want 0", store.refreshCalls)
	}
}

func TestNewLeaseManagerValidatesConfig(t *testing.T) {
	_, err := NewLeaseManager(nil, nil, LeaseManagerConfig{NodeID: 1, OwnerID: "x", LeaseWindow: time.Second})
	if !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("nil store error = %v, want ErrInvalidLeaseConfig", err)
	}
	_, err = NewLeaseManager(&fakeLeaseStore{}, nil, LeaseManagerConfig{NodeID: 0, OwnerID: "x", LeaseWindow: time.Second})
	if !errors.Is(err, sf.ErrInvalidNodeID) {
		t.Fatalf("invalid node error = %v, want ErrInvalidNodeID", err)
	}
	_, err = NewLeaseManager(&fakeLeaseStore{}, nil, LeaseManagerConfig{NodeID: 1, OwnerID: " ", LeaseWindow: time.Second})
	if !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("blank owner error = %v, want ErrInvalidLeaseConfig", err)
	}
}

func TestLeasedGeneratorRequiresAcquire(t *testing.T) {
	generator, err := NewLeasedGenerator(&fakeLeaseStore{}, &fakeClock{now: 2_000}, LeasedGeneratorConfig{
		NodeID:                7,
		OwnerID:               "api:host:1:abc",
		EpochMillis:           1_000,
		LeaseWindow:           5 * time.Millisecond,
		LeaseAcquireTimeout:   10 * time.Millisecond,
		LeaseOperationTimeout: time.Millisecond,
		LeaseRefreshInterval:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeasedGenerator() error = %v", err)
	}

	_, err = generator.Next(context.Background())
	if !errors.Is(err, ErrLeaseNotAcquired) {
		t.Fatalf("Next() error = %v, want ErrLeaseNotAcquired", err)
	}
}

func TestLeasedGeneratorNextDoesNotRefreshPerID(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 3_000},
			acquired: true,
		}},
	}
	clock := &fakeClock{now: 2_000}
	generator, err := NewLeasedGenerator(store, clock, LeasedGeneratorConfig{
		NodeID:                7,
		OwnerID:               "api:host:1:abc",
		EpochMillis:           1_000,
		LeaseWindow:           time.Second,
		LeaseAcquireTimeout:   10 * time.Millisecond,
		LeaseOperationTimeout: 10 * time.Millisecond,
		LeaseRefreshInterval:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeasedGenerator() error = %v", err)
	}
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	first, err := generator.Next(context.Background())
	if err != nil {
		t.Fatalf("Next first error = %v", err)
	}
	second, err := generator.Next(context.Background())
	if err != nil {
		t.Fatalf("Next second error = %v", err)
	}
	if first == second {
		t.Fatalf("ids should be unique: %d", first)
	}
	if store.refreshCalls != 0 {
		t.Fatalf("refreshCalls = %d, want 0", store.refreshCalls)
	}
}

func TestLeasedGeneratorRefreshesNearExpiry(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 2_050},
			acquired: true,
		}},
		refreshState: LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 3_000},
	}
	clock := &fakeClock{now: 2_000}
	generator, err := NewLeasedGenerator(store, clock, LeasedGeneratorConfig{
		NodeID:                7,
		OwnerID:               "api:host:1:abc",
		EpochMillis:           1_000,
		LeaseWindow:           time.Second,
		LeaseAcquireTimeout:   10 * time.Millisecond,
		LeaseOperationTimeout: 10 * time.Millisecond,
		LeaseRefreshInterval:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeasedGenerator() error = %v", err)
	}
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	if _, err := generator.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if store.refreshCalls != 1 {
		t.Fatalf("refreshCalls = %d, want 1", store.refreshCalls)
	}
	if state := generator.State(); state.ReservedUntilMillis != 3_000 {
		t.Fatalf("state = %+v, want refreshed lease", state)
	}
}

func TestLeasedGeneratorNextRefreshIgnoresCallerCancellationUntilRefreshCompletes(t *testing.T) {
	refreshStarted := make(chan struct{})
	allowRefresh := make(chan struct{})
	var refreshCtxErr error
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 2_050},
			acquired: true,
		}},
		refreshState: LeaseState{
			NodeID:                7,
			OwnerID:               "api:host:1:abc",
			ReservedUntilMillis:   3_000,
			DatabaseNowMillis:     2_000,
			GenerationFenceMillis: 3_000,
		},
		refreshHook: func(ctx context.Context) {
			close(refreshStarted)
			<-allowRefresh
			refreshCtxErr = ctx.Err()
		},
	}
	clock := &fakeClock{now: 2_000}
	generator, err := NewLeasedGenerator(store, clock, LeasedGeneratorConfig{
		NodeID:                7,
		OwnerID:               "api:host:1:abc",
		EpochMillis:           1_000,
		LeaseWindow:           2 * time.Second,
		LeaseAcquireTimeout:   10 * time.Millisecond,
		LeaseOperationTimeout: time.Second,
		LeaseRefreshInterval:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeasedGenerator() error = %v", err)
	}
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := generator.Next(ctx)
		done <- err
	}()
	<-refreshStarted
	// Next 在内部触发 refresh 后，刷新操作应该用脱离调用方取消信号的内部上下文跑完，
	// 这样才能避免“租约已经成功续期，但因为调用方取消而把实例打残”。
	cancel()
	close(allowRefresh)

	err = <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Next() error = %v, want context.Canceled", err)
	}
	if refreshCtxErr != nil {
		t.Fatalf("refresh ctx error = %v, want nil internal operation context", refreshCtxErr)
	}
	if state := generator.State(); state.ReservedUntilMillis != 3_000 {
		t.Fatalf("state = %+v, want successful internal refresh despite caller cancellation", state)
	}
	if _, err := generator.Next(context.Background()); err != nil {
		t.Fatalf("Next() after caller cancellation error = %v, want generator to remain usable", err)
	}
}

func TestLeasedGeneratorStopsAfterRefreshFailure(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state:    LeaseState{NodeID: 7, OwnerID: "api:host:1:abc", ReservedUntilMillis: 2_050},
			acquired: true,
		}},
		refreshErr: ErrLeaseLost,
	}
	clock := &fakeClock{now: 2_000}
	generator, err := NewLeasedGenerator(store, clock, LeasedGeneratorConfig{
		NodeID:                7,
		OwnerID:               "api:host:1:abc",
		EpochMillis:           1_000,
		LeaseWindow:           time.Second,
		LeaseAcquireTimeout:   10 * time.Millisecond,
		LeaseOperationTimeout: 10 * time.Millisecond,
		LeaseRefreshInterval:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeasedGenerator() error = %v", err)
	}
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	_, err = generator.Next(context.Background())
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Next() error = %v, want ErrLeaseLost", err)
	}
	_, err = generator.Next(context.Background())
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Next() after failure error = %v, want ErrLeaseLost", err)
	}
}

func TestLeaseManagerWaitsForGenerationFenceBeforeAcquire(t *testing.T) {
	store := &fakeLeaseStore{acquireResults: []fakeAcquireResult{
		{
			state: LeaseState{
				NodeID:                7,
				OwnerID:               "old-owner",
				ReservedUntilMillis:   1_999,
				DatabaseNowMillis:     2_000,
				GenerationFenceMillis: 2_003,
			},
			acquired: false,
		},
		{
			state: LeaseState{
				NodeID:                7,
				OwnerID:               "new-owner",
				ReservedUntilMillis:   2_008,
				DatabaseNowMillis:     2_003,
				GenerationFenceMillis: 2_008,
			},
			acquired: true,
		},
	}}
	clock := &fakeClock{now: 2_000}
	manager, err := NewLeaseManager(store, clock, LeaseManagerConfig{
		NodeID:         7,
		OwnerID:        "new-owner",
		LeaseWindow:    5 * time.Millisecond,
		FenceWindow:    5 * time.Millisecond,
		MaxClockSkew:   time.Second,
		AcquireTimeout: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager() error = %v", err)
	}

	state, err := manager.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	// 即使旧租约已经过期，只要持久化 fence 还没过去，新 owner 也必须继续等待，
	// 否则可能在 wall clock 上和上一个 owner 产生时间重叠。
	if state.GenerationFloorMillis != 2_003 || state.GenerationFenceMillis != 2_008 {
		t.Fatalf("state = %+v, want floor=2003 fence=2008", state)
	}
	if len(clock.durationSleeps) != 1 || clock.durationSleeps[0] != 3*time.Millisecond {
		t.Fatalf("duration sleeps = %#v, want [3ms]", clock.durationSleeps)
	}
}

func TestLeasedGeneratorRejectsWallClockRollbackBelowAcquireFloor(t *testing.T) {
	store := &fakeLeaseStore{acquireResults: []fakeAcquireResult{{
		state: LeaseState{
			NodeID:                7,
			OwnerID:               "api:host:1:abc",
			ReservedUntilMillis:   3_000,
			DatabaseNowMillis:     2_000,
			GenerationFenceMillis: 2_005,
		},
		acquired: true,
	}}}
	clock := &fakeClock{now: 2_000}
	generator, err := NewLeasedGenerator(store, clock, LeasedGeneratorConfig{
		NodeID:                7,
		OwnerID:               "api:host:1:abc",
		EpochMillis:           1_000,
		LeaseWindow:           time.Second,
		FenceWindow:           5 * time.Millisecond,
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

	clock.now = 1_999
	_, err = generator.Next(context.Background())
	if !errors.Is(err, sf.ErrTimestampBelowGenerationFloor) {
		t.Fatalf("Next() error = %v, want ErrTimestampBelowGenerationFloor", err)
	}
	if store.refreshCalls != 0 {
		t.Fatalf("refreshCalls = %d, want 0", store.refreshCalls)
	}
}

func TestLeasedGeneratorExtendsFenceAfterSequenceWait(t *testing.T) {
	store := &fakeLeaseStore{
		acquireResults: []fakeAcquireResult{{
			state: LeaseState{
				NodeID:                7,
				OwnerID:               "api:host:1:abc",
				ReservedUntilMillis:   3_000,
				DatabaseNowMillis:     2_000,
				GenerationFenceMillis: 2_005,
			},
			acquired: true,
		}},
		refreshState: LeaseState{
			NodeID:                7,
			OwnerID:               "api:host:1:abc",
			ReservedUntilMillis:   3_005,
			DatabaseNowMillis:     2_005,
			GenerationFenceMillis: 2_010,
		},
	}
	clock := &fakeClock{now: 2_004}
	generator, err := NewLeasedGenerator(store, clock, LeasedGeneratorConfig{
		NodeID:                7,
		OwnerID:               "api:host:1:abc",
		EpochMillis:           1_000,
		LeaseWindow:           time.Second,
		FenceWindow:           5 * time.Millisecond,
		MaxClockSkew:          time.Second,
		LeaseAcquireTimeout:   10 * time.Millisecond,
		LeaseOperationTimeout: time.Millisecond,
		LeaseRefreshInterval:  500 * time.Microsecond,
	})
	if err != nil {
		t.Fatalf("NewLeasedGenerator() error = %v", err)
	}
	if _, err := generator.Acquire(context.Background()); err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	// 先把同一毫秒内的 sequence 用满，让下一次发号必须推进到 2005ms。
	// 由于 2005ms 正好触到旧 fence，正确行为是先刷新 fence，再返回这个时间戳。
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
	if parts.TimestampMillis != 2_005 || parts.Sequence != 0 {
		t.Fatalf("parts = %+v, want timestamp=2005 sequence=0", parts)
	}
	if store.refreshCalls != 1 {
		t.Fatalf("refreshCalls = %d, want fence extension before returning timestamp 2005", store.refreshCalls)
	}
	if state := generator.State(); state.GenerationFenceMillis != 2_010 {
		t.Fatalf("state = %+v, want extended fence=2010", state)
	}
}

type fakeLeaseDB struct {
	row       pgx.Row
	querySQL  string
	queryArgs []any
	execSQL   string
	execArgs  []any
}

func (db *fakeLeaseDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	db.querySQL = sql
	db.queryArgs = args
	return db.row
}

func (db *fakeLeaseDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	db.execSQL = sql
	db.execArgs = args
	return pgconn.CommandTag{}, nil
}

type fakeLeaseRow struct {
	scan func(dest ...any) error
}

func (r fakeLeaseRow) Scan(dest ...any) error {
	return r.scan(dest...)
}

func TestPGLeaseStoreTryAcquireMapsRow(t *testing.T) {
	db := &fakeLeaseDB{row: fakeLeaseRow{scan: func(dest ...any) error {
		*(dest[0].(*bool)) = true
		*(dest[1].(*int)) = 7
		*(dest[2].(*string)) = "api:host:1:abc"
		*(dest[3].(*int64)) = 2_005
		*(dest[4].(*int64)) = 2_005
		*(dest[5].(*int64)) = 2_000
		*(dest[6].(*string)) = "acquired"
		return nil
	}}}
	store, err := NewPGLeaseStore(db)
	if err != nil {
		t.Fatalf("NewPGLeaseStore() error = %v", err)
	}

	state, acquired, err := store.TryAcquire(context.Background(), 7, "api:host:1:abc", 2_000, 5, 2_005, 1_000)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if !acquired || state.NodeID != 7 || state.OwnerID != "api:host:1:abc" ||
		state.ReservedUntilMillis != 2_005 || state.DatabaseNowMillis != 2_000 ||
		state.GenerationFenceMillis != 2_005 {
		t.Fatalf("acquired=%v state=%+v", acquired, state)
	}
	if len(db.queryArgs) != 6 || db.queryArgs[0] != 7 || db.queryArgs[1] != "api:host:1:abc" ||
		db.queryArgs[2] != int64(2_000) || db.queryArgs[3] != int64(5) ||
		db.queryArgs[4] != int64(2_005) || db.queryArgs[5] != int64(1_000) {
		t.Fatalf("query args = %#v, want node, owner, local time, lease window, fence, max skew", db.queryArgs)
	}
	// 这里同时验证 SQL 的两个关键约束：
	// 1. 所有租约裁决都基于数据库时钟；
	// 2. generation fence 只能前进，不能倒退。
	lowerSQL := strings.ToLower(db.querySQL)
	if !strings.Contains(lowerSQL, "clock_timestamp()") {
		t.Fatalf("try acquire SQL must use database clock: %s", db.querySQL)
	}
	if !strings.Contains(lowerSQL, "generation_fence_ms = greatest") || !strings.Contains(lowerSQL, "clock_ok") {
		t.Fatalf("try acquire SQL must extend the fence and validate clock skew: %s", db.querySQL)
	}
	if strings.Contains(lowerSQL, "reserved_until_ms <= $3") {
		t.Fatalf("try acquire SQL must compare lease expiry against database time: %s", db.querySQL)
	}
}

func TestPGLeaseStoreTryAcquireNoRowsRetries(t *testing.T) {
	db := &fakeLeaseDB{row: fakeLeaseRow{scan: func(dest ...any) error {
		return pgx.ErrNoRows
	}}}
	store, err := NewPGLeaseStore(db)
	if err != nil {
		t.Fatalf("NewPGLeaseStore() error = %v", err)
	}

	state, acquired, err := store.TryAcquire(context.Background(), 7, "api:host:1:abc", 2_000, 5, 2_005, 1_000)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if acquired {
		t.Fatalf("acquired = true, want false")
	}
	if state.NodeID != 7 || state.DatabaseNowMillis != 2_000 || state.GenerationFenceMillis != 2_005 {
		t.Fatalf("state = %+v, want retry marker", state)
	}
}

func TestPGLeaseStoreTryAcquireMapsBusyLeaseToLocalDeadline(t *testing.T) {
	db := &fakeLeaseDB{row: fakeLeaseRow{scan: func(dest ...any) error {
		*(dest[0].(*bool)) = false
		*(dest[1].(*int)) = 7
		*(dest[2].(*string)) = "other"
		*(dest[3].(*int64)) = 2_003
		*(dest[4].(*int64)) = 2_010
		*(dest[5].(*int64)) = 2_000
		*(dest[6].(*string)) = "lease_busy"
		return nil
	}}}
	store, err := NewPGLeaseStore(db)
	if err != nil {
		t.Fatalf("NewPGLeaseStore() error = %v", err)
	}

	state, acquired, err := store.TryAcquire(context.Background(), 7, "api:host:1:abc", 2_000, 5, 2_005, 1_000)
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if acquired {
		t.Fatalf("acquired = true, want false")
	}
	if state.OwnerID != "other" || state.ReservedUntilMillis != 2_003 ||
		state.DatabaseNowMillis != 2_000 || state.GenerationFenceMillis != 2_010 {
		t.Fatalf("state = %+v, want database lease and persisted fence", state)
	}
}

func TestPGLeaseStoreRefreshLost(t *testing.T) {
	db := &fakeLeaseDB{row: fakeLeaseRow{scan: func(dest ...any) error {
		return pgx.ErrNoRows
	}}}
	store, err := NewPGLeaseStore(db)
	if err != nil {
		t.Fatalf("NewPGLeaseStore() error = %v", err)
	}

	_, err = store.Refresh(context.Background(), 7, "api:host:1:abc", 2_000, 5, 2_005, 1_000)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Refresh() error = %v, want ErrLeaseLost", err)
	}
}

func TestPGLeaseStoreRefreshUsesDatabaseClock(t *testing.T) {
	db := &fakeLeaseDB{row: fakeLeaseRow{scan: func(dest ...any) error {
		*(dest[0].(*bool)) = true
		*(dest[1].(*int)) = 7
		*(dest[2].(*string)) = "api:host:1:abc"
		*(dest[3].(*int64)) = 2_005
		*(dest[4].(*int64)) = 2_005
		*(dest[5].(*int64)) = 2_000
		*(dest[6].(*string)) = "refreshed"
		return nil
	}}}
	store, err := NewPGLeaseStore(db)
	if err != nil {
		t.Fatalf("NewPGLeaseStore() error = %v", err)
	}

	state, err := store.Refresh(context.Background(), 7, "api:host:1:abc", 2_000, 5, 2_005, 1_000)
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if state.ReservedUntilMillis != 2_005 || state.DatabaseNowMillis != 2_000 || state.GenerationFenceMillis != 2_005 {
		t.Fatalf("state = %+v, want database lease and persisted fence", state)
	}
	if len(db.queryArgs) != 6 || db.queryArgs[0] != 7 || db.queryArgs[1] != "api:host:1:abc" ||
		db.queryArgs[2] != int64(2_000) || db.queryArgs[3] != int64(5) ||
		db.queryArgs[4] != int64(2_005) || db.queryArgs[5] != int64(1_000) {
		t.Fatalf("query args = %#v, want node, owner, local time, lease window, fence, max skew", db.queryArgs)
	}
	lowerSQL := strings.ToLower(db.querySQL)
	if !strings.Contains(lowerSQL, "clock_timestamp()") {
		t.Fatalf("refresh SQL must use database clock: %s", db.querySQL)
	}
	if !strings.Contains(lowerSQL, "generation_fence_ms = greatest") || !strings.Contains(lowerSQL, "clock_ok") {
		t.Fatalf("refresh SQL must extend the fence and validate clock skew: %s", db.querySQL)
	}
	if strings.Contains(lowerSQL, "reserved_until_ms > $3") {
		t.Fatalf("refresh SQL must not compare lease expiry against application time: %s", db.querySQL)
	}
}

func TestPGLeaseStoreTryAcquireRejectsClockSkew(t *testing.T) {
	db := &fakeLeaseDB{row: fakeLeaseRow{scan: func(dest ...any) error {
		*(dest[0].(*bool)) = false
		*(dest[1].(*int)) = 7
		*(dest[2].(*string)) = ""
		*(dest[3].(*int64)) = 0
		*(dest[4].(*int64)) = 0
		*(dest[5].(*int64)) = 4_000
		*(dest[6].(*string)) = "clock_skew"
		return nil
	}}}
	store, err := NewPGLeaseStore(db)
	if err != nil {
		t.Fatalf("NewPGLeaseStore() error = %v", err)
	}

	_, _, err = store.TryAcquire(context.Background(), 7, "api:host:1:abc", 2_000, 5, 2_005, 1_000)
	if !errors.Is(err, ErrClockSkew) {
		t.Fatalf("TryAcquire() error = %v, want ErrClockSkew", err)
	}
}

func TestPGLeaseStoreRejectsInvalidIdentityBeforeDatabase(t *testing.T) {
	store, err := NewPGLeaseStore(&fakeLeaseDB{})
	if err != nil {
		t.Fatalf("NewPGLeaseStore() error = %v", err)
	}

	if _, _, err := store.TryAcquire(context.Background(), 0, "owner-a", 2_000, 5, 2_005, 1_000); !errors.Is(err, sf.ErrInvalidNodeID) {
		t.Fatalf("TryAcquire(invalid node) error = %v, want ErrInvalidNodeID", err)
	}
	if _, err := store.Refresh(context.Background(), 7, " ", 2_000, 5, 2_005, 1_000); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("Refresh(blank owner) error = %v, want ErrInvalidLeaseConfig", err)
	}
	if err := store.Release(context.Background(), 7, " owner-a "); !errors.Is(err, ErrInvalidLeaseConfig) {
		t.Fatalf("Release(untrimmed owner) error = %v, want ErrInvalidLeaseConfig", err)
	}
}

func TestPGLeaseStoreRelease(t *testing.T) {
	db := &fakeLeaseDB{}
	store, err := NewPGLeaseStore(db)
	if err != nil {
		t.Fatalf("NewPGLeaseStore() error = %v", err)
	}

	if err := store.Release(context.Background(), 7, "api:host:1:abc"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if len(db.execArgs) != 2 || db.execArgs[0] != 7 || db.execArgs[1] != "api:host:1:abc" {
		t.Fatalf("exec args = %#v", db.execArgs)
	}
	if strings.Contains(strings.ToLower(db.execSQL), "delete") {
		t.Fatalf("release SQL must not delete lease row: %s", db.execSQL)
	}
	if strings.Contains(strings.ToLower(db.execSQL), "reserved_until_ms") {
		t.Fatalf("release SQL must not shorten reserved_until_ms: %s", db.execSQL)
	}
}
