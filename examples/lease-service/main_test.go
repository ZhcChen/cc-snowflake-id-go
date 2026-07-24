package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	leaseidgen "github.com/ZhcChen/cc-snowflake-id-go/lease"
)

type testAcquireResult struct {
	state    leaseidgen.LeaseState
	acquired bool
	err      error
}

type testLeaseStore struct {
	mu             sync.Mutex
	acquireResults []testAcquireResult
	defaultAcquire testAcquireResult
	refreshState   leaseidgen.LeaseState
	refreshErr     error
	releaseErr     error
	acquireCalls   int
	refreshCalls   int
	releaseCalls   int
}

func (s *testLeaseStore) TryAcquire(
	_ context.Context,
	nodeID int,
	ownerID string,
	localNowMillis int64,
	leaseWindowMillis int64,
	generationFenceMillis int64,
	_ int64,
) (leaseidgen.LeaseState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.acquireCalls++
	result := s.defaultAcquire
	if len(s.acquireResults) > 0 {
		result = s.acquireResults[0]
		s.acquireResults = s.acquireResults[1:]
	}

	state := result.state
	if state.NodeID == 0 {
		state.NodeID = nodeID
	}
	if state.OwnerID == "" {
		state.OwnerID = ownerID
	}
	if state.DatabaseNowMillis == 0 {
		state.DatabaseNowMillis = localNowMillis
	}
	if state.ReservedUntilMillis == 0 {
		state.ReservedUntilMillis = localNowMillis + maxInt64(leaseWindowMillis, 500)
	}
	if state.GenerationFenceMillis == 0 {
		state.GenerationFenceMillis = generationFenceMillis
	}
	return state, result.acquired, result.err
}

func (s *testLeaseStore) Refresh(
	_ context.Context,
	nodeID int,
	ownerID string,
	localNowMillis int64,
	leaseWindowMillis int64,
	generationFenceMillis int64,
	_ int64,
) (leaseidgen.LeaseState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.refreshCalls++
	state := s.refreshState
	if state.NodeID == 0 {
		state.NodeID = nodeID
	}
	if state.OwnerID == "" {
		state.OwnerID = ownerID
	}
	if state.DatabaseNowMillis == 0 {
		state.DatabaseNowMillis = localNowMillis
	}
	if state.ReservedUntilMillis == 0 {
		state.ReservedUntilMillis = localNowMillis + maxInt64(leaseWindowMillis, 500)
	}
	if state.GenerationFenceMillis == 0 {
		state.GenerationFenceMillis = generationFenceMillis
	}
	return state, s.refreshErr
}

func (s *testLeaseStore) Release(context.Context, int, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCalls++
	return s.releaseErr
}

func (s *testLeaseStore) acquireCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquireCalls
}

func testManagerSettings() componentManagerSettings {
	ownerSeq := 0
	return componentManagerSettings{
		leaseWindow:           200 * time.Millisecond,
		fenceWindow:           200 * time.Millisecond,
		leaseAcquireTimeout:   100 * time.Millisecond,
		leaseOperationTimeout: 10 * time.Millisecond,
		leaseRefreshInterval:  20 * time.Millisecond,
		rebuildInitialDelay:   5 * time.Millisecond,
		rebuildMaxDelay:       10 * time.Millisecond,
		componentStopTimeout:  50 * time.Millisecond,
		ownerIDBuilder: func(serviceName string) (string, error) {
			ownerSeq++
			return fmt.Sprintf("%s-owner-%d", serviceName, ownerSeq), nil
		},
		startReporter: func(context.Context, string, *leaseidgen.Telemetry, leaseidgen.SnapshotSource) {},
	}
}

func TestComponentManagerRebuildsAfterRuntimeFailure(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &testLeaseStore{
		acquireResults: []testAcquireResult{
			{acquired: true},
			{acquired: true},
		},
	}
	manager := newComponentManager(rootCtx, demoConfig{
		ServiceName: "svc",
		NodeID:      7,
	}, store, testManagerSettings())
	if err := manager.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		cancel()
		if err := manager.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	}()

	before := manager.Snapshot()
	if err := manager.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() before rebuild error = %v", err)
	}

	manager.mu.RLock()
	component := manager.component
	manager.mu.RUnlock()
	if component == nil {
		t.Fatal("component = nil, want active component")
	}

	manager.handleRuntimeFailure(component, leaseidgen.ErrLeaseLost)

	waitForCondition(t, time.Second, func() bool {
		snapshot := manager.Snapshot()
		return snapshot.Ready && snapshot.OwnerID != "" && snapshot.OwnerID != before.OwnerID
	})

	after := manager.Snapshot()
	if after.NodeID != 7 {
		t.Fatalf("snapshot node_id = %d, want 7", after.NodeID)
	}
	if after.OwnerID == before.OwnerID {
		t.Fatalf("owner_id = %q, want rebuilt owner distinct from %q", after.OwnerID, before.OwnerID)
	}
	if calls := store.acquireCallCount(); calls < 2 {
		t.Fatalf("acquire calls = %d, want at least 2", calls)
	}
}

func TestComponentManagerStaysNotReadyWhenRebuildKeepsFailing(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storeFailure := errors.New("db unavailable")
	store := &testLeaseStore{
		acquireResults: []testAcquireResult{
			{acquired: true},
		},
		defaultAcquire: testAcquireResult{
			err: storeFailure,
		},
	}
	manager := newComponentManager(rootCtx, demoConfig{
		ServiceName: "svc",
		NodeID:      9,
	}, store, testManagerSettings())
	if err := manager.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		cancel()
		if err := manager.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	}()

	manager.mu.RLock()
	component := manager.component
	manager.mu.RUnlock()
	if component == nil {
		t.Fatal("component = nil, want active component")
	}

	manager.handleRuntimeFailure(component, leaseidgen.ErrLeaseLost)

	waitForCondition(t, 300*time.Millisecond, func() bool {
		err := manager.Ready(context.Background())
		snapshot := manager.Snapshot()
		return err != nil &&
			snapshot.Lifecycle == leaseidgen.LifecycleFailed &&
			snapshot.ReadinessErrorClass == leaseidgen.ErrorClassStoreFailure &&
			snapshot.LastErrorClass == leaseidgen.ErrorClassStoreFailure
	})

	if calls := store.acquireCallCount(); calls < 2 {
		t.Fatalf("acquire calls = %d, want repeated rebuild acquires", calls)
	}
}

func TestDemoServerHandlersReflectUnavailableComponentState(t *testing.T) {
	manager := newComponentManager(context.Background(), demoConfig{
		ServiceName: "svc",
		NodeID:      11,
	}, nil, testManagerSettings())
	manager.mu.Lock()
	manager.rebuilding = true
	manager.lastErr = leaseidgen.ErrLeaseLost
	manager.lastOwnerID = "svc-owner-1"
	manager.lastSnapshot = leaseidgen.Snapshot{
		NodeID:              11,
		OwnerID:             "svc-owner-1",
		Lifecycle:           leaseidgen.LifecycleFailed,
		Ready:               false,
		LeaseOwned:          false,
		LastErrorClass:      leaseidgen.ErrorClassLeaseLost,
		ReadinessErrorClass: leaseidgen.ErrorClassLeaseLost,
	}
	manager.mu.Unlock()

	server := newDemoServer("svc", manager).routes()

	t.Run("readyz", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		recorder := httptest.NewRecorder()

		server.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}

		var payload map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode readyz body: %v", err)
		}
		if payload["error_class"] != string(leaseidgen.ErrorClassLeaseLost) {
			t.Fatalf("error_class = %v, want %q", payload["error_class"], leaseidgen.ErrorClassLeaseLost)
		}
	})

	t.Run("next", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/next", nil)
		recorder := httptest.NewRecorder()

		server.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}

		var payload map[string]any
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode next body: %v", err)
		}
		if payload["error_class"] != string(leaseidgen.ErrorClassLeaseLost) {
			t.Fatalf("error_class = %v, want %q", payload["error_class"], leaseidgen.ErrorClassLeaseLost)
		}
	})
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func maxInt64(left int64, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
