package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
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

// testLeaseStore 允许测试按顺序注入租约获取与续租结果，用来模拟短暂抖动和持续故障。
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

// testManagerSettings 使用更短的超时和退避，避免示例级测试等待过久。
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

func TestLoadConfig(t *testing.T) {
	t.Run("requires database url", func(t *testing.T) {
		t.Setenv("IDGEN_DATABASE_URL", "")
		t.Setenv("IDGEN_HTTP_ADDR", "")
		t.Setenv("IDGEN_SERVICE_NAME", "")
		t.Setenv("IDGEN_NODE_ID", "")

		_, err := loadConfig()
		if err == nil || err.Error() != "IDGEN_DATABASE_URL is required" {
			t.Fatalf("loadConfig() error = %v, want missing database url error", err)
		}
	})

	t.Run("uses defaults and trims values", func(t *testing.T) {
		t.Setenv("IDGEN_DATABASE_URL", "  postgres://demo  ")
		t.Setenv("IDGEN_HTTP_ADDR", " ")
		t.Setenv("IDGEN_SERVICE_NAME", "\t")
		t.Setenv("IDGEN_NODE_ID", "")

		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("loadConfig() error = %v", err)
		}

		if cfg.DatabaseURL != "postgres://demo" {
			t.Fatalf("DatabaseURL = %q, want %q", cfg.DatabaseURL, "postgres://demo")
		}
		if cfg.HTTPAddr != defaultHTTPAddr {
			t.Fatalf("HTTPAddr = %q, want %q", cfg.HTTPAddr, defaultHTTPAddr)
		}
		if cfg.ServiceName != defaultServiceName {
			t.Fatalf("ServiceName = %q, want %q", cfg.ServiceName, defaultServiceName)
		}
		if cfg.NodeID != defaultNodeID {
			t.Fatalf("NodeID = %d, want %d", cfg.NodeID, defaultNodeID)
		}
	})

	t.Run("uses explicit values", func(t *testing.T) {
		t.Setenv("IDGEN_DATABASE_URL", "postgres://custom")
		t.Setenv("IDGEN_HTTP_ADDR", " 127.0.0.1:9090 ")
		t.Setenv("IDGEN_SERVICE_NAME", " custom-svc ")
		t.Setenv("IDGEN_NODE_ID", " 42 ")

		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("loadConfig() error = %v", err)
		}

		if cfg.DatabaseURL != "postgres://custom" {
			t.Fatalf("DatabaseURL = %q, want %q", cfg.DatabaseURL, "postgres://custom")
		}
		if cfg.HTTPAddr != "127.0.0.1:9090" {
			t.Fatalf("HTTPAddr = %q, want %q", cfg.HTTPAddr, "127.0.0.1:9090")
		}
		if cfg.ServiceName != "custom-svc" {
			t.Fatalf("ServiceName = %q, want %q", cfg.ServiceName, "custom-svc")
		}
		if cfg.NodeID != 42 {
			t.Fatalf("NodeID = %d, want 42", cfg.NodeID)
		}
	})

	t.Run("rejects invalid node id", func(t *testing.T) {
		t.Setenv("IDGEN_DATABASE_URL", "postgres://demo")
		t.Setenv("IDGEN_HTTP_ADDR", "")
		t.Setenv("IDGEN_SERVICE_NAME", "")
		t.Setenv("IDGEN_NODE_ID", "abc")

		_, err := loadConfig()
		if err == nil || err.Error() != "parse IDGEN_NODE_ID: strconv.Atoi: parsing \"abc\": invalid syntax" {
			t.Fatalf("loadConfig() error = %v, want invalid node id parse error", err)
		}
	})
}

func TestComponentManagerSettingsWithDefaults(t *testing.T) {
	t.Run("fills zero values", func(t *testing.T) {
		settings := (componentManagerSettings{}).withDefaults()

		if settings.leaseWindow != leaseWindow {
			t.Fatalf("leaseWindow = %s, want %s", settings.leaseWindow, leaseWindow)
		}
		if settings.fenceWindow != leaseWindow {
			t.Fatalf("fenceWindow = %s, want %s", settings.fenceWindow, leaseWindow)
		}
		if settings.leaseAcquireTimeout != leaseAcquireTimeout {
			t.Fatalf("leaseAcquireTimeout = %s, want %s", settings.leaseAcquireTimeout, leaseAcquireTimeout)
		}
		if settings.leaseOperationTimeout != leaseOperationTimeout {
			t.Fatalf("leaseOperationTimeout = %s, want %s", settings.leaseOperationTimeout, leaseOperationTimeout)
		}
		if settings.leaseRefreshInterval != leaseRefreshInterval {
			t.Fatalf("leaseRefreshInterval = %s, want %s", settings.leaseRefreshInterval, leaseRefreshInterval)
		}
		if settings.rebuildInitialDelay != rebuildInitialDelay {
			t.Fatalf("rebuildInitialDelay = %s, want %s", settings.rebuildInitialDelay, rebuildInitialDelay)
		}
		if settings.rebuildMaxDelay != rebuildMaxDelay {
			t.Fatalf("rebuildMaxDelay = %s, want %s", settings.rebuildMaxDelay, rebuildMaxDelay)
		}
		if settings.componentStopTimeout != componentStopTimeout {
			t.Fatalf("componentStopTimeout = %s, want %s", settings.componentStopTimeout, componentStopTimeout)
		}
		if settings.ownerIDBuilder == nil {
			t.Fatal("ownerIDBuilder = nil, want default builder")
		}
		if settings.startReporter == nil {
			t.Fatal("startReporter = nil, want default reporter")
		}
	})

	t.Run("preserves explicit values", func(t *testing.T) {
		ownerIDBuilder := func(serviceName string) (string, error) {
			return serviceName + "-owner", nil
		}
		startReporter := func(context.Context, string, *leaseidgen.Telemetry, leaseidgen.SnapshotSource) {}

		settings := componentManagerSettings{
			leaseWindow:    300 * time.Millisecond,
			fenceWindow:    450 * time.Millisecond,
			ownerIDBuilder: ownerIDBuilder,
			startReporter:  startReporter,
		}.withDefaults()

		if settings.leaseWindow != 300*time.Millisecond {
			t.Fatalf("leaseWindow = %s, want 300ms", settings.leaseWindow)
		}
		if settings.fenceWindow != 450*time.Millisecond {
			t.Fatalf("fenceWindow = %s, want 450ms", settings.fenceWindow)
		}
		if reflect.ValueOf(settings.ownerIDBuilder).Pointer() != reflect.ValueOf(ownerIDBuilder).Pointer() {
			t.Fatal("ownerIDBuilder pointer changed, want explicit builder to be preserved")
		}
		if reflect.ValueOf(settings.startReporter).Pointer() != reflect.ValueOf(startReporter).Pointer() {
			t.Fatal("startReporter pointer changed, want explicit reporter to be preserved")
		}
	})
}

func TestComponentManagerStartFailsOnInvalidSetup(t *testing.T) {
	t.Run("owner id builder failure", func(t *testing.T) {
		manager := newComponentManager(context.Background(), demoConfig{
			ServiceName: "svc",
			NodeID:      5,
		}, &testLeaseStore{}, componentManagerSettings{
			ownerIDBuilder: func(string) (string, error) {
				return "", errors.New("owner builder failed")
			},
			startReporter: func(context.Context, string, *leaseidgen.Telemetry, leaseidgen.SnapshotSource) {},
		})

		err := manager.Start()
		if err == nil || err.Error() != "build owner id: owner builder failed" {
			t.Fatalf("Start() error = %v, want owner builder failure", err)
		}
	})

	t.Run("acquire failure", func(t *testing.T) {
		store := &testLeaseStore{
			defaultAcquire: testAcquireResult{
				err: fmt.Errorf("%w: startup unavailable", leaseidgen.ErrLeaseStore),
			},
		}
		manager := newComponentManager(context.Background(), demoConfig{
			ServiceName: "svc",
			NodeID:      6,
		}, store, testManagerSettings())

		err := manager.Start()
		if err == nil || !errors.Is(err, leaseidgen.ErrLeaseStore) {
			t.Fatalf("Start() error = %v, want wrapped ErrLeaseStore", err)
		}
	})
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

func TestComponentManagerRecoversAfterTransientAcquireFailure(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	transientStoreErr := fmt.Errorf("%w: transient acquire timeout", leaseidgen.ErrLeaseStore)
	settings := testManagerSettings()
	settings.rebuildInitialDelay = 20 * time.Millisecond
	settings.rebuildMaxDelay = 20 * time.Millisecond

	store := &testLeaseStore{
		acquireResults: []testAcquireResult{
			{acquired: true},
			{err: transientStoreErr},
			{acquired: true},
		},
	}
	manager := newComponentManager(rootCtx, demoConfig{
		ServiceName: "svc",
		NodeID:      8,
	}, store, settings)
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
	component := activeTestComponent(t, manager)

	manager.handleRuntimeFailure(component, leaseidgen.ErrLeaseLost)

	waitForCondition(t, time.Second, func() bool {
		snapshot := manager.Snapshot()
		return store.acquireCallCount() >= 2 &&
			!snapshot.Ready &&
			snapshot.ReadinessErrorClass == leaseidgen.ErrorClassStoreFailure &&
			snapshot.LastErrorClass == leaseidgen.ErrorClassStoreFailure
	})

	waitForCondition(t, time.Second, func() bool {
		snapshot := manager.Snapshot()
		return snapshot.Ready &&
			snapshot.OwnerID != "" &&
			snapshot.OwnerID != before.OwnerID &&
			store.acquireCallCount() >= 3
	})

	if err := manager.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() after transient rebuild error = %v", err)
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

func TestComponentManagerUnavailableState(t *testing.T) {
	manager := newComponentManager(context.Background(), demoConfig{
		ServiceName: "svc",
		NodeID:      12,
	}, nil, testManagerSettings())
	manager.lastOwnerID = "svc-owner-1"
	manager.lastSnapshot = leaseidgen.Snapshot{
		GenerationFenceMillis: time.Now().Add(time.Second).UnixMilli(),
	}

	manager.rebuilding = true
	manager.lastErr = leaseidgen.ErrLeaseLost
	if err := manager.componentUnavailableErrorLocked(); !errors.Is(err, leaseidgen.ErrLeaseLost) {
		t.Fatalf("componentUnavailableErrorLocked() error = %v, want ErrLeaseLost", err)
	}

	snapshot := manager.Snapshot()
	if snapshot.Lifecycle != leaseidgen.LifecycleFailed {
		t.Fatalf("snapshot lifecycle = %q, want %q", snapshot.Lifecycle, leaseidgen.LifecycleFailed)
	}
	if snapshot.NodeID != 12 {
		t.Fatalf("snapshot node_id = %d, want 12", snapshot.NodeID)
	}
	if snapshot.OwnerID != "svc-owner-1" {
		t.Fatalf("snapshot owner_id = %q, want %q", snapshot.OwnerID, "svc-owner-1")
	}
	if snapshot.ReadinessErrorClass != leaseidgen.ErrorClassLeaseLost {
		t.Fatalf("snapshot readiness_error_class = %q, want %q", snapshot.ReadinessErrorClass, leaseidgen.ErrorClassLeaseLost)
	}
	if snapshot.LastErrorClass != leaseidgen.ErrorClassLeaseLost {
		t.Fatalf("snapshot last_error_class = %q, want %q", snapshot.LastErrorClass, leaseidgen.ErrorClassLeaseLost)
	}

	manager.rebuilding = false
	manager.lastErr = nil
	snapshot = manager.Snapshot()
	if snapshot.Lifecycle != leaseidgen.LifecycleClosed {
		t.Fatalf("snapshot lifecycle = %q, want %q", snapshot.Lifecycle, leaseidgen.LifecycleClosed)
	}
	if snapshot.ReadinessErrorClass != leaseidgen.ErrorClassNone {
		t.Fatalf("snapshot readiness_error_class = %q, want %q", snapshot.ReadinessErrorClass, leaseidgen.ErrorClassNone)
	}
	if snapshot.LastErrorClass != leaseidgen.ErrorClassNone {
		t.Fatalf("snapshot last_error_class = %q, want %q", snapshot.LastErrorClass, leaseidgen.ErrorClassNone)
	}

	manager.rebuilding = true
	if err := manager.componentUnavailableErrorLocked(); err == nil || err.Error() != "idgen: component rebuilding" {
		t.Fatalf("componentUnavailableErrorLocked() error = %v, want rebuilding error", err)
	}

	manager.rebuilding = false
	if err := manager.componentUnavailableErrorLocked(); err == nil || err.Error() != "idgen: component unavailable" {
		t.Fatalf("componentUnavailableErrorLocked() error = %v, want unavailable error", err)
	}
}

func TestDemoServerHandlersSuccessPath(t *testing.T) {
	manager, cleanup := newStartedTestManager(t, demoConfig{
		ServiceName: "svc",
		NodeID:      15,
	}, &testLeaseStore{
		acquireResults: []testAcquireResult{
			{acquired: true},
		},
	})
	defer cleanup()

	server := newDemoServer("svc", manager).routes()
	snapshot := manager.Snapshot()
	expectedOwnerID := leaseidgen.RedactOwnerID(snapshot.OwnerID)

	t.Run("healthz", func(t *testing.T) {
		payload := performJSONRequest(t, server, http.MethodGet, "/healthz")
		if payload["status"] != "ok" {
			t.Fatalf("status = %v, want ok", payload["status"])
		}
		if payload["service"] != "svc" {
			t.Fatalf("service = %v, want svc", payload["service"])
		}
	})

	t.Run("readyz", func(t *testing.T) {
		payload := performJSONRequest(t, server, http.MethodGet, "/readyz")
		if payload["status"] != "ready" {
			t.Fatalf("status = %v, want ready", payload["status"])
		}

		checks, ok := payload["checks"].(map[string]any)
		if !ok {
			t.Fatalf("checks = %T, want map[string]any", payload["checks"])
		}
		if checks["id_generator"] != "ok" {
			t.Fatalf("checks.id_generator = %v, want ok", checks["id_generator"])
		}
	})

	t.Run("next", func(t *testing.T) {
		payload := performJSONRequest(t, server, http.MethodGet, "/next")
		rawID, ok := payload["id"].(string)
		if !ok || rawID == "" {
			t.Fatalf("id = %v, want non-empty string", payload["id"])
		}
		if _, err := strconv.ParseInt(rawID, 10, 64); err != nil {
			t.Fatalf("parse id %q: %v", rawID, err)
		}
		if payload["node_id"] != float64(15) {
			t.Fatalf("node_id = %v, want 15", payload["node_id"])
		}
		if payload["owner_id"] != expectedOwnerID {
			t.Fatalf("owner_id = %v, want %q", payload["owner_id"], expectedOwnerID)
		}
	})

	t.Run("snapshot", func(t *testing.T) {
		payload := performJSONRequest(t, server, http.MethodGet, "/snapshot")
		if payload["node_id"] != float64(15) {
			t.Fatalf("node_id = %v, want 15", payload["node_id"])
		}
		if payload["owner_id"] != expectedOwnerID {
			t.Fatalf("owner_id = %v, want %q", payload["owner_id"], expectedOwnerID)
		}
		if payload["lifecycle"] != string(leaseidgen.LifecycleActive) {
			t.Fatalf("lifecycle = %v, want %q", payload["lifecycle"], leaseidgen.LifecycleActive)
		}
		if payload["ready"] != true {
			t.Fatalf("ready = %v, want true", payload["ready"])
		}
		failures, ok := payload["failures"].(map[string]any)
		if !ok {
			t.Fatalf("failures = %T, want map[string]any", payload["failures"])
		}
		if failures["lease_lost"] != float64(0) {
			t.Fatalf("failures.lease_lost = %v, want 0", failures["lease_lost"])
		}
	})
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

func TestShutdownDemoStopsStartedResources(t *testing.T) {
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()

	store := &testLeaseStore{
		acquireResults: []testAcquireResult{
			{acquired: true},
		},
	}
	manager := newComponentManager(rootCtx, demoConfig{
		ServiceName: "svc",
		NodeID:      16,
	}, store, testManagerSettings())
	if err := manager.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	primaryErr := errors.New("primary shutdown failure")
	cancelCalled := false
	err := shutdownDemo(primaryErr, &http.Server{}, manager, func() {
		cancelCalled = true
		rootCancel()
	})
	if !cancelCalled {
		t.Fatal("cancelRoot was not called")
	}
	if !errors.Is(err, primaryErr) {
		t.Fatalf("shutdownDemo() error = %v, want wrapped primary error", err)
	}
	if manager.component != nil {
		t.Fatal("manager.component != nil, want component to be cleared after shutdown")
	}
}

func TestStripExpectedStopError(t *testing.T) {
	expected := leaseidgen.ErrLeaseLost
	unexpected := errors.New("unexpected close failure")

	tests := []struct {
		name    string
		stopErr error
		wantNil bool
		wantErr error
	}{
		{name: "nil stop error", stopErr: nil, wantNil: true},
		{name: "no expected error", stopErr: unexpected, wantErr: unexpected},
		{name: "expected only", stopErr: expected, wantNil: true, wantErr: expected},
		{name: "joined keeps unexpected", stopErr: errors.Join(expected, unexpected), wantErr: unexpected},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := stripExpectedStopError(test.stopErr, expected)
			if test.wantNil {
				if got != nil {
					t.Fatalf("stripExpectedStopError() = %v, want nil", got)
				}
				return
			}
			if !errors.Is(got, test.wantErr) {
				t.Fatalf("stripExpectedStopError() = %v, want wrapped %v", got, test.wantErr)
			}
		})
	}
}

func TestSleepWithContext(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepWithContext(canceled, 10*time.Millisecond) {
		t.Fatal("sleepWithContext(canceled) = true, want false")
	}

	if !sleepWithContext(context.Background(), 10*time.Millisecond) {
		t.Fatal("sleepWithContext(background) = false, want true")
	}
}

func TestServeHTTPReturnsListenError(t *testing.T) {
	err := serveHTTP(&http.Server{
		Addr: "127.0.0.1:-1",
	})
	if err == nil {
		t.Fatal("serveHTTP() error = nil, want listen error")
	}
}

func newStartedTestManager(t *testing.T, cfg demoConfig, store leaseidgen.LeaseStore) (*componentManager, func()) {
	t.Helper()

	rootCtx, cancel := context.WithCancel(context.Background())
	manager := newComponentManager(rootCtx, cfg, store, testManagerSettings())
	if err := manager.Start(); err != nil {
		cancel()
		t.Fatalf("Start() error = %v", err)
	}

	return manager, func() {
		cancel()
		if err := manager.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	}
}

func activeTestComponent(t *testing.T, manager *componentManager) *managedComponent {
	t.Helper()

	manager.mu.RLock()
	component := manager.component
	manager.mu.RUnlock()
	if component == nil {
		t.Fatal("component = nil, want active component")
	}
	return component
}

func performJSONRequest(t *testing.T, handler http.Handler, method string, path string) map[string]any {
	t.Helper()

	request := httptest.NewRequest(method, path, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}

	return decodeJSONPayload(t, recorder.Body.Bytes())
}

func decodeJSONPayload(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return payload
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
