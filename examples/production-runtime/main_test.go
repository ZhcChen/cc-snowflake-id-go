package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ZhcChen/cc-snowflake-id-go/lease"
)

func TestLoadConfigValidatesRequiredAndTimingValues(t *testing.T) {
	values := map[string]string{"IDGEN_DATABASE_URL": "postgres://test", "IDGEN_LEASE_WINDOW": "5s", "IDGEN_FENCE_WINDOW": "5s", "IDGEN_LEASE_REFRESH_INTERVAL": "1s", "IDGEN_LEASE_OPERATION_TIMEOUT": "1s"}
	get := func(key string) string { return values[key] }
	if _, err := loadConfig(get); err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	delete(values, "IDGEN_DATABASE_URL")
	if _, err := loadConfig(get); err == nil || !strings.Contains(err.Error(), "IDGEN_DATABASE_URL") {
		t.Fatalf("missing database URL error = %v", err)
	}
	values["IDGEN_DATABASE_URL"] = "postgres://test"
	values["IDGEN_LEASE_ACQUIRE_TIMEOUT"] = "-1s"
	if _, err := loadConfig(get); err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("negative acquire timeout error = %v", err)
	}
}

func TestHostCloseBeforeStartClosesAcquiredGenerator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	generator := &fakeGenerator{owner: "owner-1", next: 1, ready: true}
	h := &host{rootCtx: ctx, cancel: cancel, cfg: Config{NodeID: 1}, component: &component{generator: generator}}
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !generator.wasClosed() {
		t.Fatal("generator was not closed before runtime start")
	}
}

func TestProductionHandlerReturnsReadinessAndStringID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	generator := &fakeGenerator{ready: true, next: 9007199254740993}
	h := &host{rootCtx: ctx, cancel: cancel, cfg: Config{NodeID: 1}, component: &component{generator: generator}}
	handler := newHandler(h)
	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("ready status = %d", ready.Code)
	}
	idResponse := httptest.NewRecorder()
	handler.ServeHTTP(idResponse, httptest.NewRequest(http.MethodGet, "/id", nil))
	var payload map[string]any
	if err := json.Unmarshal(idResponse.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload["id"].(string); !ok {
		t.Fatalf("id payload = %#v, want string", payload)
	}
	generator.setReady(false)
	notReady := httptest.NewRecorder()
	handler.ServeHTTP(notReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if notReady.Code != http.StatusServiceUnavailable {
		t.Fatalf("not-ready status = %d", notReady.Code)
	}
}

func TestHostRebuildsAfterRuntimeFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := &fakeGenerator{owner: "owner-1", next: 1, fail: make(chan struct{})}
	second := &fakeGenerator{owner: "owner-2", next: 2, ready: true, stop: make(chan struct{})}
	builds := 0
	h := &host{
		logger: slog.Default(), service: "test", cfg: Config{NodeID: 1, LeaseRefreshInterval: time.Millisecond, ShutdownTimeout: time.Second, ReportInterval: time.Hour}, rootCtx: ctx, cancel: cancel,
		build: func(context.Context) (*component, error) {
			builds++
			if builds == 1 {
				return &component{ownerID: first.owner, generator: first}, nil
			}
			return &component{ownerID: second.owner, generator: second}, nil
		}, component: &component{ownerID: first.owner, generator: first},
	}
	if err := h.Start(); err != nil {
		t.Fatal(err)
	}
	close(first.fail)
	waitUntil(t, time.Second, func() bool { return h.Snapshot().OwnerID == "owner-2" && h.Ready(context.Background()) == nil })
	value, err := h.Next(context.Background())
	if err != nil || value != 2 {
		t.Fatalf("rebuilt Next() = %d, %v", value, err)
	}
	_ = h.Close(context.Background())
}

func TestHostStartIsSingleUse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	generator := &fakeGenerator{owner: "owner-1", next: 1, ready: true}
	h := &host{logger: slog.Default(), cfg: Config{NodeID: 1, ReportInterval: time.Hour}, rootCtx: ctx, cancel: cancel, component: &component{ownerID: generator.owner, generator: generator}}
	if err := h.Start(); err != nil {
		t.Fatal(err)
	}
	if err := h.Start(); err == nil {
		t.Fatal("second Start() succeeded")
	}
	_ = h.Close(context.Background())
}

type fakeGenerator struct {
	mu     sync.Mutex
	owner  string
	next   int64
	ready  bool
	fail   chan struct{}
	stop   chan struct{}
	closed bool
}

func (g *fakeGenerator) Next(context.Context) (int64, error) {
	if err := g.Ready(context.Background()); err != nil {
		return 0, err
	}
	return g.next, nil
}

func (g *fakeGenerator) Ready(context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.ready && g.fail == nil {
		return errors.New("not ready")
	}
	return nil
}

func (g *fakeGenerator) RunRefreshLoop(ctx context.Context) error {
	if g.fail == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-g.fail:
		return errors.New("refresh failed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *fakeGenerator) Close(context.Context) error {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	if g.stop != nil {
		select {
		case <-g.stop:
		default:
			close(g.stop)
		}
	}
	return nil
}

func (g *fakeGenerator) wasClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

func (g *fakeGenerator) Snapshot() lease.Snapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	return lease.Snapshot{NodeID: 1, OwnerID: g.owner, Ready: g.ready || g.fail != nil, Lifecycle: lease.LifecycleActive}
}

func (g *fakeGenerator) setReady(ready bool) {
	g.mu.Lock()
	g.ready = ready
	g.fail = nil
	g.mu.Unlock()
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}
