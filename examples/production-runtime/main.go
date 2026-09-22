package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	idgen "github.com/ZhcChen/cc-snowflake-id-go/generator"
	leaseidgen "github.com/ZhcChen/cc-snowflake-id-go/lease"
	"github.com/jackc/pgx/v5/pgxpool"
)

var errHostUnavailable = errors.New("idgen: production host unavailable")

type runtimeGenerator interface {
	idgen.NextGenerator
	leaseidgen.RuntimeGenerator
	leaseidgen.SnapshotSource
	Ready(context.Context) error
}

type component struct {
	ownerID   string
	telemetry *leaseidgen.Telemetry
	generator runtimeGenerator
	runtime   *leaseidgen.Runtime
	stopLog   func()
}

type Config struct {
	DatabaseURL           string
	ServiceName           string
	NodeID                int
	ListenAddress         string
	LeaseWindow           time.Duration
	FenceWindow           time.Duration
	LeaseRefreshInterval  time.Duration
	LeaseAcquireTimeout   time.Duration
	LeaseOperationTimeout time.Duration
	MaxClockSkew          time.Duration
	SmallRollbackWait     time.Duration
	ShutdownTimeout       time.Duration
	ReportInterval        time.Duration
}

func loadConfig(env func(string) string) (Config, error) {
	get := func(key, fallback string) string {
		if value := env(key); value != "" {
			return value
		}
		return fallback
	}
	parseDuration := func(key, fallback string) (time.Duration, error) {
		value, err := time.ParseDuration(get(key, fallback))
		if err != nil {
			return 0, fmt.Errorf("%s: %w", key, err)
		}
		return value, nil
	}
	if get("IDGEN_DATABASE_URL", "") == "" {
		return Config{}, errors.New("IDGEN_DATABASE_URL is required")
	}
	nodeID, err := strconv.Atoi(get("IDGEN_NODE_ID", "1"))
	if err != nil {
		return Config{}, fmt.Errorf("IDGEN_NODE_ID: %w", err)
	}
	if err := idgen.ValidateNodeID(nodeID); err != nil {
		return Config{}, fmt.Errorf("IDGEN_NODE_ID: %w", err)
	}
	leaseWindow, err := parseDuration("IDGEN_LEASE_WINDOW", "10s")
	if err != nil {
		return Config{}, err
	}
	fenceWindow, err := parseDuration("IDGEN_FENCE_WINDOW", "10s")
	if err != nil {
		return Config{}, err
	}
	refreshInterval, err := parseDuration("IDGEN_LEASE_REFRESH_INTERVAL", "3s")
	if err != nil {
		return Config{}, err
	}
	operationTimeout, err := parseDuration("IDGEN_LEASE_OPERATION_TIMEOUT", "1s")
	if err != nil {
		return Config{}, err
	}
	acquireTimeout, err := parseDuration("IDGEN_LEASE_ACQUIRE_TIMEOUT", "5s")
	if err != nil {
		return Config{}, err
	}
	maxClockSkew, err := parseDuration("IDGEN_MAX_CLOCK_SKEW", "1s")
	if err != nil {
		return Config{}, err
	}
	rollbackWait, err := parseDuration("IDGEN_SMALL_ROLLBACK_WAIT", "10ms")
	if err != nil {
		return Config{}, err
	}
	shutdownTimeout, err := parseDuration("IDGEN_SHUTDOWN_TIMEOUT", "5s")
	if err != nil {
		return Config{}, err
	}
	reportInterval, err := parseDuration("IDGEN_REPORT_INTERVAL", "30s")
	if err != nil {
		return Config{}, err
	}
	if acquireTimeout < 0 || rollbackWait < 0 {
		return Config{}, errors.New("IDGEN_LEASE_ACQUIRE_TIMEOUT and IDGEN_SMALL_ROLLBACK_WAIT must not be negative")
	}
	if refreshInterval <= 0 || operationTimeout <= 0 || leaseWindow <= refreshInterval+operationTimeout || fenceWindow <= refreshInterval+operationTimeout || maxClockSkew <= 0 || shutdownTimeout <= 0 || reportInterval <= 0 {
		return Config{}, errors.New("lease/fence windows must exceed refresh interval plus operation timeout; all other durations must be positive")
	}
	return Config{
		DatabaseURL: get("IDGEN_DATABASE_URL", ""), ServiceName: get("IDGEN_SERVICE_NAME", "cc-snowflake-id-production-demo"), NodeID: nodeID,
		ListenAddress: get("IDGEN_LISTEN_ADDRESS", ":8080"), LeaseWindow: leaseWindow, FenceWindow: fenceWindow,
		LeaseRefreshInterval: refreshInterval, LeaseAcquireTimeout: acquireTimeout, LeaseOperationTimeout: operationTimeout,
		MaxClockSkew: maxClockSkew, SmallRollbackWait: rollbackWait, ShutdownTimeout: shutdownTimeout, ReportInterval: reportInterval,
	}, nil
}

type host struct {
	mu          sync.RWMutex
	startMu     sync.Mutex
	logger      *slog.Logger
	service     string
	cfg         Config
	store       leaseidgen.LeaseStore
	rootCtx     context.Context
	cancel      context.CancelFunc
	component   *component
	rebuilding  bool
	closed      bool
	started     bool
	lastErr     error
	lastSnap    leaseidgen.Snapshot
	build       func(context.Context) (*component, error)
	rebuildDone chan struct{}
}

func newHost(ctx context.Context, logger *slog.Logger, cfg Config, store leaseidgen.LeaseStore) (*host, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}
	rootCtx, cancel := context.WithCancel(ctx)
	h := &host{logger: logger, service: cfg.ServiceName, cfg: cfg, store: store, rootCtx: rootCtx, cancel: cancel}
	h.build = h.buildComponent
	component, err := h.buildWithRetry(rootCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	h.component = component
	h.lastSnap = component.generator.Snapshot()
	return h, nil
}

func (h *host) buildComponent(ctx context.Context) (*component, error) {
	ownerID, err := leaseidgen.NewOwnerID(h.service)
	if err != nil {
		return nil, err
	}
	telemetry := leaseidgen.NewTelemetry()
	generator, err := leaseidgen.NewLeasedGenerator(h.store, nil, leaseidgen.LeasedGeneratorConfig{
		NodeID: h.cfg.NodeID, OwnerID: ownerID, SmallRollbackWait: h.cfg.SmallRollbackWait,
		LeaseWindow: h.cfg.LeaseWindow, FenceWindow: h.cfg.FenceWindow, MaxClockSkew: h.cfg.MaxClockSkew,
		LeaseRefreshInterval: h.cfg.LeaseRefreshInterval, LeaseAcquireTimeout: h.cfg.LeaseAcquireTimeout,
		LeaseOperationTimeout: h.cfg.LeaseOperationTimeout, Observer: telemetry,
	})
	if err != nil {
		return nil, err
	}
	if _, err := generator.Acquire(ctx); err != nil {
		_ = generator.Close(context.Background())
		return nil, fmt.Errorf("acquire lease: %w", err)
	}
	return &component{ownerID: ownerID, telemetry: telemetry, generator: generator}, nil
}

func (h *host) buildWithRetry(ctx context.Context) (*component, error) {
	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		component, err := h.build(ctx)
		if err == nil {
			return component, nil
		}
		lastErr = err
		if attempt == 5 {
			break
		}
		delay := time.Duration(1<<(attempt-1)) * 500 * time.Millisecond
		h.logger.Warn("idgen startup acquire failed; retrying", "attempt", attempt, "retry_in", delay, "error", err)
		if !sleepContext(ctx, delay) {
			return nil, errors.Join(lastErr, ctx.Err())
		}
	}
	return nil, lastErr
}

func (h *host) Start() error {
	h.startMu.Lock()
	defer h.startMu.Unlock()
	h.mu.Lock()
	if h.closed || h.started {
		h.mu.Unlock()
		return errors.New("idgen: production host already started or closed")
	}
	component := h.component
	h.mu.Unlock()
	if component == nil {
		return errHostUnavailable
	}
	if err := h.startComponent(component); err != nil {
		return err
	}
	h.mu.Lock()
	h.started = true
	h.mu.Unlock()
	go h.watch(component)
	return nil
}

func (h *host) startComponent(component *component) error {
	runtime, err := leaseidgen.StartRuntime(h.rootCtx, component.generator)
	if err != nil {
		return err
	}
	component.runtime = runtime
	component.stopLog = h.startReporter(component)
	h.logger.Info("idgen component started", "event", "component_started", "node_id", h.cfg.NodeID, "owner_id", leaseidgen.RedactOwnerID(component.ownerID))
	return nil
}

func (h *host) watch(component *component) {
	<-component.runtime.Done()
	if h.rootCtx.Err() != nil || component.runtime.Err() == nil {
		return
	}
	err := component.runtime.Err()
	h.logger.Error("idgen runtime failed", "event", "runtime_failed", "node_id", h.cfg.NodeID, "error_class", leaseidgen.ClassifyError(err), "error", err)
	h.mu.Lock()
	if h.component != component {
		h.mu.Unlock()
		return
	}
	h.component = nil
	h.rebuilding = true
	h.lastErr = err
	h.lastSnap = component.generator.Snapshot()
	h.lastSnap.Ready = false
	h.rebuildDone = make(chan struct{})
	h.mu.Unlock()
	stopCtx, cancel := context.WithTimeout(context.Background(), h.cfg.ShutdownTimeout)
	_ = component.runtime.Stop(stopCtx)
	if component.stopLog != nil {
		component.stopLog()
	}
	cancel()
	go func() {
		defer close(h.rebuildDone)
		h.rebuild()
	}()
}

func (h *host) rebuild() {
	for attempt := 1; ; attempt++ {
		if h.rootCtx.Err() != nil {
			h.finishRebuild()
			return
		}
		h.logger.Info("idgen component rebuild started", "event", "component_rebuild_started", "attempt", attempt)
		component, err := h.build(h.rootCtx)
		if err == nil {
			err = h.startComponent(component)
		}
		if err == nil && h.rootCtx.Err() != nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), h.cfg.ShutdownTimeout)
			err = component.runtime.Stop(stopCtx)
			if component.stopLog != nil {
				component.stopLog()
			}
			cancel()
		}
		if err == nil {
			h.mu.Lock()
			closed := h.closed || h.rootCtx.Err() != nil
			if !closed {
				h.component = component
				h.rebuilding = false
				h.lastErr = nil
				h.lastSnap = component.generator.Snapshot()
			}
			h.mu.Unlock()
			if closed {
				stopCtx, cancel := context.WithTimeout(context.Background(), h.cfg.ShutdownTimeout)
				_ = component.runtime.Stop(stopCtx)
				if component.stopLog != nil {
					component.stopLog()
				}
				cancel()
				return
			}
			h.logger.Info("idgen component rebuilt", "event", "component_rebuilt", "owner_id", leaseidgen.RedactOwnerID(component.ownerID))
			go h.watch(component)
			return
		}
		h.logger.Error("idgen component rebuild failed", "event", "component_rebuild_failed", "attempt", attempt, "error_class", leaseidgen.ClassifyError(err), "error", err)
		if component != nil && component.runtime != nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), h.cfg.ShutdownTimeout)
			_ = component.runtime.Stop(stopCtx)
			cancel()
		}
		if !sleepContext(h.rootCtx, h.cfg.LeaseRefreshInterval) {
			h.finishRebuild()
			return
		}
	}
}

func (h *host) finishRebuild() {
	h.mu.Lock()
	h.rebuilding = false
	h.mu.Unlock()
}

func (h *host) Next(ctx context.Context) (int64, error) {
	h.mu.RLock()
	component := h.component
	h.mu.RUnlock()
	if component == nil {
		return 0, errHostUnavailable
	}
	return component.generator.Next(ctx)
}

func (h *host) Ready(ctx context.Context) error {
	h.mu.RLock()
	component := h.component
	lastErr := h.lastErr
	rebuilding := h.rebuilding
	h.mu.RUnlock()
	if component == nil {
		if lastErr != nil {
			return fmt.Errorf("idgen: rebuilding: %w", lastErr)
		}
		if rebuilding {
			return errors.New("idgen: rebuilding")
		}
		return errHostUnavailable
	}
	return component.generator.Ready(ctx)
}

func (h *host) Snapshot() leaseidgen.Snapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.component != nil {
		return h.component.generator.Snapshot()
	}
	snapshot := h.lastSnap
	snapshot.Ready = false
	snapshot.NodeID = h.cfg.NodeID
	snapshot.ReadinessErrorClass = leaseidgen.ClassifyError(h.lastErr)
	return snapshot
}

func (h *host) Close(ctx context.Context) error {
	if h == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	h.cancel()
	h.mu.Lock()
	component := h.component
	rebuildDone := h.rebuildDone
	h.closed = true
	h.component = nil
	h.rebuilding = false
	h.mu.Unlock()
	if rebuildDone != nil {
		select {
		case <-rebuildDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if component == nil {
		return nil
	}
	var err error
	if component.runtime != nil {
		err = component.runtime.Stop(ctx)
	} else if component.generator != nil {
		err = component.generator.Close(ctx)
	}
	if component.stopLog != nil {
		component.stopLog()
	}
	return err
}

func (h *host) startReporter(component *component) func() {
	ctx, cancel := context.WithCancel(h.rootCtx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = leaseidgen.RunReporter(ctx, h.cfg.ReportInterval, component.generator, component.telemetry,
			func(report leaseidgen.StatusReport) {
				h.logger.Info("idgen status", "event", "idgen_status", "node_id", report.NodeID, "ready", report.Ready, "lifecycle", report.Lifecycle, "generated_total", report.GeneratedTotal, "refresh_failure_total", report.RefreshFailureTotal)
			},
			func(event leaseidgen.Event) {
				h.logger.Error("idgen event", "event", "idgen_event", "action", event.Kind, "node_id", event.NodeID, "owner_id", leaseidgen.RedactOwnerID(event.OwnerID), "error_class", event.ErrorClass, "error", event.Err)
			},
		)
	}()
	return func() {
		cancel()
		<-done
	}
}

func newHandler(h *host) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := h.Ready(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "error": "id_generator_unavailable", "checks": map[string]string{"id_generator": "error"}})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "checks": map[string]string{"id_generator": "ok"}})
	})
	mux.HandleFunc("/id", func(w http.ResponseWriter, r *http.Request) {
		value, err := h.Next(r.Context())
		if err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "id_generator_unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": strconv.FormatInt(value, 10)})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		logger.Error("invalid idgen config", "event", "idgen_config_failed", "error", err)
		os.Exit(1)
	}
	logger.Info("idgen config", "event", "idgen_config", "service", cfg.ServiceName, "node_id", cfg.NodeID,
		"lease_window", cfg.LeaseWindow, "fence_window", cfg.FenceWindow,
		"refresh_interval", cfg.LeaseRefreshInterval, "operation_timeout", cfg.LeaseOperationTimeout)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("open postgres pool failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		logger.Error("postgres ping failed", "error", err)
		os.Exit(1)
	}
	store, err := leaseidgen.NewPGLeaseStore(pool)
	if err != nil {
		logger.Error("create lease store failed", "error", err)
		os.Exit(1)
	}
	h, err := newHost(ctx, logger, cfg, store)
	if err != nil {
		logger.Error("create idgen host failed", "error", err)
		os.Exit(1)
	}
	if err := h.Start(); err != nil {
		logger.Error("start idgen host failed", "error", err)
		os.Exit(1)
	}
	server := &http.Server{Addr: cfg.ListenAddress, Handler: newHandler(h)}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		_ = h.Close(shutdownCtx)
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		_ = h.Close(shutdownCtx)
		cancel()
	}
}
