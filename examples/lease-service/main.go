package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	sf "github.com/ZhcChen/cc-snowflake-id-go/generator"
	leaseidgen "github.com/ZhcChen/cc-snowflake-id-go/lease"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultHTTPAddr    = ":8080"
	defaultServiceName = "cc-snowflake-id-go-lease-service-demo"
	defaultNodeID      = 100
	demoRole           = "service"

	reportInterval        = 15 * time.Second
	leaseWindow           = 15 * time.Second
	leaseAcquireTimeout   = 5 * time.Second
	leaseOperationTimeout = 2 * time.Second
	leaseRefreshInterval  = 5 * time.Second
	rebuildInitialDelay   = time.Second
	rebuildMaxDelay       = 10 * time.Second
	componentStopTimeout  = 10 * time.Second
)

var stdoutMu sync.Mutex

type demoConfig struct {
	DatabaseURL string
	HTTPAddr    string
	ServiceName string
	NodeID      int
}

type managedComponent struct {
	ownerID        string
	telemetry      *leaseidgen.Telemetry
	generator      *leaseidgen.LeasedGenerator
	runtime        *leaseidgen.Runtime
	cancelReporter context.CancelFunc
}

type componentManagerSettings struct {
	leaseWindow           time.Duration
	fenceWindow           time.Duration
	leaseAcquireTimeout   time.Duration
	leaseOperationTimeout time.Duration
	leaseRefreshInterval  time.Duration
	rebuildInitialDelay   time.Duration
	rebuildMaxDelay       time.Duration
	componentStopTimeout  time.Duration
	clock                 sf.Clock
	ownerIDBuilder        func(string) (string, error)
	startReporter         func(context.Context, string, *leaseidgen.Telemetry, leaseidgen.SnapshotSource)
}

// componentManager 负责持有当前可用的雪花 ID 组件，并在终态后重建它。
type componentManager struct {
	rootCtx context.Context
	cancel  context.CancelFunc
	cfg     demoConfig
	store   leaseidgen.LeaseStore
	config  componentManagerSettings

	mu           sync.RWMutex
	component    *managedComponent
	rebuilding   bool
	lastErr      error
	lastOwnerID  string
	lastSnapshot leaseidgen.Snapshot
}

type demoServer struct {
	serviceName string
	manager     *componentManager
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "lease-service demo: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	signalCtx, stopSignal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignal()

	rootCtx, cancelRoot := context.WithCancel(signalCtx)
	defer cancelRoot()

	pool, err := pgxpool.New(rootCtx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open postgres pool: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(rootCtx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	store, err := leaseidgen.NewPGLeaseStore(pool)
	if err != nil {
		return fmt.Errorf("create lease store: %w", err)
	}

	settings := defaultComponentManagerSettings()
	manager := newComponentManager(rootCtx, cfg, store, settings)
	if err := manager.Start(); err != nil {
		return fmt.Errorf("start id component: %w", err)
	}

	snapshot := manager.Snapshot()
	emitJSON(map[string]any{
		"diagnostic_scope":           "snowflake_id",
		"event":                      "idgen_config",
		"role":                       demoRole,
		"service":                    cfg.ServiceName,
		"node_id":                    cfg.NodeID,
		"owner_id":                   leaseidgen.RedactOwnerID(snapshot.OwnerID),
		"lease_window_ms":            settings.leaseWindow.Milliseconds(),
		"fence_window_ms":            settings.fenceWindow.Milliseconds(),
		"lease_refresh_interval_ms":  settings.leaseRefreshInterval.Milliseconds(),
		"lease_operation_timeout_ms": settings.leaseOperationTimeout.Milliseconds(),
		"lease_acquire_timeout_ms":   settings.leaseAcquireTimeout.Milliseconds(),
		"component_rebuild_strategy": "component_rebuild",
		"rebuild_initial_delay_ms":   settings.rebuildInitialDelay.Milliseconds(),
		"rebuild_max_delay_ms":       settings.rebuildMaxDelay.Milliseconds(),
		"http_addr":                  cfg.HTTPAddr,
	})

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: newDemoServer(cfg.ServiceName, manager).routes(),
	}

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- serveHTTP(server)
	}()

	select {
	case err := <-serveErrCh:
		return shutdownDemo(err, server, manager, cancelRoot)
	case <-rootCtx.Done():
		return shutdownDemo(nil, server, manager, cancelRoot)
	}
}

func loadConfig() (demoConfig, error) {
	cfg := demoConfig{
		DatabaseURL: stringsTrim(os.Getenv("IDGEN_DATABASE_URL")),
		HTTPAddr:    valueOrDefault(stringsTrim(os.Getenv("IDGEN_HTTP_ADDR")), defaultHTTPAddr),
		ServiceName: valueOrDefault(stringsTrim(os.Getenv("IDGEN_SERVICE_NAME")), defaultServiceName),
		NodeID:      defaultNodeID,
	}
	if cfg.DatabaseURL == "" {
		return demoConfig{}, errors.New("IDGEN_DATABASE_URL is required")
	}

	nodeIDRaw := stringsTrim(os.Getenv("IDGEN_NODE_ID"))
	if nodeIDRaw == "" {
		return cfg, nil
	}

	nodeID, err := strconv.Atoi(nodeIDRaw)
	if err != nil {
		return demoConfig{}, fmt.Errorf("parse IDGEN_NODE_ID: %w", err)
	}
	cfg.NodeID = nodeID
	return cfg, nil
}

func newComponentManager(
	rootCtx context.Context,
	cfg demoConfig,
	store leaseidgen.LeaseStore,
	settings componentManagerSettings,
) *componentManager {
	managerCtx, cancel := context.WithCancel(rootCtx)
	return &componentManager{
		rootCtx: managerCtx,
		cancel:  cancel,
		cfg:     cfg,
		store:   store,
		config:  settings.withDefaults(),
	}
}

func defaultComponentManagerSettings() componentManagerSettings {
	return componentManagerSettings{
		leaseWindow:           leaseWindow,
		fenceWindow:           leaseWindow,
		leaseAcquireTimeout:   leaseAcquireTimeout,
		leaseOperationTimeout: leaseOperationTimeout,
		leaseRefreshInterval:  leaseRefreshInterval,
		rebuildInitialDelay:   rebuildInitialDelay,
		rebuildMaxDelay:       rebuildMaxDelay,
		componentStopTimeout:  componentStopTimeout,
		clock:                 nil,
		ownerIDBuilder:        leaseidgen.NewOwnerID,
		startReporter:         startReporter,
	}
}

func (s componentManagerSettings) withDefaults() componentManagerSettings {
	defaults := defaultComponentManagerSettings()

	if s.leaseWindow <= 0 {
		s.leaseWindow = defaults.leaseWindow
	}
	if s.fenceWindow <= 0 {
		s.fenceWindow = s.leaseWindow
	}
	if s.leaseAcquireTimeout <= 0 {
		s.leaseAcquireTimeout = defaults.leaseAcquireTimeout
	}
	if s.leaseOperationTimeout <= 0 {
		s.leaseOperationTimeout = defaults.leaseOperationTimeout
	}
	if s.leaseRefreshInterval <= 0 {
		s.leaseRefreshInterval = defaults.leaseRefreshInterval
	}
	if s.rebuildInitialDelay <= 0 {
		s.rebuildInitialDelay = defaults.rebuildInitialDelay
	}
	if s.rebuildMaxDelay <= 0 {
		s.rebuildMaxDelay = defaults.rebuildMaxDelay
	}
	if s.componentStopTimeout <= 0 {
		s.componentStopTimeout = defaults.componentStopTimeout
	}
	if s.ownerIDBuilder == nil {
		s.ownerIDBuilder = defaults.ownerIDBuilder
	}
	if s.startReporter == nil {
		s.startReporter = defaults.startReporter
	}
	return s
}

func (m *componentManager) Start() error {
	component, err := m.buildComponent(m.rootCtx)
	if err != nil {
		return err
	}
	m.installComponent(component, "component_started", 1)
	return nil
}

func (m *componentManager) Ready(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.component == nil {
		return m.componentUnavailableErrorLocked()
	}
	return m.component.generator.Ready(ctx)
}

func (m *componentManager) Next(ctx context.Context) (int64, leaseidgen.LeaseState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.component == nil {
		return 0, leaseidgen.LeaseState{
			NodeID:  m.cfg.NodeID,
			OwnerID: m.lastOwnerID,
		}, m.componentUnavailableErrorLocked()
	}

	value, err := m.component.generator.Next(ctx)
	return value, m.component.generator.State(), err
}

func (m *componentManager) Snapshot() leaseidgen.Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.component != nil {
		return m.component.generator.Snapshot()
	}
	return m.unavailableSnapshotLocked()
}

func (m *componentManager) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.cancel != nil {
		m.cancel()
	}

	m.mu.Lock()
	component := m.component
	m.component = nil
	m.rebuilding = false
	m.mu.Unlock()

	return m.stopComponent(component, ctx)
}

func (m *componentManager) buildComponent(ctx context.Context) (*managedComponent, error) {
	ownerID, err := m.config.ownerIDBuilder(m.cfg.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("build owner id: %w", err)
	}

	telemetry := leaseidgen.NewTelemetry()
	generator, err := leaseidgen.NewLeasedGenerator(m.store, m.config.clock, leaseidgen.LeasedGeneratorConfig{
		NodeID:                m.cfg.NodeID,
		OwnerID:               ownerID,
		LeaseWindow:           m.config.leaseWindow,
		FenceWindow:           m.config.fenceWindow,
		LeaseAcquireTimeout:   m.config.leaseAcquireTimeout,
		LeaseOperationTimeout: m.config.leaseOperationTimeout,
		LeaseRefreshInterval:  m.config.leaseRefreshInterval,
		Observer:              telemetry,
	})
	if err != nil {
		return nil, fmt.Errorf("create leased generator: %w", err)
	}

	if _, err := generator.Acquire(ctx); err != nil {
		closeErr := generator.Close(context.Background())
		return nil, errors.Join(fmt.Errorf("acquire lease: %w", err), closeErr)
	}

	runtime, err := leaseidgen.StartRuntime(ctx, generator)
	if err != nil {
		closeErr := generator.Close(context.Background())
		return nil, errors.Join(fmt.Errorf("start runtime: %w", err), closeErr)
	}

	reporterCtx, cancelReporter := context.WithCancel(context.Background())
	m.config.startReporter(reporterCtx, m.cfg.ServiceName, telemetry, generator)

	return &managedComponent{
		ownerID:        ownerID,
		telemetry:      telemetry,
		generator:      generator,
		runtime:        runtime,
		cancelReporter: cancelReporter,
	}, nil
}

func (m *componentManager) installComponent(
	component *managedComponent,
	action string,
	attempt int,
) {
	m.mu.Lock()
	m.component = component
	m.rebuilding = false
	m.lastErr = nil
	m.lastOwnerID = component.ownerID
	m.lastSnapshot = component.generator.Snapshot()
	m.mu.Unlock()

	emitJSON(map[string]any{
		"diagnostic_scope": "snowflake_id",
		"event":            "idgen_event",
		"action":           action,
		"role":             demoRole,
		"service":          m.cfg.ServiceName,
		"node_id":          m.cfg.NodeID,
		"owner_id":         leaseidgen.RedactOwnerID(component.ownerID),
		"attempt":          attempt,
	})

	go m.watchComponent(component)
}

func (m *componentManager) watchComponent(component *managedComponent) {
	if component == nil || component.runtime == nil {
		return
	}

	<-component.runtime.Done()
	err := component.runtime.Err()
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}

	m.handleRuntimeFailure(component, err)
}

func (m *componentManager) handleRuntimeFailure(component *managedComponent, err error) {
	snapshot, ok := m.markComponentForRebuild(component, err)
	if !ok {
		return
	}

	emitJSON(map[string]any{
		"diagnostic_scope": "snowflake_id",
		"event":            "idgen_event",
		"action":           "runtime_failed",
		"role":             demoRole,
		"service":          m.cfg.ServiceName,
		"node_id":          snapshot.NodeID,
		"owner_id":         leaseidgen.RedactOwnerID(snapshot.OwnerID),
		"error_class":      string(leaseidgen.ClassifyError(err)),
		"error":            err.Error(),
	})

	stopCtx, cancel := context.WithTimeout(context.Background(), m.config.componentStopTimeout)
	defer cancel()
	_ = m.stopComponent(component, stopCtx)

	go m.rebuildLoop()
}

func (m *componentManager) markComponentForRebuild(
	component *managedComponent,
	err error,
) (leaseidgen.Snapshot, bool) {
	snapshot := component.generator.Snapshot()
	snapshot.Lifecycle = leaseidgen.LifecycleFailed
	snapshot.Ready = false
	snapshot.LeaseOwned = false
	snapshot.LeaseRemainingMillis = 0
	snapshot.ReadinessErrorClass = leaseidgen.ClassifyError(err)
	snapshot.LastErrorClass = leaseidgen.ClassifyError(err)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.component != component {
		return snapshot, false
	}

	m.component = nil
	m.rebuilding = true
	m.lastErr = err
	m.lastOwnerID = component.ownerID
	m.lastSnapshot = snapshot
	return snapshot, true
}

func (m *componentManager) rebuildLoop() {
	backoff := m.config.rebuildInitialDelay
	attempt := 1

	for {
		if m.rootCtx.Err() != nil {
			m.finishRebuild()
			return
		}

		lastErr := m.lastRuntimeErr()
		fields := map[string]any{
			"diagnostic_scope": "snowflake_id",
			"event":            "idgen_event",
			"action":           "component_rebuild_started",
			"role":             demoRole,
			"service":          m.cfg.ServiceName,
			"node_id":          m.cfg.NodeID,
			"attempt":          attempt,
			"error_class":      string(leaseidgen.ClassifyError(lastErr)),
		}
		if lastErr != nil {
			fields["error"] = lastErr.Error()
		}
		emitJSON(fields)

		component, err := m.buildComponent(m.rootCtx)
		if err == nil {
			m.installComponent(component, "component_rebuilt", attempt)
			return
		}

		if m.rootCtx.Err() != nil {
			m.finishRebuild()
			return
		}

		m.recordLastError(err)
		emitJSON(map[string]any{
			"diagnostic_scope": "snowflake_id",
			"event":            "idgen_event",
			"action":           "component_rebuild_failed",
			"role":             demoRole,
			"service":          m.cfg.ServiceName,
			"node_id":          m.cfg.NodeID,
			"attempt":          attempt,
			"error_class":      string(leaseidgen.ClassifyError(err)),
			"error":            err.Error(),
		})

		if !sleepWithContext(m.rootCtx, backoff) {
			m.finishRebuild()
			return
		}

		attempt++
		if backoff < m.config.rebuildMaxDelay {
			backoff *= 2
			if backoff > m.config.rebuildMaxDelay {
				backoff = m.config.rebuildMaxDelay
			}
		}
	}
}

func (m *componentManager) stopComponent(component *managedComponent, ctx context.Context) error {
	if component == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var stopErr error
	if component.runtime != nil {
		expectedRuntimeErr := component.runtime.Err()
		stopErr = stripExpectedStopError(component.runtime.Stop(ctx), expectedRuntimeErr)
	} else if component.generator != nil {
		stopErr = component.generator.Close(ctx)
	}

	if component.cancelReporter != nil {
		component.cancelReporter()
	}
	return stopErr
}

func (m *componentManager) finishRebuild() {
	m.mu.Lock()
	m.rebuilding = false
	m.mu.Unlock()
}

func (m *componentManager) lastRuntimeErr() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastErr
}

func (m *componentManager) recordLastError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastErr = err
	m.lastSnapshot.ReadinessErrorClass = leaseidgen.ClassifyError(err)
	m.lastSnapshot.LastErrorClass = leaseidgen.ClassifyError(err)
}

func (m *componentManager) componentUnavailableErrorLocked() error {
	if m.lastErr != nil {
		return fmt.Errorf("idgen: component rebuilding: %w", m.lastErr)
	}
	if m.rebuilding {
		return errors.New("idgen: component rebuilding")
	}
	return errors.New("idgen: component unavailable")
}

func (m *componentManager) unavailableSnapshotLocked() leaseidgen.Snapshot {
	snapshot := m.lastSnapshot
	snapshot.CapturedAtMillis = time.Now().UnixMilli()
	if snapshot.NodeID == 0 {
		snapshot.NodeID = m.cfg.NodeID
	}
	if snapshot.OwnerID == "" {
		snapshot.OwnerID = m.lastOwnerID
	}
	if m.rebuilding {
		snapshot.Lifecycle = leaseidgen.LifecycleFailed
	} else {
		snapshot.Lifecycle = leaseidgen.LifecycleClosed
	}
	snapshot.Ready = false
	snapshot.LeaseOwned = false
	snapshot.LeaseRemainingMillis = 0
	snapshot.FenceLeadMillis = snapshot.GenerationFenceMillis - snapshot.CapturedAtMillis
	snapshot.ReadinessErrorClass = leaseidgen.ClassifyError(m.lastErr)
	snapshot.LastErrorClass = leaseidgen.ClassifyError(m.lastErr)
	return snapshot
}

func newDemoServer(serviceName string, manager *componentManager) *demoServer {
	return &demoServer{
		serviceName: serviceName,
		manager:     manager,
	}
}

func (s *demoServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/next", s.handleNext)
	mux.HandleFunc("/snapshot", s.handleSnapshot)
	return mux
}

func (s *demoServer) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": s.serviceName,
	})
}

func (s *demoServer) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.manager.Ready(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status":      "not_ready",
			"error":       "id_generator_unavailable",
			"error_class": string(leaseidgen.ClassifyError(err)),
			"checks": map[string]string{
				"id_generator": "error",
			},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ready",
		"checks": map[string]string{
			"id_generator": "ok",
		},
	})
}

func (s *demoServer) handleNext(w http.ResponseWriter, r *http.Request) {
	value, state, err := s.manager.Next(r.Context())
	if err != nil {
		emitJSON(map[string]any{
			"diagnostic_scope": "snowflake_id",
			"event":            "idgen_usage_event",
			"action":           "next_failed",
			"role":             demoRole,
			"service":          s.serviceName,
			"node_id":          state.NodeID,
			"owner_id":         leaseidgen.RedactOwnerID(state.OwnerID),
			"error_class":      string(leaseidgen.ClassifyError(err)),
			"error":            err.Error(),
			"path":             r.URL.Path,
		})
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":       "generate_id_failed",
			"error_class": string(leaseidgen.ClassifyError(err)),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id":       strconv.FormatInt(value, 10),
		"node_id":  state.NodeID,
		"owner_id": leaseidgen.RedactOwnerID(state.OwnerID),
	})
}

func (s *demoServer) handleSnapshot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, snapshotPayload(s.manager.Snapshot()))
}

func snapshotPayload(snapshot leaseidgen.Snapshot) map[string]any {
	return map[string]any{
		"captured_at_ms":        snapshot.CapturedAtMillis,
		"node_id":               snapshot.NodeID,
		"owner_id":              leaseidgen.RedactOwnerID(snapshot.OwnerID),
		"lifecycle":             snapshot.Lifecycle,
		"ready":                 snapshot.Ready,
		"readiness_error_class": snapshot.ReadinessErrorClass,
		"lease_owned":           snapshot.LeaseOwned,
		"lease_remaining_ms":    snapshot.LeaseRemainingMillis,
		"generation_floor_ms":   snapshot.GenerationFloorMillis,
		"generation_fence_ms":   snapshot.GenerationFenceMillis,
		"fence_lead_ms":         snapshot.FenceLeadMillis,
		"generated_total":       snapshot.GeneratedTotal,
		"acquire_success_total": snapshot.AcquireSuccessTotal,
		"refresh_success_total": snapshot.RefreshSuccessTotal,
		"refresh_failure_total": snapshot.RefreshFailureTotal,
		"failure_total":         snapshot.FailureTotal,
		"close_total":           snapshot.CloseTotal,
		"dropped_events_total":  snapshot.DroppedEventsTotal,
		"last_error_class":      snapshot.LastErrorClass,
		"failures": map[string]uint64{
			"clock_rollback": snapshot.Failures.ClockRollback,
			"clock_skew":     snapshot.Failures.ClockSkew,
			"fence_ahead":    snapshot.Failures.FenceAhead,
			"lease_busy":     snapshot.Failures.LeaseBusy,
			"lease_lost":     snapshot.Failures.LeaseLost,
			"closed":         snapshot.Failures.Closed,
			"canceled":       snapshot.Failures.Canceled,
			"store_failure":  snapshot.Failures.StoreFailure,
			"unknown":        snapshot.Failures.Unknown,
		},
	}
}

func startReporter(
	ctx context.Context,
	serviceName string,
	telemetry *leaseidgen.Telemetry,
	source leaseidgen.SnapshotSource,
) {
	go func() {
		err := leaseidgen.RunReporter(
			ctx,
			reportInterval,
			source,
			telemetry,
			func(report leaseidgen.StatusReport) {
				emitJSON(map[string]any{
					"diagnostic_scope":        "snowflake_id",
					"event":                   "idgen_status",
					"role":                    demoRole,
					"service":                 serviceName,
					"node_id":                 report.NodeID,
					"owner_id":                leaseidgen.RedactOwnerID(report.OwnerID),
					"captured_at_ms":          report.CapturedAtMillis,
					"ready":                   report.Ready,
					"lifecycle":               report.Lifecycle,
					"readiness_error_class":   report.ReadinessErrorClass,
					"last_error_class":        report.LastErrorClass,
					"lease_remaining_ms":      report.LeaseRemainingMillis,
					"generation_fence_ms":     report.GenerationFenceMillis,
					"generated_total":         report.GeneratedTotal,
					"generated_delta":         report.GeneratedDelta,
					"generation_rate_per_sec": report.GenerationRatePerSecond,
					"refresh_success_total":   report.RefreshSuccessTotal,
					"refresh_failure_total":   report.RefreshFailureTotal,
					"window_ms":               report.WindowMillis,
				})
			},
			func(event leaseidgen.Event) {
				fields := map[string]any{
					"diagnostic_scope": "snowflake_id",
					"event":            "idgen_event",
					"action":           string(event.Kind),
					"role":             demoRole,
					"service":          serviceName,
					"node_id":          event.NodeID,
					"owner_id":         leaseidgen.RedactOwnerID(event.OwnerID),
					"error_class":      event.ErrorClass,
				}
				if event.Err != nil {
					fields["error"] = event.Err.Error()
				}
				emitJSON(fields)
			},
		)
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			emitJSON(map[string]any{
				"diagnostic_scope": "snowflake_id",
				"event":            "idgen_event",
				"action":           "reporter_failed",
				"role":             demoRole,
				"service":          serviceName,
				"error_class":      string(leaseidgen.ClassifyError(err)),
				"error":            err.Error(),
			})
		}
	}()
}

func serveHTTP(server *http.Server) error {
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func shutdownDemo(
	primaryErr error,
	server *http.Server,
	manager *componentManager,
	cancelRoot context.CancelFunc,
) error {
	if cancelRoot != nil {
		cancelRoot()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), componentStopTimeout)
	defer cancel()

	var errs []error
	if primaryErr != nil {
		errs = append(errs, primaryErr)
	}
	if server != nil {
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, fmt.Errorf("shutdown http server: %w", err))
		}
	}
	if manager != nil {
		if err := manager.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			errs = append(errs, fmt.Errorf("shutdown id component: %w", err))
		}
	}
	return errors.Join(errs...)
}

func stripExpectedStopError(stopErr error, expected error) error {
	if stopErr == nil {
		return nil
	}
	if expected == nil {
		return stopErr
	}

	type joined interface {
		Unwrap() []error
	}
	if multi, ok := stopErr.(joined); ok {
		var unexpected []error
		for _, err := range multi.Unwrap() {
			if err == nil || errors.Is(err, expected) {
				continue
			}
			unexpected = append(unexpected, err)
		}
		return errors.Join(unexpected...)
	}
	if errors.Is(stopErr, expected) {
		return nil
	}
	return stopErr
}

func sleepWithContext(ctx context.Context, wait time.Duration) bool {
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(payload)
}

func emitJSON(fields map[string]any) {
	if _, exists := fields["ts"]; !exists {
		fields["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	}

	stdoutMu.Lock()
	defer stdoutMu.Unlock()

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(fields)
}

func valueOrDefault(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func stringsTrim(value string) string {
	return strings.TrimSpace(value)
}
