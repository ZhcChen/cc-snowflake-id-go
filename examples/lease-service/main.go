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

	leaseidgen "github.com/ZhcChen/cc-snowflake-id-go/lease"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultHTTPAddr    = ":8080"
	defaultServiceName = "cc-snowflake-id-go-lease-service-demo"
	defaultNodeID      = 100
	demoRole           = "service"
	reportInterval     = 15 * time.Second
)

var stdoutMu sync.Mutex

type demoConfig struct {
	DatabaseURL string
	HTTPAddr    string
	ServiceName string
	NodeID      int
}

type demoServer struct {
	serviceName string
	generator   *leaseidgen.LeasedGenerator
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

	rootCtx, stopSignal := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignal()

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

	ownerID, err := leaseidgen.NewOwnerID(cfg.ServiceName)
	if err != nil {
		return fmt.Errorf("build owner id: %w", err)
	}

	telemetry := leaseidgen.NewTelemetry()
	generator, err := leaseidgen.NewLeasedGenerator(store, nil, leaseidgen.LeasedGeneratorConfig{
		NodeID:                cfg.NodeID,
		OwnerID:               ownerID,
		LeaseWindow:           15 * time.Second,
		FenceWindow:           15 * time.Second,
		LeaseAcquireTimeout:   5 * time.Second,
		LeaseOperationTimeout: 2 * time.Second,
		LeaseRefreshInterval:  5 * time.Second,
		Observer:              telemetry,
	})
	if err != nil {
		return fmt.Errorf("create leased generator: %w", err)
	}

	if _, err := generator.Acquire(rootCtx); err != nil {
		return fmt.Errorf("acquire lease: %w", err)
	}

	runtime, err := leaseidgen.StartRuntime(rootCtx, generator)
	if err != nil {
		closeErr := generator.Close(context.Background())
		return errors.Join(fmt.Errorf("start runtime: %w", err), closeErr)
	}

	reporterCtx, cancelReporter := context.WithCancel(context.Background())
	startReporter(reporterCtx, cfg.ServiceName, telemetry, generator)

	emitJSON(map[string]any{
		"diagnostic_scope":           "snowflake_id",
		"event":                      "idgen_config",
		"role":                       demoRole,
		"service":                    cfg.ServiceName,
		"node_id":                    cfg.NodeID,
		"owner_id":                   leaseidgen.RedactOwnerID(ownerID),
		"lease_window_ms":            (15 * time.Second).Milliseconds(),
		"fence_window_ms":            (15 * time.Second).Milliseconds(),
		"lease_refresh_interval_ms":  (5 * time.Second).Milliseconds(),
		"lease_operation_timeout_ms": (2 * time.Second).Milliseconds(),
		"http_addr":                  cfg.HTTPAddr,
	})

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: newDemoServer(cfg.ServiceName, generator).routes(),
	}

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- serveHTTP(server)
	}()

	runtimeErrCh := watchRuntime(runtime)
	select {
	case err := <-serveErrCh:
		return shutdownDemo(err, server, runtime, cancelReporter)
	case err := <-runtimeErrCh:
		emitJSON(map[string]any{
			"diagnostic_scope": "snowflake_id",
			"event":            "idgen_event",
			"action":           "runtime_failed",
			"role":             demoRole,
			"service":          cfg.ServiceName,
			"error_class":      string(leaseidgen.ClassifyError(err)),
			"error":            err.Error(),
		})
		return shutdownDemo(fmt.Errorf("id generator runtime failed: %w", err), server, runtime, cancelReporter)
	case <-rootCtx.Done():
		return shutdownDemo(nil, server, runtime, cancelReporter)
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

func newDemoServer(serviceName string, generator *leaseidgen.LeasedGenerator) *demoServer {
	return &demoServer{
		serviceName: serviceName,
		generator:   generator,
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
	if err := s.generator.Ready(r.Context()); err != nil {
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
	value, err := s.generator.Next(r.Context())
	if err != nil {
		emitJSON(map[string]any{
			"diagnostic_scope": "snowflake_id",
			"event":            "idgen_usage_event",
			"action":           "next_failed",
			"role":             demoRole,
			"service":          s.serviceName,
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

	state := s.generator.State()
	writeJSON(w, http.StatusOK, map[string]any{
		"id":       strconv.FormatInt(value, 10),
		"node_id":  state.NodeID,
		"owner_id": leaseidgen.RedactOwnerID(state.OwnerID),
	})
}

func (s *demoServer) handleSnapshot(w http.ResponseWriter, _ *http.Request) {
	snapshot := s.generator.Snapshot()
	writeJSON(w, http.StatusOK, snapshotPayload(snapshot))
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

func watchRuntime(runtime *leaseidgen.Runtime) <-chan error {
	if runtime == nil {
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		<-runtime.Done()
		err := runtime.Err()
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		errCh <- err
	}()
	return errCh
}

func shutdownDemo(
	primaryErr error,
	server *http.Server,
	runtime *leaseidgen.Runtime,
	cancelReporter context.CancelFunc,
) error {
	if cancelReporter != nil {
		cancelReporter()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	if runtime != nil {
		if err := runtime.Stop(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			errs = append(errs, fmt.Errorf("stop id generator runtime: %w", err))
		}
	}
	return errors.Join(errs...)
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
