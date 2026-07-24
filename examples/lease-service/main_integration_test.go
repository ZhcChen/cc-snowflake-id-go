//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	leaseidgen "github.com/ZhcChen/cc-snowflake-id-go/lease"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLeaseServiceRebuildsAfterDatabaseProxyOutageIntegration(t *testing.T) {
	databaseURL := os.Getenv("IDGEN_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("IDGEN_TEST_DATABASE_URL is required for integration tests")
	}

	toxiproxyURL := os.Getenv("IDGEN_TEST_TOXIPROXY_URL")
	if toxiproxyURL == "" {
		t.Skip("IDGEN_TEST_TOXIPROXY_URL is not set; skipping lease-service network fault integration test")
	}

	ctx := context.Background()
	adminPool, schemaName := newLeaseServiceIntegrationAdminPool(t, databaseURL)
	createLeaseServiceIntegrationTable(t, adminPool, schemaName)

	apiURL, err := url.Parse(toxiproxyURL)
	if err != nil {
		t.Fatalf("parse IDGEN_TEST_TOXIPROXY_URL: %v", err)
	}

	proxyClient := newToxiproxyClient(toxiproxyURL)
	proxy, proxiedDatabaseURL := createPostgresProxyForIntegration(t, proxyClient, apiURL, databaseURL)

	proxiedPool := newLeaseServiceIntegrationPool(t, proxiedDatabaseURL, schemaName)
	store, err := leaseidgen.NewPGLeaseStore(proxiedPool)
	if err != nil {
		t.Fatalf("NewPGLeaseStore() error = %v", err)
	}

	settings := testManagerSettings()
	settings.leaseWindow = 800 * time.Millisecond
	settings.fenceWindow = 800 * time.Millisecond
	settings.leaseAcquireTimeout = 250 * time.Millisecond
	settings.leaseOperationTimeout = 150 * time.Millisecond
	settings.leaseRefreshInterval = 100 * time.Millisecond
	settings.rebuildInitialDelay = 50 * time.Millisecond
	settings.rebuildMaxDelay = 150 * time.Millisecond
	settings.componentStopTimeout = 250 * time.Millisecond

	rootCtx, cancel := context.WithCancel(context.Background())
	manager := newComponentManager(rootCtx, demoConfig{
		ServiceName: "integration-svc",
		NodeID:      21,
	}, store, settings)
	if err := manager.Start(); err != nil {
		cancel()
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		cancel()
		if err := manager.Shutdown(context.Background()); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	})

	handler := newDemoServer("integration-svc", manager).routes()
	initialSnapshot := manager.Snapshot()
	if !initialSnapshot.Ready {
		t.Fatalf("initial snapshot ready = %v, want true", initialSnapshot.Ready)
	}

	readyPayload, statusCode := performJSONRequestWithStatus(t, handler, http.MethodGet, "/readyz")
	if statusCode != http.StatusOK || readyPayload["status"] != "ready" {
		t.Fatalf("initial readyz status = %d payload = %#v, want 200 ready", statusCode, readyPayload)
	}

	if err := proxyClient.setProxyEnabled(ctx, proxy.Name, false); err != nil {
		t.Fatalf("disable toxiproxy proxy: %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		payload, code := performJSONRequestWithStatus(t, handler, http.MethodGet, "/readyz")
		if code != http.StatusServiceUnavailable {
			return false
		}

		snapshot := manager.Snapshot()
		return payload["error_class"] == string(leaseidgen.ErrorClassStoreFailure) &&
			snapshot.Lifecycle == leaseidgen.LifecycleFailed &&
			snapshot.ReadinessErrorClass == leaseidgen.ErrorClassStoreFailure &&
			snapshot.LastErrorClass == leaseidgen.ErrorClassStoreFailure
	})

	if err := proxyClient.setProxyEnabled(ctx, proxy.Name, true); err != nil {
		t.Fatalf("reenable toxiproxy proxy: %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		snapshot := manager.Snapshot()
		payload, code := performJSONRequestWithStatus(t, handler, http.MethodGet, "/readyz")
		return code == http.StatusOK &&
			payload["status"] == "ready" &&
			snapshot.Ready &&
			snapshot.OwnerID != "" &&
			snapshot.OwnerID != initialSnapshot.OwnerID
	})

	nextPayload, nextStatus := performJSONRequestWithStatus(t, handler, http.MethodGet, "/next")
	if nextStatus != http.StatusOK {
		t.Fatalf("next status = %d payload = %#v, want 200", nextStatus, nextPayload)
	}
	if nextPayload["node_id"] != float64(21) {
		t.Fatalf("next node_id = %v, want 21", nextPayload["node_id"])
	}
}

func newLeaseServiceIntegrationAdminPool(t *testing.T, databaseURL string) (*pgxpool.Pool, string) {
	t.Helper()

	ctx := context.Background()
	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect integration database: %v", err)
	}
	t.Cleanup(adminPool.Close)

	schemaName := fmt.Sprintf("idgen_lease_service_integration_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatalf("create integration schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+quotedSchema+" CASCADE")
	})

	return adminPool, schemaName
}

func createLeaseServiceIntegrationTable(t *testing.T, pool *pgxpool.Pool, schemaName string) {
	t.Helper()

	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	if _, err := pool.Exec(context.Background(), `
CREATE TABLE `+quotedSchema+`.id_generator_node_leases (
    node_id INTEGER PRIMARY KEY,
    owner_id TEXT NOT NULL,
    reserved_until_ms BIGINT NOT NULL CHECK (reserved_until_ms > 0),
    generation_fence_ms BIGINT NOT NULL CHECK (generation_fence_ms > 0),
    acquired_at_ms BIGINT NOT NULL,
    refreshed_at_ms BIGINT NOT NULL,
    heartbeat_at_ms BIGINT NOT NULL,
    lease_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`); err != nil {
		t.Fatalf("create integration lease table: %v", err)
	}
}

func newLeaseServiceIntegrationPool(t *testing.T, databaseURL string, schemaName string) *pgxpool.Pool {
	t.Helper()

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse integration database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schemaName

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("create integration pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func createPostgresProxyForIntegration(
	t *testing.T,
	client *toxiproxyClient,
	apiURL *url.URL,
	databaseURL string,
) (toxiproxyProxy, string) {
	t.Helper()

	listenAddress := valueOrDefault(
		stringsTrim(os.Getenv("IDGEN_TEST_TOXIPROXY_LISTEN")),
		"127.0.0.1:15432",
	)
	upstream := stringsTrim(os.Getenv("IDGEN_TEST_TOXIPROXY_UPSTREAM"))
	if upstream == "" {
		parsedDatabaseURL, err := url.Parse(databaseURL)
		if err != nil {
			t.Fatalf("parse IDGEN_TEST_DATABASE_URL: %v", err)
		}
		upstream = parsedDatabaseURL.Host
	}

	proxyName := fmt.Sprintf("idgen-lease-service-%d", time.Now().UnixNano())
	proxy, err := client.createProxy(context.Background(), toxiproxyProxy{
		Name:     proxyName,
		Listen:   listenAddress,
		Upstream: upstream,
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("create toxiproxy proxy: %v", err)
	}
	t.Cleanup(func() {
		_ = client.deleteProxy(context.Background(), proxy.Name)
	})

	proxiedDatabaseURL, err := buildProxiedDatabaseURL(databaseURL, apiURL, proxy.Listen)
	if err != nil {
		t.Fatalf("build proxied database URL: %v", err)
	}
	return proxy, proxiedDatabaseURL
}

func buildProxiedDatabaseURL(databaseURL string, apiURL *url.URL, listenAddress string) (string, error) {
	parsedDatabaseURL, err := url.Parse(databaseURL)
	if err != nil {
		return "", err
	}

	proxyListenURL, err := url.Parse("tcp://" + listenAddress)
	if err != nil {
		return "", err
	}

	connectHost := apiURL.Hostname()
	if connectHost == "" {
		connectHost = proxyListenURL.Hostname()
	}
	if connectHost == "" || connectHost == "0.0.0.0" || connectHost == "::" {
		connectHost = "127.0.0.1"
	}
	port := proxyListenURL.Port()
	if port == "" {
		return "", fmt.Errorf("proxy listen address %q does not include a port", listenAddress)
	}

	parsedDatabaseURL.Host = connectHost + ":" + port
	return parsedDatabaseURL.String(), nil
}

func performJSONRequestWithStatus(
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
) (map[string]any, int) {
	t.Helper()

	request := httptest.NewRequest(method, path, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return decodeJSONPayload(t, recorder.Body.Bytes()), recorder.Code
}

type toxiproxyClient struct {
	baseURL    string
	httpClient *http.Client
}

type toxiproxyProxy struct {
	Name     string `json:"name"`
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
	Enabled  bool   `json:"enabled"`
}

func newToxiproxyClient(baseURL string) *toxiproxyClient {
	return &toxiproxyClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (c *toxiproxyClient) createProxy(ctx context.Context, proxy toxiproxyProxy) (toxiproxyProxy, error) {
	var created toxiproxyProxy
	if err := c.doJSON(ctx, http.MethodPost, "/proxies", proxy, &created); err != nil {
		return toxiproxyProxy{}, err
	}
	return created, nil
}

func (c *toxiproxyClient) setProxyEnabled(ctx context.Context, proxyName string, enabled bool) error {
	return c.doJSON(ctx, http.MethodPost, "/proxies/"+proxyName, map[string]any{
		"enabled": enabled,
	}, nil)
}

func (c *toxiproxyClient) deleteProxy(ctx context.Context, proxyName string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.baseURL+"/proxies/"+proxyName, nil)
	if err != nil {
		return err
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("toxiproxy delete proxy status = %d", response.StatusCode)
	}
	return nil
}

func (c *toxiproxyClient) doJSON(
	ctx context.Context,
	method string,
	path string,
	requestBody any,
	responseBody any,
) error {
	var body bytes.Buffer
	if requestBody != nil {
		if err := json.NewEncoder(&body).Encode(requestBody); err != nil {
			return err
		}
	}

	request, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, &body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("toxiproxy %s %s status = %d", method, path, response.StatusCode)
	}
	if responseBody == nil {
		return nil
	}
	return json.NewDecoder(response.Body).Decode(responseBody)
}
