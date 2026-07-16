package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	idgen "github.com/ZhcChen/cc-snowflake-id-go/generator"
	leaseidgen "github.com/ZhcChen/cc-snowflake-id-go/lease"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	demoServiceName = "cc-snowflake-id-go-lease-demo"
	demoNodeID      = 100
)

func main() {
	databaseURL := os.Getenv("IDGEN_DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("IDGEN_DATABASE_URL is required")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("open postgres pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}

	store, err := leaseidgen.NewPGLeaseStore(pool)
	if err != nil {
		log.Fatalf("create lease store: %v", err)
	}

	ownerID, err := leaseidgen.NewOwnerID(demoServiceName)
	if err != nil {
		log.Fatalf("build owner id: %v", err)
	}

	telemetry := leaseidgen.NewTelemetry()
	generator, err := leaseidgen.NewLeasedGenerator(store, nil, leaseidgen.LeasedGeneratorConfig{
		NodeID:                demoNodeID,
		OwnerID:               ownerID,
		LeaseWindow:           10 * time.Second,
		FenceWindow:           10 * time.Second,
		LeaseAcquireTimeout:   3 * time.Second,
		LeaseOperationTimeout: time.Second,
		LeaseRefreshInterval:  3 * time.Second,
		Observer:              telemetry,
	})
	if err != nil {
		log.Fatalf("create leased generator: %v", err)
	}

	state, err := generator.Acquire(ctx)
	if err != nil {
		log.Fatalf("acquire lease: %v", err)
	}

	runtime, err := leaseidgen.StartRuntime(ctx, generator)
	if err != nil {
		log.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := runtime.Stop(context.Background()); err != nil {
			log.Printf("stop runtime: %v", err)
		}
	}()

	value, err := generator.Next(ctx)
	if err != nil {
		log.Fatalf("generate leased snowflake id: %v", err)
	}

	parts, err := idgen.Decode(value, 0)
	if err != nil {
		log.Fatalf("decode leased snowflake id: %v", err)
	}

	snapshot := generator.Snapshot()

	fmt.Printf("lease_node_id=%d\n", state.NodeID)
	fmt.Printf("lease_owner_id=%s\n", leaseidgen.RedactOwnerID(state.OwnerID))
	fmt.Printf("lease_fence_ms=%d\n", state.GenerationFenceMillis)
	fmt.Printf("snowflake_id=%d\n", value)
	fmt.Printf("decoded_node_id=%d\n", parts.NodeID)
	fmt.Printf("decoded_sequence=%d\n", parts.Sequence)
	fmt.Printf("decoded_timestamp_utc=%s\n", time.UnixMilli(parts.TimestampMillis).UTC().Format(time.RFC3339Nano))
	fmt.Printf("ready=%t\n", snapshot.Ready)
	fmt.Printf("lifecycle=%s\n", snapshot.Lifecycle)
	fmt.Printf("refresh_success_total=%d\n", snapshot.RefreshSuccessTotal)
}
