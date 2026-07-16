package main

import (
	"context"
	"fmt"
	"log"
	"time"

	idgen "github.com/ZhcChen/cc-snowflake-id-go/generator"
)

func main() {
	ctx := context.Background()

	generator, err := idgen.NewGenerator(idgen.Config{
		NodeID: 1,
	}, nil)
	if err != nil {
		log.Fatalf("create generator: %v", err)
	}

	value, err := generator.Next(ctx)
	if err != nil {
		log.Fatalf("generate snowflake id: %v", err)
	}

	parts, err := idgen.Decode(value, 0)
	if err != nil {
		log.Fatalf("decode snowflake id: %v", err)
	}

	fmt.Printf("snowflake_id=%d\n", value)
	fmt.Printf("node_id=%d\n", parts.NodeID)
	fmt.Printf("sequence=%d\n", parts.Sequence)
	fmt.Printf("timestamp_utc=%s\n", time.UnixMilli(parts.TimestampMillis).UTC().Format(time.RFC3339Nano))
}
