// Command workloadgen drives a seeded, reproducible workload of
// INSERT/UPDATE/DELETE statements against the source database, for testing
// the pipeline's replication. It never touches the downstream — that's what
// diffcheck is for, run afterward.
package main

import (
	"context"
	"flag"
	"log"

	"github.com/extrem21/watermark/harness"
	"github.com/extrem21/watermark/shared"
)

func main() {
	var cfg shared.Config
	cfg.RegisterFlags(flag.CommandLine)
	flag.Parse()

	ctx := context.Background()

	gen, err := harness.NewWorkloadGen(ctx, &cfg)
	if err != nil {
		log.Fatalf("new workload generator: %v", err)
	}
	defer gen.Close(ctx)

	if err := gen.Run(ctx, cfg.WorkloadOps); err != nil {
		log.Fatalf("run workload: %v", err)
	}
	log.Printf("workload complete: %d ops, seed=%d", cfg.WorkloadOps, cfg.Seed)
}
