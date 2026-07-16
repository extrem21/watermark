// Command benchmark measures end-to-end pipeline throughput independently of
// the pipeline process's own instrumentation: it times a fixed, seeded
// workload from just before the first op is sent to the source, to the
// moment the downstream is verified row-for-row equal via the same diff
// oracle that proves correctness. Nothing here trusts anything cmd/pipeline
// logged about itself — completion is detected by directly reading both
// databases, and the clock runs in this separate process.
//
// This is an end-to-end number (source commit -> downstream verified), not
// apply-only throughput isolated from workload-injection speed — say so
// wherever it's reported.
//
// With -runs > 1, the same seed is replayed every run (not a new seed per
// run): the point is to sample timing noise on an identical logical
// workload, not to average across different workloads. Every sample is
// printed, plus the median.
//
// Requires cmd/pipeline already running. Each run truncates the accounts
// table on BOTH source and downstream first — that's WorkloadGen's existing
// empty-table precondition, applied automatically instead of by hand. This
// is destructive by design: never point it at anything but the disposable
// Stage 1 test databases.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/extrem21/watermark/harness"
	"github.com/extrem21/watermark/shared"
)

// convergePollInterval and convergeTimeout bound how we detect "the
// downstream has caught up." Polling the diff oracle is a direct SQL read of
// both databases on every attempt; if it never reports equal, that's a real
// signal something is wrong (pipeline down, apply broken) and this fails
// loudly rather than report a bogus number.
const (
	convergePollInterval = 50 * time.Millisecond
	convergeTimeout      = 30 * time.Second
)

func main() {
	var cfg shared.Config
	cfg.RegisterFlags(flag.CommandLine)
	runs := flag.Int("runs", 1, "number of timed runs; with runs > 1, prints every sample plus the median")
	flag.Parse()

	ctx := context.Background()

	samples := make([]time.Duration, 0, *runs)
	for i := 1; i <= *runs; i++ {
		if err := resetTables(ctx, &cfg); err != nil {
			log.Fatalf("reset accounts tables before run %d: %v", i, err)
		}

		d, err := runOnce(ctx, &cfg)
		if err != nil {
			log.Fatalf("run %d: %v", i, err)
		}
		samples = append(samples, d)

		log.Printf("BENCHMARK run=%d/%d seed=%d ops=%d elapsed=%s throughput=%.2f changes/sec",
			i, *runs, cfg.Seed, cfg.WorkloadOps, d.Round(time.Millisecond), float64(cfg.WorkloadOps)/d.Seconds())
	}

	if *runs > 1 {
		med := median(samples)
		log.Printf("BENCHMARK MEDIAN seed=%d ops=%d runs=%d elapsed=%s throughput=%.2f changes/sec (end-to-end: source commit -> downstream verified equal)",
			cfg.Seed, cfg.WorkloadOps, *runs, med.Round(time.Millisecond), float64(cfg.WorkloadOps)/med.Seconds())
	}
}

// runOnce injects cfg.WorkloadOps seeded ops against the (assumed empty)
// source, then polls the diff oracle until the downstream is verified
// row-for-row equal, and returns the elapsed time between the two.
func runOnce(ctx context.Context, cfg *shared.Config) (time.Duration, error) {
	gen, err := harness.NewWorkloadGen(ctx, cfg)
	if err != nil {
		return 0, fmt.Errorf("new workload generator: %w", err)
	}
	defer gen.Close(ctx)

	start := time.Now()
	if err := gen.Run(ctx, cfg.WorkloadOps); err != nil {
		return 0, fmt.Errorf("run workload: %w", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, convergeTimeout)
	defer cancel()

	for {
		result, err := harness.RunDiffOracle(waitCtx, cfg)
		if err != nil {
			return 0, fmt.Errorf("run diff oracle: %w", err)
		}
		if result.Equal() {
			return time.Since(start), nil
		}

		select {
		case <-waitCtx.Done():
			return 0, fmt.Errorf("timed out after %s waiting for downstream to converge (%d mismatch(es) remaining) — is cmd/pipeline running?",
				convergeTimeout, len(result.Mismatches))
		case <-time.After(convergePollInterval):
		}
	}
}

// resetTables truncates accounts on both source and downstream directly —
// not by relying on TRUNCATE replicating through the pipeline, since
// consumer.go's decode loop has no case for pglogrepl.TruncateMessage and
// silently drops it (schema/DDL-adjacent handling is out of scope, per
// CLAUDE.md). Both sides are emptied explicitly so each run starts from the
// same known state WorkloadGen requires.
func resetTables(ctx context.Context, cfg *shared.Config) error {
	for _, dsn := range []string{cfg.SourceDSN, cfg.DownstreamDSN} {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("connect: %w", err)
		}
		_, execErr := conn.Exec(ctx, "TRUNCATE accounts")
		closeErr := conn.Close(ctx)
		if execErr != nil {
			return fmt.Errorf("truncate accounts: %w", execErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close connection: %w", closeErr)
		}
	}
	return nil
}

func median(ds []time.Duration) time.Duration {
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}
