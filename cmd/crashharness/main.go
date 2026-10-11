// Command crashharness seeds a deterministic sequence of process kills
// against the real cmd/pipeline binary — some timed to land inside an apply
// transaction, some timed to land in the acknowledgment gap after a commit
// but before Postgres has been told — restarts it, and asserts the diff
// oracle reports exact equality after every single one. This is Stage 2's
// crash-safety proof: not "it seems to work," but N reproducible seeded
// crash scenarios, all passing, or the exact failing one named.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"time"

	"github.com/extrem21/watermark/harness"
	"github.com/extrem21/watermark/shared"
)

func main() {
	var cfg shared.Config
	fs := flag.CommandLine
	cfg.RegisterFlags(fs)
	// Dedicated defaults so this never collides with a manually-run
	// cmd/pipeline pointed at the real watermark_slot/watermark_pub —
	// still overridable via the usual flags if that's actually wanted.
	_ = fs.Set("slot-name", "watermark_crash_slot")
	_ = fs.Set("publication-name", "watermark_crash_pub")
	_ = fs.Set("workload-ops", "30") // ops per trial; small keeps each trial's cycle fast

	trials := flag.Int("trials", 10, "number of seeded crash trials to run")
	flag.Parse()

	ctx := context.Background()

	binPath, cleanup, err := buildPipelineBinary(ctx)
	if err != nil {
		log.Fatalf("build cmd/pipeline: %v", err)
	}
	defer cleanup()

	crashTrials := harness.GenerateTrials(cfg.Seed, *trials, cfg.WorkloadOps)

	results, err := harness.RunCrashHarness(ctx, &cfg, binPath, crashTrials)
	report(results)
	if err != nil {
		log.Fatalf("crash harness stopped early: %v", err)
	}

	if failed := countFailed(results); failed > 0 {
		log.Fatalf("FAIL: %d/%d trials diverged", failed, len(results))
	}
	log.Printf("PASS: %d/%d seeded crash trials converged to exact row-for-row equality", len(results), len(results))
}

// buildPipelineBinary compiles cmd/pipeline to a real binary once, up
// front. The crash trials exec this binary directly rather than `go run
// ./cmd/pipeline` — `go run` wraps the compiled binary in a build-and-exec
// parent process, and killing that parent does not propagate SIGKILL to
// the actual pipeline child, so nothing would really crash.
func buildPipelineBinary(ctx context.Context) (string, func(), error) {
	dir, err := os.MkdirTemp("", "watermark-crashharness-*")
	if err != nil {
		return "", nil, err
	}
	binPath := dir + "/pipeline"

	cmd := exec.CommandContext(ctx, "go", "build", "-o", binPath, "./cmd/pipeline")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		os.RemoveAll(dir)
		return "", nil, fmt.Errorf("go build: %w", err)
	}
	return binPath, func() { os.RemoveAll(dir) }, nil
}

func countFailed(results []harness.CrashResult) int {
	n := 0
	for _, r := range results {
		if !r.DiffEqual {
			n++
		}
	}
	return n
}

func report(results []harness.CrashResult) {
	var recoveries []time.Duration
	for _, r := range results {
		status := "PASS"
		if !r.DiffEqual {
			status = "FAIL"
		}
		log.Printf("TRIAL %d point=%s killAfterN=%d recovery=%s duplicatesSkipped=%d %s",
			r.Trial.Index, r.Trial.Point, r.Trial.KillAfterN,
			r.RecoveryTime.Round(time.Millisecond), r.DuplicatesSkipped, status)

		if !r.DiffEqual {
			for _, m := range r.Mismatches {
				log.Printf("  - %s", m)
			}
			continue
		}
		recoveries = append(recoveries, r.RecoveryTime)
	}

	if len(recoveries) == 0 {
		return
	}
	sort.Slice(recoveries, func(i, j int) bool { return recoveries[i] < recoveries[j] })
	p50 := recoveries[len(recoveries)*50/100]
	p90 := recoveries[min(len(recoveries)*90/100, len(recoveries)-1)]
	log.Printf("RECOVERY recovery_p50=%s recovery_p90=%s (n=%d passing trials)",
		p50.Round(time.Millisecond), p90.Round(time.Millisecond), len(recoveries))
}
