package harness

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/extrem21/watermark/shared"
)

// Dedicated slot/publication so a crash-harness run never collides with a
// manually-run cmd/pipeline pointed at the real watermark_slot/watermark_pub.
const (
	crashSlotName = "watermark_crash_slot"
	crashPubName  = "watermark_crash_pub"
)

const (
	// killWaitTimeout bounds how long we'll wait to observe KillAfterN
	// "applied" log lines before giving up on a trial.
	killWaitTimeout = 30 * time.Second
	// ackGapSettleDelay is added before killing for CrashAckGap trials, so
	// the Nth change's transaction has certainly committed downstream
	// before the process dies — landing the crash in the acknowledgment
	// gap CLAUDE.md names, not mid-transaction. CrashMidTransaction trials
	// add no delay: killing as fast as possible after the signal is the
	// entire mechanism for racing into the next change's still-open
	// transaction.
	ackGapSettleDelay = 300 * time.Millisecond
	// restartDelay gives Postgres a moment to notice the killed process's
	// connection is gone before the restarted process tries to attach to
	// the same replication slot — without it, a still-active slot causes
	// the restart itself to fail, which is a restart-timing artifact, not
	// the crash-safety failure this harness exists to catch.
	restartDelay = 300 * time.Millisecond

	convergeTimeout = 30 * time.Second
	convergePoll    = 50 * time.Millisecond

	// workloadTailBuffer is how many extra ops beyond KillAfterN the
	// generator issues, so a CrashMidTransaction trial always has a "next"
	// change actively in flight to interrupt.
	workloadTailBuffer = 10
)

// CrashPoint names which of CLAUDE.md's two crash windows a trial targets.
type CrashPoint int

const (
	// CrashMidTransaction kills as fast as possible right after the Nth
	// applied line, racing to catch the (N+1)th change's transaction
	// still open (between its BEGIN and COMMIT).
	CrashMidTransaction CrashPoint = iota
	// CrashAckGap kills only after ackGapSettleDelay, so the Nth change's
	// transaction has certainly committed downstream, but the consumer
	// has not necessarily told Postgres it's confirmed yet.
	CrashAckGap
)

func (p CrashPoint) String() string {
	switch p {
	case CrashMidTransaction:
		return "mid-transaction"
	case CrashAckGap:
		return "ack-gap"
	default:
		return "unknown"
	}
}

// CrashTrial is one seeded crash scenario.
type CrashTrial struct {
	Index      int
	Point      CrashPoint
	KillAfterN int // kill once this many "applied" lines have been observed
}

// GenerateTrials deterministically derives n crash trials from seed: same
// seed, same n, same opsPerTrial -> the identical sequence of trials every
// time, per CLAUDE.md's "a failing seed can be replayed deterministically"
// requirement. Never seeded from time.Now().
func GenerateTrials(seed int64, n int, opsPerTrial int) []CrashTrial {
	rng := rand.New(rand.NewSource(seed))
	trials := make([]CrashTrial, n)
	for i := 0; i < n; i++ {
		point := CrashMidTransaction
		if rng.Intn(2) == 1 {
			point = CrashAckGap
		}
		killAfterN := 1 + rng.Intn(opsPerTrial-1)
		trials[i] = CrashTrial{Index: i, Point: point, KillAfterN: killAfterN}
	}
	return trials
}

// CrashResult is the outcome of one crash trial.
type CrashResult struct {
	Trial             CrashTrial
	RecoveryTime      time.Duration // from the kill to the diff oracle reporting equality again
	DiffEqual         bool
	Mismatches        []string
	DuplicatesSkipped int // "skipped duplicate" lines seen from the restarted process
}

// RunCrashHarness runs each trial in sequence against one persistent
// replication slot shared across the whole session — a fresh slot per
// trial would make every "restart" look like a cold start, not a resume,
// which is exactly the property under test. pipelineBinPath must be a
// pre-built cmd/pipeline binary: `go run` wraps the real process in a
// build-and-exec shim that does not propagate SIGKILL to the child, so
// killing the `go run` process would not actually crash the pipeline.
func RunCrashHarness(ctx context.Context, cfg *shared.Config, pipelineBinPath string, trials []CrashTrial) ([]CrashResult, error) {
	cfg.SlotName = crashSlotName
	cfg.PublicationName = crashPubName

	if err := truncateBoth(ctx, cfg); err != nil {
		return nil, fmt.Errorf("truncate accounts: %w", err)
	}
	if err := resetProgressRows(ctx, cfg); err != nil {
		return nil, fmt.Errorf("reset progress rows: %w", err)
	}

	gen, err := NewWorkloadGen(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("new workload generator: %w", err)
	}
	defer gen.Close(ctx)
	defer dropCrashSlot(cfg)

	results := make([]CrashResult, 0, len(trials))
	for _, trial := range trials {
		result, err := runOneTrial(ctx, cfg, pipelineBinPath, gen, trial)
		if err != nil {
			return results, fmt.Errorf("trial %d (%s, killAfterN=%d): %w",
				trial.Index, trial.Point, trial.KillAfterN, err)
		}
		results = append(results, *result)
	}
	return results, nil
}

func runOneTrial(ctx context.Context, cfg *shared.Config, binPath string, gen *WorkloadGen, trial CrashTrial) (*CrashResult, error) {
	h, err := startPipeline(binPath, cfg)
	if err != nil {
		return nil, fmt.Errorf("start pipeline: %w", err)
	}

	// The generator races ahead independently of the pipeline, writing
	// directly to the source — it doesn't need to be synchronized with
	// the kill itself, only with how many ops we ask it to issue.
	opsToRun := trial.KillAfterN + workloadTailBuffer
	genErrCh := make(chan error, 1)
	go func() { genErrCh <- gen.Run(ctx, opsToRun) }()

	killTime, err := waitAndKill(ctx, cfg, h, trial)
	if err != nil {
		return nil, err
	}
	if err := <-genErrCh; err != nil {
		return nil, fmt.Errorf("workload gen: %w", err)
	}

	time.Sleep(restartDelay)

	restartHandle, err := startPipeline(binPath, cfg)
	if err != nil {
		return nil, fmt.Errorf("restart pipeline: %w", err)
	}
	defer func() {
		_ = restartHandle.cmd.Process.Kill()
		_ = restartHandle.cmd.Wait()
	}()

	diffResult, err := waitForConvergence(ctx, cfg)
	if err != nil {
		return nil, err
	}
	recoveryTime := time.Since(killTime)

	return &CrashResult{
		Trial:             trial,
		RecoveryTime:      recoveryTime,
		DiffEqual:         diffResult.Equal(),
		Mismatches:        diffResult.Mismatches,
		DuplicatesSkipped: int(restartHandle.skippedTotal.Load()),
	}, nil
}

// pipelineHandle wraps a running cmd/pipeline subprocess plus counters
// derived from scanning its log output in real time.
type pipelineHandle struct {
	cmd *exec.Cmd

	ready         chan struct{} // fires once when streaming has actually started
	appliedSignal chan struct{} // fires once per "applied" line, for low-latency kill timing
	skippedTotal  atomic.Int64  // "skipped duplicate" lines — the monotonic gate engaging
}

// startPipelineReadyTimeout bounds how long startPipeline waits for the
// "replication started" line before giving up — a fresh process that never
// gets there is a real startup failure, not something to wait out forever.
const startPipelineReadyTimeout = 15 * time.Second

// startPipeline launches binPath and blocks until it has logged that
// replication has actually started (see the comment on that log line in
// consumer.go) before returning. Returning earlier would let a caller start
// writing a workload against a slot that isn't reading yet — for a brand
// new slot, whose retained-WAL starting point is fixed at creation, any
// write that lands in that gap is invisible to it forever, not delayed.
func startPipeline(binPath string, cfg *shared.Config) (*pipelineHandle, error) {
	cmd := exec.Command(binPath,
		"-source-dsn", cfg.SourceDSN,
		"-downstream-dsn", cfg.DownstreamDSN,
		"-slot-name", cfg.SlotName,
		"-publication-name", cfg.PublicationName,
	)

	// cmd/pipeline logs exclusively via the "log" package, which writes to
	// stderr — there is nothing to read from stdout, so only stderr is
	// piped.
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	h := &pipelineHandle{
		cmd:           cmd,
		ready:         make(chan struct{}, 1),
		appliedSignal: make(chan struct{}, 4096),
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start: %w", err)
	}

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case strings.Contains(line, "replication started"):
				select {
				case h.ready <- struct{}{}:
				default:
				}
			case strings.Contains(line, "applied op="):
				select {
				case h.appliedSignal <- struct{}{}:
				default:
				}
			case strings.Contains(line, "skipped duplicate"):
				h.skippedTotal.Add(1)
			}
		}
	}()

	select {
	case <-h.ready:
	case <-time.After(startPipelineReadyTimeout):
		killAndReap(h)
		return nil, fmt.Errorf("pipeline did not log \"replication started\" within %s", startPipelineReadyTimeout)
	}

	return h, nil
}

// waitAndKill blocks until trial.KillAfterN "applied" lines have been
// observed from h, then kills the process — immediately for
// CrashMidTransaction, or after ackGapSettleDelay for CrashAckGap. The
// process is killed and reaped on every exit path, including timeout: an
// abandoned, still-running pipeline subprocess would hold the replication
// slot "active" forever, breaking every subsequent trial (and this
// harness's own cleanup) with "replication slot ... is active for PID ...".
func waitAndKill(ctx context.Context, cfg *shared.Config, h *pipelineHandle, trial CrashTrial) (time.Time, error) {
	seen := 0
	timeout := time.After(killWaitTimeout)
	for seen < trial.KillAfterN {
		select {
		case <-h.appliedSignal:
			seen++
		case <-timeout:
			killAndReap(h)
			return time.Time{}, fmt.Errorf("timed out after seeing %d/%d applied events", seen, trial.KillAfterN)
		case <-ctx.Done():
			killAndReap(h)
			return time.Time{}, ctx.Err()
		}
	}

	if trial.Point == CrashAckGap {
		time.Sleep(ackGapSettleDelay)
	}

	killTime := time.Now()
	killAndReap(h)

	// Postgres does not always notice a killed client's dead TCP
	// connection immediately (no read/write has happened on that socket
	// to trigger detection) — it can keep reporting the slot "active" for
	// a lingering backend well after the OS process is gone. Explicitly
	// terminating that backend makes the restart deterministic instead of
	// racing Postgres's own detection timing.
	_ = terminateSlotBackend(ctx, cfg)

	return killTime, nil
}

func killAndReap(h *pipelineHandle) {
	_ = h.cmd.Process.Kill()
	_ = h.cmd.Wait() // a killed process returns a non-nil, expected error
}

// terminateSlotBackend force-ends any backend still attached to cfg's
// replication slot, so a restart never has to wait on Postgres's own
// dead-connection detection.
func terminateSlotBackend(ctx context.Context, cfg *shared.Config) error {
	conn, err := pgx.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx,
		"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active",
		cfg.SlotName,
	)
	return err
}

// waitForConvergence polls the diff oracle — a direct read of both
// databases, not anything the pipeline reports about itself — until it
// reports exact equality or convergeTimeout elapses.
func waitForConvergence(ctx context.Context, cfg *shared.Config) (*DiffResult, error) {
	waitCtx, cancel := context.WithTimeout(ctx, convergeTimeout)
	defer cancel()
	for {
		result, err := RunDiffOracle(waitCtx, cfg)
		if err != nil {
			return nil, fmt.Errorf("run diff oracle: %w", err)
		}
		if result.Equal() {
			return result, nil
		}
		select {
		case <-waitCtx.Done():
			return result, nil // last (non-equal) result; caller reports the mismatches
		case <-time.After(convergePoll):
		}
	}
}

func truncateBoth(ctx context.Context, cfg *shared.Config) error {
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
			return closeErr
		}
	}
	return nil
}

func resetProgressRows(ctx context.Context, cfg *shared.Config) error {
	conn, err := pgx.Connect(ctx, cfg.DownstreamDSN)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "DELETE FROM replication_progress")
	return err
}

// dropCrashSlot best-effort drops the crash harness's dedicated
// slot/publication so it never retains WAL on the source indefinitely —
// the exact hazard CLAUDE.md's Stage 4 section is about.
func dropCrashSlot(cfg *shared.Config) {
	ctx := context.Background()
	_ = terminateSlotBackend(ctx, cfg)

	conn, err := pgx.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	for i := 0; i < 10; i++ {
		if _, err := conn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", cfg.SlotName)); err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	_, _ = conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", cfg.PublicationName))
}
