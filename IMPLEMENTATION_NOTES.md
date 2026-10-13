# Implementation Notes

Things that came out of actually building and debugging each stage — decisions, tradeoffs, and
behavior discovered empirically — that aren't obvious just from reading the final code. The README
covers the why-this-architecture story; this file is the "here's what happened while building it" log.

---

## Stage 2 — atomic checkpoint + crash harness

### Design decisions and why

- **Kill timing is log-line-triggered, not a random delay.** The crash harness (`cmd/crashharness`,
  `harness/crash_harness.go`) counts `"applied op="` lines from the pipeline's own stderr and kills
  once a seed-derived count (`KillAfterN`) is reached. A wall-clock random delay would be simpler,
  but the timing wouldn't be tied to the seed — replaying a failing seed
  deterministically needs the trigger itself to be deterministic, not just the
  workload.
- **Two crash windows, one mechanism.** `CrashMidTransaction` kills as fast as possible right after
  the Nth applied line (racing to catch the (N+1)th change's transaction still open).
  `CrashAckGap` kills after a settle delay (`ackGapSettleDelay`, 300ms), guaranteeing the Nth
  change already committed. Same counting logic, just a conditional delay before the kill —
  deliberately not two separate code paths.
- **The replication slot is persistent across all trials in a session, not recreated per trial.**
  A fresh slot per trial would make every "restart" indistinguishable from a cold start. The
  property actually under test — resuming correctly from wherever a crash left off — requires
  continuity, so `RunCrashHarness` truncates tables and resets the watermark *once*, then reuses
  the same slot for every kill+restart in the loop.
- **The harness builds a real binary and execs it — it does not use `go run ./cmd/pipeline`.**
  `go run` wraps the actual compiled binary in a separate build-and-exec parent process. Sending
  `SIGKILL` to that parent does not propagate to the child, so nothing would actually crash.
  `buildPipelineBinary` compiles once to a temp dir up front; every trial execs that binary
  directly.
- **Dedicated slot/publication names** (`watermark_crash_slot` / `watermark_crash_pub`), distinct
  from the default `watermark_slot` / `watermark_pub` — so a crash-harness run never collides with
  a manually-run `cmd/pipeline` someone left open in another terminal.
- **`duplicatesSkipped` is only counted from the *restarted* process**, not the one that got
  killed. It's a corroborating signal ("is the monotonic dedup gate actually engaging on restart,"
  not just "did the diff oracle happen to pass") and only means something post-crash.
- **Explicit `pg_terminate_backend` after every kill, not just trusting Postgres to notice the
  dead connection.** Found empirically: `kill -9 <pid>` on the pipeline process succeeds
  immediately, but `pg_replication_slots.active` can stay `true` for several seconds afterward.
  Without forcing it, a restart attempted too soon fails with `replication slot ... is active for
  PID ...` — a restart-timing artifact that looks like a crash-safety failure but isn't one.
- **`-workload-ops` defaults to 30 for this binary specifically** (vs. the shared default of 200),
  set via `fs.Set(...)` before `flag.Parse()` rather than changing `shared/config.go`'s default —
  keeps each trial's cycle fast without affecting `cmd/benchmark` or other binaries that share the
  same flag.

### Two real bugs found while building this (not in the final design, but worth knowing the shape of)

1. **Leaked subprocess on timeout.** Early version of `waitAndKill` returned an error on timeout
   without killing the still-running pipeline process. That abandoned process held the
   replication slot "active" forever, breaking every later attempt (including the harness's own
   cleanup) with the same "slot is active" error. Fixed by killing-and-reaping on *every* exit
   path — timeout, context-cancelled, and the intentional kill.
2. **The real one: workload racing the slot's creation.** The harness fired the seeded workload
   generator concurrently with starting the pipeline subprocess, no synchronization between them.
   A brand-new replication slot's retained-WAL starting point is fixed the instant it's created —
   writes landing before that instant are invisible to it forever, not delayed. Confirmed by
   timestamping `gen.Run`'s completion: it finished *before* the pipeline even logged "created
   replication slot." Fixed by adding one log line to `consumer.go` —
   `"replication started on slot %q"`, logged unconditionally (unlike the slot-creation log line,
   which only fires on a brand-new slot, not a resumed one) — and having the harness block on
   seeing it before firing any workload.

   A red herring chased on the way there: I suspected `cmd.Stderr = cmd.Stdout` (aliasing both
   streams to one pipe) was racy and switched to a plain `cmd.StderrPipe()`. The exact same
   failure persisted afterward, unchanged — ruling that out. Not worth re-investigating if it
   looks suspicious again; the aliasing itself was fine.

### Non-obvious behavior confirmed empirically (not just reasoned about)

- **Per-row LSNs are real, not just theoretical.** Tested three shapes against a live Postgres
  instance: a multi-statement transaction (3 separate `INSERT`s in one `BEGIN`/`COMMIT`), a single
  `INSERT` with 3 `VALUES` rows, and a single multi-row `UPDATE`. All three produced 3 distinct,
  strictly increasing LSNs — Postgres writes one heap-level WAL record per row even within one SQL
  statement, and `pgoutput` streams one message per record. `consumer.go`'s
  `c.decode(xld.WALStart, ...)` call is what makes this per-message, not per-transaction; the
  library's `BeginMessage.FinalLSN` (the commit LSN) exists but is never read into a
  `ChangeEvent`.
- **`go test` result caching can silently replay a stale run.** Re-running
  `go test ./pipeline/... -v` with no source changes served a cached result from an earlier run —
  including one whose printed output looked like a correctness bug (`applied=0/3` with 3 rows
  present downstream, which shouldn't be possible). It wasn't a bug; it was cached stdout from a
  prior invocation. `-count=1` forces genuine re-execution. Worth remembering before trusting any
  surprising output from these DB-touching tests.
- **Recovery time is not flat over a long crash-harness session — and only for mid-transaction
  kills.** Recorded run (seed=7, 50 trials, see `RESULTS.md`): ack-gap recovery stayed tight
  (391–523ms) across the whole session; mid-transaction recovery grew from ~440ms to ~1.78s,
  tracking almost exactly with `duplicatesSkipped` climbing from 25 to 1200+. Root cause:
  `consumer.go`'s standby status update reports only `WALWritePosition`, never
  `WALFlushPosition`, so the slot's `confirmed_flush_lsn` likely never advances — every restart
  re-streams more already-applied history than the last one did. Correctly rejected by the
  monotonic gate every time (that's what's growing), but not free to redeliver and check. This is
  real Stage 4 (slot retention) territory, not a Stage 2 correctness gap — the diff oracle passed
  50/50 regardless.
- **The "missing progress row" defensive error path isn't just theoretical.** Triggered it for
  real while debugging, by deleting the `replication_progress` row out from under an
  already-running pipeline process. It failed loudly (`no replication_progress row for partition
  0: refusing to treat a missing row as a stale duplicate`) instead of silently skipping the
  change — confirming the defensive check added during the Stage 2 cleanup pass actually does its
  job under a real, not just hypothetical, misuse.

### Gotchas for whoever touches this next

- **Never delete `replication_progress` rows while any pipeline process is actively running
  against them** (manual `go run ./cmd/pipeline` or the crash harness) — you'll hit the "refusing
  to treat a missing row as stale" error path above. Correct behavior, but confusing if you don't
  expect it.
- **Fully reset Docker state before manually re-running the crash harness or the consumer
  investigation tests**: truncate `accounts` on both sides, delete `replication_progress`, and
  check for stray replication slots/publications (`SELECT slot_name, active FROM
  pg_replication_slots;`). Leftover state from an earlier manual `go run ./cmd/pipeline` left open
  in another terminal was the source of several confusing failures during development — always check
  `ps aux | grep pipeline` before assuming a bug.
