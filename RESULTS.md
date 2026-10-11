# Results

Every number here was produced by a command in this repo, on the hardware and workload stated next
to it. No estimated or borrowed numbers. If a number can't be regenerated on demand with the given
command, seed, and run count, it doesn't belong in this file — see `CLAUDE.md`'s ideology section.

## Hardware

- MacBook, Apple M2, 8 cores, 8 GB RAM
- macOS 26.2 (build 25C56)
- Go 1.26.5 darwin/arm64
- Docker 29.6.1
- Postgres 16-alpine (both source and downstream containers, via `docker-compose.yml`)

## Stage 1 — baseline throughput (single consumer, happy path, no faults)

**What this measures:** end-to-end throughput — source commit to downstream verified row-for-row
equal — for the single-threaded Stage 1 pipeline. This is the baseline Stage 3's parallel-apply
number will be measured against.

**How it's measured:** `cmd/benchmark` truncates both `accounts` tables, injects a fixed seeded
workload via `harness.WorkloadGen`, times from just before the first op to the moment
`harness.RunDiffOracle` reports exact equality (a direct read of both databases — not anything
`cmd/pipeline` reports about itself), and repeats for N runs. Same seed replayed every run, so
run-to-run variation reflects timing noise, not workload differences.

**Command:**
```
# terminal 1
docker compose up -d
# terminal 2
go run ./cmd/pipeline
# terminal 3
go run ./cmd/benchmark -workload-ops 1000 -runs 10
```

**Config:** seed=1, workload-ops=1000, runs=10

| run | elapsed | throughput (changes/sec) |
|-----|---------|---------------------------|
| 1   | 496ms   | 2014.53 |
| 2   | 363ms   | 2755.54 |
| 3   | 337ms   | 2963.22 |
| 4   | 413ms   | 2419.76 |
| 5   | 420ms   | 2378.45 |
| 6   | 502ms   | 1993.33 |
| 7   | 387ms   | 2584.24 |
| 8   | 464ms   | 2153.76 |
| 9   | 425ms   | 2352.65 |
| 10  | 414ms   | 2414.02 |

**Median: 417ms elapsed, 2396.10 changes/sec** (end-to-end: source commit → downstream verified
equal)

**Notes / honest caveats:**
- This is a single sample set (N=10 runs, one sitting). No claim of statistical significance beyond
  "this is what 10 runs looked like on this machine on this date."
- Run-to-run spread (~2000–2960 changes/sec) is real system noise (connection handling, Postgres
  planner/cache state, goroutine scheduling, GC) — not measurement error. The median is reported
  specifically because it's robust to that spread; do not read the per-run numbers as independently
  meaningful.
- This is end-to-end throughput (source commit → downstream confirmed applied), not raw apply-loop
  throughput isolated from workload-injection speed — those are not separable with the current
  single-process harness design, and conflating them is called out as dishonest in `CLAUDE.md`.
- Not a latency claim — see the separate lag section below. Throughput and lag are two different
  measurements here, not the same number restated (`CLAUDE.md` bans presenting throughput and
  per-op latency as independent wins specifically when the second is just 1/throughput; lag is
  measured from a distinct timestamp per event, so it isn't that).

## Stage 1 — end-to-end replication lag (single consumer, happy path, no faults)

**What this measures:** per-change delay from source commit to downstream apply
(`time.Since(ev.CommitTime)` at the moment `pipeline.Applier.Apply` returns), not a rate. This is
the second of the two required Stage 1 measurements — a different question from throughput ("how
stale can the downstream be behind the source," not "how many changes fit through per second").

**How it's measured:** `cmd/pipeline` now accumulates per-event lag within each throughput burst
(see the throughput section above for what a "burst" is) and reports p50/p90/p99 alongside the
burst's throughput line, computed via nearest-rank on the sorted in-burst samples. This is
self-reported by the pipeline, not independently re-derived the way the throughput number is —
lag requires knowing `ev.CommitTime`, which only the pipeline observes.

**Not a tail-latency claim in the sense `CLAUDE.md` bans.** This is I/O-bound, GC'd Go; lag here is
milliseconds, dominated by network/Postgres round-trips, not a claim of sub-millisecond precision.

**Command:**
```
# terminal 1
docker compose up -d
# terminal 2 (read the METRICS line from here)
go run ./cmd/pipeline
# terminal 3
go run ./cmd/benchmark -workload-ops 1000 -seed 1 -runs 5
```

**Config:** seed=1, workload-ops=1000, runs=5 — pooled into one 5000-event sample (see caveat)

| stat | value |
|------|-------|
| p50  | 10ms |
| p90  | 25ms |
| p99  | 34ms |

(from `cmd/pipeline`'s `METRICS` line: `throughput=2431.27 changes/sec burst=2.057s applied=5000
lag_p50=10ms lag_p90=25ms lag_p99=34ms`, 2026-07-17)

**Notes / honest caveats:**
- The 5 runs did **not** produce 5 separate bursts. `cmd/pipeline`'s burst detector only closes a
  burst after `burstIdleGap` (500ms) of silence; the gap between one run finishing and the next
  run's `resetTables` + first insert landing was apparently under that threshold, so all 5 runs'
  events (5000 total) were reported as one continuous burst instead of 5 discrete ones. This is a
  real, timing-dependent artifact of the burst-detection design — whether N runs merge into one
  burst or stay separate isn't guaranteed identical run to run, since it depends on how fast
  `resetTables`/reconnect happens to be relative to the 500ms threshold.
- Consequence: this is a pooled 5000-sample measurement, not a median of 5 independent per-run
  samples as originally planned. No warm-up exclusion was applied — whatever cold-start effect run
  1 has is diluted into the pool along with runs 2–5, not excluded from it. In exchange, the sample
  size is 5x larger than a single run would give, which is a real (if different) improvement in
  statistical robustness.
- Consistent with the earlier informal 500-sample capture (p50 11.6ms, p99 19ms) and the single
  cold-run measurement (p50 17ms, p99 49ms) — this pooled number sits between them, which is
  exactly what you'd expect from blending one cold-start-affected run into a larger warm pool.
- This is I/O-bound, GC'd Go; these are millisecond-scale, network/Postgres-round-trip-dominated
  numbers, not a sub-millisecond precision claim.

## Stage 2 — crash-safety proof + recovery time (seeded process kills, atomic apply-and-checkpoint)

**What this measures:** whether the atomic apply-and-checkpoint transaction (`pipeline/checkpoint.go`)
actually survives a real `SIGKILL` of the running pipeline process, not just the in-process unit
tests in `pipeline/checkpoint_test.go`. Each trial kills the real `cmd/pipeline` binary at one of
the two crash windows named in `CLAUDE.md` — mid-transaction (as fast as possible after the Nth
applied change, racing to catch the next change's still-open transaction) or the acknowledgment
gap (after a settle delay, so the Nth change has certainly committed downstream but Postgres hasn't
necessarily been told so) — restarts it against the same persistent replication slot, and asserts
`harness.RunDiffOracle` converges to exact row-for-row equality. This is the headline claim of the
whole project: not "it seems to work," but N reproducible seeded crash scenarios, passing or the
exact failing one named.

**How it's measured:** `cmd/crashharness` builds a real `cmd/pipeline` binary (not `go run`, which
wraps the compiled binary in a build-and-exec parent that doesn't propagate `SIGKILL` to the child),
derives N trials deterministically from one seed (`harness.GenerateTrials`), and runs them in
sequence against one continuous session — the replication slot and the downstream's
`replication_progress` watermark both persist across trials and restarts, exactly as they would
across real crashes in production. Recovery time is measured from the moment the kill signal is
sent to the moment the diff oracle reports equality again.

**Command:**
```
# terminal 1
docker compose up -d
# terminal 2
go run ./cmd/crashharness -trials 50 -seed 7
```

**Config:** seed=7, trials=50, workload-ops=30 (ops per trial, the `-workload-ops` default this
binary uses)

**Result: 50/50 trials passed — exact row-for-row equality after every single seeded crash+restart.**

| crash point | n | recovery min | recovery p50 | recovery p90 | recovery max |
|---|---|---|---|---|---|
| ack-gap | 27 | 391ms | 418ms | 443ms | 523ms |
| mid-transaction | 23 | 441ms | 719ms | 1.28s | 1.78s |
| **all 50** | 50 | 391ms | 443ms | 1.15s | 1.78s |

(from the run's own `TRIAL ...` / `RECOVERY ...` log lines, seed=7, 2026-07-27; wall-clock ~65s for
all 50 trials)

**Notes / honest caveats:**
- **Recovery time is not constant — it grows over the session, and only for mid-transaction
  kills.** Ack-gap recovery stays tight (391–523ms) across the whole run. Mid-transaction recovery
  grows from ~440ms early in the run to ~1.8s by trial 49, tracking almost exactly with the
  `duplicatesSkipped` count reported per trial (25 early on, over 1200 by the last trials). The
  cause: `pipeline/consumer.go`'s standby status update only reports `WALWritePosition`, never
  `WALFlushPosition`, so Postgres's `confirmed_flush_lsn` for the slot likely never advances. Every
  restart therefore resumes from further back than strictly necessary and re-streams more
  already-applied history than the last restart did — correctly rejected by the monotonic
  `last_lsn < $1` gate every time (that's what `duplicatesSkipped` counts), but not for free: more
  redelivered history to stream and check per restart, dominating mid-transaction recovery time as
  the session goes on. This is a real, honest limitation of the current implementation, not a
  correctness gap — the diff oracle passed 50/50 regardless — and the natural fix (properly
  advancing the flush position) is Stage 4 slot-retention territory, not Stage 2.
- Ack-gap trials don't show this growth because `ackGapSettleDelay` gives each one a similar, small,
  roughly-constant amount of unconfirmed history to redeliver, regardless of session length.
- `-workload-ops 30` (ops per trial) is a `cmd/crashharness`-specific default, not the same
  `-workload-ops` semantics as `cmd/benchmark`'s single big batch — here it's replayed once per
  trial, 50 times, against one continuously-running session.
- Single sample set (N=50 trials, one sitting, one seed), same caveat as the Stage 1 numbers above:
  this is what seed=7 looked like on this machine on this date, reproducible on demand with the
  exact command above, not a claim of statistical significance beyond that.
