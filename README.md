# Watermark

A crash-safe change-data-capture (CDC) pipeline in Go. It streams row changes out of a source
Postgres via logical replication and applies them to a downstream Postgres so that, after the
pipeline process is killed at an arbitrary point and restarted, the two databases are **exactly
equal, row for row**. "Exactly equal" is checked by an independent diff oracle under seeded,
reproducible crashes, not inferred from the pipeline's own logs.

> **Scope, stated up front.** WAL decoding is done by [`jackc/pglogrepl`](https://github.com/jackc/pglogrepl)
> (Postgres's `pgoutput` protocol). We did not build a WAL decoder. What this repo builds is
> everything downstream of decode: the apply path, the atomic checkpoint, idempotency, the
> monotonic gate, and the harness that proves them under crashes.

---

## Status

| Stage | What it adds | State |
|---|---|---|
| 1 | Single consumer, happy path, diff oracle, throughput + lag instrumentation | **Done** |
| 2 | Atomic apply-and-checkpoint, idempotent apply, monotonic LSN gate, seeded crash harness | **Done** |
| 3 | Key-partitioned parallel apply, retry with backoff + jitter, fault injector | Not started |

---

## The problem: CDC fails silently

Postgres can only offer **at-least-once** delivery to a logical-replication consumer. The consumer
tells Postgres "I've durably handled everything up to LSN X" (`confirmed_flush_lsn`); anything after
X is resent on reconnect. A consumer that crashes after doing work but before saying so gets that work
again.

A naive apply loop does two things per change: (1) write the change downstream, (2) record "done
up to X". A crash between them goes one of two ways:

- **Record, then crash before writing** → the change is marked done but never applied. Lost.
- **Write, then crash before recording** → the change is redelivered and applied again. If the
  apply was `balance = balance - 10`, it's now subtracted twice.

Neither raises an error. The pipeline keeps running and the databases quietly disagree until a
human notices. Shrinking the window between the two steps doesn't fix this; rare is not correct.
Every design decision below exists to **remove** that window, and the harness exists to prove it's gone.

---

## Architecture

```
 source Postgres (wal_level=logical)
   │  publication watermark_pub + logical replication slot watermark_slot
   │  pgoutput stream, decoded by pglogrepl
   ▼
 pipeline/consumer.go     builds shared.ChangeEvent {LSN, TxID, CommitTime, Table, Op, Key, Columns}
   │  chan ChangeEvent (cap 100), single goroutine — no concurrency until Stage 3
   ▼
 pipeline/checkpoint.go   ONE downstream transaction per change:
   │                        BEGIN
   │                          UPDATE replication_progress
   │                             SET last_lsn = $lsn
   │                           WHERE partition_id = $p AND last_lsn < $lsn   -- gate + lock
   │                          ── 0 rows → stale/duplicate: ROLLBACK, count as skipped
   │                          ── 1 row  → apply the change (pipeline/apply.go), same tx
   │                        COMMIT
   ▼
 downstream Postgres      accounts + replication_progress(partition_id, last_lsn)

 harness/workload_gen.go  seeded INSERT/UPDATE/DELETE stream → source
 harness/diff_oracle.go   reads BOTH databases directly → exact row/column diff
 harness/crash_harness.go seeded SIGKILL + restart + convergence check
```

---

## Design decisions

Each decision lists the alternative it beat and what it costs, since that is the part worth
defending.

### 1. The watermark lives in the downstream, committed in the same transaction as the apply

**Why.** Atomicity is scoped to one database. If the progress record lives anywhere else (a file,
the source, Redis), "apply" and "record" are two commits again and the crash window comes back.
Putting `last_lsn` in a downstream table and updating it inside the apply transaction means a crash
before `COMMIT` loses both, a crash after keeps both, and there is no state in between. We lean on
Postgres's own atomicity rather than building one.

**Alternatives rejected.** Two-phase commit across source and downstream: blocks on coordinator
failure, and the source isn't a transaction participant in a replication stream anyway. A watermark
file with `fsync`: still two separate durable writes.

**Cost.** Progress tracking is now coupled to the downstream having transactions. A
non-transactional sink (a search index, S3) would need a different technique, e.g. per-record
version checks in the sink.

### 2. The gate is one gated `UPDATE`, not read-compare-write

```sql
UPDATE replication_progress SET last_lsn = $1
 WHERE partition_id = $2 AND last_lsn < $1;
```

**Why.** The comparison and the row lock happen in a single statement and a single round trip.
`RowsAffected() == 0` means "at or below the watermark → already accounted for". The earlier version
did `SELECT ... FOR UPDATE`, compared in Go, then wrote: same guarantee, one extra round trip.

**Detail that makes it safe.** `0 rows` must only ever mean *stale*, never *row missing*. The
progress row is pre-seeded at startup, and a missing row surfaces as an error rather than a silent
skip.

**Why the gate is needed at all, given atomicity.** Atomicity closes the crash-*mid*-transaction
window. It does nothing about the **acknowledgment gap**: we commit downstream, then die before
telling Postgres. Postgres correctly resends. Logical decoding also redelivers whole transactions
past the restart point. Some redelivery is unavoidable; the gate makes it a no-op.

### 3. Idempotent apply for every operation

Doing an operation twice must be indistinguishable from doing it once.

| Op | Statement | Why it's idempotent |
|---|---|---|
| Insert | `INSERT ... ON CONFLICT (id) DO UPDATE` | Second apply lands the same row |
| Update | Absolute set of every column from the new tuple | `pgoutput` sends the full new row, so we never need a delta |
| Delete | `DELETE WHERE id = $1` | Second apply affects 0 rows |

Idempotency and the gate overlap on purpose: the gate stops stale *reorders* (an older value
overwriting a newer one), idempotency makes any duplicate that does slip through harmless.

**The phrase to use:** exactly-once *delivery* across a crash boundary isn't achievable here.
At-least-once delivery plus an idempotent, gated apply gives exactly-once **effect**.

### 4. `replication_progress` is keyed by partition from day one

Stage 2 runs one consumer and one partition, so the key does nothing yet. It exists so Stage 3
(`hash(pk) % N` workers, one watermark per partition) adds rows instead of changing the schema.
Changing N later invalidates every watermark (keys rehash to other partitions) and forces a resync.
The row lock taken by the gated `UPDATE` is likewise a no-op today and matters once workers exist.

### 5. Logical replication, not polling an `updated_at` column

Polling can't see deletes (the row is gone, so nothing carries a timestamp), collapses multiple
changes between polls into one, can tear a transaction whose rows straddle the poll boundary, and
scans whether or not anything changed. Polling is simpler and needs no replication privileges, which
is a legitimate reason to choose it when deletes don't matter. Here, complete delete-aware capture is
the point.

### 6. Go

This is an I/O-bound coordination problem: the process spends its time waiting on the replication
socket and on downstream commits. C++'s advantage (no GC, deterministic latency) buys nothing on that
workload. Goroutines and channels map directly onto Stage 3's per-partition worker structure.
Consequence: **we make no tail-latency claims.** Lag below is milliseconds and network-dominated.

---

## Proving it: the crash harness

The method is the same at every stage:

```
Workload:   fixed seed → identical INSERT/UPDATE/DELETE sequence against the source.
Fault:      the stage's fault set, driven by the same seed.
Assert:     diff(source, downstream) is empty. Full outer join on primary key, so an extra
            downstream row (a missed delete) also fails. Any mismatch names the row and column.
Reproduce:  same seed → same workload → same crash schedule.
```

The oracle reads both databases with plain SQL and never uses anything the pipeline reports about
itself. The pipeline's "duplicates skipped" counter is logged only as corroboration that the gate
actually fired after a restart.

### What one trial does

1. The seed picks the crash window (**mid-transaction** or **ack gap**) and `KillAfterN`.
2. Start the pre-built pipeline binary; block until it logs `replication started`.
3. Fire `KillAfterN + 10` workload ops (the extra 10 guarantee something is in flight to interrupt).
4. Count `applied` lines on the pipeline's stderr. At N, `SIGKILL` it: immediately for
   mid-transaction, after a 300 ms settle for ack gap.
5. Reap the process and `pg_terminate_backend` the slot's walsender.
6. Wait 300 ms, restart, poll the oracle every 50 ms (30 s timeout) until the databases are equal.
7. Record recovery time (kill → equal), oracle result, and duplicates skipped by the restarted process.

### Why each choice

| Choice | Reason |
|---|---|
| `SIGKILL`, not `SIGTERM`/panic | Can't be caught; no `defer`s, no graceful flush. `SIGTERM` tests shutdown, not crash safety. |
| Kill triggered by log-line count, not wall-clock delay | The trigger must depend on the seed for a failing trial to be replayable. |
| Exec a built binary, not `go run` | `go run` is a parent that execs the real program; `SIGKILL` hits the parent and the child survives. |
| One slot for the whole session | A fresh slot per trial makes every restart a cold start; the property under test is *resume*. |
| Explicit `pg_terminate_backend` | Postgres takes seconds to notice a dead client and keeps the slot "active", so restart fails with a timing artifact, not a correctness failure. |
| Wait for `replication started` | A new slot's start point is fixed when it's created. Writes before that are invisible to it forever, not merely late. |
| Kill and reap on every exit path, including timeouts | An orphaned pipeline holds the slot and poisons every later trial. |

### Bugs the harness found

- **Workload racing slot creation.** Trials diverged because the workload committed before the slot
  existed, so those changes were never in the stream. Proven by timestamping workload completion
  against slot creation; fixed by gating the workload on the `replication started` signal.
  (Red herring on the way: suspected stdout/stderr aliasing, changed it, failure unchanged.)
- **Leaked subprocess on timeout.** The harness returned an error without killing the pipeline; the
  orphan kept the slot active and broke every subsequent trial. Fixed by kill-and-reap on all paths.

---

## Results

Full methodology, commands and per-run tables are in [RESULTS.md](RESULTS.md). Every number below
was produced on our hardware with a stated seed. Anything unmeasured is marked `[to be measured]`.

**Hardware:** MacBook Air M2, 8 cores, 8 GB, macOS 26.2, Go 1.26.5, Postgres 16-alpine ×2 in Docker.

| Measurement | Result | How |
|---|---|---|
| Crash safety | **50 / 50** seeded trials converged to exact equality | seed 7: 27 ack-gap + 23 mid-transaction `SIGKILL`s, one persistent slot |
| Recovery time (kill → oracle-equal) | **p50 443 ms** overall; ack-gap 391–523 ms; mid-transaction p50 719 ms, max 1.78 s | Includes ~300 ms fixed harness restart delay; see caveats |
| Redelivery suppressed | **1,200+** already-applied changes dropped by the gate on a single restart, 0 divergent rows | Counted from the restarted process only |
| Throughput, Stage 1 apply path | **2,396 changes/s** median (10 runs × 1,000 ops, seed 1) | Source commit → oracle-verified equality; external, not self-reported |
| Commit-to-apply lag, Stage 1 | **p50 10 ms / p90 25 ms / p99 34 ms** (5,000 pooled events) | Self-reported by the pipeline from `CommitTime` |
| Throughput, Stage 2 checkpointed path | `[to be measured]` | |

**Caveats on these numbers.**

- **The throughput figure predates the checkpoint.** It was measured on Stage 1's one-statement
  apply. The Stage 2 path adds an explicit `BEGIN`, the gated `UPDATE`, and `COMMIT` per change. That
  is still **one** WAL flush per change (Stage 1's autocommit also committed), so the expected cost is
  extra round trips, not extra durability.
- **Recovery time is mostly harness constants.** The 300 ms restart delay, 50 ms poll granularity
  and process start explain most of the ~390 ms floor.
- **Mid-transaction recovery grew across the session** (≈440 ms → 1.78 s). The slot's
  `confirmed_flush_lsn` never advanced during trials (no trial outlived the 10 s status interval), so
  each restart replayed more history. The rising duplicates-skipped count confirms it. This is a
  performance effect, not a correctness one: every trial still converged.
- **Throughput and lag are separate measurements**, not one number restated: throughput is
  wall-clock over a run, lag is a per-event timestamp delta.
- macOS numbers only. Linux x86 will differ.

---

## Scope and simplifications

Things a reviewer will notice, and what a production system would do instead.

- **Library WAL decode.** See the scope note at the top.
- **Downstream is a second Postgres.** The consistency problem (apply each change with exactly-once
  effect, in order, across crashes) is the same whatever the sink is. The atomic-checkpoint technique
  does depend on the sink being transactional.
- **One downstream transaction per row change**, not per source transaction. A reader of the
  downstream can see half of a source transaction (a transfer debited, not yet credited). `TxID` is
  carried in every event so this can be fixed by grouping on it.
- **One table, single-column integer PK, single-row autocommit workload from one connection.** The
  workload never produces interleaved or multi-row source transactions, which is exactly where
  [gap 1](#known-gaps-open) hides.
- **UPDATEs that change the primary key are not handled.** `OldTuple` is ignored, so the old row is
  orphaned downstream.
- **NULL values don't propagate.** NULL columns are dropped from the `SET` clause; the `NOT NULL`
  schema hides this.
- **No DDL.** `ALTER TABLE` on the source mid-stream is out of scope.
- **The oracle is a full row diff.** Cheap and exact at this scale. At billions of rows you'd compare
  per-key-range checksums or sample.
- **The harness kills the process, not the machine.** The OS page cache survives, so this does not
  test durability against power loss; that is Postgres's guarantee, not the pipeline's.
- **Reproducible crash schedule, not deterministic replay.** The seed fixes the workload and *when*
  the kill fires. Goroutine scheduling, Postgres timing and the network are not controlled. This is not
  FoundationDB-style deterministic simulation.
- **"Mid-transaction" kills are not provably mid-transaction.** The `applied` line is logged after
  `COMMIT` and passes through a pipe, a scanner goroutine and a channel before the kill. The fix is a
  build-tagged failpoint that has the process `SIGKILL` itself between the apply and `COMMIT`.

---

## Known gaps (open)

Found in review after the 50-trial run. None was exercised by the current workload, which is why the
oracle stayed green. Listed with the fix.

1. **The watermark is a per-row LSN, which is not monotonic across interleaved transactions.**
   `pgoutput` delivers transactions in *commit* order, but each row carries the LSN of its own WAL
   record. With T1 writing at LSN 100, T2 writing at 110 and committing at 120, then T1 committing at
   130, T2's row arrives first, the watermark becomes 110, and T1's row at 100 fails the gate and is
   dropped as a "duplicate". Permanent, silent loss.
   **Fix:** watermark on the transaction's **commit LSN** (`CommitMessage.CommitLSN`), which is
   monotonic in stream order, and apply each source transaction as one downstream transaction. This is
   what Postgres's built-in subscriber does with replication origins, and it also fixes the
   partial-transaction visibility above.
2. **The consumer acknowledges the *received* position, not the *applied* one.** The 10 s status
   update reports the last position read off the socket (advanced further by keepalives), which can
   include events still in the 100-slot channel. Once acknowledged, Postgres may recycle that WAL; a
   crash then loses those events and they are never redelivered.
   **Fix:** only ever acknowledge the durable downstream watermark. The invariant is
   `confirmed_flush_lsn ≤ downstream last_lsn`. Then make the status interval configurable and add a
   harness trial that kills the process *after* an acknowledgment.
3. **An apply error is logged and skipped.** The next successful change moves the watermark past the
   failed one forever.
   **Fix:** exit non-zero and let the restart replay it (simplest and obviously correct), or retry the
   same event with backoff.

Planned order: extend `WorkloadGen` to multiple connections with interleaved multi-row transactions,
watch the oracle fail on gap 1, then fix gaps 1 and 2 together (the commit LSN is exactly what gap 2
should acknowledge).

---

## Running it

Requires Docker and Go 1.26+.

```bash
docker compose up -d                       # source on :5433, downstream on :5434, schema auto-applied

go run ./cmd/pipeline                      # terminal 1: consume + apply; prints METRICS lines
go run ./cmd/workloadgen -seed 1 -workload-ops 1000   # terminal 2: drive the source
go run ./cmd/diffcheck                     # exact source-vs-downstream diff; names any mismatch

go run ./cmd/benchmark -workload-ops 1000 -runs 10    # throughput, oracle-timed (pipeline must be running)
go test ./pipeline/...                     # checkpoint integration tests (needs both DBs up)

go run ./cmd/crashharness -trials 50 -seed 7   # seeded SIGKILL trials; builds and execs cmd/pipeline itself
```

Every binary shares the flags in `shared/config.go` (`-source-dsn`, `-downstream-dsn`, `-slot-name`,
`-publication-name`, `-seed`, `-workload-ops`). All randomness flows from `-seed`; nothing is seeded
from `time.Now()`.

---

## Layout

```
pipeline/
  consumer.go          replication slot + publication setup, pgoutput → ChangeEvent       (S1)
  apply.go             idempotent upsert / absolute update / delete against a pgx.Tx      (S1, S2)
  checkpoint.go        gated watermark UPDATE + apply in one transaction                  (S2)
  checkpoint_test.go   duplicate, equal-LSN, stale-overwrite, rollback-atomicity tests    (S2)
  consumer_test.go     consumer tests                                                      (S2)
harness/               test infrastructure only; never imported by /pipeline
  workload_gen.go      seeded INSERT/UPDATE/DELETE generator                              (S1)
  diff_oracle.go       independent row-by-row diff of both databases                      (S1)
  crash_harness.go     seeded SIGKILL / restart / convergence trials                      (S2)
shared/
  types.go             ChangeEvent: a plain value, so a fault injector can copy, duplicate
                       or reorder it without touching the production path
  config.go            the one CLI surface every binary shares
cmd/                   pipeline, workloadgen, diffcheck, benchmark, crashharness, consumer
schema.sql             accounts + replication_progress (applied to both DBs)
RESULTS.md             every measured number, with hardware, seed, command and run count
```

---

## Roadmap

- **Stage 3, parallel apply.** `hash(pk) % N` dispatch so all changes to one key land on one worker
  in arrival order; different keys run in parallel. Order only matters per key, so this is the
  maximum safe parallelism, not arbitrary parallelism. Retry with exponential backoff + jitter. Proof:
  oracle green on a same-row-contended workload under crashes, plus a throughput-scaling curve against
  the single-worker baseline.
