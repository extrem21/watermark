-- Applied identically to both source-db and downstream-db.
-- Stage 1: a single table is enough to prove happy-path replication.
CREATE TABLE IF NOT EXISTS accounts (
    id      INTEGER PRIMARY KEY,
    owner   TEXT NOT NULL,
    balance INTEGER NOT NULL
);

-- Stage 2: the watermark table behind atomic apply-and-checkpoint. Only the
-- downstream copy is ever read or written by the pipeline — the source's
-- copy (created by this same init script running on both containers) is
-- inert. It must live in the downstream specifically because the watermark
-- has to commit in the same transaction as the row write it accompanies, and
-- a transaction is scoped to one database.
--
-- last_lsn is BIGINT via bit-reinterpretation of the uint64 LSN (Postgres
-- has no native unsigned 64-bit type). The monotonic "already applied" check
-- runs as a signed SQL comparison (`last_lsn < $1` in the gated UPDATE in
-- pipeline/checkpoint.go) — the sign bit never matters in practice because
-- real LSN values won't approach 2^63 within this project's lifetime.
--
-- One row per partition, keyed from day one even though Stage 2 has exactly
-- one partition (id 0, the pipeline is still single-threaded) — so Stage 3's
-- key-partitioned dispatch only has to start writing more rows, never change
-- this table's shape.
--
-- That same gated UPDATE takes an implicit row-level lock on the partition's
-- row. Today that lock is a no-op: Stage 1/2 run a single consumer goroutine,
-- so nothing ever contends for this row. It exists for Stage 3, where each
-- partition is owned by its own worker goroutine and the lock is what keeps
-- concurrent access to a given partition's row safe.
--
-- Changing the partition count (N in hash(pk) % N, Stage 3+) invalidates
-- every watermark in this table: the same key rehashes to a different
-- partition_id, so a partition's recorded last_lsn no longer describes the
-- set of keys actually routed to it. There is no in-place migration for
-- this — changing N requires a full resync from a fresh snapshot, not an
-- edit to this table.
CREATE TABLE IF NOT EXISTS replication_progress (
    partition_id INTEGER PRIMARY KEY,
    last_lsn     BIGINT NOT NULL
);
