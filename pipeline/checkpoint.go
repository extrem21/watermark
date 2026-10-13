// APPLY PATH — must be idempotent; must commit atomically with the watermark.
package pipeline

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/extrem21/watermark/shared"
)

// singlePartitionID is the only partition that exists before Stage 3 adds
// key-partitioned dispatch. replication_progress is keyed by partition_id
// from day one specifically so Stage 3 only has to start writing more rows,
// never change this table's shape.
const singlePartitionID = 0

// Checkpointer is the production apply path from Stage 2 on: every change is
// applied and its watermark advanced in one downstream transaction, so a
// crash at any point leaves no in-between state — either both happened or
// neither did. Two separate steps (apply, then record progress) can't be made safe by
// shrinking the gap between them; this closes the gap instead of shrinking
// it.
type Checkpointer struct {
	pool        *pgxpool.Pool
	partitionID int32
}

// NewCheckpointer connects to the downstream and ensures a progress row
// exists for the partition, starting at watermark 0 on a fresh database.
// This pre-seeding is load-bearing: ApplyAndCheckpoint's gated UPDATE
// depends on the row already existing so that "0 rows affected" can only
// ever mean "stale," never "no row to update."
func NewCheckpointer(ctx context.Context, cfg *shared.Config) (*Checkpointer, error) {
	pool, err := pgxpool.New(ctx, cfg.DownstreamDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to downstream: %w", err)
	}

	c := &Checkpointer{pool: pool, partitionID: singlePartitionID}
	if err := c.ensureProgressRow(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return c, nil
}

// Close releases the downstream connection pool.
func (c *Checkpointer) Close() {
	c.pool.Close()
}

func (c *Checkpointer) ensureProgressRow(ctx context.Context) error {
	_, err := c.pool.Exec(ctx,
		"INSERT INTO replication_progress (partition_id, last_lsn) VALUES ($1, 0) ON CONFLICT (partition_id) DO NOTHING",
		c.partitionID,
	)
	if err != nil {
		return fmt.Errorf("ensure progress row: %w", err)
	}
	return nil
}

// Watermark returns the partition's last committed LSN — for recovery-time
// measurement and any future consumer resume logic.
func (c *Checkpointer) Watermark(ctx context.Context) (uint64, error) {
	var last int64
	err := c.pool.QueryRow(ctx,
		"SELECT last_lsn FROM replication_progress WHERE partition_id = $1", c.partitionID,
	).Scan(&last)
	if err != nil {
		return 0, fmt.Errorf("read watermark: %w", err)
	}
	return uint64(last), nil
}

// ApplyAndCheckpoint applies ev and advances the watermark in a single
// transaction.
//
// The monotonic check and the row lock that makes it race-safe are both
// folded into one gated UPDATE, replacing a separate SELECT ... FOR UPDATE
// plus an in-Go comparison: if last_lsn is already >= ev.LSN, the WHERE
// clause matches no row, RowsAffected() is 0, and that alone is the
// "already applied, skip it" decision — made by Postgres in one round trip
// instead of a read followed by a Go-side comparison. (The row-level lock
// this UPDATE takes is documented in schema.sql — it's a no-op today, since
// Stage 1/2 run a single consumer goroutine.)
//
// applied reports whether ev was actually written (false = skipped as a
// duplicate). Both outcomes are correct; callers use this only for
// logging/metrics, never to decide whether to retry.
func (c *Checkpointer) ApplyAndCheckpoint(ctx context.Context, ev shared.ChangeEvent) (applied bool, err error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once Commit below succeeds

	tag, err := tx.Exec(ctx,
		"UPDATE replication_progress SET last_lsn = $1 WHERE partition_id = $2 AND last_lsn < $1",
		int64(ev.LSN), c.partitionID,
	)
	if err != nil {
		return false, fmt.Errorf("advance watermark: %w", err)
	}

	if tag.RowsAffected() == 0 {
		// This must mean exactly one thing: ev.LSN is at or below the
		// recorded watermark. It must never silently mean "no progress row
		// for this partition" — that would be a change getting dropped
		// forever, not a duplicate being correctly skipped. NewCheckpointer
		// seeds the row before the pipeline ever applies anything and
		// nothing ever deletes it, so the row should always exist here;
		// checking anyway costs one query on an already-rare path (this
		// only runs when a change turns out to be a duplicate) and turns a
		// "should never happen" into a loud error instead of silent data
		// loss if it ever does.
		exists, existsErr := c.progressRowExists(ctx, tx)
		if existsErr != nil {
			return false, fmt.Errorf("verify progress row exists: %w", existsErr)
		}
		if !exists {
			return false, fmt.Errorf(
				"no replication_progress row for partition %d: refusing to treat a missing row as a stale duplicate",
				c.partitionID)
		}
		// Genuinely stale/duplicate: the gated UPDATE's WHERE clause
		// excluded it, so nothing was written. Let the deferred Rollback
		// close the transaction — there's nothing to commit.
		return false, nil
	}

	if err := applyChange(ctx, tx, ev); err != nil {
		return false, fmt.Errorf("apply change: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}

func (c *Checkpointer) progressRowExists(ctx context.Context, tx pgx.Tx) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM replication_progress WHERE partition_id = $1)",
		c.partitionID,
	).Scan(&exists)
	return exists, err
}
