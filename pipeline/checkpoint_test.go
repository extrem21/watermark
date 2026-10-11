package pipeline

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/extrem21/watermark/shared"
)

// downstreamTestDSN matches shared.Config's default -downstream-dsn. These
// tests exercise the real atomic apply-and-checkpoint transaction against
// the disposable Stage 1/2 downstream container (docker-compose up), the
// same way cmd/benchmark and cmd/diffcheck do — there is no mock here,
// because the property under test (atomicity, monotonicity) is a database
// transaction guarantee, not something a mock can stand in for.
const downstreamTestDSN = "postgres://watermark:watermark@localhost:5434/watermark_downstream"

func newTestCheckpointer(t *testing.T) *Checkpointer {
	t.Helper()
	ctx := context.Background()
	cfg := &shared.Config{DownstreamDSN: downstreamTestDSN}

	conn, err := pgx.Connect(ctx, downstreamTestDSN)
	if err != nil {
		t.Skipf("downstream not reachable (is docker-compose up?): %v", err)
	}
	if _, err := conn.Exec(ctx, "TRUNCATE accounts"); err != nil {
		t.Fatalf("truncate accounts: %v", err)
	}
	if _, err := conn.Exec(ctx, "DELETE FROM replication_progress WHERE partition_id = $1", singlePartitionID); err != nil {
		t.Fatalf("reset progress row: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close setup conn: %v", err)
	}

	c, err := NewCheckpointer(ctx, cfg)
	if err != nil {
		t.Fatalf("new checkpointer: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func insertEvent(lsn uint64, key string, balance string) shared.ChangeEvent {
	return shared.ChangeEvent{
		LSN:       lsn,
		Table:     "accounts",
		Operation: shared.OpInsert,
		Key:       key,
		Columns: map[string][]byte{
			"id":      []byte(key),
			"owner":   []byte("alice"),
			"balance": []byte(balance),
		},
	}
}

func updateEvent(lsn uint64, key string, balance string) shared.ChangeEvent {
	return shared.ChangeEvent{
		LSN:       lsn,
		Table:     "accounts",
		Operation: shared.OpUpdate,
		Key:       key,
		Columns: map[string][]byte{
			"id":      []byte(key),
			"owner":   []byte("alice"),
			"balance": []byte(balance),
		},
	}
}

func readBalance(t *testing.T, ctx context.Context, id string) int64 {
	t.Helper()
	conn, err := pgx.Connect(ctx, downstreamTestDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	var balance int64
	if err := conn.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = $1", id).Scan(&balance); err != nil {
		t.Fatalf("read balance: %v", err)
	}
	return balance
}

// TestApplyAndCheckpoint_DuplicateIsSkipped covers the acknowledgment-gap
// case: the same change delivered twice must be applied exactly once.
func TestApplyAndCheckpoint_DuplicateIsSkipped(t *testing.T) {
	ctx := context.Background()
	c := newTestCheckpointer(t)

	applied, err := c.ApplyAndCheckpoint(ctx, insertEvent(10, "1", "100"))
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if !applied {
		t.Fatalf("first delivery of a new change must be applied")
	}

	applied, err = c.ApplyAndCheckpoint(ctx, insertEvent(10, "1", "100"))
	if err != nil {
		t.Fatalf("duplicate apply: %v", err)
	}
	if applied {
		t.Fatalf("redelivering the same LSN must be skipped, not re-applied")
	}

	watermark, err := c.Watermark(ctx)
	if err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	if watermark != 10 {
		t.Fatalf("watermark = %d, want 10 (duplicate must not move it)", watermark)
	}
}

// TestApplyAndCheckpoint_StaleOverwriteRejected is the exact scenario
// CLAUDE.md names: source updates a row to 90, then to 80; a replay of the
// older change must not clobber the newer value.
func TestApplyAndCheckpoint_StaleOverwriteRejected(t *testing.T) {
	ctx := context.Background()
	c := newTestCheckpointer(t)

	if _, err := c.ApplyAndCheckpoint(ctx, insertEvent(10, "42", "100")); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := c.ApplyAndCheckpoint(ctx, updateEvent(20, "42", "90")); err != nil {
		t.Fatalf("update to 90: %v", err)
	}
	if _, err := c.ApplyAndCheckpoint(ctx, updateEvent(30, "42", "80")); err != nil {
		t.Fatalf("update to 80: %v", err)
	}

	// A replay of the LSN=20 update (balance=90) arrives after LSN=30
	// already committed. It must be rejected by the monotonic <= check.
	applied, err := c.ApplyAndCheckpoint(ctx, updateEvent(20, "42", "90"))
	if err != nil {
		t.Fatalf("stale replay: %v", err)
	}
	if applied {
		t.Fatalf("a replay at or below the watermark must be rejected, not re-applied")
	}

	if got := readBalance(t, ctx, "42"); got != 80 {
		t.Fatalf("balance = %d, want 80 (stale replay must not overwrite the newer value)", got)
	}
}

// TestApplyAndCheckpoint_ExactWatermarkLSNIsSkipped covers the gated
// UPDATE's boundary condition: `last_lsn < $1` must exclude LSNs equal to
// the watermark, not just ones below it. Unlike the duplicate-delivery
// test, this sends a change with genuinely different column values at the
// boundary LSN, so a wrongly-applied write would be visible in the data,
// not just masked by writing the same values again.
func TestApplyAndCheckpoint_ExactWatermarkLSNIsSkipped(t *testing.T) {
	ctx := context.Background()
	c := newTestCheckpointer(t)

	if _, err := c.ApplyAndCheckpoint(ctx, insertEvent(10, "7", "100")); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Same LSN as the current watermark (10), but a different balance. If
	// the boundary were off by one (e.g. `last_lsn <= $1`), this would
	// wrongly apply and balance would become 999.
	applied, err := c.ApplyAndCheckpoint(ctx, updateEvent(10, "7", "999"))
	if err != nil {
		t.Fatalf("apply at exact watermark: %v", err)
	}
	if applied {
		t.Fatalf("a change whose LSN equals the current watermark must be skipped, not applied")
	}

	if got := readBalance(t, ctx, "7"); got != 100 {
		t.Fatalf("balance = %d, want 100 (the exact-watermark change must not have been applied)", got)
	}

	watermark, err := c.Watermark(ctx)
	if err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	if watermark != 10 {
		t.Fatalf("watermark = %d, want 10 (unchanged)", watermark)
	}
}

// TestApplyAndCheckpoint_FirstChangeForPartition covers "no progress row
// yet" from both directions: the row is pre-seeded before any change is
// ever applied (the normal case, exercised by every other test in this file
// via newTestCheckpointer), and — if that invariant were ever violated — a
// genuinely missing row must produce an explicit error, never a silent
// "treat it as stale" skip.
func TestApplyAndCheckpoint_FirstChangeForPartition(t *testing.T) {
	ctx := context.Background()

	t.Run("pre-seeded row accepts the first change", func(t *testing.T) {
		c := newTestCheckpointer(t)

		watermark, err := c.Watermark(ctx)
		if err != nil {
			t.Fatalf("read watermark: %v", err)
		}
		if watermark != 0 {
			t.Fatalf("watermark = %d, want 0 for a freshly-seeded partition", watermark)
		}

		applied, err := c.ApplyAndCheckpoint(ctx, insertEvent(1, "1", "100"))
		if err != nil {
			t.Fatalf("apply: %v", err)
		}
		if !applied {
			t.Fatalf("the first change to a fresh partition (watermark 0) must be applied")
		}
	})

	t.Run("a genuinely missing row errors instead of skipping", func(t *testing.T) {
		c := newTestCheckpointer(t)
		if _, err := c.pool.Exec(ctx, "DELETE FROM replication_progress WHERE partition_id = $1", singlePartitionID); err != nil {
			t.Fatalf("remove progress row: %v", err)
		}

		applied, err := c.ApplyAndCheckpoint(ctx, insertEvent(1, "1", "100"))
		if err == nil {
			t.Fatalf("expected an explicit error when the progress row is missing, got applied=%v, err=nil", applied)
		}
		if applied {
			t.Fatalf("must not report a change as applied when its checkpoint couldn't be recorded")
		}
	})
}

// TestApplyAndCheckpoint_CommitFailureRollsBackBoth is the atomicity proof:
// if the transaction fails after the data write and the watermark update
// have both been issued, but before COMMIT succeeds, neither may persist.
//
// A DEFERRABLE UNIQUE constraint on accounts.owner lets Postgres accept the
// conflicting write during the transaction (the check is deferred) and only
// reject it when COMMIT evaluates the deferred constraint — giving a real
// "error after apply, before commit" without adding any fault-injection hook
// to production code.
func TestApplyAndCheckpoint_CommitFailureRollsBackBoth(t *testing.T) {
	ctx := context.Background()
	c := newTestCheckpointer(t)

	setupConn, err := pgx.Connect(ctx, downstreamTestDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer setupConn.Close(ctx)

	if _, err := setupConn.Exec(ctx,
		"ALTER TABLE accounts ADD CONSTRAINT checkpoint_test_unique_owner UNIQUE (owner) DEFERRABLE INITIALLY DEFERRED",
	); err != nil {
		t.Fatalf("add deferred constraint: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		conn, err := pgx.Connect(cleanupCtx, downstreamTestDSN)
		if err != nil {
			return
		}
		defer conn.Close(cleanupCtx)
		_, _ = conn.Exec(cleanupCtx, "ALTER TABLE accounts DROP CONSTRAINT IF EXISTS checkpoint_test_unique_owner")
	})

	if _, err := setupConn.Exec(ctx,
		"INSERT INTO accounts (id, owner, balance) VALUES (1, 'poison', 1)",
	); err != nil {
		t.Fatalf("seed poison row: %v", err)
	}

	watermarkBefore, err := c.Watermark(ctx)
	if err != nil {
		t.Fatalf("read watermark before: %v", err)
	}

	// This write succeeds at statement time inside ApplyAndCheckpoint's
	// transaction (the unique check is deferred) but COMMIT must fail once
	// Postgres evaluates the deferred constraint and finds two rows with
	// owner 'poison'.
	poisonEvent := shared.ChangeEvent{
		LSN:       watermarkBefore + 1,
		Table:     "accounts",
		Operation: shared.OpInsert,
		Key:       "2",
		Columns: map[string][]byte{
			"id":      []byte("2"),
			"owner":   []byte("poison"),
			"balance": []byte("500"),
		},
	}

	applied, err := c.ApplyAndCheckpoint(ctx, poisonEvent)
	if err == nil {
		t.Fatalf("expected commit to fail due to the deferred unique constraint, got no error")
	}
	if applied {
		t.Fatalf("must not report applied when commit failed")
	}

	watermarkAfter, err := c.Watermark(ctx)
	if err != nil {
		t.Fatalf("read watermark after: %v", err)
	}
	if watermarkAfter != watermarkBefore {
		t.Fatalf("watermark changed from %d to %d despite the commit failing", watermarkBefore, watermarkAfter)
	}

	var count int
	if err := setupConn.QueryRow(ctx, "SELECT count(*) FROM accounts WHERE id = 2").Scan(&count); err != nil {
		t.Fatalf("check row: %v", err)
	}
	if count != 0 {
		t.Fatalf("row id=2 exists despite the commit failing — atomicity violated")
	}
}
