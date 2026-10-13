package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/extrem21/watermark/shared"
)

// sourceTestDSN matches shared.Config's default -source-dsn.
const sourceTestDSN = "postgres://watermark:watermark@localhost:5433/watermark_source"

// newInvestigationConfig builds a Config pointing at a dedicated slot and
// publication, named uniquely per case so the three investigation tests in
// this file never collide with each other or with the pipeline's real
// watermark_slot.
func newInvestigationConfig(caseName string) *shared.Config {
	return &shared.Config{
		SourceDSN:       sourceTestDSN,
		DownstreamDSN:   downstreamTestDSN,
		SlotName:        "watermark_investigate_" + caseName + "_slot",
		PublicationName: "watermark_investigate_" + caseName + "_pub",
	}
}

// registerSlotCleanup guarantees cfg's slot and publication are dropped when
// the test ends, even on failure. An un-dropped slot retains WAL on the
// source indefinitely, so this investigation must never leave one behind.
func registerSlotCleanup(t *testing.T, cfg *shared.Config) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		conn, err := pgx.Connect(cleanupCtx, sourceTestDSN)
		if err != nil {
			return
		}
		defer conn.Close(cleanupCtx)
		for i := 0; i < 5; i++ {
			if _, err := conn.Exec(cleanupCtx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", cfg.SlotName)); err == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		_, _ = conn.Exec(cleanupCtx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", cfg.PublicationName))
	})
}

// resetInvestigationTables truncates both accounts tables and clears the
// partition's progress row, so each investigation starts from the same
// known-empty state.
func resetInvestigationTables(t *testing.T, ctx context.Context, sourceConn *pgx.Conn) {
	t.Helper()
	if _, err := sourceConn.Exec(ctx, "TRUNCATE accounts"); err != nil {
		t.Fatalf("truncate source accounts: %v", err)
	}
	downstreamConn, err := pgx.Connect(ctx, downstreamTestDSN)
	if err != nil {
		t.Fatalf("connect downstream: %v", err)
	}
	defer downstreamConn.Close(ctx)
	if _, err := downstreamConn.Exec(ctx, "TRUNCATE accounts"); err != nil {
		t.Fatalf("truncate downstream accounts: %v", err)
	}
	if _, err := downstreamConn.Exec(ctx, "DELETE FROM replication_progress WHERE partition_id = $1", singlePartitionID); err != nil {
		t.Fatalf("reset progress row: %v", err)
	}
}

// collectEvents reads exactly want events from events, failing the test if
// timeout elapses first.
func collectEvents(t *testing.T, ctx context.Context, events chan shared.ChangeEvent, want int, timeout time.Duration) []shared.ChangeEvent {
	t.Helper()
	var received []shared.ChangeEvent
	collectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
collect:
	for len(received) < want {
		select {
		case ev := <-events:
			received = append(received, ev)
		case <-collectCtx.Done():
			break collect
		}
	}
	if len(received) != want {
		t.Fatalf("received %d change events, want %d (consumer.go dropped or merged rows before they even reached the checkpoint gate)", len(received), want)
	}
	return received
}

// reportAndAssertDistinctLSNs logs the observed LSNs — reporting the
// finding either way, not just on failure — then asserts they're all
// distinct. Per this investigation's scope: if they collide, stop and
// report it; checkpoint.go's gate logic is not touched here regardless of
// outcome.
func reportAndAssertDistinctLSNs(t *testing.T, received []shared.ChangeEvent) {
	t.Helper()
	lsns := make([]uint64, len(received))
	distinct := make(map[uint64]bool)
	for i, ev := range received {
		lsns[i] = ev.LSN
		distinct[ev.LSN] = true
	}
	t.Logf("received LSNs (in arrival order): %v — %d distinct value(s) across %d rows", lsns, len(distinct), len(received))

	if len(distinct) == len(received) {
		t.Logf("FINDING: every row got its own distinct LSN")
		return
	}
	t.Fatalf("FINDING: only %d of %d rows had distinct LSNs (%v) — rows in this change set share an LSN; "+
		"stopping here per scope, not modifying checkpoint.go's gate", len(distinct), len(received), lsns)
}

// assertAllOps fails the test if any received event's Operation isn't want —
// a sanity check that the rows collected are actually the op type the case
// claims to be testing.
func assertAllOps(t *testing.T, received []shared.ChangeEvent, want shared.OpType) {
	t.Helper()
	for _, ev := range received {
		if ev.Operation != want {
			t.Fatalf("event lsn=%d has Operation=%d, want %d", ev.LSN, ev.Operation, want)
		}
	}
}

// applyThroughCheckpoint applies every received event through a fresh
// Checkpointer for cfg and returns how many were actually applied (not
// skipped as stale/duplicate) — the "passes the checkpoint gate" half of
// the assertion.
func applyThroughCheckpoint(t *testing.T, ctx context.Context, cfg *shared.Config, received []shared.ChangeEvent) int {
	t.Helper()
	checkpointer, err := NewCheckpointer(ctx, cfg)
	if err != nil {
		t.Fatalf("new checkpointer: %v", err)
	}
	defer checkpointer.Close()

	var appliedCount int
	for _, ev := range received {
		applied, err := checkpointer.ApplyAndCheckpoint(ctx, ev)
		if err != nil {
			t.Fatalf("apply event lsn=%d: %v", ev.LSN, err)
		}
		if applied {
			appliedCount++
		}
	}
	return appliedCount
}

// TestConsumer_MultiStatementTransactionLSNs is an investigation, not a
// regression test for a fix already made.
//
// Reading consumer.go: emitRow sets ChangeEvent.LSN from the lsn argument
// decode() is called with, which is xld.WALStart — the wire-level starting
// position of the physical CopyData frame carrying that one decoded
// message (see the `c.decode(xld.WALStart, xld.WALData, events)` call in
// stream()). It is never set from the transaction's commit LSN
// (pglogrepl.BeginMessage.FinalLSN exists but consumer.go never reads it —
// only Xid and CommitTime are stashed from Begin). So by construction the
// code intends a per-message LSN, not a per-transaction one.
//
// This test checks whether that holds up against a real Postgres logical
// replication stream: does a 3-row single-transaction INSERT actually
// arrive as 3 frames with 3 distinct, increasing WALStart values, and — if
// so or if not — do all 3 rows survive checkpoint.go's strict
// `last_lsn < $1` gate. It does not attempt to fix anything either way.
func TestConsumer_MultiStatementTransactionLSNs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := newInvestigationConfig("multistmt")
	registerSlotCleanup(t, cfg)

	sourceConn, err := pgx.Connect(ctx, sourceTestDSN)
	if err != nil {
		t.Skipf("source not reachable (is docker-compose up?): %v", err)
	}
	defer sourceConn.Close(ctx)
	resetInvestigationTables(t, ctx, sourceConn)

	consumer, err := NewConsumer(ctx, cfg)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close(context.Background())

	events := make(chan shared.ChangeEvent, 10)
	go func() { _ = consumer.Start(ctx, events) }()

	// The case under investigation: three separate INSERT statements, one
	// explicit transaction.
	tx, err := sourceConn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin source transaction: %v", err)
	}
	for i, id := range []int{1, 2, 3} {
		if _, err := tx.Exec(ctx,
			"INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)",
			id, "alice", 100+i,
		); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit source transaction: %v", err)
	}

	received := collectEvents(t, ctx, events, 3, 10*time.Second)
	reportAndAssertDistinctLSNs(t, received)
	assertAllOps(t, received, shared.OpInsert)

	appliedCount := applyThroughCheckpoint(t, ctx, cfg, received)

	var downstreamCount int
	verifyConn, err := pgx.Connect(ctx, downstreamTestDSN)
	if err != nil {
		t.Fatalf("connect downstream for verification: %v", err)
	}
	defer verifyConn.Close(ctx)
	if err := verifyConn.QueryRow(ctx, "SELECT count(*) FROM accounts").Scan(&downstreamCount); err != nil {
		t.Fatalf("count downstream rows: %v", err)
	}

	t.Logf("checkpoint gate applied %d/3 events; downstream row count = %d", appliedCount, downstreamCount)
	if downstreamCount != 3 {
		t.Errorf("FINDING CONFIRMED: only %d of 3 rows from one multi-statement transaction landed downstream — "+
			"checkpoint.go's strict `last_lsn < $1` gate dropped %d row(s) as \"stale.\" Not fixed here — investigation only.",
			downstreamCount, 3-downstreamCount)
	}
}

// TestConsumer_SingleMultiRowInsertLSNs extends the investigation to a
// single INSERT statement carrying three VALUES rows (as opposed to three
// separate INSERT statements). Postgres still writes one heap-insert WAL
// record per row even within one SQL statement, so the same question
// applies: does each row still get emitted with its own distinct LSN, and
// does the checkpoint gate still let all three through?
func TestConsumer_SingleMultiRowInsertLSNs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := newInvestigationConfig("multirowinsert")
	registerSlotCleanup(t, cfg)

	sourceConn, err := pgx.Connect(ctx, sourceTestDSN)
	if err != nil {
		t.Skipf("source not reachable (is docker-compose up?): %v", err)
	}
	defer sourceConn.Close(ctx)
	resetInvestigationTables(t, ctx, sourceConn)

	consumer, err := NewConsumer(ctx, cfg)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close(context.Background())

	events := make(chan shared.ChangeEvent, 10)
	go func() { _ = consumer.Start(ctx, events) }()

	// The case under investigation: one INSERT statement, three VALUES rows.
	if _, err := sourceConn.Exec(ctx,
		"INSERT INTO accounts (id, owner, balance) VALUES (1, 'alice', 100), (2, 'alice', 101), (3, 'alice', 102)",
	); err != nil {
		t.Fatalf("multi-row insert: %v", err)
	}

	received := collectEvents(t, ctx, events, 3, 10*time.Second)
	reportAndAssertDistinctLSNs(t, received)
	assertAllOps(t, received, shared.OpInsert)

	appliedCount := applyThroughCheckpoint(t, ctx, cfg, received)

	var downstreamCount int
	verifyConn, err := pgx.Connect(ctx, downstreamTestDSN)
	if err != nil {
		t.Fatalf("connect downstream for verification: %v", err)
	}
	defer verifyConn.Close(ctx)
	if err := verifyConn.QueryRow(ctx, "SELECT count(*) FROM accounts").Scan(&downstreamCount); err != nil {
		t.Fatalf("count downstream rows: %v", err)
	}

	t.Logf("checkpoint gate applied %d/3 events; downstream row count = %d", appliedCount, downstreamCount)
	if downstreamCount != 3 {
		t.Errorf("FINDING CONFIRMED: only %d of 3 rows from one multi-row INSERT statement landed downstream — "+
			"checkpoint.go's strict `last_lsn < $1` gate dropped %d row(s) as \"stale.\" Not fixed here — investigation only.",
			downstreamCount, 3-downstreamCount)
	}
}

// TestConsumer_MultiRowUpdateLSNs extends the investigation to a single
// UPDATE statement hitting three existing rows. The three baseline rows are
// inserted before the consumer's slot even exists, so only the UPDATE
// itself is observed — isolating exactly the case in question: one UPDATE
// statement, three affected rows, does each still get its own LSN.
func TestConsumer_MultiRowUpdateLSNs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := newInvestigationConfig("multirowupdate")
	registerSlotCleanup(t, cfg)

	sourceConn, err := pgx.Connect(ctx, sourceTestDSN)
	if err != nil {
		t.Skipf("source not reachable (is docker-compose up?): %v", err)
	}
	defer sourceConn.Close(ctx)
	resetInvestigationTables(t, ctx, sourceConn)

	// Baseline rows, inserted before the slot exists — deliberately not
	// part of what this test observes.
	if _, err := sourceConn.Exec(ctx,
		"INSERT INTO accounts (id, owner, balance) VALUES (1, 'alice', 100), (2, 'alice', 200), (3, 'alice', 300)",
	); err != nil {
		t.Fatalf("seed baseline rows: %v", err)
	}

	consumer, err := NewConsumer(ctx, cfg)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close(context.Background())

	events := make(chan shared.ChangeEvent, 10)
	go func() { _ = consumer.Start(ctx, events) }()

	// The case under investigation: one UPDATE statement, three affected
	// rows.
	if _, err := sourceConn.Exec(ctx,
		"UPDATE accounts SET balance = balance + 1000 WHERE id IN (1, 2, 3)",
	); err != nil {
		t.Fatalf("multi-row update: %v", err)
	}

	received := collectEvents(t, ctx, events, 3, 10*time.Second)
	reportAndAssertDistinctLSNs(t, received)
	assertAllOps(t, received, shared.OpUpdate)

	appliedCount := applyThroughCheckpoint(t, ctx, cfg, received)

	verifyConn, err := pgx.Connect(ctx, downstreamTestDSN)
	if err != nil {
		t.Fatalf("connect downstream for verification: %v", err)
	}
	defer verifyConn.Close(ctx)

	var downstreamCount int
	if err := verifyConn.QueryRow(ctx, "SELECT count(*) FROM accounts").Scan(&downstreamCount); err != nil {
		t.Fatalf("count downstream rows: %v", err)
	}
	t.Logf("checkpoint gate applied %d/3 events; downstream row count = %d", appliedCount, downstreamCount)

	wantBalances := map[string]int64{"1": 1100, "2": 1200, "3": 1300}
	var mismatches []string
	for id, want := range wantBalances {
		var got int64
		if err := verifyConn.QueryRow(ctx, "SELECT balance FROM accounts WHERE id = $1", id).Scan(&got); err != nil {
			mismatches = append(mismatches, fmt.Sprintf("id=%s: read error: %v", id, err))
			continue
		}
		if got != want {
			mismatches = append(mismatches, fmt.Sprintf("id=%s: got balance=%d, want %d", id, got, want))
		}
	}

	if downstreamCount != 3 || len(mismatches) > 0 {
		t.Errorf("FINDING CONFIRMED: multi-row UPDATE did not fully land downstream — row count=%d (want 3), "+
			"mismatches=%v. checkpoint.go's strict `last_lsn < $1` gate likely dropped row(s) as \"stale.\" "+
			"Not fixed here — investigation only.", downstreamCount, mismatches)
	}
}
