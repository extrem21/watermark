// Package harness holds test/break/prove infrastructure: the workload
// generator and diff oracle that prove the pipeline correct, and (from
// Stage 2 onward) the crash and fault injection harnesses. Never imported
// by /pipeline production code.
package harness

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/jackc/pgx/v5"

	"github.com/extrem21/watermark/shared"
)

// Operation weights out of 100; the remainder goes to delete.
const (
	insertWeight = 40
	updateWeight = 40
)

var ownerNames = []string{"alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"}

// WorkloadGen drives a seeded, reproducible sequence of INSERT/UPDATE/DELETE
// statements against the source database's accounts table. Same seed, same
// NumOps -> the identical sequence of operations every time, which is what
// lets a diff-oracle result be regenerated on demand.
//
// Precondition: run against an empty accounts table. WorkloadGen tracks the
// ids it has created entirely in memory — it does not query the table to
// discover pre-existing rows — so reproducibility depends on starting from a
// known-empty table rather than on whatever state the table happened to be
// in.
type WorkloadGen struct {
	conn *pgx.Conn
	rng  *rand.Rand

	live   []int64 // ids WorkloadGen has inserted and not yet deleted
	nextID int64
}

// NewWorkloadGen connects to the source database configured in cfg and seeds
// its random source from cfg.Seed.
func NewWorkloadGen(ctx context.Context, cfg *shared.Config) (*WorkloadGen, error) {
	conn, err := pgx.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to source: %w", err)
	}
	return &WorkloadGen{
		conn:   conn,
		rng:    rand.New(rand.NewSource(cfg.Seed)),
		nextID: 1,
	}, nil
}

// Close releases the source connection.
func (w *WorkloadGen) Close(ctx context.Context) error {
	return w.conn.Close(ctx)
}

// Run generates numOps INSERT/UPDATE/DELETE operations against the source,
// in order, each committed as its own transaction — so each becomes one
// separate change on the replication stream, matching how a real workload
// commits.
func (w *WorkloadGen) Run(ctx context.Context, numOps int) error {
	for i := 0; i < numOps; i++ {
		op := w.pickOp()

		var err error
		switch op {
		case shared.OpInsert:
			err = w.insert(ctx)
		case shared.OpUpdate:
			err = w.update(ctx)
		case shared.OpDelete:
			err = w.delete(ctx)
		}
		if err != nil {
			return fmt.Errorf("op %d (%v): %w", i, op, err)
		}
	}
	return nil
}

// pickOp chooses the next operation, weighted, forcing an insert when there
// are no live rows left to update or delete.
func (w *WorkloadGen) pickOp() shared.OpType {
	if len(w.live) == 0 {
		return shared.OpInsert
	}
	switch n := w.rng.Intn(100); {
	case n < insertWeight:
		return shared.OpInsert
	case n < insertWeight+updateWeight:
		return shared.OpUpdate
	default:
		return shared.OpDelete
	}
}

func (w *WorkloadGen) insert(ctx context.Context) error {
	id := w.nextID
	w.nextID++
	owner := ownerNames[w.rng.Intn(len(ownerNames))]
	balance := w.rng.Intn(100_000)

	if _, err := w.conn.Exec(ctx,
		"INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)",
		id, owner, balance,
	); err != nil {
		return err
	}
	w.live = append(w.live, id)
	return nil
}

// update always sets an absolute balance, never a relative delta — matching
// the apply path's idempotency requirement and giving the diff oracle an
// unambiguous expected final value.
func (w *WorkloadGen) update(ctx context.Context) error {
	id := w.live[w.rng.Intn(len(w.live))]
	balance := w.rng.Intn(100_000)

	_, err := w.conn.Exec(ctx, "UPDATE accounts SET balance = $1 WHERE id = $2", balance, id)
	return err
}

func (w *WorkloadGen) delete(ctx context.Context) error {
	idx := w.rng.Intn(len(w.live))
	id := w.live[idx]

	if _, err := w.conn.Exec(ctx, "DELETE FROM accounts WHERE id = $1", id); err != nil {
		return err
	}
	w.live[idx] = w.live[len(w.live)-1]
	w.live = w.live[:len(w.live)-1]
	return nil
}
