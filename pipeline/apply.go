// APPLY PATH — must be idempotent; must commit atomically with the watermark.
package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/extrem21/watermark/shared"
)

// primaryKeyColumn is the single-column primary key for each replicated
// table. Stage 1 has exactly one table; this is not meant to generalize to
// arbitrary schemas — schema/DDL discovery is explicitly out of scope.
var primaryKeyColumn = map[string]string{
	"accounts": "id",
}

// applyChange writes a single change to the downstream, idempotently. It
// takes a pgx.Tx specifically, not a pool or a bare connection — a *pgxpool.
// Pool exposes an Exec method too, so accepting anything looser than pgx.Tx
// here would let a future call site apply a change outside the transaction
// that also advances the watermark in checkpoint.go. That's exactly the
// two-separate-steps bug Stage 2's atomic apply-and-checkpoint exists to
// close, so the compiler is made to forbid it rather than relying on
// convention.
//
// Insert and Update share one code path: Postgres's logical decode sends
// the full new row for an update, not just the changed columns, so both
// are just "upsert this row" with absolute values — never a relative delta.
func applyChange(ctx context.Context, tx pgx.Tx, ev shared.ChangeEvent) error {
	pk, ok := primaryKeyColumn[ev.Table]
	if !ok {
		return fmt.Errorf("no known primary key column for table %q", ev.Table)
	}

	switch ev.Operation {
	case shared.OpInsert, shared.OpUpdate:
		return upsert(ctx, tx, ev.Table, pk, ev.Columns)
	case shared.OpDelete:
		return deleteRow(ctx, tx, ev.Table, pk, ev.Key)
	default:
		return fmt.Errorf("unknown operation %d", ev.Operation)
	}
}

func upsert(ctx context.Context, tx pgx.Tx, table, pk string, columns map[string][]byte) error {
	if len(columns) == 0 {
		return fmt.Errorf("upsert on table %q with no column values", table)
	}

	names := make([]string, 0, len(columns))
	for name := range columns {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic SQL text, and keeps args aligned to placeholders

	placeholders := make([]string, len(names))
	quotedNames := make([]string, len(names))
	updateSet := make([]string, 0, len(names)-1)
	args := make([]any, len(names))
	for i, name := range names {
		quoted := pgx.Identifier{name}.Sanitize()
		quotedNames[i] = quoted
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = string(columns[name])
		if name != pk {
			updateSet = append(updateSet, fmt.Sprintf("%s = EXCLUDED.%s", quoted, quoted))
		}
	}

	stmt := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		pgx.Identifier{table}.Sanitize(),
		strings.Join(quotedNames, ", "),
		strings.Join(placeholders, ", "),
		pgx.Identifier{pk}.Sanitize(),
		strings.Join(updateSet, ", "),
	)

	_, err := tx.Exec(ctx, stmt, args...)
	return err
}

func deleteRow(ctx context.Context, tx pgx.Tx, table, pk, key string) error {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s = $1",
		pgx.Identifier{table}.Sanitize(), pgx.Identifier{pk}.Sanitize())
	_, err := tx.Exec(ctx, stmt, key)
	return err
}
