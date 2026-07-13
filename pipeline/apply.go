// APPLY PATH — must be idempotent; must commit atomically with the watermark.
package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/extrem21/watermark/shared"
)

// primaryKeyColumn is the single-column primary key for each replicated
// table. Stage 1 has exactly one table; this is not meant to generalize to
// arbitrary schemas — schema/DDL discovery is explicitly out of scope.
var primaryKeyColumn = map[string]string{
	"accounts": "id",
}

// Applier writes decoded changes to the downstream database.
//
// Stage 1 note: this is happy-path only — there is no watermark table or
// apply-and-checkpoint transaction yet (that's Stage 2), so a crash
// mid-apply is not yet survived. It IS written idempotently (upsert /
// absolute update / delete) because a consumer reconnect can redeliver a
// change even with zero fault injection, and writing it idempotently from
// the start costs nothing extra.
type Applier struct {
	pool *pgxpool.Pool
}

func NewApplier(ctx context.Context, cfg *shared.Config) (*Applier, error) {
	pool, err := pgxpool.New(ctx, cfg.DownstreamDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to downstream: %w", err)
	}
	return &Applier{pool: pool}, nil
}

func (a *Applier) Close() {
	a.pool.Close()
}

// Apply writes a single change to the downstream, idempotently.
//
// Insert and Update share one code path: Postgres's logical decode sends
// the full new row for an update, not just the changed columns, so both
// are just "upsert this row" with absolute values — never a relative delta.
func (a *Applier) Apply(ctx context.Context, ev shared.ChangeEvent) error {
	pk, ok := primaryKeyColumn[ev.Table]
	if !ok {
		return fmt.Errorf("no known primary key column for table %q", ev.Table)
	}

	switch ev.Operation {
	case shared.OpInsert, shared.OpUpdate:
		return a.upsert(ctx, ev.Table, pk, ev.Columns)
	case shared.OpDelete:
		return a.delete(ctx, ev.Table, pk, ev.Key)
	default:
		return fmt.Errorf("unknown operation %d", ev.Operation)
	}
}

func (a *Applier) upsert(ctx context.Context, table, pk string, columns map[string][]byte) error {
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

	_, err := a.pool.Exec(ctx, stmt, args...)
	return err
}

func (a *Applier) delete(ctx context.Context, table, pk, key string) error {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s = $1",
		pgx.Identifier{table}.Sanitize(), pgx.Identifier{pk}.Sanitize())
	_, err := a.pool.Exec(ctx, stmt, key)
	return err
}
