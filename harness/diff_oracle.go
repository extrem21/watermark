package harness

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/extrem21/watermark/shared"
)

// accountRow is one row of the accounts table, read directly with plain SQL
// — independent of shared.ChangeEvent, since the oracle must not trust
// anything the pipeline produced; it re-derives truth from both databases.
type accountRow struct {
	id      int64
	owner   string
	balance int64
}

// DiffResult is the outcome of one diff-oracle run. A nil/empty Mismatches
// means the source and downstream were found exactly, row-for-row equal.
type DiffResult struct {
	Mismatches []string
}

// Equal reports whether the source and downstream were found identical.
func (r *DiffResult) Equal() bool {
	return len(r.Mismatches) == 0
}

// RunDiffOracle asserts exact row-by-row, column-by-column equality between
// the source and downstream accounts tables. It never mutates either
// database. Any divergence is named — the specific id and column — not just
// reported as "not equal".
func RunDiffOracle(ctx context.Context, cfg *shared.Config) (*DiffResult, error) {
	source, err := fetchAccounts(ctx, cfg.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("read source: %w", err)
	}
	downstream, err := fetchAccounts(ctx, cfg.DownstreamDSN)
	if err != nil {
		return nil, fmt.Errorf("read downstream: %w", err)
	}
	return diffAccounts(source, downstream), nil
}

func fetchAccounts(ctx context.Context, dsn string) ([]accountRow, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SELECT id, owner, balance FROM accounts ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []accountRow
	for rows.Next() {
		var r accountRow
		if err := rows.Scan(&r.id, &r.owner, &r.balance); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// diffAccounts walks both id-ordered row sets like a merge, so it runs in a
// single pass and names every row present on only one side, plus every row
// present on both sides with a differing column value.
func diffAccounts(source, downstream []accountRow) *DiffResult {
	result := &DiffResult{}
	i, j := 0, 0

	for i < len(source) && j < len(downstream) {
		s, d := source[i], downstream[j]
		switch {
		case s.id < d.id:
			result.Mismatches = append(result.Mismatches,
				fmt.Sprintf("id=%d present in source but missing in downstream", s.id))
			i++
		case s.id > d.id:
			result.Mismatches = append(result.Mismatches,
				fmt.Sprintf("id=%d present in downstream but missing in source", d.id))
			j++
		default:
			if s.owner != d.owner || s.balance != d.balance {
				result.Mismatches = append(result.Mismatches,
					fmt.Sprintf("id=%d differs: source={owner=%q balance=%d} downstream={owner=%q balance=%d}",
						s.id, s.owner, s.balance, d.owner, d.balance))
			}
			i++
			j++
		}
	}
	for ; i < len(source); i++ {
		result.Mismatches = append(result.Mismatches,
			fmt.Sprintf("id=%d present in source but missing in downstream", source[i].id))
	}
	for ; j < len(downstream); j++ {
		result.Mismatches = append(result.Mismatches,
			fmt.Sprintf("id=%d present in downstream but missing in source", downstream[j].id))
	}
	return result
}
