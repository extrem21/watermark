package shared

// ChangeEvent is one decoded row-level change from the source's logical replication
// stream. It is a self-contained value: no pointers to shared state, no channels,
// nothing that prevents it being copied, stored, duplicated, or re-emitted out of
// order. That property is what lets the fault injector simulate duplicate and
// reordered delivery without touching the production apply path.
//
// APPLY PATH — the apply built from this must be idempotent and must commit
// atomically with the watermark.
type ChangeEvent struct {
	LSN       uint64            // WAL position; the ordering + watermark authority
	TxID      uint32            // source transaction this change committed in
	Table     string            // source table, e.g. "accounts"
	Operation OpType            // Insert / Update / Delete
	Key       string            // primary-key value, stringified; single-column PK only
	Columns   map[string][]byte // column name → new value as raw bytes; empty for Delete
}

type OpType uint8

const (
	OpInsert OpType = iota
	OpUpdate
	OpDelete
)