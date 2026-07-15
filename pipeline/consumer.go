package pipeline

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/extrem21/watermark/shared"
)

const standbyStatusInterval = 10 * time.Second

// Consumer streams decoded row-level changes from the source's logical
// replication slot. Postgres's pgoutput wire format is decoded by
// jackc/pglogrepl — that library does the WAL decode. Everything past that
// (turning decoded messages into shared.ChangeEvent, ordering, and — in
// later stages — crash-safe apply) is ours.
type Consumer struct {
	cfg  *shared.Config
	conn *pgconn.PgConn

	relations         map[uint32]*pglogrepl.RelationMessage
	currentTxID       uint32
	currentCommitTime time.Time
}

// NewConsumer ensures the publication and replication slot exist (creating
// them on first run) and opens the replication-mode connection used for
// streaming.
func NewConsumer(ctx context.Context, cfg *shared.Config) (*Consumer, error) {
	if err := ensurePublication(ctx, cfg); err != nil {
		return nil, fmt.Errorf("ensure publication: %w", err)
	}

	if err := ensureReplicationSlot(ctx, cfg); err != nil {
		return nil, fmt.Errorf("ensure replication slot: %w", err)
	}

	conn, err := pgconn.Connect(ctx, replicationDSN(cfg.SourceDSN))
	if err != nil {
		return nil, fmt.Errorf("connect in replication mode: %w", err)
	}

	return &Consumer{
		cfg:       cfg,
		conn:      conn,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}, nil
}

// Close releases the replication-mode connection.
func (c *Consumer) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}

// Start begins streaming and decoding changes, sending one shared.ChangeEvent
// per row-level change onto events. It blocks until ctx is cancelled or a
// non-recoverable error occurs.
func (c *Consumer) Start(ctx context.Context, events chan<- shared.ChangeEvent) error {
	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", c.cfg.PublicationName),
	}
	if err := pglogrepl.StartReplication(ctx, c.conn, c.cfg.SlotName, 0, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	return c.stream(ctx, events)
}

func (c *Consumer) stream(ctx context.Context, events chan<- shared.ChangeEvent) error {
	var clientXLogPos pglogrepl.LSN
	lastStatus := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if time.Since(lastStatus) >= standbyStatusInterval {
			update := pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos}
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, c.conn, update); err != nil {
				return fmt.Errorf("send standby status update: %w", err)
			}
			lastStatus = time.Now()
		}

		recvCtx, cancel := context.WithTimeout(ctx, standbyStatusInterval)
		msg, err := c.conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("receive message: %w", err)
		}

		cd, ok := msg.(*pgproto3.CopyData)
		if !ok || len(cd.Data) == 0 {
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				lastStatus = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog data: %w", err)
			}
			if err := c.decode(xld.WALStart, xld.WALData, events); err != nil {
				return fmt.Errorf("decode wal data: %w", err)
			}
			if end := xld.WALStart + pglogrepl.LSN(len(xld.WALData)); end > clientXLogPos {
				clientXLogPos = end
			}
		}
	}
}

// decode turns one pgoutput message into zero or one shared.ChangeEvent.
// Begin/Commit carry no row data (Begin's Xid is stashed for the following
// row messages); Relation messages just update the column-name cache.
func (c *Consumer) decode(lsn pglogrepl.LSN, walData []byte, events chan<- shared.ChangeEvent) error {
	msg, err := pglogrepl.Parse(walData)
	if err != nil {
		return fmt.Errorf("parse logical replication message: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		c.relations[m.RelationID] = m

	case *pglogrepl.BeginMessage:
		c.currentTxID = m.Xid
		c.currentCommitTime = m.CommitTime

	case *pglogrepl.InsertMessage:
		return c.emitRow(events, lsn, m.RelationID, shared.OpInsert, m.Tuple)

	case *pglogrepl.UpdateMessage:
		return c.emitRow(events, lsn, m.RelationID, shared.OpUpdate, m.NewTuple)

	case *pglogrepl.DeleteMessage:
		return c.emitRow(events, lsn, m.RelationID, shared.OpDelete, m.OldTuple)
	}

	return nil
}

func (c *Consumer) emitRow(events chan<- shared.ChangeEvent, lsn pglogrepl.LSN, relationID uint32, op shared.OpType, tuple *pglogrepl.TupleData) error {
	rel, ok := c.relations[relationID]
	if !ok {
		return fmt.Errorf("received row for unknown relation ID %d (no prior Relation message)", relationID)
	}
	if tuple == nil {
		return fmt.Errorf("%s message for table %s carried no tuple data", rel.RelationName, rel.RelationName)
	}

	event := shared.ChangeEvent{
		LSN:        uint64(lsn),
		TxID:       c.currentTxID,
		CommitTime: c.currentCommitTime,
		Table:      rel.RelationName,
		Operation:  op,
		Columns:    make(map[string][]byte, len(tuple.Columns)),
	}

	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		colDef := rel.Columns[i]

		if col.DataType != pglogrepl.TupleDataTypeText {
			continue // null or unchanged-TOAST column; not part of this apply's absolute-set values
		}
		event.Columns[colDef.Name] = col.Data

		if colDef.Flags&1 != 0 { // bit 1 marks the column as part of the replica identity key
			event.Key = string(col.Data)
		}
	}

	events <- event
	return nil
}

func ensurePublication(ctx context.Context, cfg *shared.Config) error {
	conn, err := pgx.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	var exists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
		cfg.PublicationName,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check publication: %w", err)
	}
	if exists {
		return nil
	}

	stmt := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE accounts", pgx.Identifier{cfg.PublicationName}.Sanitize())
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}
	return nil
}

// ensureReplicationSlot creates the slot if it doesn't already exist.
func ensureReplicationSlot(ctx context.Context, cfg *shared.Config) error {
	conn, err := pgx.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	var exists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		cfg.SlotName,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check replication slot: %w", err)
	}
	if exists {
		return nil
	}

	replConn, err := pgconn.Connect(ctx, replicationDSN(cfg.SourceDSN))
	if err != nil {
		return fmt.Errorf("connect in replication mode: %w", err)
	}
	defer replConn.Close(ctx)

	if _, err := pglogrepl.CreateReplicationSlot(ctx, replConn, cfg.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication}); err != nil {
		return fmt.Errorf("create replication slot: %w", err)
	}
	log.Printf("created replication slot %q", cfg.SlotName)
	return nil
}

// replicationDSN adds the replication=database query parameter a
// replication-mode connection needs, without disturbing a DSN that already
// has query parameters.
func replicationDSN(dsn string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "replication=database"
}
