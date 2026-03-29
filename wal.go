package outboxd

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type walListener struct {
	replConn  *pgconn.PgConn
	dbConn    *pgx.Conn
	relations map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map
	lastLSN   pglogrepl.LSN

	tableName       string
	idColumn        string
	topicColumn     string
	payloadColumn   string
	createdAtColumn string
	deleteQuery     string

	buffered []Message
}

func newWALListener(ctx context.Context, dsn string, cfg Config) (*walListener, error) {
	replCfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("outbox: parse dsn: %w", err)
	}
	replCfg.RuntimeParams["replication"] = "database"
	replConn, err := pgconn.ConnectConfig(ctx, replCfg)
	if err != nil {
		return nil, fmt.Errorf("outbox: replication connect: %w", err)
	}

	dbConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		_ = replConn.Close(ctx)
		return nil, fmt.Errorf("outbox: db connect: %w", err)
	}

	var active *bool
	err = dbConn.QueryRow(ctx,
		"SELECT active FROM pg_replication_slots WHERE slot_name = $1",
		cfg.SlotName).Scan(&active)
	if err == pgx.ErrNoRows {
		_, err = pglogrepl.CreateReplicationSlot(ctx, replConn, cfg.SlotName, "pgoutput",
			pglogrepl.CreateReplicationSlotOptions{Temporary: false})
		if err != nil {
			_ = replConn.Close(ctx)
			_ = dbConn.Close(ctx)
			return nil, fmt.Errorf("outbox: create slot: %w", err)
		}
	} else if err != nil {
		_ = replConn.Close(ctx)
		_ = dbConn.Close(ctx)
		return nil, fmt.Errorf("outbox: check slot: %w", err)
	} else if active != nil && *active {
		_ = replConn.Close(ctx)
		_ = dbConn.Close(ctx)
		return nil, fmt.Errorf("outbox: replication slot %q is active", cfg.SlotName)
	}

	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", strings.Join(cfg.Publications, ",")),
	}
	if err = pglogrepl.StartReplication(ctx, replConn, cfg.SlotName, 0,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		_ = replConn.Close(ctx)
		_ = dbConn.Close(ctx)
		return nil, fmt.Errorf("outbox: start replication: %w", err)
	}

	schema := cfg.Schema
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s = ANY($1)",
		pgx.Identifier{schema.Table}.Sanitize(),
		pgx.Identifier{schema.IDColumn}.Sanitize())

	return &walListener{
		replConn:        replConn,
		dbConn:          dbConn,
		relations:       make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:         pgtype.NewMap(),
		tableName:       schema.Table,
		idColumn:        schema.IDColumn,
		topicColumn:     schema.TopicColumn,
		payloadColumn:   schema.PayloadColumn,
		createdAtColumn: schema.CreatedAtColumn,
		deleteQuery:     deleteQuery,
	}, nil
}

func (w *walListener) Close(ctx context.Context) {
	_ = w.replConn.Close(ctx)
	_ = w.dbConn.Close(ctx)
}

func (w *walListener) Next(ctx context.Context) (Message, error) {
	if len(w.buffered) > 0 {
		msg := w.buffered[0]
		w.buffered = w.buffered[1:]
		return msg, nil
	}

	var (
		pending []Message
		txLSN   pglogrepl.LSN
	)

	for {
		rawMsg, err := w.replConn.ReceiveMessage(ctx)
		if err != nil {
			return Message{}, fmt.Errorf("outbox: wal receive: %w", err)
		}

		cd, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return Message{}, fmt.Errorf("outbox: parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				if err := w.sendStandbyStatus(ctx); err != nil {
					return Message{}, err
				}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return Message{}, fmt.Errorf("outbox: parse xlogdata: %w", err)
			}

			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return Message{}, fmt.Errorf("outbox: parse logical msg: %w", err)
			}

			switch m := logicalMsg.(type) {
			case *pglogrepl.BeginMessage:
				txLSN = m.FinalLSN
				pending = nil

			case *pglogrepl.RelationMessage:
				w.relations[m.RelationID] = m

			case *pglogrepl.InsertMessage:
				rel, ok := w.relations[m.RelationID]
				if !ok || rel.RelationName != w.tableName {
					continue
				}
				msg, err := w.decodeInsert(rel, m.Tuple)
				if err != nil {
					return Message{}, err
				}
				pending = append(pending, msg)

			case *pglogrepl.CommitMessage:
				if len(pending) > 0 {
					w.lastLSN = txLSN
					w.buffered = pending[1:]
					return pending[0], nil
				}
			}
		}
	}
}

func (w *walListener) Remaining() int {
	return len(w.buffered)
}

func (w *walListener) Confirm(ctx context.Context, ids ...int64) error {
	if _, err := w.dbConn.Exec(ctx, w.deleteQuery, ids); err != nil {
		return fmt.Errorf("outbox: delete ids=%v: %w", ids, err)
	}
	return w.sendStandbyStatus(ctx)
}

func (w *walListener) sendStandbyStatus(ctx context.Context) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, w.replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: w.lastLSN,
		WALFlushPosition: w.lastLSN,
		WALApplyPosition: w.lastLSN,
	})
	if err != nil {
		return fmt.Errorf("outbox: standby status update: %w", err)
	}
	return nil
}

func (w *walListener) decodeInsert(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (Message, error) {
	var msg Message
	for i, col := range tuple.Columns {
		if col.DataType != 't' {
			continue
		}
		if i >= len(rel.Columns) {
			break
		}
		colDef := rel.Columns[i]
		switch colDef.Name {
		case w.idColumn:
			var v int64
			if err := w.typeMap.Scan(colDef.DataType, pgtype.TextFormatCode, col.Data, &v); err != nil {
				return Message{}, fmt.Errorf("outbox: decode id: %w", err)
			}
			msg.ID = v
		case w.topicColumn:
			msg.Topic = string(col.Data)
		case w.payloadColumn:
			var v []byte
			if err := w.typeMap.Scan(colDef.DataType, pgtype.TextFormatCode, col.Data, &v); err != nil {
				return Message{}, fmt.Errorf("outbox: decode payload: %w", err)
			}
			msg.Payload = v
		case w.createdAtColumn:
			var v pgtype.Timestamptz
			if err := w.typeMap.Scan(colDef.DataType, pgtype.TextFormatCode, col.Data, &v); err != nil {
				return Message{}, fmt.Errorf("outbox: decode created_at: %w", err)
			}
			msg.CreatedAt = v.Time
		}
	}
	return msg, nil
}
