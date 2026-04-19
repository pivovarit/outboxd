package outboxd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

type walListener struct {
	replConn *pgconn.PgConn
	dbConn   *pgx.Conn

	tableName       string
	idColumn        string
	topicColumn     string
	payloadColumn   string
	createdAtColumn string
	deleteQuery     string

	standbyInterval time.Duration

	relations map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map

	batchCh  chan walBatch
	errCh    chan error
	readCtx  context.Context
	readStop context.CancelFunc
	readDone chan struct{}

	buffered []Message

	mu      sync.Mutex
	tracker *inFlightTracker

	logger interface {
		Error(msg string, args ...any)
	}
}

type walBatch struct {
	messages []Message
	lsn      pglogrepl.LSN
}

var errWALClosed = errors.New("outbox: wal listener closed")

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

	readCtx, readStop := context.WithCancel(context.Background())
	w := &walListener{
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
		standbyInterval: cfg.KeepaliveInterval,
		batchCh:         make(chan walBatch),
		errCh:           make(chan error, 1),
		readCtx:         readCtx,
		readStop:        readStop,
		readDone:        make(chan struct{}),
		tracker:         newInFlightTracker(),
		logger:          cfg.Logger,
	}

	go w.readLoop()
	return w, nil
}

func (w *walListener) readLoop() {
	defer close(w.readDone)

	var pending []Message
	var txLSN pglogrepl.LSN
	nextStandby := time.Now().Add(w.standbyInterval)

	for {
		recvCtx, cancel := context.WithDeadline(w.readCtx, nextStandby)
		rawMsg, err := w.replConn.ReceiveMessage(recvCtx)
		cancel()

		if err != nil {
			if w.readCtx.Err() != nil {
				return
			}
			if !pgconn.Timeout(err) {
				w.emitErr(fmt.Errorf("outbox: wal receive: %w", err))
				return
			}
		}

		if !time.Now().Before(nextStandby) {
			if sErr := w.sendStandbyStatus(w.readCtx); sErr != nil && w.readCtx.Err() == nil {
				w.emitErr(fmt.Errorf("outbox: standby status update: %w", sErr))
				return
			}
			nextStandby = time.Now().Add(w.standbyInterval)
		}

		if err != nil {
			// err was pgconn.Timeout; standby (if due) has been sent. Loop.
			continue
		}

		cd, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, pErr := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if pErr != nil {
				w.emitErr(fmt.Errorf("outbox: parse keepalive: %w", pErr))
				return
			}
			if pkm.ReplyRequested {
				if sErr := w.sendStandbyStatus(w.readCtx); sErr != nil && w.readCtx.Err() == nil {
					w.emitErr(fmt.Errorf("outbox: standby reply: %w", sErr))
					return
				}
				nextStandby = time.Now().Add(w.standbyInterval)
			}

		case pglogrepl.XLogDataByteID:
			xld, pErr := pglogrepl.ParseXLogData(cd.Data[1:])
			if pErr != nil {
				w.emitErr(fmt.Errorf("outbox: parse xlogdata: %w", pErr))
				return
			}
			logicalMsg, pErr := pglogrepl.Parse(xld.WALData)
			if pErr != nil {
				w.emitErr(fmt.Errorf("outbox: parse logical msg: %w", pErr))
				return
			}

			switch m := logicalMsg.(type) {
			case *pglogrepl.BeginMessage:
				txLSN = m.FinalLSN
				pending = nil
			case *pglogrepl.RelationMessage:
				w.relations[m.RelationID] = m
			case *pglogrepl.InsertMessage:
				rel, rOK := w.relations[m.RelationID]
				if !rOK || rel.RelationName != w.tableName {
					continue
				}
				decoded, dErr := w.decodeInsert(rel, m.Tuple)
				if dErr != nil {
					w.emitErr(dErr)
					return
				}
				pending = append(pending, decoded)
			case *pglogrepl.CommitMessage:
				if len(pending) == 0 {
					continue
				}
				batch := walBatch{messages: pending, lsn: txLSN}
				pending = nil
				delivered, dErr := w.deliverBatch(batch, &nextStandby)
				if dErr != nil {
					w.emitErr(dErr)
					return
				}
				if !delivered {
					return
				}
			}
		}
	}
}

func (w *walListener) deliverBatch(batch walBatch, nextStandby *time.Time) (bool, error) {
	for {
		wait := time.Until(*nextStandby)
		if wait <= 0 {
			if err := w.sendStandbyStatus(w.readCtx); err != nil && w.readCtx.Err() == nil {
				return false, fmt.Errorf("outbox: standby status update: %w", err)
			}
			if w.readCtx.Err() != nil {
				return false, nil
			}
			*nextStandby = time.Now().Add(w.standbyInterval)
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case w.batchCh <- batch:
			timer.Stop()
			return true, nil
		case <-timer.C:
		case <-w.readCtx.Done():
			timer.Stop()
			return false, nil
		}
	}
}

func (w *walListener) emitErr(err error) {
	select {
	case w.errCh <- err:
	default:
	}
}

func (w *walListener) Next(ctx context.Context) (Message, int, error) {
	if len(w.buffered) > 0 {
		msg := w.buffered[0]
		w.buffered = w.buffered[1:]
		return msg, len(w.buffered), nil
	}
	select {
	case batch, ok := <-w.batchCh:
		if !ok {
			return Message{}, 0, errWALClosed
		}
		w.mu.Lock()
		ids := make([]int64, len(batch.messages))
		for i, m := range batch.messages {
			ids[i] = m.ID
		}
		w.tracker.Register(batch.lsn, ids)
		w.mu.Unlock()
		if len(batch.messages) > 1 {
			w.buffered = batch.messages[1:]
		}
		return batch.messages[0], len(w.buffered), nil
	case err := <-w.errCh:
		return Message{}, 0, err
	case <-ctx.Done():
		return Message{}, 0, ctx.Err()
	}
}

func (w *walListener) Confirm(ctx context.Context, ids ...int64) error {
	w.mu.Lock()
	if err := w.tracker.Validate(ids); err != nil {
		w.mu.Unlock()
		panic(err)
	}
	w.mu.Unlock()

	if _, err := w.dbConn.Exec(ctx, w.deleteQuery, ids); err != nil {
		return fmt.Errorf("outbox: delete ids=%v: %w", ids, err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.tracker.Apply(ids)
	return nil
}

func (w *walListener) Close(ctx context.Context) {
	w.readStop()
	<-w.readDone

	flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := w.sendStandbyStatus(flushCtx); err != nil && w.logger != nil {
		w.logger.Error("outbox: final standby status failed", "err", err)
	}
	cancel()

	_ = w.replConn.Close(ctx)
	_ = w.dbConn.Close(ctx)
}

func (w *walListener) sendStandbyStatus(ctx context.Context) error {
	w.mu.Lock()
	lsn := w.tracker.ConfirmedLSN()
	w.mu.Unlock()
	return pglogrepl.SendStandbyStatusUpdate(ctx, w.replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
	})
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
