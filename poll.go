package outboxd

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

type pollSource struct {
	conn          *pgx.Conn
	deleteConn    *pgx.Conn
	selectQuery   string
	deleteQuery   string
	pollInterval  time.Duration
	batchSize     int
	notifyChannel string
	buffered      []Message
	batchInFlight atomic.Int32
	confirmCh     chan struct{}
}

func newPollSource(ctx context.Context, dsn string, cfg Config) (*pollSource, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("outbox: poll connect: %w", err)
	}

	schema := cfg.Schema

	h := fnv.New32a()
	h.Write([]byte(schema.Table))
	lockKey := int64(h.Sum32())

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockKey); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("outbox: advisory lock: %w", err)
	}

	if cfg.Polling.NotifyChannel != "" {
		if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{cfg.Polling.NotifyChannel}.Sanitize()); err != nil {
			_ = conn.Close(ctx)
			return nil, fmt.Errorf("outbox: listen: %w", err)
		}
	}

	deleteConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("outbox: poll delete connect: %w", err)
	}

	id := pgx.Identifier{schema.IDColumn}.Sanitize()
	selectQuery := fmt.Sprintf("SELECT %s, %s, %s, %s FROM %s ORDER BY %s LIMIT $1",
		id,
		pgx.Identifier{schema.TopicColumn}.Sanitize(),
		pgx.Identifier{schema.PayloadColumn}.Sanitize(),
		pgx.Identifier{schema.CreatedAtColumn}.Sanitize(),
		pgx.Identifier{schema.Table}.Sanitize(),
		id,
	)

	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s = ANY($1)",
		pgx.Identifier{schema.Table}.Sanitize(),
		id,
	)

	return &pollSource{
		conn:          conn,
		deleteConn:    deleteConn,
		selectQuery:   selectQuery,
		deleteQuery:   deleteQuery,
		pollInterval:  cfg.Polling.PollInterval,
		batchSize:     cfg.Polling.BatchSize,
		notifyChannel: cfg.Polling.NotifyChannel,
		confirmCh:     make(chan struct{}, 1),
	}, nil
}

func (p *pollSource) Next(ctx context.Context) (Message, error) {
	for {
		if len(p.buffered) > 0 {
			msg := p.buffered[0]
			p.buffered = p.buffered[1:]
			return msg, nil
		}

		if p.batchInFlight.Load() > 0 {
			select {
			case <-p.confirmCh:
			case <-ctx.Done():
				return Message{}, ctx.Err()
			}
		}

		rows, err := p.conn.Query(ctx, p.selectQuery, p.batchSize)
		if err != nil {
			return Message{}, fmt.Errorf("outbox: poll query: %w", err)
		}

		var messages []Message
		for rows.Next() {
			var msg Message
			if err := rows.Scan(&msg.ID, &msg.Topic, &msg.Payload, &msg.CreatedAt); err != nil {
				rows.Close()
				return Message{}, fmt.Errorf("outbox: poll scan: %w", err)
			}
			messages = append(messages, msg)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return Message{}, fmt.Errorf("outbox: poll rows: %w", err)
		}

		if len(messages) > 0 {
			return p.armBatch(messages), nil
		}

		if err := p.waitForActivity(ctx); err != nil {
			return Message{}, err
		}
	}
}

func (p *pollSource) waitForActivity(ctx context.Context) error {
	if p.notifyChannel == "" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.pollInterval):
			return nil
		}
	}

	waitCtx, cancel := context.WithTimeout(ctx, p.pollInterval)
	defer cancel()

	_, err := p.conn.WaitForNotification(waitCtx)
	if waitCtx.Err() != nil && ctx.Err() == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		return nil
	}
	return nil
}

func (p *pollSource) armBatch(messages []Message) Message {
	select {
	case <-p.confirmCh:
	default:
	}
	p.buffered = messages[1:]
	p.batchInFlight.Store(int32(len(messages)))
	return messages[0]
}

func (p *pollSource) Remaining() int {
	return len(p.buffered)
}

func (p *pollSource) Confirm(ctx context.Context, ids ...int64) error {
	if _, err := p.deleteConn.Exec(ctx, p.deleteQuery, ids); err != nil {
		return fmt.Errorf("outbox: delete ids=%v: %w", ids, err)
	}
	if remaining := p.batchInFlight.Add(-int32(len(ids))); remaining <= 0 {
		p.batchInFlight.Store(0)
		select {
		case p.confirmCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func (p *pollSource) Close(ctx context.Context) {
	_ = p.conn.Close(ctx)
	_ = p.deleteConn.Close(ctx)
}
