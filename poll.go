package outboxd

import (
	"context"
	"fmt"
	"hash/fnv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

type pollSource struct {
	conn             *pgx.Conn
	deleteConn       *pgx.Conn
	selectQuery      string
	deleteQuery      string
	pollInterval     time.Duration
	batchSize        int
	notifyChannel    string
	buffered         []Message
	batchInFlight    atomic.Int32
	confirmCh        chan struct{}
	topicEnabled     bool
	createdAtEnabled bool
	extraColumns     []string
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
	tableIdent := schema.tableIdent().Sanitize()

	cols := []string{id, pgx.Identifier{schema.PayloadColumn}.Sanitize()}
	if schema.topicEnabled() {
		cols = append(cols, pgx.Identifier{schema.TopicColumn}.Sanitize())
	}
	if schema.createdAtEnabled() {
		cols = append(cols, pgx.Identifier{schema.CreatedAtColumn}.Sanitize())
	}
	for _, ec := range schema.ExtraColumns {
		cols = append(cols, pgx.Identifier{ec}.Sanitize())
	}

	selectQuery := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT $1",
		strings.Join(cols, ", "),
		tableIdent,
		id,
	)

	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s = ANY($1)",
		tableIdent,
		id,
	)

	return &pollSource{
		conn:             conn,
		deleteConn:       deleteConn,
		selectQuery:      selectQuery,
		deleteQuery:      deleteQuery,
		pollInterval:     cfg.Polling.PollInterval,
		batchSize:        cfg.Polling.BatchSize,
		notifyChannel:    cfg.Polling.NotifyChannel,
		confirmCh:        make(chan struct{}, 1),
		topicEnabled:     schema.topicEnabled(),
		createdAtEnabled: schema.createdAtEnabled(),
		extraColumns:     schema.ExtraColumns,
	}, nil
}

func (p *pollSource) Next(ctx context.Context) (Message, int, error) {
	for {
		if len(p.buffered) > 0 {
			msg := p.buffered[0]
			p.buffered = p.buffered[1:]
			return msg, len(p.buffered), nil
		}

		if p.batchInFlight.Load() > 0 {
			select {
			case <-p.confirmCh:
			case <-ctx.Done():
				return Message{}, 0, ctx.Err()
			}
		}

		rows, err := p.conn.Query(ctx, p.selectQuery, p.batchSize)
		if err != nil {
			return Message{}, 0, fmt.Errorf("outbox: poll query: %w", err)
		}

		var messages []Message
		for rows.Next() {
			var msg Message
			dests := []any{&msg.ID, &msg.Payload}
			if p.topicEnabled {
				dests = append(dests, &msg.Topic)
			}
			if p.createdAtEnabled {
				dests = append(dests, &msg.CreatedAt)
			}
			var extraVals []any
			if len(p.extraColumns) > 0 {
				extraVals = make([]any, len(p.extraColumns))
				for i := range extraVals {
					dests = append(dests, &extraVals[i])
				}
			}
			if err := rows.Scan(dests...); err != nil {
				rows.Close()
				return Message{}, 0, fmt.Errorf("outbox: poll scan: %w", err)
			}
			if len(p.extraColumns) > 0 {
				msg.Extras = make(map[string]any, len(p.extraColumns))
				for i, ec := range p.extraColumns {
					msg.Extras[ec] = extraVals[i]
				}
			}
			messages = append(messages, msg)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return Message{}, 0, fmt.Errorf("outbox: poll rows: %w", err)
		}

		if len(messages) > 0 {
			msg := p.armBatch(messages)
			return msg, len(p.buffered), nil
		}

		if err := p.waitForActivity(ctx); err != nil {
			return Message{}, 0, err
		}
	}
}

func (p *pollSource) waitForActivity(ctx context.Context) error {
	if p.notifyChannel == "" {
		timer := time.NewTimer(p.pollInterval)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
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
