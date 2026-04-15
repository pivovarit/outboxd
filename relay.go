package outboxd

import (
	"context"
	"log/slog"
	"time"
)

const maxRetryDelay = time.Minute

type Message struct {
	ID        int64
	Topic     string
	Payload   []byte
	CreatedAt time.Time
}

type Handler func(ctx context.Context, msg Message) error

type SchemaConfig struct {
	Table           string
	IDColumn        string
	TopicColumn     string
	PayloadColumn   string
	CreatedAtColumn string
}

type PollingConfig struct {
	PollInterval  time.Duration
	BatchSize     int
	NotifyChannel string
}

type Config struct {
	SlotName     string
	Publications []string
	RetryDelay   time.Duration
	MaxRetries   int
	OnDropped    func(msg Message, err error)
	Schema       SchemaConfig
	Logger       *slog.Logger
	Polling      *PollingConfig
}

func (c *Config) setDefaults() {
	if c.SlotName == "" {
		c.SlotName = "outbox_relay"
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = time.Second
	}
	if c.Schema.Table == "" {
		c.Schema.Table = "outbox"
	}
	if c.Schema.IDColumn == "" {
		c.Schema.IDColumn = "id"
	}
	if c.Schema.TopicColumn == "" {
		c.Schema.TopicColumn = "topic"
	}
	if c.Schema.PayloadColumn == "" {
		c.Schema.PayloadColumn = "payload"
	}
	if c.Schema.CreatedAtColumn == "" {
		c.Schema.CreatedAtColumn = "created_at"
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Polling != nil {
		if c.Polling.PollInterval == 0 {
			c.Polling.PollInterval = time.Second
		}
		if c.Polling.BatchSize == 0 {
			c.Polling.BatchSize = 100
		}
	}
}

type Relay struct {
	dsn     string
	handler Handler
	cfg     Config
}

func New(dsn string, handler Handler, cfg Config) *Relay {
	cfg.setDefaults()
	return &Relay{dsn: dsn, handler: handler, cfg: cfg}
}

func (r *Relay) Start(ctx context.Context) error {
	delay := r.cfg.RetryDelay
	for {
		var src source
		var err error
		if r.cfg.Polling != nil {
			src, err = newPollSource(ctx, r.dsn, r.cfg)
		} else {
			src, err = newWALListener(ctx, r.dsn, r.cfg)
		}
		if err == nil {
			delay = r.cfg.RetryDelay
			err = r.run(ctx, src)
			src.Close(ctx)
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		r.cfg.Logger.Error("outbox: connection error", "err", err, "retry_in", delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > maxRetryDelay {
			delay = maxRetryDelay
		}
	}
}

func (r *Relay) run(ctx context.Context, src source) error {
	var pendingConfirm []int64

	for {
		msg, err := src.Next(ctx)
		if err != nil {
			return err
		}

		r.cfg.Logger.Info("outbox: message received", "id", msg.ID, "topic", msg.Topic)

		if err := r.deliverWithRetry(ctx, msg); err != nil {
			return err
		}

		pendingConfirm = append(pendingConfirm, msg.ID)

		if src.Remaining() == 0 {
			if err := src.Confirm(ctx, pendingConfirm...); err != nil {
				return err
			}
			pendingConfirm = nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (r *Relay) deliverWithRetry(ctx context.Context, msg Message) error {
	delay := r.cfg.RetryDelay
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := r.handler(ctx, msg); err != nil {
			if r.cfg.MaxRetries > 0 && attempt >= r.cfg.MaxRetries {
				r.cfg.Logger.Error("outbox: message dropped",
					"id", msg.ID, "attempts", attempt, "err", err)
				if r.cfg.OnDropped != nil {
					r.cfg.OnDropped(msg, err)
				}
				return nil
			}
			r.cfg.Logger.Error("outbox: handler error",
				"id", msg.ID, "attempt", attempt, "err", err, "retry_in", delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
			if delay > maxRetryDelay {
				delay = maxRetryDelay
			}
			continue
		}

		r.cfg.Logger.Info("outbox: message delivered", "id", msg.ID, "topic", msg.Topic)
		return nil
	}
}
