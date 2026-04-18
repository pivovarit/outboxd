package outboxd

import (
	"context"
	"log/slog"
	"time"
)

const maxRetryDelay = time.Minute

const flushTimeout = 5 * time.Second

type Message struct {
	ID        int64
	Topic     string
	Payload   []byte
	CreatedAt time.Time
}

type Handler func(ctx context.Context, msg Message) error

// Middleware wraps a Handler and returns a new Handler. It is used to compose
// cross-cutting concerns (panic recovery, logging, metrics, tracing) around
// the user's handler without changing its signature.
type Middleware func(Handler) Handler

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
	// Middlewares wrap the user handler in the order given. The first element
	// is outermost: it is entered first and exits last. Retry is applied by
	// the relay outside the chain, so each retry attempt flows through every
	// middleware. A nil or empty slice disables middleware entirely.
	Middlewares []Middleware
	// KeepaliveInterval drives both the relay's periodic-confirm ticker and
	// the WAL listener's standby-status ticker. Bounds how long delivered
	// rows can linger before deletion and how long a slow handler can
	// starve replication slot updates. Must stay well below Postgres'
	// wal_sender_timeout (default 60s).
	KeepaliveInterval time.Duration
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
	if c.KeepaliveInterval == 0 {
		c.KeepaliveInterval = 5 * time.Second
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
	return &Relay{dsn: dsn, handler: wrap(handler, cfg.Middlewares), cfg: cfg}
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

type nextResult struct {
	msg       Message
	remaining int
}

func (r *Relay) run(ctx context.Context, src source) error {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	msgCh := make(chan nextResult)
	nextErrCh := make(chan error, 1)

	go func() {
		for {
			msg, remaining, err := src.Next(subCtx)
			if err != nil {
				nextErrCh <- err
				return
			}
			select {
			case msgCh <- nextResult{msg: msg, remaining: remaining}:
			case <-subCtx.Done():
				return
			}
		}
	}()

	var pending []int64
	ticker := time.NewTicker(r.cfg.KeepaliveInterval)
	defer ticker.Stop()

	flush := func(c context.Context) error {
		if len(pending) == 0 {
			return nil
		}
		if err := src.Confirm(c, pending...); err != nil {
			return err
		}
		pending = nil
		return nil
	}

	defer func() {
		if len(pending) == 0 {
			return
		}
		ids := pending
		fCtx, fCancel := context.WithTimeout(context.Background(), flushTimeout)
		defer fCancel()
		if err := flush(fCtx); err != nil {
			r.cfg.Logger.Error("outbox: flush on close failed", "ids", ids, "err", err)
		}
	}()

	for {
		select {
		case err := <-nextErrCh:
			return err
		case nr := <-msgCh:
			r.cfg.Logger.Info("outbox: message received", "id", nr.msg.ID, "topic", nr.msg.Topic)
			if err := r.deliverWithRetry(ctx, nr.msg); err != nil {
				return err
			}
			pending = append(pending, nr.msg.ID)
			if nr.remaining == 0 {
				fCtx, fCancel := context.WithTimeout(context.Background(), flushTimeout)
				err := flush(fCtx)
				fCancel()
				if err != nil {
					return err
				}
			}
		case <-ticker.C:
			fCtx, fCancel := context.WithTimeout(context.Background(), flushTimeout)
			err := flush(fCtx)
			fCancel()
			if err != nil {
				return err
			}
		case <-ctx.Done():
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

// wrap composes mws around h so that mws[0] is the outermost wrapper.
// Calling wrap(h, nil) or wrap(h, []Middleware{}) returns h unchanged.
func wrap(h Handler, mws []Middleware) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
