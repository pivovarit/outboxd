package outboxd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const maxRetryDelay = time.Minute

// ErrRetriesExhausted is returned by [Relay.Start] when [Config.FailStop] is
// set and a message failed all MaxRetries+1 delivery attempts. The failing
// message is not confirmed: it stays in the outbox and is redelivered when
// the relay is restarted. The underlying handler error is wrapped and can be
// inspected with [errors.Unwrap] or [errors.As].
var ErrRetriesExhausted = errors.New("outbox: message retries exhausted")

const flushTimeout = 5 * time.Second

// Message is a single outbox row delivered to the handler.
type Message struct {
	ID        int64
	Topic     string
	Payload   []byte
	CreatedAt time.Time
	Extras    map[string]any
}

// Handler processes a single outbox message. Returning a non-nil error
// triggers the relay's retry logic; returning nil marks the message as
// delivered.
type Handler func(ctx context.Context, msg Message) error

// Middleware wraps a Handler and returns a new Handler. It is used to compose
// cross-cutting concerns (panic recovery, logging, metrics, tracing) around
// the user's handler without changing its signature.
type Middleware func(Handler) Handler

const columnDisabled = "-"

// SchemaConfig maps the outbox table and column names. All fields default
// to the conventional names ("outbox", "id", "topic", "payload", "created_at")
// when left empty.
type SchemaConfig struct {
	Table           string
	IDColumn        string
	TopicColumn     string
	PayloadColumn   string
	CreatedAtColumn string
	ExtraColumns    []string
}

func (s SchemaConfig) tableIdent() pgx.Identifier {
	if ns, name, ok := strings.Cut(s.Table, "."); ok {
		return pgx.Identifier{ns, name}
	}
	return pgx.Identifier{s.Table}
}

func (s SchemaConfig) topicEnabled() bool {
	return s.TopicColumn != "" && s.TopicColumn != columnDisabled
}
func (s SchemaConfig) createdAtEnabled() bool {
	return s.CreatedAtColumn != "" && s.CreatedAtColumn != columnDisabled
}

// PollingConfig enables poll-based delivery instead of WAL replication.
// When set on [Config], the relay polls the outbox table at PollInterval,
// optionally waking early via a PostgreSQL LISTEN/NOTIFY channel.
type PollingConfig struct {
	PollInterval  time.Duration
	BatchSize     int
	NotifyChannel string
}

// Config controls relay behaviour. Zero-value fields use sensible defaults;
// see [Config.setDefaults] for the full list. Set Polling to a non-nil
// [PollingConfig] to switch from WAL replication to poll-based delivery.
type Config struct {
	SlotName     string
	Publications []string
	RetryDelay   time.Duration
	// MaxRetries bounds delivery attempts per message: after 1+MaxRetries
	// failed attempts the message is resolved (dropped, or halted on if
	// FailStop is set). 0 means retry forever — delivery stalls on the
	// failing message and the replication slot retains WAL until it
	// succeeds, so pair it with slot-lag alerting (see README).
	MaxRetries int
	// FailStop halts the relay instead of dropping a message that exhausted
	// MaxRetries: [Relay.Start] returns an error wrapping
	// [ErrRetriesExhausted], the message stays unconfirmed, and it is
	// redelivered after a restart. Ignored when MaxRetries is 0.
	FailStop bool
	// OnDropped is invoked after a message exhausts MaxRetries and FailStop
	// is unset — the last chance to persist it (dead-letter) before the
	// relay confirms and deletes it.
	OnDropped func(msg Message, err error)
	Schema    SchemaConfig
	Logger    *slog.Logger
	Polling   *PollingConfig
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
	// HealthAddr, when set, starts an HTTP server exposing /health
	// (liveness) and /ready (readiness) probe endpoints, plus /status,
	// which reports delivery progress as JSON (see [Status]). The readiness
	// probe returns 200 only while the relay has an active connection to
	// PostgreSQL; note that a relay stuck retrying a poison message still
	// reports ready — watch /status or [Relay.Status] for that.
	// Example: ":8080".
	HealthAddr string
}

func (c *Config) setDefaults() {
	if c.SlotName == "" {
		c.SlotName = "outbox_relay"
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = time.Second
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
	if len(c.Schema.ExtraColumns) > 0 {
		core := map[string]struct{}{
			c.Schema.IDColumn:      {},
			c.Schema.PayloadColumn: {},
		}
		if c.Schema.topicEnabled() {
			core[c.Schema.TopicColumn] = struct{}{}
		}
		if c.Schema.createdAtEnabled() {
			core[c.Schema.CreatedAtColumn] = struct{}{}
		}
		filtered := c.Schema.ExtraColumns[:0]
		for _, ec := range c.Schema.ExtraColumns {
			if _, collision := core[ec]; !collision {
				filtered = append(filtered, ec)
			}
		}
		c.Schema.ExtraColumns = filtered
	}
}

// Relay connects to PostgreSQL, reads outbox rows, and delivers them to
// the configured [Handler]. Create one with [New] and run it with [Relay.Start].
type Relay struct {
	dsn      string
	handler  Handler
	cfg      Config
	health   *healthServer
	progress progressTracker
	// newSource overrides source construction; used by tests to inject fakes.
	newSource func(ctx context.Context, dsn string, cfg Config) (source, error)
}

// New creates a Relay that will connect to dsn and deliver outbox messages to
// handler. Call [Relay.Start] to begin processing.
func New(dsn string, handler Handler, cfg Config) *Relay {
	cfg.setDefaults()
	r := &Relay{dsn: dsn, handler: wrap(handler, cfg.Middlewares), cfg: cfg}
	if cfg.HealthAddr != "" {
		r.health = newHealthServer(cfg.HealthAddr, r.Status)
	}
	return r
}

// Start connects to PostgreSQL and begins delivering outbox messages to the
// handler. It blocks until ctx is cancelled, reconnecting automatically on
// transient errors with exponential back-off. The returned error is the
// context's cancellation cause, or an error wrapping [ErrRetriesExhausted]
// when [Config.FailStop] halts the relay on a message that exhausted its
// retries.
func (r *Relay) Start(ctx context.Context) error {
	if r.health != nil {
		go func() {
			if err := r.health.listenAndServe(); err != nil && err != http.ErrServerClosed {
				r.cfg.Logger.Error("outbox: health server error", "err", err)
			}
		}()
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			r.health.shutdown(shutdownCtx)
		}()
	}

	delay := r.cfg.RetryDelay
	for {
		var src source
		var err error
		switch {
		case r.newSource != nil:
			src, err = r.newSource(ctx, r.dsn, r.cfg)
		case r.cfg.Polling != nil:
			src, err = newPollSource(ctx, r.dsn, r.cfg)
		default:
			src, err = newWALListener(ctx, r.dsn, r.cfg)
		}
		var ranFor time.Duration
		if err == nil {
			if r.health != nil {
				r.health.ready.Store(true)
			}
			started := time.Now()
			err = r.run(ctx, src)
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			src.Close(closeCtx)
			closeCancel()
			ranFor = time.Since(started)
			if r.health != nil {
				r.health.ready.Store(false)
			}
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, ErrRetriesExhausted) {
			return err
		}

		r.cfg.Logger.Error("outbox: connection error", "err", err, "retry_in", delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = nextDelay(delay, r.cfg.RetryDelay, ranFor)
	}
}

func nextDelay(current, base, ranFor time.Duration) time.Duration {
	if ranFor >= maxRetryDelay {
		return base
	}
	next := current * 2
	if next > maxRetryDelay {
		next = maxRetryDelay
	}
	return next
}

type nextResult struct {
	msg       Message
	remaining int
}

func (r *Relay) run(ctx context.Context, src source) error {
	subCtx, cancel := context.WithCancel(ctx)

	msgCh := make(chan nextResult)
	nextErrCh := make(chan error, 1)
	nextDone := make(chan struct{})
	defer func() {
		cancel()
		<-nextDone
	}()

	go func() {
		defer close(nextDone)
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
		pending = pending[:0]
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
			r.cfg.Logger.Debug("outbox: message received", "id", nr.msg.ID, "topic", nr.msg.Topic)
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
	for retries := 0; ; retries++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := r.handler(ctx, msg); err != nil {
			// A failure caused by shutdown is not a real attempt: dropping here
			// would let the deferred flush delete a message that was never handled.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if r.cfg.MaxRetries > 0 && retries >= r.cfg.MaxRetries {
				if r.cfg.FailStop {
					// Keep the retry state visible: the message is still
					// unresolved when the relay halts.
					r.progress.retrying(msg.ID, retries+1, time.Now())
					r.cfg.Logger.Error("outbox: retries exhausted, halting relay",
						"id", msg.ID, "attempts", retries+1, "err", err)
					return fmt.Errorf("%w: message %d: %w", ErrRetriesExhausted, msg.ID, err)
				}
				r.progress.resolved()
				r.cfg.Logger.Error("outbox: message dropped",
					"id", msg.ID, "attempts", retries+1, "err", err)
				if r.cfg.OnDropped != nil {
					r.cfg.OnDropped(msg, err)
				}
				return nil
			}
			r.progress.retrying(msg.ID, retries+1, time.Now())
			r.cfg.Logger.Error("outbox: handler error",
				"id", msg.ID, "attempt", retries+1, "err", err, "retry_in", delay)
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

		r.progress.delivered(time.Now())
		r.cfg.Logger.Debug("outbox: message delivered", "id", msg.ID, "topic", msg.Topic)
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
