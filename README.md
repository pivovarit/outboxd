# outboxd

[![CI](https://github.com/pivovarit/outboxd/actions/workflows/ci.yml/badge.svg)](https://github.com/pivovarit/outboxd/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/pivovarit/outboxd.svg)](https://pkg.go.dev/github.com/pivovarit/outboxd)
[![Go Report Card](https://goreportcard.com/badge/github.com/pivovarit/outboxd)](https://goreportcard.com/report/github.com/pivovarit/outboxd)

A lightweight low-latency outbox event relay powered by PostgreSQL logical replication - bridging the gap between naive polling and full-blown CDC solutions like Debezium.

When a service writes to the database and needs to notify other services, doing both in a single transaction is impossible (dual-write problem). The [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) solves this by writing events to an outbox table within the same transaction, then relaying them to a message broker separately.

`outboxd` handles the relay part - it listens for changes via a replication slot and delivers messages to a handler you provide as soon as they're committed. When logical replication is not available, it can fall back to a polling-based strategy with optional `pg_notify` acceleration. The handler is just a Go function anyone can implement. Everything else (WAL streaming, replication slot management, retries, cleanup) is taken care of. No external infrastructure beyond PostgreSQL, no JVM.

## How it works

1. Your application inserts a row into the `outbox` table inside a transaction
2. `outboxd` picks up the INSERT via a PostgreSQL logical replication slot
3. Your handler receives the message and delivers it (e.g. to RabbitMQ, Kafka, HTTP)
4. The row is deleted from the outbox table and the WAL position is acknowledged

## Usage

The handler is just a function - return `nil` when done, return an error to retry:

```go
handler := func(ctx context.Context, msg outboxd.Message) error {
    return rabbitCh.PublishWithContext(ctx, "exchange", msg.Topic, false, false,
        amqp.Publishing{Body: msg.Payload},
    )
}
```

That's it. Plug it in and start relaying:

```go
relay := outboxd.New(databaseURL, handler, outboxd.Config{
    SlotName:     "outbox_relay",
    Publications: []string{"outbox_pub"},
    Schema: outboxd.SchemaConfig{
        Table:           "outbox",
        IDColumn:        "id",
        TopicColumn:     "topic",
        PayloadColumn:   "payload",
        CreatedAtColumn: "created_at",
    },
})

relay.Start(ctx)
```

No framework to learn, no interfaces to implement, no configuration files. Just a function.

## Middleware

Cross-cutting concerns like panic recovery, logging, metrics, tracing can be composed
around your handler via `Config.Middlewares`. 

A middleware is just a function:

```go
type Middleware func(Handler) Handler
```

The first entry in `Middlewares` is **outermost**: it is entered first and exits last. Retry is applied by the relay *outside* the middleware chain, so every
middleware observes each retry attempt.

`outboxd` ships one stock middleware - `Recover()` - which catches handler
panics and converts them to errors, letting the normal retry/drop path handle
them instead of crashing the relay:

```go
import (
    "github.com/pivovarit/outboxd"
    "github.com/pivovarit/outboxd/middleware"
)

relay := outboxd.New(databaseURL, handler, outboxd.Config{
    SlotName:     "outbox_relay",
    Publications: []string{"outbox_pub"},
    Schema: outboxd.SchemaConfig{
        Table:           "outbox",
        IDColumn:        "id",
        TopicColumn:     "topic",
        PayloadColumn:   "payload",
        CreatedAtColumn: "created_at",
    },
    Middlewares: []outboxd.Middleware{
        middleware.Recover(), // place first so it also catches panics from later middleware
    },
})
```

`Recover()` is safe but not additive - registering it more than once does nothing useful: the innermost instance converts the panic to an error, and outer instances see a normal return.

Writing your own middleware is equally simple - any function matching the `Middleware` signature works:

```go
logging := func(next outboxd.Handler) outboxd.Handler {
    return func(ctx context.Context, msg outboxd.Message) error {
        start := time.Now()
        err := next(ctx, msg)
        log.Printf("delivered id=%d topic=%s took=%s err=%v", msg.ID, msg.Topic, time.Since(start), err)
        return err
    }
}
```

### OpenTelemetry

The `middleware/otel` package provides ready-made middleware for tracing and metrics:

```go
import (
    "github.com/pivovarit/outboxd"
    "github.com/pivovarit/outboxd/middleware"
    "github.com/pivovarit/outboxd/middleware/otel"
)

relay := outboxd.New(databaseURL, handler, outboxd.Config{
    SlotName:     "outbox_relay",
    Publications: []string{"outbox_pub"},
    Schema: outboxd.SchemaConfig{
        Table:           "outbox",
        IDColumn:        "id",
        TopicColumn:     "topic",
        PayloadColumn:   "payload",
        CreatedAtColumn: "created_at",
    },
    Middlewares: []outboxd.Middleware{
        middleware.Recover(),
        otel.Tracing(),
        otel.Metrics(),
    },
})
```

`Tracing()` emits a span per delivery attempt. `Metrics()` records a `messaging.publish.messages` counter and a `messaging.publish.duration` histogram (in ms). Both follow the OpenTelemetry [messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/).

By default, the middleware uses the global `TracerProvider` and `MeterProvider`. Override them with options:

```go
otel.Tracing(otel.WithTracerProvider(tp), otel.WithMessagingSystem("kafka"))
otel.Metrics(otel.WithMeterProvider(mp), otel.WithMessagingSystem("kafka"))
```

## Health checks

`outboxd` can expose liveness and readiness probe endpoints over HTTP. Set `HealthAddr` to start a lightweight HTTP server alongside the relay:

```go
relay := outboxd.New(databaseURL, handler, outboxd.Config{
    HealthAddr: ":8080",
    Schema: outboxd.SchemaConfig{
        Table:           "outbox",
        IDColumn:        "id",
        TopicColumn:     "topic",
        PayloadColumn:   "payload",
        CreatedAtColumn: "created_at",
    },
})
```

- `GET /health` - liveness probe, returns `200` while the relay is running (the server starts with `Start` and stops when it returns)
- `GET /ready` - readiness probe, returns `200` while connected to PostgreSQL and delivery is not stalled; `503` otherwise
- `GET /status` - delivery progress as JSON: total delivered, time of last delivery, and whether delivery is currently blocked retrying a message (and if so, which one, for how long, and how many attempts)

If `HealthAddr` cannot be bound, `Start` returns the error immediately rather than running without probes.

Readiness covers progress, not just connectivity: once the same message has been failing and retrying for longer than `StalledAfter` (default 5 minutes), `/ready` flips to `503` so an orchestrator can alert or restart the relay instead of leaving it wedged silently. Set `StalledAfter` to a negative value to opt out and make readiness reflect connectivity only. For the full retry picture (which message, how many attempts, since when), use `/status` or `Relay.Status()` - see [Retries and poison messages](#retries-and-poison-messages).

## Running the example

Try it with a single command:

```bash
cd $(mktemp -d) && git clone https://github.com/pivovarit/outboxd.git && cd outboxd/example && docker compose up --build
```

The included Docker Compose example starts PostgreSQL, RabbitMQ, a producer, a consumer, and two competing relay instances.

Since PostgreSQL replication slots are exclusive, only one relay can hold the slot at a time. The second relay retries silently until the first one stops.

## Prerequisites

PostgreSQL must have `wal_level=logical` enabled. The outbox table and publication must exist:

```sql
CREATE TABLE outbox (
    id         BIGSERIAL PRIMARY KEY,
    topic      TEXT NOT NULL,
    payload    BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE PUBLICATION outbox_pub FOR TABLE outbox WITH (publish = 'insert');
```

Column names, table name, and the column set itself are configurable via `SchemaConfig`:

```go
relay := outboxd.New(databaseURL, handler, outboxd.Config{
    Schema: outboxd.SchemaConfig{
        Table:           "domain_events",
        IDColumn:        "event_id",
        TopicColumn:     "event_type",
        PayloadColumn:   "data",
        CreatedAtColumn: "inserted_at",
    },
})
```

### Optional columns

`topic` and `created_at` can be disabled entirely by setting their column name to `"-"`. This lets the relay work with minimal tables that only have an ID and payload:

```sql
CREATE TABLE outbox (
    id      BIGSERIAL PRIMARY KEY,
    payload BYTEA NOT NULL
);
```

```go
relay := outboxd.New(databaseURL, handler, outboxd.Config{
    Schema: outboxd.SchemaConfig{
        Table:           "outbox",
        IDColumn:        "id",
        PayloadColumn:   "payload",
        TopicColumn:     "-",
        CreatedAtColumn: "-",
    },
})
```

When disabled, `msg.Topic` is `""` and `msg.CreatedAt` is the zero value.

### Extra columns

If your outbox table has additional columns beyond the standard four, declare them in `ExtraColumns` to have the relay read them into `msg.Extras`:

```sql
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    topic         TEXT NOT NULL,
    payload       BYTEA NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    aggregate_id  TEXT NOT NULL,
    partition_key TEXT NOT NULL
);
```

```go
relay := outboxd.New(databaseURL, handler, outboxd.Config{
    Schema: outboxd.SchemaConfig{
        Table:           "outbox",
        IDColumn:        "id",
        TopicColumn:     "topic",
        PayloadColumn:   "payload",
        CreatedAtColumn: "created_at",
        ExtraColumns:    []string{"aggregate_id", "partition_key"},
    },
})
```

Extra column values are available as `msg.Extras["aggregate_id"]`, `msg.Extras["partition_key"]`, etc. The map is `nil` when no extra columns are configured (zero overhead for the default case). Values receive their natural Go types from pgx (`string` for `TEXT`, `int64` for `BIGINT`, etc.).

### Payload type flexibility

The `payload` column can be `BYTEA`, `TEXT`, or `JSONB` — all three scan into `msg.Payload` (`[]byte`) transparently.

## First start and pre-existing rows

`outboxd` creates the replication slot on first start if it does not already exist. The slot's consistent point is fixed at creation, so rows inserted **before** the slot existed are not in its WAL stream and will not be delivered. If you need to relay a pre-existing backlog, drain it first (e.g. via polling mode) before switching to WAL mode.

## Delivery semantics

`outboxd` is at-least-once: a message can be delivered more than once if the relay crashes after the handler returns but before the WAL position is acknowledged (or, in polling mode, before the row is deleted). Consumers should treat `Message.ID` as a deduplication key.

In WAL mode, messages are delivered in **commit order**, not in `Message.ID` order. If transaction A inserts `id=10` and transaction B inserts `id=11`, but B commits first, the consumer sees `11` before `10`. `Message.ID` is unique and safe to dedupe on, but it is **not** a high-watermark - a downstream that assumes "if I see id=N, I've seen everything < N" will be wrong.

Polling mode observes only rows that are committed when each poll query runs, and orders that visible set by `Message.ID`. This still means `Message.ID` is not a high-watermark: a long-running transaction can allocate `id=10`, another transaction can allocate and commit `id=11`, and a poll between those commits can deliver `11` before `10` is even visible. When the long-running transaction later commits, `10` is picked up by a later poll or `NotifyChannel` wakeup.

## Retries and poison messages

When the handler returns an error, the relay retries the message with exponential backoff (starting at `RetryDelay`, capped at one minute). Messages are delivered serially, so a message that keeps failing blocks everything behind it. What happens next is a policy decision, controlled by `MaxRetries` and `FailStop`:

| Config | Behaviour | Trade-off |
|---|---|---|
| `MaxRetries: 0` (default) | Retry forever | Nothing is ever lost, but a poison message stalls delivery indefinitely |
| `MaxRetries: N` | Drop after 1+N attempts, invoking `OnDropped` | Delivery keeps flowing, but the message is deleted - persist it in `OnDropped` (dead-letter table, log, alert) or it is gone |
| `MaxRetries: N, FailStop: true` | Halt: `Start` returns an error wrapping `ErrRetriesExhausted` | Nothing is lost and the failure is loud, but the relay stops until restarted; the message is redelivered then |

This mirrors PostgreSQL's own logical replication: a subscription retries a failing transaction forever by default, `disable_on_error` is its fail-stop, and `ALTER SUBSCRIPTION ... SKIP` is its manual drop.

`OnDropped` runs synchronously on the delivery goroutine - it must return before the relay moves on, which is what guarantees the dead-letter write lands before the message is deleted. The flip side: a hung callback stalls delivery just like a hung handler, so bound any I/O in it with a timeout. Panics in `OnDropped` are recovered and logged, but the message is still deleted, so don't rely on panicking to keep a message.

### If you keep the default, alert on slot lag

`MaxRetries: 0` is the safe default for data integrity, but it turns a poison message into a silent operational hazard in WAL mode: while delivery is stalled the replication slot stops advancing, and PostgreSQL retains all WAL the slot has not confirmed - **unbounded disk growth on the primary**. `/ready` flips to `503` after `StalledAfter` (default 5 minutes) of retrying the same message, but a readiness flip alone does not free the disk: a relay running the default **must** still be paired with slot-lag alerting:

```sql
SELECT slot_name,
       wal_status,
       safe_wal_size,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal
FROM pg_replication_slots
WHERE slot_name = 'outbox_relay';
```

Alert when `retained_wal` grows past what your disk can absorb, or when `wal_status` leaves `reserved`. On the relay side, `GET /status` reports the stall directly: `retrying: true` with a `retrying_since` timestamp that keeps aging while `delivered` stands still.

As a last line of defense, cap how much WAL a stalled slot can pin:

```sql
ALTER SYSTEM SET max_slot_wal_keep_size = '10GB';
```

When the cap is exceeded PostgreSQL invalidates the slot instead of filling the disk - the primary survives, but the relay's WAL stream is gone. Undelivered rows still sitting in the outbox table are **not** replayed through a recreated slot (its consistent point is fixed at creation); drain them via polling mode before switching back to WAL mode. That is the trade the cap buys you: a recoverable drain instead of a full primary outage.

### Ejecting a poison message

To skip a message that will never succeed without restarting into a different policy, handle it in the handler - return `nil` for the offending ID (persisting it wherever your dead letters go) and let the relay confirm it:

```go
handler := func(ctx context.Context, msg outboxd.Message) error {
    if msg.ID == poisonID {
        log.Printf("skipping poison message %d", msg.ID)
        return nil // confirms and deletes the row
    }
    return publish(ctx, msg)
}
```

Note that in WAL mode, deleting the row from the outbox table does **not** skip it: the INSERT is already in the slot's WAL stream and will still be delivered. Deleting the row only works in polling mode.

## Polling mode

If logical replication is not available (e.g. managed PostgreSQL without `wal_level=logical`, restricted permissions, or shared hosting), `outboxd` can fall back to a polling-based strategy. Enable it by providing a `PollingConfig`:

```go
relay := outboxd.New(databaseURL, handler, outboxd.Config{
    Schema: outboxd.SchemaConfig{
        Table:           "outbox",
        IDColumn:        "id",
        TopicColumn:     "topic",
        PayloadColumn:   "payload",
        CreatedAtColumn: "created_at",
    },
    Polling: &outboxd.PollingConfig{
        PollInterval: 500 * time.Millisecond,
        BatchSize:    100,
    },
})
```

In polling mode, `outboxd` periodically queries the outbox table for new rows, delivers them through the handler, and deletes processed rows. An advisory lock ensures only one relay instance processes messages at a time.

### NOTIFY-accelerated polling

For near-real-time delivery without logical replication, combine polling with PostgreSQL `NOTIFY`. Set `NotifyChannel` and create a trigger on the outbox table:

```sql
CREATE OR REPLACE FUNCTION outbox_notify() RETURNS trigger AS $$
BEGIN PERFORM pg_notify('outbox_events', ''); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER outbox_notify_trigger
    AFTER INSERT ON outbox
    FOR EACH ROW EXECUTE FUNCTION outbox_notify();
```

```go
relay := outboxd.New(databaseURL, handler, outboxd.Config{
    Schema: outboxd.SchemaConfig{
        Table:           "outbox",
        IDColumn:        "id",
        TopicColumn:     "topic",
        PayloadColumn:   "payload",
        CreatedAtColumn: "created_at",
    },
    Polling: &outboxd.PollingConfig{
        PollInterval:  10 * time.Second,
        BatchSize:     100,
        NotifyChannel: "outbox_events",
    },
})
```

The relay listens on the channel and wakes up immediately on new inserts. The poll interval acts as a safety net in case a notification is missed.
