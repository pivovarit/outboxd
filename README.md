# outboxd

> **Note:** This project is a work in progress and is not yet production-ready.

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

Column names and table name are configurable via `SchemaConfig`.

## First start and pre-existing rows

`outboxd` creates the replication slot on first start if it does not already exist. The slot's consistent point is fixed at creation, so rows inserted **before** the slot existed are not in its WAL stream and will not be delivered. If you need to relay a pre-existing backlog, drain it first (e.g. via polling mode) before switching to WAL mode.

## Delivery semantics

`outboxd` is at-least-once: a message can be delivered more than once if the relay crashes after the handler returns but before the WAL position is acknowledged (or, in polling mode, before the row is deleted). Consumers should treat `Message.ID` as a deduplication key.

Messages are delivered in **commit order**, not in `Message.ID` order. If transaction A inserts `id=10` and transaction B inserts `id=11`, but B commits first, the consumer sees `11` before `10`. `Message.ID` is unique and safe to dedupe on, but it is **not** a high-watermark - a downstream that assumes "if I see id=N, I've seen everything < N" will be wrong.

In polling mode this also affects latency: with `NotifyChannel` enabled, the leading committer is delivered immediately, but a late-committing transaction (`id=10` above) waits for the next `PollInterval` tick - its NOTIFY was already drained when the earlier commit fired.

## Polling mode

If logical replication is not available (e.g. managed PostgreSQL without `wal_level=logical`, restricted permissions, or shared hosting), `outboxd` can fall back to a polling-based strategy. Enable it by providing a `PollingConfig`:

```go
relay := outboxd.New(databaseURL, handler, outboxd.Config{
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
    Polling: &outboxd.PollingConfig{
        PollInterval:  10 * time.Second,
        BatchSize:     100,
        NotifyChannel: "outbox_events",
    },
})
```

The relay listens on the channel and wakes up immediately on new inserts. The poll interval acts as a safety net in case a notification is missed.
