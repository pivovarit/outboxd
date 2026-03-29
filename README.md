# outboxd

> **Note:** This project is a work in progress and is not yet production-ready.

A lightweight Go library for reliable and low-latency outbox publishing - bridging the gap between naive polling and full-blown CDC solutions like Debezium.

When a service writes to the database and needs to notify other services, doing both in a single transaction is impossible (dual-write problem). The [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) solves this by writing events to an outbox table within the same transaction, then relaying them to a message broker separately.

`outboxd` handles the relay part using PostgreSQL logical replication (WAL) - listening for changes via a replication slot and delivering messages to your handler as soon as they're committed. No external infrastructure beyond PostgreSQL, no complex deployment, no JVM.

## How it works

1. Your application inserts a row into the `outbox` table inside a transaction
2. `outboxd` picks up the INSERT via a PostgreSQL logical replication slot
3. Your handler receives the message and delivers it (e.g. to RabbitMQ, Kafka, HTTP)
4. The row is deleted from the outbox table and the WAL position is acknowledged

## Usage

The only thing you need to provide is a handler - a function that takes a message and does something with it. Everything else is handled for you (retries, WAL tracking, slot management):

```go
relay := outbox.New(databaseURL, func(ctx context.Context, msg outbox.Message) error {
    // msg.Topic   - e.g. "orders"
    // msg.Payload - the raw bytes you inserted
    // return nil when done, return an error to retry
}, outbox.Config{
    SlotName:     "outbox_relay",
    Publications: []string{"outbox_pub"},
})

relay.Start(ctx)
```

That's it. No framework to learn, no interfaces to implement, no configuration files. Just a function.

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

CREATE PUBLICATION outbox_pub FOR TABLE outbox;
```

Column names and table name are configurable via `SchemaConfig`.
