// Package outboxd is a transactional outbox relay for PostgreSQL.
//
// It reads messages written to an outbox table and
// delivers them to an application-defined [Handler] at-least-once, in commit
// order. Two delivery backends are supported: WAL-based logical replication
// (default) and polling with LISTEN/NOTIFY.
//
// A minimal relay requires only a DSN, a handler, and a [Config]:
//
//	relay := outboxd.New(dsn, handler, outboxd.Config{
//	    SlotName:     "my_slot",
//	    Publications: []string{"my_pub"},
//	})
//	err := relay.Start(ctx)
//
// See the Config type for schema mapping, retry behaviour, polling mode, and
// middleware options.
package outboxd
