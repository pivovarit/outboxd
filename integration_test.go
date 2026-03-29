//go:build integration

package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	outbox "github.com/pivovarit/outboxd"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startPostgres(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image: "postgres:16-alpine",
		Env: map[string]string{
			"POSTGRES_DB":       "testdb",
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
		},
		Cmd:          []string{"postgres", "-c", "wal_level=logical"},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(30 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("get host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("get port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb", host, port.Port())
	return dsn, func() { _ = ctr.Terminate(ctx) }
}

func setupOutbox(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		CREATE TABLE outbox (
			id         BIGSERIAL PRIMARY KEY,
			topic      TEXT NOT NULL,
			payload    BYTEA NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err = conn.Exec(ctx, "CREATE PUBLICATION outbox_pub FOR TABLE outbox")
	if err != nil {
		t.Fatalf("create publication: %v", err)
	}
}

func insertRows(t *testing.T, dsn string, n int) []int64 {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		var id int64
		err = conn.QueryRow(ctx,
			"INSERT INTO outbox (topic, payload) VALUES ($1, $2) RETURNING id",
			"test-topic",
			[]byte(fmt.Sprintf("payload-%d", i)),
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestIntegration_DeliversInOrder(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupOutbox(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu          sync.Mutex
		receivedIDs []int64
		done        = make(chan struct{})
	)
	handler := func(_ context.Context, msg outbox.Message) error {
		mu.Lock()
		receivedIDs = append(receivedIDs, msg.ID)
		n := len(receivedIDs)
		mu.Unlock()
		if n == 3 {
			close(done)
		}
		return nil
	}

	relay := outbox.New(dsn, handler, outbox.Config{
		SlotName:     "test_slot_order",
		Publications: []string{"outbox_pub"},
		RetryDelay:   10 * time.Millisecond,
	})

	relayErr := make(chan error, 1)
	go func() {
		relayErr <- relay.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)
	insertedIDs := insertRows(t, dsn, 3)

	select {
	case <-done:
		time.Sleep(200 * time.Millisecond)
		cancel()
	case <-ctx.Done():
	}

	<-relayErr

	mu.Lock()
	defer mu.Unlock()

	if len(receivedIDs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(receivedIDs))
	}
	for i, want := range insertedIDs {
		if receivedIDs[i] != want {
			t.Errorf("message[%d]: want id %d, got %d", i, want, receivedIDs[i])
		}
	}

	verifyCtx := context.Background()
	conn, err := pgx.Connect(verifyCtx, dsn)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	defer conn.Close(verifyCtx)
	var count int
	if err := conn.QueryRow(verifyCtx, "SELECT COUNT(*) FROM outbox").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 0 {
		t.Errorf("expected outbox to be empty after delivery, got %d rows", count)
	}
}

func setupCustomOutbox(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		CREATE TABLE events_outbox (
			event_id    BIGSERIAL PRIMARY KEY,
			event_type  TEXT NOT NULL,
			data        BYTEA NOT NULL,
			inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err = conn.Exec(ctx, "CREATE PUBLICATION custom_outbox_pub FOR TABLE events_outbox")
	if err != nil {
		t.Fatalf("create publication: %v", err)
	}
}

func insertCustomRows(t *testing.T, dsn string, n int) []int64 {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		var id int64
		err = conn.QueryRow(ctx,
			"INSERT INTO events_outbox (event_type, data) VALUES ($1, $2) RETURNING event_id",
			"test-topic",
			[]byte(fmt.Sprintf("payload-%d", i)),
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestIntegration_CustomSchema(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupCustomOutbox(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu          sync.Mutex
		receivedIDs []int64
		done        = make(chan struct{})
	)
	handler := func(_ context.Context, msg outbox.Message) error {
		mu.Lock()
		receivedIDs = append(receivedIDs, msg.ID)
		n := len(receivedIDs)
		mu.Unlock()
		if n == 3 {
			close(done)
		}
		return nil
	}

	relay := outbox.New(dsn, handler, outbox.Config{
		SlotName:     "test_slot_custom",
		Publications: []string{"custom_outbox_pub"},
		RetryDelay:   10 * time.Millisecond,
		Schema: outbox.SchemaConfig{
			Table:           "events_outbox",
			IDColumn:        "event_id",
			TopicColumn:     "event_type",
			PayloadColumn:   "data",
			CreatedAtColumn: "inserted_at",
		},
	})

	relayErr := make(chan error, 1)
	go func() {
		relayErr <- relay.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)
	insertedIDs := insertCustomRows(t, dsn, 3)

	select {
	case <-done:
		time.Sleep(200 * time.Millisecond)
		cancel()
	case <-ctx.Done():
	}

	<-relayErr

	mu.Lock()
	defer mu.Unlock()

	if len(receivedIDs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(receivedIDs))
	}
	for i, want := range insertedIDs {
		if receivedIDs[i] != want {
			t.Errorf("message[%d]: want id %d, got %d", i, want, receivedIDs[i])
		}
	}

	verifyCtx := context.Background()
	conn, err := pgx.Connect(verifyCtx, dsn)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	defer conn.Close(verifyCtx)
	var count int
	if err := conn.QueryRow(verifyCtx, "SELECT COUNT(*) FROM events_outbox").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 0 {
		t.Errorf("expected events_outbox to be empty after delivery, got %d rows", count)
	}
}

func insertRowsInSingleTx(t *testing.T, dsn string, n int) []int64 {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		var id int64
		err = tx.QueryRow(ctx,
			"INSERT INTO outbox (topic, payload) VALUES ($1, $2) RETURNING id",
			"test-topic",
			[]byte(fmt.Sprintf("payload-%d", i)),
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
		ids = append(ids, id)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
	return ids
}

func TestIntegration_DeliversAllMessagesFromSingleTransaction(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupOutbox(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const messageCount = 3

	var (
		mu          sync.Mutex
		receivedIDs []int64
		done        = make(chan struct{})
	)
	handler := func(_ context.Context, msg outbox.Message) error {
		mu.Lock()
		receivedIDs = append(receivedIDs, msg.ID)
		n := len(receivedIDs)
		mu.Unlock()
		if n == messageCount {
			close(done)
		}
		return nil
	}

	relay := outbox.New(dsn, handler, outbox.Config{
		SlotName:     "test_slot_single_tx",
		Publications: []string{"outbox_pub"},
		RetryDelay:   10 * time.Millisecond,
	})

	relayErr := make(chan error, 1)
	go func() {
		relayErr <- relay.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)
	insertedIDs := insertRowsInSingleTx(t, dsn, messageCount)

	select {
	case <-done:
		time.Sleep(200 * time.Millisecond)
		cancel()
	case <-ctx.Done():
	}

	<-relayErr

	mu.Lock()
	defer mu.Unlock()

	if len(receivedIDs) != messageCount {
		t.Fatalf("expected %d messages, got %d (bug: only last INSERT per transaction is delivered)", messageCount, len(receivedIDs))
	}
	for i, want := range insertedIDs {
		if receivedIDs[i] != want {
			t.Errorf("message[%d]: want id %d, got %d", i, want, receivedIDs[i])
		}
	}
}

func TestIntegration_RetriesOnHandlerError(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupOutbox(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu       sync.Mutex
		attempts int
		done     = make(chan struct{})
	)
	handler := func(_ context.Context, msg outbox.Message) error {
		mu.Lock()
		attempts++
		a := attempts
		mu.Unlock()
		if a < 3 {
			return errors.New("transient error")
		}
		close(done)
		return nil
	}

	relay := outbox.New(dsn, handler, outbox.Config{
		SlotName:     "test_slot_retry",
		Publications: []string{"outbox_pub"},
		RetryDelay:   10 * time.Millisecond,
	})

	relayErr := make(chan error, 1)
	go func() {
		relayErr <- relay.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)
	insertRows(t, dsn, 1)

	select {
	case <-done:
		time.Sleep(200 * time.Millisecond)
		cancel()
	case <-ctx.Done():
	}

	<-relayErr

	mu.Lock()
	a := attempts
	mu.Unlock()
	if a != 3 {
		t.Errorf("expected 3 handler attempts, got %d", a)
	}

	verifyCtx := context.Background()
	conn, err := pgx.Connect(verifyCtx, dsn)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	defer conn.Close(verifyCtx)
	var count int
	if err := conn.QueryRow(verifyCtx, "SELECT COUNT(*) FROM outbox").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 0 {
		t.Errorf("expected outbox empty after retry success, got %d rows", count)
	}
}
