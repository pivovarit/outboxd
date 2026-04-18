//go:build integration

package outboxd_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/pivovarit/outboxd"
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
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		receivedIDs = append(receivedIDs, msg.ID)
		n := len(receivedIDs)
		mu.Unlock()
		if n == 3 {
			close(done)
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
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
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		receivedIDs = append(receivedIDs, msg.ID)
		n := len(receivedIDs)
		mu.Unlock()
		if n == 3 {
			close(done)
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		SlotName:     "test_slot_custom",
		Publications: []string{"custom_outbox_pub"},
		RetryDelay:   10 * time.Millisecond,
		Schema: outboxd.SchemaConfig{
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
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		receivedIDs = append(receivedIDs, msg.ID)
		n := len(receivedIDs)
		mu.Unlock()
		if n == messageCount {
			close(done)
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
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

func startPostgresWithoutWAL(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image: "postgres:16-alpine",
		Env: map[string]string{
			"POSTGRES_DB":       "testdb",
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
		},
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

func setupOutboxPolling(t *testing.T, dsn string) {
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
}

func TestIntegration_Polling_DeliversInOrder(t *testing.T) {
	dsn, cleanup := startPostgresWithoutWAL(t)
	defer cleanup()
	setupOutboxPolling(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu          sync.Mutex
		receivedIDs []int64
		done        = make(chan struct{})
	)
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		receivedIDs = append(receivedIDs, msg.ID)
		n := len(receivedIDs)
		mu.Unlock()
		if n == 3 {
			close(done)
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		RetryDelay: 10 * time.Millisecond,
		Polling:    &outboxd.PollingConfig{PollInterval: 100 * time.Millisecond},
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

func setupOutboxWithNotify(t *testing.T, dsn string) {
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
		);
		CREATE OR REPLACE FUNCTION outbox_notify() RETURNS trigger AS $$
		BEGIN PERFORM pg_notify('outbox_events', ''); RETURN NEW; END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER outbox_after_insert AFTER INSERT ON outbox
		FOR EACH ROW EXECUTE FUNCTION outbox_notify();
	`)
	if err != nil {
		t.Fatalf("create table and trigger: %v", err)
	}
}

func TestIntegration_Polling_PgNotify(t *testing.T) {
	dsn, cleanup := startPostgresWithoutWAL(t)
	defer cleanup()
	setupOutboxWithNotify(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu          sync.Mutex
		receivedIDs []int64
		done        = make(chan struct{})
	)
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		receivedIDs = append(receivedIDs, msg.ID)
		n := len(receivedIDs)
		mu.Unlock()
		if n == 3 {
			close(done)
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		RetryDelay: 10 * time.Millisecond,
		Polling: &outboxd.PollingConfig{
			PollInterval:  10 * time.Second,
			NotifyChannel: "outbox_events",
		},
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
		t.Fatal("timed out: pg_notify did not wake the poller (poll interval is 10s, test timeout is 30s)")
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
}

func readConfirmedFlushLSN(t *testing.T, dsn, slot string) pglogrepl.LSN {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("lsn connect: %v", err)
	}
	defer conn.Close(ctx)
	var lsnText string
	err = conn.QueryRow(ctx,
		"SELECT COALESCE(confirmed_flush_lsn::text, '0/0') FROM pg_replication_slots WHERE slot_name = $1",
		slot).Scan(&lsnText)
	if err != nil {
		t.Fatalf("lsn query: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(lsnText)
	if err != nil {
		t.Fatalf("parse lsn %q: %v", lsnText, err)
	}
	return lsn
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
	handler := func(_ context.Context, msg outboxd.Message) error {
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

	relay := outboxd.New(dsn, handler, outboxd.Config{
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

func TestIntegration_WAL_StandbyAdvancesDuringSlowHandler(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupOutbox(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const slot = "test_slot_slow_handler"

	var delivered atomic.Int32
	handler := func(hctx context.Context, _ outboxd.Message) error {
		delivered.Add(1)
		select {
		case <-time.After(3 * time.Second):
		case <-hctx.Done():
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		SlotName:          slot,
		Publications:      []string{"outbox_pub"},
		RetryDelay:        10 * time.Millisecond,
		KeepaliveInterval: 500 * time.Millisecond,
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)
	insertRows(t, dsn, 3)

	// Wait until confirmed_flush_lsn advances past 0, which happens only
	// after the relay confirms the first batch
	deadline := time.Now().Add(15 * time.Second)
	var lsn pglogrepl.LSN
	for time.Now().Before(deadline) {
		if delivered.Load() == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		lsn = readConfirmedFlushLSN(t, dsn, slot)
		if lsn != 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if delivered.Load() == 0 {
		t.Fatal("handler never started")
	}
	if lsn == 0 {
		t.Fatalf("confirmed_flush_lsn did not advance within deadline; still 0/0 after %d deliveries", delivered.Load())
	}

	cancel()
	<-relayErr
}

func TestIntegration_Polling_RowsDeletedBeforeBufferEmpties(t *testing.T) {
	dsn, cleanup := startPostgresWithoutWAL(t)
	defer cleanup()
	setupOutboxPolling(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var delivered atomic.Int32
	handler := func(hctx context.Context, _ outboxd.Message) error {
		delivered.Add(1)
		select {
		case <-time.After(2 * time.Second):
		case <-hctx.Done():
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		RetryDelay:        10 * time.Millisecond,
		KeepaliveInterval: 200 * time.Millisecond,
		Polling: &outboxd.PollingConfig{
			PollInterval: 100 * time.Millisecond,
			BatchSize:    1,
		},
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)
	insertRows(t, dsn, 3)

	deadline := time.Now().Add(6 * time.Second)
	for delivered.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if delivered.Load() < 2 {
		t.Fatalf("expected at least 2 deliveries started, got %d", delivered.Load())
	}

	time.Sleep(200 * time.Millisecond)

	verifyConn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	defer verifyConn.Close(context.Background())
	var count int
	if err := verifyConn.QueryRow(context.Background(), "SELECT COUNT(*) FROM outbox").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count >= 3 {
		t.Errorf("expected at least one row deleted mid-batch, still have %d", count)
	}

	cancel()
	<-relayErr
}

func TestIntegration_FlushOnContextCancel(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupOutbox(t, dsn)

	ctx, cancel := context.WithCancel(context.Background())

	delivered := make(chan struct{}, 1)
	handler := func(_ context.Context, _ outboxd.Message) error {
		select {
		case delivered <- struct{}{}:
		default:
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		SlotName:          "test_slot_flush_cancel",
		Publications:      []string{"outbox_pub"},
		RetryDelay:        10 * time.Millisecond,
		KeepaliveInterval: time.Hour, // prevent ticker from doing the work
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)
	insertRows(t, dsn, 1)

	select {
	case <-delivered:
	case <-time.After(10 * time.Second):
		cancel()
		<-relayErr
		t.Fatal("handler never invoked")
	}

	cancel()
	<-relayErr

	verifyConn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	defer verifyConn.Close(context.Background())
	var count int
	if err := verifyConn.QueryRow(context.Background(), "SELECT COUNT(*) FROM outbox").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected outbox empty after flush-on-cancel, got %d", count)
	}
}
