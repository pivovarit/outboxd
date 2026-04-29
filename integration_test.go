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

var defaultSchema = outboxd.SchemaConfig{
	Table:           "outbox",
	IDColumn:        "id",
	TopicColumn:     "topic",
	PayloadColumn:   "payload",
	CreatedAtColumn: "created_at",
}

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
		Schema:       defaultSchema,
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
		Schema:       defaultSchema,
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
		Schema:     defaultSchema,
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
		Schema:     defaultSchema,
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

func readReplyTime(t *testing.T, dsn, slot string) (time.Time, bool) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("reply_time connect: %v", err)
	}
	defer conn.Close(ctx)
	var rt *time.Time
	err = conn.QueryRow(ctx, `
		SELECT r.reply_time
		FROM pg_replication_slots s
		JOIN pg_stat_replication r ON r.pid = s.active_pid
		WHERE s.slot_name = $1
	`, slot).Scan(&rt)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false
	}
	if err != nil {
		t.Fatalf("reply_time query: %v", err)
	}
	if rt == nil {
		return time.Time{}, false
	}
	return *rt, true
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
		Schema:       defaultSchema,
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

func TestIntegration_WAL_StandbyHeartbeatDuringSlowHandler(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupOutbox(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const slot = "test_slot_standby_heartbeat"
	const keepalive = 300 * time.Millisecond

	var delivered atomic.Int32
	entered := make(chan int32, 8)
	release := make(chan struct{})
	handler := func(hctx context.Context, _ outboxd.Message) error {
		n := delivered.Add(1)
		entered <- n
		if n == 1 {
			return nil
		}
		select {
		case <-release:
		case <-hctx.Done():
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		SlotName:          slot,
		Publications:      []string{"outbox_pub"},
		RetryDelay:        10 * time.Millisecond,
		KeepaliveInterval: keepalive,
		Schema:            defaultSchema,
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	shutdown := func() {
		select {
		case <-release:
		default:
			close(release)
		}
		cancel()
		<-relayErr
	}

	time.Sleep(500 * time.Millisecond)

	insertRows(t, dsn, 1)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		shutdown()
		t.Fatal("row 1 handler never invoked")
	}

	deadline := time.Now().Add(10 * time.Second)
	var lsn1 pglogrepl.LSN
	for time.Now().Before(deadline) {
		lsn1 = readConfirmedFlushLSN(t, dsn, slot)
		if lsn1 != 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lsn1 == 0 {
		shutdown()
		t.Fatal("confirmed_flush_lsn did not advance after row 1")
	}

	insertRows(t, dsn, 1)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		shutdown()
		t.Fatal("row 2 handler never invoked")
	}

	var t0 time.Time
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rt, ok := readReplyTime(t, dsn, slot); ok {
			t0 = rt
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if t0.IsZero() {
		shutdown()
		t.Fatal("reply_time unavailable while row 2 handler blocking")
	}

	time.Sleep(keepalive * 4)

	t1, ok := readReplyTime(t, dsn, slot)
	if !ok {
		shutdown()
		t.Fatal("reply_time disappeared during slow handler")
	}
	if !t1.After(t0) {
		shutdown()
		t.Fatalf("reply_time did not advance during slow handler (ticker not firing): t0=%s t1=%s", t0, t1)
	}

	shutdown()
}

func TestIntegration_WAL_HeartbeatDuringBackpressuredConsumer(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupOutbox(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const slot = "test_slot_heartbeat_backpressured_consumer"
	const keepalive = 300 * time.Millisecond

	var delivered atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	handler := func(hctx context.Context, _ outboxd.Message) error {
		if delivered.Add(1) == 1 {
			entered <- struct{}{}
			select {
			case <-release:
			case <-hctx.Done():
			}
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		SlotName:          slot,
		Publications:      []string{"outbox_pub"},
		RetryDelay:        10 * time.Millisecond,
		KeepaliveInterval: keepalive,
		Schema:            defaultSchema,
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	shutdown := func() {
		select {
		case <-release:
		default:
			close(release)
		}
		cancel()
		<-relayErr
	}

	time.Sleep(500 * time.Millisecond)

	insertRows(t, dsn, 1)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		shutdown()
		t.Fatal("first handler invocation never happened")
	}

	insertRows(t, dsn, 1)
	time.Sleep(500 * time.Millisecond)

	insertRows(t, dsn, 1)

	var t0 time.Time
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rt, ok := readReplyTime(t, dsn, slot); ok {
			t0 = rt
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if t0.IsZero() {
		shutdown()
		t.Fatal("reply_time unavailable while consumer backpressured")
	}

	time.Sleep(keepalive * 4)

	t1, ok := readReplyTime(t, dsn, slot)
	if !ok {
		shutdown()
		t.Fatal("reply_time disappeared while consumer backpressured")
	}
	if !t1.After(t0) {
		shutdown()
		t.Fatalf("reply_time did not advance with backpressured consumer (heartbeat starved): t0=%s t1=%s", t0, t1)
	}

	shutdown()
}

func TestIntegration_Polling_ConfirmBetweenFetchesWithBatchSize1(t *testing.T) {
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
		Schema:            defaultSchema,
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

func TestIntegration_Polling_RowsDeletedAtEndOfBuffer(t *testing.T) {
	dsn, cleanup := startPostgresWithoutWAL(t)
	defer cleanup()
	setupOutboxPolling(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rowExists := func(id int64) bool {
		t.Helper()
		conn, err := pgx.Connect(context.Background(), dsn)
		if err != nil {
			t.Fatalf("exists connect: %v", err)
		}
		defer conn.Close(context.Background())
		var exists bool
		if err := conn.QueryRow(context.Background(),
			"SELECT EXISTS(SELECT 1 FROM outbox WHERE id = $1)", id).Scan(&exists); err != nil {
			t.Fatalf("exists query: %v", err)
		}
		return exists
	}
	countRows := func() int {
		t.Helper()
		conn, err := pgx.Connect(context.Background(), dsn)
		if err != nil {
			t.Fatalf("count connect: %v", err)
		}
		defer conn.Close(context.Background())
		var n int
		if err := conn.QueryRow(context.Background(), "SELECT COUNT(*) FROM outbox").Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	var (
		mu       sync.Mutex
		received []int64
		existed  []bool
	)
	done := make(chan struct{})
	handler := func(_ context.Context, msg outboxd.Message) error {
		e := rowExists(msg.ID)
		mu.Lock()
		received = append(received, msg.ID)
		existed = append(existed, e)
		n := len(received)
		mu.Unlock()
		time.Sleep(200 * time.Millisecond)
		if n == 5 {
			close(done)
		}
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		RetryDelay:        10 * time.Millisecond,
		KeepaliveInterval: time.Hour,
		Schema:            defaultSchema,
		Polling: &outboxd.PollingConfig{
			PollInterval: 50 * time.Millisecond,
			BatchSize:    3,
		},
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)
	ids := insertRowsInSingleTx(t, dsn, 5)

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		cancel()
		<-relayErr
		mu.Lock()
		t.Fatalf("timed out waiting for 5 deliveries, got %d", len(received))
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if countRows() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	<-relayErr

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 5 {
		t.Fatalf("expected 5 deliveries, got %d", len(received))
	}
	for i, want := range ids {
		if received[i] != want {
			t.Errorf("delivery[%d]: want id %d, got %d", i, want, received[i])
		}
		if !existed[i] {
			t.Errorf("delivery[%d]: row id %d was already deleted when handler entered (premature confirm)", i, want)
		}
	}
	if c := countRows(); c != 0 {
		t.Errorf("expected outbox empty after all deliveries, got %d", c)
	}
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
		KeepaliveInterval: time.Hour,
		Schema:            defaultSchema,
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

func slotExists(t *testing.T, dsn, slot string) bool {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatalf("slot check connect: %v", err)
	}
	defer conn.Close(context.Background())
	var exists bool
	if err := conn.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		slot).Scan(&exists); err != nil {
		t.Fatalf("slot check query: %v", err)
	}
	return exists
}

func TestIntegration_DropSlot(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupOutbox(t, dsn)

	const slot = "test_slot_drop"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	delivered := make(chan struct{}, 1)
	relay := outboxd.New(dsn, func(_ context.Context, _ outboxd.Message) error {
		select {
		case delivered <- struct{}{}:
		default:
		}
		return nil
	}, outboxd.Config{
		SlotName:     slot,
		Publications: []string{"outbox_pub"},
		RetryDelay:   10 * time.Millisecond,
		Schema:       defaultSchema,
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

	if !slotExists(t, dsn, slot) {
		t.Fatal("slot should exist before DropSlot")
	}

	if err := outboxd.DropSlot(context.Background(), dsn, slot); err != nil {
		t.Fatalf("DropSlot: %v", err)
	}

	if slotExists(t, dsn, slot) {
		t.Fatal("slot should not exist after DropSlot")
	}
}

func TestIntegration_DropSlot_NonExistent(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	err := outboxd.DropSlot(context.Background(), dsn, "no_such_slot")
	if err == nil {
		t.Fatal("expected error when dropping non-existent slot")
	}
}

func setupOutboxWithExtras(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		CREATE TABLE outbox (
			id            BIGSERIAL PRIMARY KEY,
			topic         TEXT NOT NULL,
			payload       BYTEA NOT NULL,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
			aggregate_id  TEXT NOT NULL,
			partition_key TEXT NOT NULL
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

func insertRowsWithExtras(t *testing.T, dsn string, n int) []int64 {
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
			"INSERT INTO outbox (topic, payload, aggregate_id, partition_key) VALUES ($1, $2, $3, $4) RETURNING id",
			"test-topic",
			[]byte(fmt.Sprintf("payload-%d", i)),
			fmt.Sprintf("agg-%d", i),
			fmt.Sprintf("pk-%d", i),
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func setupOutboxPollingWithExtras(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		CREATE TABLE outbox (
			id            BIGSERIAL PRIMARY KEY,
			topic         TEXT NOT NULL,
			payload       BYTEA NOT NULL,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
			aggregate_id  TEXT NOT NULL,
			partition_key TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func TestIntegration_WAL_ExtraColumns(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupOutboxWithExtras(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu       sync.Mutex
		received []outboxd.Message
		done     = make(chan struct{})
	)
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		received = append(received, msg)
		if len(received) == 3 {
			close(done)
		}
		mu.Unlock()
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		SlotName:     "test_slot_extras",
		Publications: []string{"outbox_pub"},
		RetryDelay:   10 * time.Millisecond,
		Schema: outboxd.SchemaConfig{
			Table:           "outbox",
			IDColumn:        "id",
			TopicColumn:     "topic",
			PayloadColumn:   "payload",
			CreatedAtColumn: "created_at",
			ExtraColumns:    []string{"aggregate_id", "partition_key"},
		},
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)
	insertRowsWithExtras(t, dsn, 3)

	select {
	case <-done:
		time.Sleep(200 * time.Millisecond)
		cancel()
	case <-ctx.Done():
	}
	<-relayErr

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(received))
	}
	for i, msg := range received {
		if msg.Extras == nil {
			t.Fatalf("message[%d]: Extras is nil", i)
		}
		wantAgg := fmt.Sprintf("agg-%d", i)
		wantPK := fmt.Sprintf("pk-%d", i)
		if got, _ := msg.Extras["aggregate_id"].(string); got != wantAgg {
			t.Errorf("message[%d]: aggregate_id = %q, want %q", i, got, wantAgg)
		}
		if got, _ := msg.Extras["partition_key"].(string); got != wantPK {
			t.Errorf("message[%d]: partition_key = %q, want %q", i, got, wantPK)
		}
	}
}

func TestIntegration_Polling_ExtraColumns(t *testing.T) {
	dsn, cleanup := startPostgresWithoutWAL(t)
	defer cleanup()
	setupOutboxPollingWithExtras(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu       sync.Mutex
		received []outboxd.Message
		done     = make(chan struct{})
	)
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		received = append(received, msg)
		if len(received) == 3 {
			close(done)
		}
		mu.Unlock()
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		RetryDelay: 10 * time.Millisecond,
		Polling:    &outboxd.PollingConfig{PollInterval: 100 * time.Millisecond},
		Schema: outboxd.SchemaConfig{
			Table:           "outbox",
			IDColumn:        "id",
			TopicColumn:     "topic",
			PayloadColumn:   "payload",
			CreatedAtColumn: "created_at",
			ExtraColumns:    []string{"aggregate_id", "partition_key"},
		},
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)
	insertRowsWithExtras(t, dsn, 3)

	select {
	case <-done:
		time.Sleep(200 * time.Millisecond)
		cancel()
	case <-ctx.Done():
	}
	<-relayErr

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(received))
	}
	for i, msg := range received {
		if msg.Extras == nil {
			t.Fatalf("message[%d]: Extras is nil", i)
		}
		wantAgg := fmt.Sprintf("agg-%d", i)
		wantPK := fmt.Sprintf("pk-%d", i)
		if got, _ := msg.Extras["aggregate_id"].(string); got != wantAgg {
			t.Errorf("message[%d]: aggregate_id = %q, want %q", i, got, wantAgg)
		}
		if got, _ := msg.Extras["partition_key"].(string); got != wantPK {
			t.Errorf("message[%d]: partition_key = %q, want %q", i, got, wantPK)
		}
	}
}

func setupMinimalOutbox(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		CREATE TABLE outbox (
			id      BIGSERIAL PRIMARY KEY,
			payload BYTEA NOT NULL
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

func insertMinimalRows(t *testing.T, dsn string, n int) []int64 {
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
			"INSERT INTO outbox (payload) VALUES ($1) RETURNING id",
			[]byte(fmt.Sprintf("payload-%d", i)),
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func setupMinimalOutboxPolling(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	_, err = conn.Exec(ctx, `
		CREATE TABLE outbox (
			id      BIGSERIAL PRIMARY KEY,
			payload BYTEA NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func TestIntegration_WAL_DisabledColumns(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	setupMinimalOutbox(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu       sync.Mutex
		received []outboxd.Message
		done     = make(chan struct{})
	)
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		received = append(received, msg)
		if len(received) == 3 {
			close(done)
		}
		mu.Unlock()
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		SlotName:     "test_slot_disabled_cols",
		Publications: []string{"outbox_pub"},
		RetryDelay:   10 * time.Millisecond,
		Schema: outboxd.SchemaConfig{
			Table:           "outbox",
			IDColumn:        "id",
			PayloadColumn:   "payload",
			TopicColumn:     "-",
			CreatedAtColumn: "-",
		},
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)
	insertedIDs := insertMinimalRows(t, dsn, 3)

	select {
	case <-done:
		time.Sleep(200 * time.Millisecond)
		cancel()
	case <-ctx.Done():
	}
	<-relayErr

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(received))
	}
	for i, msg := range received {
		if msg.ID != insertedIDs[i] {
			t.Errorf("message[%d]: id = %d, want %d", i, msg.ID, insertedIDs[i])
		}
		if msg.Topic != "" {
			t.Errorf("message[%d]: topic = %q, want empty", i, msg.Topic)
		}
		if !msg.CreatedAt.IsZero() {
			t.Errorf("message[%d]: created_at should be zero, got %v", i, msg.CreatedAt)
		}
		wantPayload := fmt.Sprintf("payload-%d", i)
		if string(msg.Payload) != wantPayload {
			t.Errorf("message[%d]: payload = %q, want %q", i, string(msg.Payload), wantPayload)
		}
	}
}

func TestIntegration_Polling_DisabledColumns(t *testing.T) {
	dsn, cleanup := startPostgresWithoutWAL(t)
	defer cleanup()
	setupMinimalOutboxPolling(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu       sync.Mutex
		received []outboxd.Message
		done     = make(chan struct{})
	)
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		received = append(received, msg)
		if len(received) == 3 {
			close(done)
		}
		mu.Unlock()
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		RetryDelay: 10 * time.Millisecond,
		Polling:    &outboxd.PollingConfig{PollInterval: 100 * time.Millisecond},
		Schema: outboxd.SchemaConfig{
			Table:           "outbox",
			IDColumn:        "id",
			PayloadColumn:   "payload",
			TopicColumn:     "-",
			CreatedAtColumn: "-",
		},
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)
	insertedIDs := insertMinimalRows(t, dsn, 3)

	select {
	case <-done:
		time.Sleep(200 * time.Millisecond)
		cancel()
	case <-ctx.Done():
	}
	<-relayErr

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(received))
	}
	for i, msg := range received {
		if msg.ID != insertedIDs[i] {
			t.Errorf("message[%d]: id = %d, want %d", i, msg.ID, insertedIDs[i])
		}
		if msg.Topic != "" {
			t.Errorf("message[%d]: topic = %q, want empty", i, msg.Topic)
		}
		if !msg.CreatedAt.IsZero() {
			t.Errorf("message[%d]: created_at should be zero, got %v", i, msg.CreatedAt)
		}
		wantPayload := fmt.Sprintf("payload-%d", i)
		if string(msg.Payload) != wantPayload {
			t.Errorf("message[%d]: payload = %q, want %q", i, string(msg.Payload), wantPayload)
		}
	}
}

func setupOutboxTextPayload(t *testing.T, dsn string) {
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
			payload    TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func insertTextPayloadRows(t *testing.T, dsn string, n int) []int64 {
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
			fmt.Sprintf(`{"index":%d}`, i),
		).Scan(&id)
		if err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return ids
}

func setupOutboxJSONBPayload(t *testing.T, dsn string) {
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
			payload    JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func TestIntegration_Polling_TextPayload(t *testing.T) {
	dsn, cleanup := startPostgresWithoutWAL(t)
	defer cleanup()
	setupOutboxTextPayload(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu       sync.Mutex
		received []outboxd.Message
		done     = make(chan struct{})
	)
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		received = append(received, msg)
		if len(received) == 3 {
			close(done)
		}
		mu.Unlock()
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		RetryDelay: 10 * time.Millisecond,
		Schema:     defaultSchema,
		Polling:    &outboxd.PollingConfig{PollInterval: 100 * time.Millisecond},
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)
	insertTextPayloadRows(t, dsn, 3)

	select {
	case <-done:
		time.Sleep(200 * time.Millisecond)
		cancel()
	case <-ctx.Done():
	}
	<-relayErr

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(received))
	}
	for i, msg := range received {
		want := fmt.Sprintf(`{"index":%d}`, i)
		if string(msg.Payload) != want {
			t.Errorf("message[%d]: payload = %q, want %q", i, string(msg.Payload), want)
		}
	}
}

func TestIntegration_Polling_JSONBPayload(t *testing.T) {
	dsn, cleanup := startPostgresWithoutWAL(t)
	defer cleanup()
	setupOutboxJSONBPayload(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		mu       sync.Mutex
		received []outboxd.Message
		done     = make(chan struct{})
	)
	handler := func(_ context.Context, msg outboxd.Message) error {
		mu.Lock()
		received = append(received, msg)
		if len(received) == 3 {
			close(done)
		}
		mu.Unlock()
		return nil
	}

	relay := outboxd.New(dsn, handler, outboxd.Config{
		RetryDelay: 10 * time.Millisecond,
		Schema:     defaultSchema,
		Polling:    &outboxd.PollingConfig{PollInterval: 100 * time.Millisecond},
	})

	relayErr := make(chan error, 1)
	go func() { relayErr <- relay.Start(ctx) }()

	time.Sleep(500 * time.Millisecond)
	insertTextPayloadRows(t, dsn, 3)

	select {
	case <-done:
		time.Sleep(200 * time.Millisecond)
		cancel()
	case <-ctx.Done():
	}
	<-relayErr

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(received))
	}
	for i, msg := range received {
		if len(msg.Payload) == 0 {
			t.Errorf("message[%d]: payload is empty", i)
		}
	}
}
