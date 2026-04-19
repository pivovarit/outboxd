//go:build integration

package outboxd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startPG(t *testing.T) (string, func()) {
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
			WithOccurrence(2).WithStartupTimeout(30 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb", host, port.Port())
	return dsn, func() { _ = ctr.Terminate(ctx) }
}

func setupOutboxTable(t *testing.T, dsn string) {
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
		)`)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := conn.Exec(ctx, "CREATE PUBLICATION outbox_pub FOR TABLE outbox"); err != nil {
		t.Fatalf("create publication: %v", err)
	}
}

func insertOneTx(t *testing.T, dsn, payload string) int64 {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	var id int64
	err = conn.QueryRow(ctx,
		"INSERT INTO outbox (topic, payload) VALUES ($1, $2) RETURNING id",
		"t", []byte(payload)).Scan(&id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	return id
}

func slotConfirmedLSN(t *testing.T, dsn, slot string) pglogrepl.LSN {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("lsn connect: %v", err)
	}
	defer conn.Close(ctx)
	var txt string
	if err := conn.QueryRow(ctx,
		"SELECT COALESCE(confirmed_flush_lsn::text,'0/0') FROM pg_replication_slots WHERE slot_name=$1",
		slot).Scan(&txt); err != nil {
		t.Fatalf("lsn query: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(txt)
	if err != nil {
		t.Fatalf("parse lsn %q: %v", txt, err)
	}
	return lsn
}

func startListener(t *testing.T, dsn, slot string) *walListener {
	t.Helper()
	ctx := context.Background()
	cfg := Config{
		SlotName:     slot,
		Publications: []string{"outbox_pub"},
	}
	cfg.setDefaults()
	w, err := newWALListener(ctx, dsn, cfg)
	if err != nil {
		t.Fatalf("newWALListener: %v", err)
	}
	return w
}

func drainBatch(t *testing.T, w *walListener) []int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msg, remaining, err := w.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	ids := []int64{msg.ID}
	for remaining > 0 {
		msg, remaining, err = w.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		ids = append(ids, msg.ID)
	}
	return ids
}

func TestIntegration_WAL_ConfirmAdvancesLSNInOrder(t *testing.T) {
	dsn, cleanup := startPG(t)
	defer cleanup()
	setupOutboxTable(t, dsn)

	const slot = "test_slot_confirm_ordered"
	w := startListener(t, dsn, slot)
	defer w.Close(context.Background())

	idA := insertOneTx(t, dsn, "a")
	idB := insertOneTx(t, dsn, "b")

	batchA := drainBatch(t, w)
	batchB := drainBatch(t, w)
	if len(batchA) != 1 || batchA[0] != idA {
		t.Fatalf("batch A mismatch: got %v want [%d]", batchA, idA)
	}
	if len(batchB) != 1 || batchB[0] != idB {
		t.Fatalf("batch B mismatch: got %v want [%d]", batchB, idB)
	}

	ctx := context.Background()
	if err := w.Confirm(ctx, idA); err != nil {
		t.Fatalf("Confirm A: %v", err)
	}

	w.mu.Lock()
	lsnAfterA := w.tracker.ConfirmedLSN()
	w.mu.Unlock()
	if lsnAfterA == 0 {
		t.Error("expected in-memory ConfirmedLSN > 0 after confirming A")
	}

	if err := w.Confirm(ctx, idB); err != nil {
		t.Fatalf("Confirm B: %v", err)
	}

	w.mu.Lock()
	lsnAfterB := w.tracker.ConfirmedLSN()
	w.mu.Unlock()
	if lsnAfterB <= lsnAfterA {
		t.Errorf("expected LSN to advance after confirming B: before=%v after=%v",
			lsnAfterA, lsnAfterB)
	}
}

func TestIntegration_WAL_OutOfOrderConfirmDoesNotAdvance(t *testing.T) {
	dsn, cleanup := startPG(t)
	defer cleanup()
	setupOutboxTable(t, dsn)

	const slot = "test_slot_confirm_ooorder"
	w := startListener(t, dsn, slot)
	defer w.Close(context.Background())

	idA := insertOneTx(t, dsn, "a")
	idB := insertOneTx(t, dsn, "b")

	batchA := drainBatch(t, w)
	batchB := drainBatch(t, w)
	if batchA[0] != idA || batchB[0] != idB {
		t.Fatalf("batch ordering unexpected: A=%v B=%v", batchA, batchB)
	}

	baselineSlotLSN := slotConfirmedLSN(t, dsn, slot)

	// Confirm B first, leaving A still pending.
	if err := w.Confirm(context.Background(), idB); err != nil {
		t.Fatalf("Confirm B: %v", err)
	}

	w.mu.Lock()
	inMemLSN := w.tracker.ConfirmedLSN()
	w.mu.Unlock()
	if inMemLSN != 0 {
		t.Errorf("expected in-memory ConfirmedLSN=0 while A pending, got %v (BUG: advanced past undelivered)", inMemLSN)
	}

	if err := w.sendStandbyStatus(context.Background()); err != nil {
		t.Fatalf("sendStandbyStatus after Confirm(B): %v", err)
	}
	if slotLSN := slotConfirmedLSN(t, dsn, slot); slotLSN != baselineSlotLSN {
		t.Errorf("expected slot confirmed_flush_lsn to stay at baseline %v while A pending, got %v (BUG: slot advanced past undelivered)", baselineSlotLSN, slotLSN)
	}

	if err := w.Confirm(context.Background(), idA); err != nil {
		t.Fatalf("Confirm A: %v", err)
	}
	w.mu.Lock()
	after := w.tracker.ConfirmedLSN()
	w.mu.Unlock()
	if after == 0 {
		t.Error("expected LSN to advance once A was also confirmed")
	}

	if err := w.sendStandbyStatus(context.Background()); err != nil {
		t.Fatalf("sendStandbyStatus after Confirm(A): %v", err)
	}
	if slotLSN := slotConfirmedLSN(t, dsn, slot); slotLSN <= baselineSlotLSN {
		t.Errorf("expected slot confirmed_flush_lsn to advance beyond baseline %v after both confirmations, got %v", baselineSlotLSN, slotLSN)
	}
}

func TestIntegration_WAL_ConfirmUnknownIDPanics(t *testing.T) {
	dsn, cleanup := startPG(t)
	defer cleanup()
	setupOutboxTable(t, dsn)

	const slot = "test_slot_confirm_unknown"
	w := startListener(t, dsn, slot)
	defer w.Close(context.Background())

	idA := insertOneTx(t, dsn, "a")
	batchA := drainBatch(t, w)
	if batchA[0] != idA {
		t.Fatalf("unexpected batch A: %v", batchA)
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on unknown id, got no panic")
		}
	}()

	_ = w.Confirm(context.Background(), 999999)
}

func TestIntegration_WAL_ConfirmDeleteFailurePreservesState(t *testing.T) {
	dsn, cleanup := startPG(t)
	defer cleanup()
	setupOutboxTable(t, dsn)

	const slot = "test_slot_confirm_delfail"
	w := startListener(t, dsn, slot)
	defer w.Close(context.Background())

	idA := insertOneTx(t, dsn, "a")
	batchA := drainBatch(t, w)
	if batchA[0] != idA {
		t.Fatalf("unexpected batch A: %v", batchA)
	}

	_ = w.dbConn.Close(context.Background())

	err := w.Confirm(context.Background(), idA)
	if err == nil {
		t.Fatal("expected error from Confirm when DB conn is closed")
	}

	w.mu.Lock()
	still := w.tracker.tracks(idA)
	lsn := w.tracker.ConfirmedLSN()
	w.mu.Unlock()
	if !still {
		t.Error("expected id to remain tracked after DELETE failure")
	}
	if lsn != 0 {
		t.Errorf("expected ConfirmedLSN=0 after DELETE failure, got %v", lsn)
	}
}
