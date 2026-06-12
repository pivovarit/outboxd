package outboxd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestWAL_ExtractCopyData(t *testing.T) {
	t.Run("copy data passes through", func(t *testing.T) {
		cd := &pgproto3.CopyData{Data: []byte{pglogrepl.XLogDataByteID}}
		got, err := extractCopyData(cd)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != cd {
			t.Fatalf("got %v, want the CopyData message back", got)
		}
	})

	t.Run("error response surfaces as PgError", func(t *testing.T) {
		got, err := extractCopyData(&pgproto3.ErrorResponse{
			Severity: "ERROR",
			Code:     "42704",
			Message:  `publication "outbox_pub" does not exist`,
		})
		if got != nil {
			t.Fatalf("expected nil CopyData, got %v", got)
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Fatalf("expected *pgconn.PgError in chain, got: %v", err)
		}
		if pgErr.Code != "42704" {
			t.Fatalf("expected code 42704, got %q", pgErr.Code)
		}
	})

	t.Run("copy done terminates stream", func(t *testing.T) {
		got, err := extractCopyData(&pgproto3.CopyDone{})
		if got != nil {
			t.Fatalf("expected nil CopyData, got %v", got)
		}
		if err == nil {
			t.Fatal("expected error when walsender ends COPY mode")
		}
	})

	t.Run("unrelated messages are skipped", func(t *testing.T) {
		got, err := extractCopyData(&pgproto3.NoticeResponse{})
		if got != nil || err != nil {
			t.Fatalf("expected (nil, nil), got (%v, %v)", got, err)
		}
	})
}

func TestWAL_RegisterPrecedesDeliveryBlocksIdleAdvance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &walListener{
		batchCh:         make(chan walBatch),
		tracker:         newInFlightTracker(),
		readCtx:         ctx,
		standbyInterval: time.Hour,
	}
	nextStandby := time.Now().Add(time.Hour)

	batch := walBatch{messages: []Message{{ID: 1}}, lsn: pglogrepl.LSN(200)}

	done := make(chan struct{})
	go func() {
		if ok, err := w.registerAndDeliver(batch, &nextStandby); err != nil || !ok {
			close(done)
			return
		}
		w.mu.Lock()
		w.tracker.AdvanceIdle(pglogrepl.LSN(210))
		w.mu.Unlock()
		close(done)
	}()

	got := <-w.batchCh
	if got.lsn != pglogrepl.LSN(200) {
		t.Fatalf("received batch lsn=%v, want 200", got.lsn)
	}

	<-done

	w.mu.Lock()
	lsn := w.tracker.ConfirmedLSN()
	w.mu.Unlock()
	if lsn != 0 {
		t.Fatalf("confirmedLSN advanced to %v past in-flight undelivered batch at LSN 200 (silent message loss)", lsn)
	}
}
