package outboxd

import (
	"context"
	"errors"
	"net"
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

// TestWAL_BlackholedServerSurfacesConnectionFailure simulates a silent
// network partition: the server end drains everything the client sends (so
// standby updates "succeed") but never sends a byte back. The readLoop must
// treat prolonged server silence as a connection failure instead of an
// ordinary idle stream.
func TestWAL_BlackholedServerSurfacesConnectionFailure(t *testing.T) {
	readCtx, readStop := context.WithCancel(context.Background())
	defer readStop()

	w := &walListener{
		replConn:        replConnOver(t, fakePGPipe(t, true)),
		standbyInterval: 10 * time.Millisecond,
		batchCh:         make(chan walBatch),
		errCh:           make(chan error, 1),
		readCtx:         readCtx,
		readStop:        readStop,
		readDone:        make(chan struct{}),
		tracker:         newInFlightTracker(),
	}
	go w.readLoop()

	select {
	case err := <-w.errCh:
		if err == nil {
			t.Fatal("expected a connection-failure error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("black-holed replication stream never surfaced as an error: readLoop treats endless silence as idle")
	}

	select {
	case <-w.readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("readLoop kept running after reporting the connection dead")
	}
}

// fakeAnsweringPGPipe completes the pgconn handshake, then answers every
// standby status update that has the reply-requested flag set with a
// PrimaryKeepaliveMessage, like a healthy but idle walsender. It sends
// nothing spontaneously.
func fakeAnsweringPGPipe(t *testing.T) net.Conn {
	t.Helper()
	clientEnd, serverEnd := net.Pipe()

	go func() {
		backend := pgproto3.NewBackend(serverEnd, serverEnd)
		if _, err := backend.ReceiveStartupMessage(); err != nil {
			return
		}
		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			return
		}
		for {
			msg, err := backend.Receive()
			if err != nil {
				return
			}
			cd, ok := msg.(*pgproto3.CopyData)
			if !ok || len(cd.Data) == 0 || cd.Data[0] != pglogrepl.StandbyStatusUpdateByteID {
				continue
			}
			if cd.Data[len(cd.Data)-1] != 1 {
				continue // no reply requested
			}
			ka := make([]byte, 18) // 'k' + walEnd(8) + serverTime(8) + replyRequested(1)
			ka[0] = pglogrepl.PrimaryKeepaliveMessageByteID
			backend.Send(&pgproto3.CopyData{Data: ka})
			if err := backend.Flush(); err != nil {
				return
			}
		}
	}()

	return clientEnd
}

// TestWAL_IdleResponsiveServerIsNotDeclaredDead pins the active half of
// dead-connection detection: an idle stream must not be declared dead just
// because the server sends nothing spontaneously. The listener has to
// request replies to its standby updates, and those replies keep the
// connection alive well past the silence budget.
func TestWAL_IdleResponsiveServerIsNotDeclaredDead(t *testing.T) {
	readCtx, readStop := context.WithCancel(context.Background())
	defer readStop()

	w := &walListener{
		replConn:        replConnOver(t, fakeAnsweringPGPipe(t)),
		standbyInterval: 10 * time.Millisecond,
		batchCh:         make(chan walBatch),
		errCh:           make(chan error, 1),
		readCtx:         readCtx,
		readStop:        readStop,
		readDone:        make(chan struct{}),
		tracker:         newInFlightTracker(),
	}
	go w.readLoop()

	select {
	case err := <-w.errCh:
		t.Fatalf("idle but responsive server was declared dead: %v", err)
	case <-time.After(500 * time.Millisecond): // 50 keepalive intervals, silence budget is 3
	}
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
