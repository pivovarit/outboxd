package outboxd

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func fakePGPipe(t *testing.T, drainAfterHandshake bool) net.Conn {
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
		if drainAfterHandshake {
			_, _ = io.Copy(io.Discard, serverEnd)
		}
	}()

	return clientEnd
}

func fakeReplConn(t *testing.T) *pgconn.PgConn {
	t.Helper()
	clientEnd := fakePGPipe(t, false)

	cfg, err := pgconn.ParseConfig("postgres://user@fake:5432/db?sslmode=disable")
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) {
		return []string{host}, nil
	}
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return clientEnd, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect repl conn: %v", err)
	}
	return conn
}

func fakeDBConn(t *testing.T) *pgx.Conn {
	t.Helper()
	clientEnd := fakePGPipe(t, true)

	cfg, err := pgx.ParseConfig("postgres://user@fake:5432/db?sslmode=disable")
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) {
		return []string{host}, nil
	}
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		return clientEnd, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect db conn: %v", err)
	}
	return conn
}

func TestWAL_CloseWithReadLoopStuckInStandbySend(t *testing.T) {
	readCtx, readStop := context.WithCancel(context.Background())
	w := &walListener{
		replConn:        fakeReplConn(t),
		dbConn:          fakeDBConn(t),
		standbyInterval: 5 * time.Millisecond,
		closeTimeout:    100 * time.Millisecond,
		batchCh:         make(chan walBatch),
		errCh:           make(chan error, 1),
		readCtx:         readCtx,
		readStop:        readStop,
		readDone:        make(chan struct{}),
		tracker:         newInFlightTracker(),
	}
	go w.readLoop()

	time.Sleep(300 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		w.Close(context.Background())
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return: blocked writing to a dead connection")
	}

	select {
	case <-w.readDone:
	default:
		t.Fatal("Close returned while readLoop was still running: they race on replConn")
	}
}
