package outboxd

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"
)

func startTestHealthServer(t *testing.T) (*healthServer, string) {
	t.Helper()
	return startTestHealthServerWithStatus(t, func() Status { return Status{} })
}

func startTestHealthServerWithStatus(t *testing.T, status func() Status) (*healthServer, string) {
	t.Helper()
	return startTestHealthServerStalledAfter(t, status, 5*time.Minute)
}

func startTestHealthServerStalledAfter(t *testing.T, status func() Status, stalledAfter time.Duration) (*healthServer, string) {
	t.Helper()
	h := newHealthServer("127.0.0.1:0", status, stalledAfter, slog.Default())
	if err := h.start(); err != nil {
		t.Fatalf("start health server: %v", err)
	}
	t.Cleanup(func() { h.shutdown(context.Background()) })
	return h, "http://" + h.boundAddr()
}

func TestHealth_AlwaysReturns200(t *testing.T) {
	_, addr := startTestHealthServer(t)

	resp, err := http.Get(addr + "/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestReady_Returns503WhenNotReady(t *testing.T) {
	_, addr := startTestHealthServer(t)

	resp, err := http.Get(addr + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", resp.StatusCode)
	}
}

func TestReady_Returns200WhenReady(t *testing.T) {
	h, addr := startTestHealthServer(t)
	h.ready.Store(true)

	resp, err := http.Get(addr + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestReady_Returns503WhenStalledOnRetry(t *testing.T) {
	status := func() Status {
		return Status{Retrying: true, RetryingID: 7, RetryingSince: time.Now().Add(-10 * time.Minute)}
	}
	h, addr := startTestHealthServerStalledAfter(t, status, 5*time.Minute)
	h.ready.Store(true)

	resp, err := http.Get(addr + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 while stalled on a retrying message, got %d", resp.StatusCode)
	}
}

func TestReady_Returns200DuringBriefRetry(t *testing.T) {
	status := func() Status {
		return Status{Retrying: true, RetryingID: 7, RetryingSince: time.Now().Add(-time.Second)}
	}
	h, addr := startTestHealthServerStalledAfter(t, status, 5*time.Minute)
	h.ready.Store(true)

	resp, err := http.Get(addr + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 during a retry younger than the stall threshold, got %d", resp.StatusCode)
	}
}

func TestReady_NegativeStalledAfterDisablesStallDetection(t *testing.T) {
	status := func() Status {
		return Status{Retrying: true, RetryingID: 7, RetryingSince: time.Now().Add(-time.Hour)}
	}
	h, addr := startTestHealthServerStalledAfter(t, status, -1)
	h.ready.Store(true)

	resp, err := http.Get(addr + "/ready")
	if err != nil {
		t.Fatalf("GET /ready: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 with stall detection disabled, got %d", resp.StatusCode)
	}
}

func TestHealthServer_RestartsAfterShutdown(t *testing.T) {
	h := newHealthServer("127.0.0.1:0", func() Status { return Status{} }, 5*time.Minute, slog.Default())
	if err := h.start(); err != nil {
		t.Fatalf("first start: %v", err)
	}
	if err := h.shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if err := h.start(); err != nil {
		t.Fatalf("second start: %v", err)
	}
	t.Cleanup(func() { h.shutdown(context.Background()) })

	resp, err := http.Get("http://" + h.boundAddr() + "/health")
	if err != nil {
		t.Fatalf("GET /health after restart: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 after restart, got %d", resp.StatusCode)
	}
}

func TestStart_ReturnsErrorWhenHealthAddrBindFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	r := New("", func(context.Context, Message) error { return nil },
		Config{HealthAddr: ln.Addr().String(), RetryDelay: time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- r.Start(ctx) }()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected bind error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return on health server bind failure")
	}
}

func TestStatus_ExposesRelayProgressAsJSON(t *testing.T) {
	want := Status{
		Delivered:     3,
		Retrying:      true,
		RetryingID:    7,
		RetryAttempts: 12,
		RetryingSince: time.Now().Add(-time.Minute).Truncate(time.Second),
	}
	_, addr := startTestHealthServerWithStatus(t, func() Status { return want })

	resp, err := http.Get(addr + "/status")
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json, got %q", ct)
	}

	var got Status
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got.Delivered != want.Delivered || !got.Retrying ||
		got.RetryingID != want.RetryingID || got.RetryAttempts != want.RetryAttempts ||
		!got.RetryingSince.Equal(want.RetryingSince) {
		t.Errorf("status mismatch: got %+v, want %+v", got, want)
	}
}

func TestReady_ReflectsStateChange(t *testing.T) {
	h, addr := startTestHealthServer(t)

	h.ready.Store(true)
	resp, err := http.Get(addr + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 when ready, got %d", resp.StatusCode)
	}

	h.ready.Store(false)
	resp, err = http.Get(addr + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503 after unready, got %d", resp.StatusCode)
	}
}
