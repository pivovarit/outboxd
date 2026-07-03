package outboxd

import (
	"context"
	"encoding/json"
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
	h := newHealthServer(":0", status)
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := "http://" + ln.Addr().String()
	go h.listenAndServeOn(ln)
	t.Cleanup(func() { h.shutdown(context.Background()) })
	return h, addr
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
