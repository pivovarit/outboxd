package outboxd

import (
	"context"
	"net"
	"net/http"
	"testing"
)

func startTestHealthServer(t *testing.T) (*healthServer, string) {
	t.Helper()
	h := newHealthServer(":0")
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
