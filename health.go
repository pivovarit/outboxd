package outboxd

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type healthServer struct {
	ready   atomic.Bool
	addr    string
	handler http.Handler
	logger  *slog.Logger

	mu     sync.Mutex
	server *http.Server
	lnAddr string
}

func newHealthServer(addr string, status func() Status, stalledAfter time.Duration, logger *slog.Logger) *healthServer {
	h := &healthServer{addr: addr, logger: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		if h.ready.Load() && !stalled(status(), stalledAfter) {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(status()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	h.handler = mux
	return h
}

func stalled(st Status, stalledAfter time.Duration) bool {
	if stalledAfter < 0 {
		return false
	}
	return st.Retrying && time.Since(st.RetryingSince) >= stalledAfter
}

func (h *healthServer) start() error {
	ln, err := net.Listen("tcp", h.addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: h.handler}
	h.mu.Lock()
	h.server = srv
	h.lnAddr = ln.Addr().String()
	h.mu.Unlock()
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			h.logger.Error("outbox: health server error", "err", err)
		}
	}()
	return nil
}

func (h *healthServer) boundAddr() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lnAddr
}

func (h *healthServer) shutdown(ctx context.Context) error {
	h.mu.Lock()
	srv := h.server
	h.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}
