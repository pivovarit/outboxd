package outboxd

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
)

type healthServer struct {
	ready  atomic.Bool
	server *http.Server
}

func newHealthServer(addr string) *healthServer {
	h := &healthServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, _ *http.Request) {
		if h.ready.Load() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	h.server = &http.Server{Addr: addr, Handler: mux}
	return h
}

func (h *healthServer) listenAndServe() error {
	return h.server.ListenAndServe()
}

func (h *healthServer) listenAndServeOn(ln net.Listener) error {
	return h.server.Serve(ln)
}

func (h *healthServer) shutdown(ctx context.Context) error {
	return h.server.Shutdown(ctx)
}
