// Package standby serves a minimal "waiting for the database lock" HTTP
// responder while a freshly started MicroFlow process waits for the
// single-instance lock.
//
// Why: on a rolling deploy (Render) the NEW instance must report healthy
// before the platform stops the OLD one, but the old one holds the
// single-instance advisory lock until it stops. Without a listener the new
// instance can never become healthy, so the lock is never released and the
// deploy fails. The standby listener breaks that cycle WITHOUT letting two
// servers work at once: it touches no database, runs no scheduler, executes
// nothing and serves no application routes.
//
// Responses: GET/HEAD on "/", "/healthz", "/health" -> 200 (liveness only);
// everything else -> 503 + Retry-After, so webhook senders retry later
// instead of hitting a server that cannot process them yet.
package standby

import (
	"context"
	"log"
	"net"
	"net/http"
	"time"
)

// Server is a running standby listener. A nil *Server is valid.
type Server struct {
	srv  *http.Server
	done chan struct{}
}

func handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		switch {
		case (r.Method == http.MethodGet || r.Method == http.MethodHead) &&
			(r.URL.Path == "/" || r.URL.Path == "/healthz" || r.URL.Path == "/health"):
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte("MicroFlow standby: waiting for the previous instance to release the database lock\n"))
			}
		default:
			w.Header().Set("Retry-After", "5")
			http.Error(w, "MicroFlow is starting (standby); retry shortly", http.StatusServiceUnavailable)
		}
	})
}

// Start binds addr and serves the standby responder. It returns nil (after
// logging) if the address cannot be bound; callers carry on without it.
func Start(addr string) *Server {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("standby: cannot listen on %s (%v); waiting for the lock without a health endpoint", addr, err)
		return nil
	}
	return serve(ln)
}

func serve(ln net.Listener) *Server {
	s := &Server{
		srv:  &http.Server{Handler: handler(), ReadHeaderTimeout: 10 * time.Second},
		done: make(chan struct{}),
	}
	go func() {
		defer close(s.done)
		_ = s.srv.Serve(ln)
	}()
	log.Printf("standby: listening on %s (health only; no executions are processed until the instance lock is held)", ln.Addr())
	return s
}

// Close stops the listener and waits until the port is free again, so the
// real server can bind the same address immediately afterwards.
func (s *Server) Close() {
	if s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.srv.Shutdown(ctx); err != nil {
		_ = s.srv.Close()
	}
	<-s.done
}
