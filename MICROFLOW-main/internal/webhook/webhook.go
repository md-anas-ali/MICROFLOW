// Package webhook exposes GET/POST endpoints per rule 14. Kept as
// plain net/http (stdlib) -- no separate reverse-proxy process, which
// would cost extra RAM for no benefit at this scale.
package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

// Handler is called with the parsed request; it should kick off
// engine.Run and return the response body/status to send back.
type Handler func(headers map[string]string, query map[string]string, body map[string]any) (status int, response any)

const maxBodyBytes = 2 * 1024 * 1024 // 2MB request-size limit (rule 14)

// rateLimiter is a minimal fixed-window limiter per remote IP, enough to
// blunt accidental hammering without pulling in a dependency or Redis
// (rule 14: "basic rate limit").
type rateLimiter struct {
	mu       sync.Mutex
	window   time.Duration
	limit    int
	counts   map[string]int
	resetsAt time.Time
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	return &rateLimiter{limit: limit, window: window, counts: map[string]int{}, resetsAt: time.Now().Add(window)}
}

func (r *rateLimiter) Allow(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Now().After(r.resetsAt) {
		r.counts = map[string]int{}
		r.resetsAt = time.Now().Add(r.window)
	}
	r.counts[key]++
	return r.counts[key] <= r.limit
}

type Server struct {
	mux     *http.ServeMux
	limiter *rateLimiter
}

func NewServer() *Server {
	return &Server{mux: http.NewServeMux(), limiter: newRateLimiter(60, time.Minute)}
}

func (s *Server) Handler() http.Handler { return s.mux }

// Register wires a webhook path (e.g. /webhook/<workflowId>/<path>) to h.
// authToken, if non-empty, is checked against an X-Webhook-Token header
// (simple shared-secret auth -- sufficient for a self-hosted single-user
// tool; not meant as a general auth framework).
func (s *Server) Register(path string, authToken string, h Handler) {
	s.mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.limiter.Allow(r.RemoteAddr) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		if authToken != "" && r.Header.Get("X-Webhook-Token") != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		r = r.WithContext(ctx)

		headers := map[string]string{}
		for k := range r.Header {
			headers[k] = r.Header.Get(k)
		}
		query := map[string]string{}
		for k := range r.URL.Query() {
			query[k] = r.URL.Query().Get(k)
		}

		body := map[string]any{}
		if r.Method == http.MethodPost {
			limited := io.LimitReader(r.Body, maxBodyBytes+1)
			raw, err := io.ReadAll(limited)
			if err != nil || len(raw) > maxBodyBytes {
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
				return
			}
			if len(raw) > 0 {
				_ = json.Unmarshal(raw, &body) // non-JSON bodies just yield an empty map; not an error
			}
		}

		status, resp := h(headers, query, body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(resp)
	})
}
