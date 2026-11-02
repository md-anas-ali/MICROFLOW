// Package httpx holds small, dependency-free HTTP helpers.
package httpx

import (
	"compress/gzip"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// minGzipSize is the smallest response worth compressing when the handler
// announced its Content-Length up front.
const minGzipSize = 860

var gzipPool = sync.Pool{
	New: func() any {
		zw, _ := gzip.NewWriterLevel(io.Discard, 5)
		return zw
	},
}

// Gzip compresses text-like responses (HTML, JS, CSS, JSON, ...) for clients
// that send "Accept-Encoding: gzip". It never changes what the response
// means, only how many bytes travel:
//
//   - Server-Sent Events (text/event-stream), images, archives and other
//     already-compressed or binary bodies pass through untouched.
//   - Range requests, HEAD requests, WebSocket upgrades and /webhook/ routes
//     bypass it entirely.
//   - Flush is forwarded, so streaming handlers keep working.
func Gzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !acceptsGzip(r) || r.Method == http.MethodHead || r.Header.Get("Range") != "" ||
			r.Header.Get("Upgrade") != "" || strings.HasPrefix(r.URL.Path, "/webhook/") {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Add("Vary", "Accept-Encoding")
		gw := &gzipWriter{ResponseWriter: w}
		defer gw.finish()
		next.ServeHTTP(gw, r)
	})
}

func acceptsGzip(r *http.Request) bool {
	for _, part := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if !strings.EqualFold(strings.TrimSpace(name), "gzip") {
			continue
		}
		params = strings.ReplaceAll(strings.ToLower(params), " ", "")
		if params == "q=0" || params == "q=0.0" || params == "q=0.00" || params == "q=0.000" {
			return false
		}
		return true
	}
	return false
}

func compressible(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	mt = strings.ToLower(mt)
	switch {
	case mt == "text/event-stream":
		return false
	case strings.HasPrefix(mt, "text/"):
		return true
	case mt == "application/json", mt == "application/javascript", mt == "application/x-javascript",
		mt == "application/xml", mt == "application/x-ndjson", mt == "image/svg+xml":
		return true
	case strings.HasSuffix(mt, "+json"), strings.HasSuffix(mt, "+xml"):
		return true
	}
	return false
}

type gzipWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	started     bool
	gz          *gzip.Writer
}

// Unwrap lets http.ResponseController reach the real writer.
func (g *gzipWriter) Unwrap() http.ResponseWriter { return g.ResponseWriter }

func (g *gzipWriter) WriteHeader(code int) {
	if g.wroteHeader {
		return
	}
	g.wroteHeader = true
	g.status = code
}

// start decides, once, whether this response is compressed and sends the header.
func (g *gzipWriter) start(first []byte) {
	g.started = true
	if g.status == 0 {
		g.status = http.StatusOK
	}
	h := g.Header()
	ct := h.Get("Content-Type")
	if ct == "" && len(first) > 0 {
		ct = http.DetectContentType(first)
		h.Set("Content-Type", ct)
	}
	small := false
	if cl := h.Get("Content-Length"); cl != "" {
		if n, err := strconv.ParseInt(cl, 10, 64); err == nil && n < minGzipSize {
			small = true
		}
	}
	if g.status < 200 || g.status == http.StatusNoContent || g.status == http.StatusNotModified ||
		h.Get("Content-Encoding") != "" || h.Get("Content-Range") != "" || small || !compressible(ct) {
		g.ResponseWriter.WriteHeader(g.status)
		return
	}
	h.Del("Content-Length")
	h.Set("Content-Encoding", "gzip")
	g.ResponseWriter.WriteHeader(g.status)
	zw := gzipPool.Get().(*gzip.Writer)
	zw.Reset(g.ResponseWriter)
	g.gz = zw
}

func (g *gzipWriter) Write(p []byte) (int, error) {
	if !g.started {
		g.start(p)
	}
	if g.gz != nil {
		return g.gz.Write(p)
	}
	return g.ResponseWriter.Write(p)
}

// Flush keeps streaming handlers working.
func (g *gzipWriter) Flush() {
	if !g.started {
		g.start(nil)
	}
	if g.gz != nil {
		_ = g.gz.Flush()
	}
	if f, ok := g.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (g *gzipWriter) finish() {
	if !g.started {
		if g.wroteHeader {
			g.ResponseWriter.WriteHeader(g.status)
		}
		return
	}
	if g.gz != nil {
		_ = g.gz.Close()
		gzipPool.Put(g.gz)
		g.gz = nil
	}
}
