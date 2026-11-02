package httpx

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func do(t *testing.T, h http.Handler, method, path string, hdr map[string]string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result()
}

func body(t *testing.T, r *http.Response) []byte {
	t.Helper()
	b, _ := io.ReadAll(r.Body)
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			t.Fatalf("bad gzip: %v", err)
		}
		b, err = io.ReadAll(zr)
		if err != nil {
			t.Fatalf("bad gzip body: %v", err)
		}
	}
	return b
}

var big = strings.Repeat(`{"id":"abc","status":"success","data":[1,2,3]}`, 400)

func jsonHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = w.Write([]byte(big))
	})
}

func TestCompressesJSONAndRoundTrips(t *testing.T) {
	r := do(t, Gzip(jsonHandler()), "GET", "/api/x", map[string]string{"Accept-Encoding": "gzip, deflate"})
	if r.Header.Get("Content-Encoding") != "gzip" || r.StatusCode != 201 {
		t.Fatalf("enc=%q status=%d", r.Header.Get("Content-Encoding"), r.StatusCode)
	}
	if !strings.Contains(r.Header.Get("Vary"), "Accept-Encoding") {
		t.Fatal("missing Vary")
	}
	if got := string(body(t, r)); got != big {
		t.Fatal("body changed after gzip round trip")
	}
}

func TestNoGzipWhenNotAccepted(t *testing.T) {
	for _, ae := range []string{"", "identity", "gzip;q=0", "br"} {
		r := do(t, Gzip(jsonHandler()), "GET", "/api/x", map[string]string{"Accept-Encoding": ae})
		if r.Header.Get("Content-Encoding") != "" || string(body(t, r)) != big {
			t.Fatalf("ae=%q should be untouched", ae)
		}
	}
}

func TestSkips(t *testing.T) {
	h := Gzip(jsonHandler())
	for name, tc := range map[string]struct {
		method, path string
		hdr          map[string]string
	}{
		"range":   {"GET", "/api/x", map[string]string{"Accept-Encoding": "gzip", "Range": "bytes=0-9"}},
		"upgrade": {"GET", "/api/x", map[string]string{"Accept-Encoding": "gzip", "Upgrade": "websocket"}},
		"webhook": {"GET", "/webhook/abc", map[string]string{"Accept-Encoding": "gzip"}},
		"head":    {"HEAD", "/api/x", map[string]string{"Accept-Encoding": "gzip"}},
	} {
		if r := do(t, h, tc.method, tc.path, tc.hdr); r.Header.Get("Content-Encoding") != "" {
			t.Errorf("%s: should not be compressed", name)
		}
	}
}

func TestSmallAndBinaryAndEncodedPassThrough(t *testing.T) {
	mk := func(ct string, setLen bool, enc string, payload string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", ct)
			if setLen {
				w.Header().Set("Content-Length", "20")
			}
			if enc != "" {
				w.Header().Set("Content-Encoding", enc)
			}
			_, _ = w.Write([]byte(payload))
		})
	}
	hdr := map[string]string{"Accept-Encoding": "gzip"}
	if r := do(t, Gzip(mk("application/json", true, "", strings.Repeat("a", 20))), "GET", "/", hdr); r.Header.Get("Content-Encoding") != "" || r.Header.Get("Content-Length") != "20" {
		t.Error("small body with Content-Length must pass through unchanged")
	}
	for _, ct := range []string{"image/png", "application/zip", "application/octet-stream", "video/mp4"} {
		if r := do(t, Gzip(mk(ct, false, "", big)), "GET", "/", hdr); r.Header.Get("Content-Encoding") != "" {
			t.Errorf("%s must not be compressed", ct)
		}
	}
	r := do(t, Gzip(mk("application/json", false, "br", big)), "GET", "/", hdr)
	if r.Header.Get("Content-Encoding") != "br" {
		t.Error("existing Content-Encoding must be kept")
	}
}

func TestSniffsContentTypeWhenMissing(t *testing.T) {
	h := Gzip(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("<!DOCTYPE html><html><body>" + strings.Repeat("hello ", 500) + "</body></html>"))
	}))
	r := do(t, h, "GET", "/", map[string]string{"Accept-Encoding": "gzip"})
	if r.Header.Get("Content-Encoding") != "gzip" || !strings.HasPrefix(r.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("enc=%q ct=%q", r.Header.Get("Content-Encoding"), r.Header.Get("Content-Type"))
	}
	if !strings.Contains(string(body(t, r)), "hello hello") {
		t.Fatal("bad body")
	}
}

func TestStatusOnlyResponses(t *testing.T) {
	for _, code := range []int{204, 304, 404} {
		h := Gzip(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }))
		r := do(t, h, "GET", "/", map[string]string{"Accept-Encoding": "gzip"})
		if r.StatusCode != code || r.Header.Get("Content-Encoding") != "" {
			t.Errorf("code %d: got %d enc=%q", code, r.StatusCode, r.Header.Get("Content-Encoding"))
		}
	}
}

// SSE must stay uncompressed and every Flush must reach the client at once.
func TestEventStreamIsNotCompressedAndFlushes(t *testing.T) {
	h := Gzip(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, ok := w.(http.Flusher)
		if !ok {
			t.Error("wrapper must implement http.Flusher")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		f.Flush()
		_, _ = w.Write([]byte("data: one\n\n"))
		f.Flush()
	}))
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("Accept-Encoding", "gzip")
	tr := &http.Transport{DisableCompression: true}
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Encoding") != "" {
		t.Fatal("SSE must not be compressed")
	}
	buf := make([]byte, 64)
	n, _ := resp.Body.Read(buf)
	if string(buf[:n]) != "data: one\n\n" {
		t.Fatalf("got %q", buf[:n])
	}
}

func TestFlushWithGzipStreamsDecodableData(t *testing.T) {
	h := Gzip(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(strings.Repeat("a", 2000)))
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte(strings.Repeat("b", 2000)))
	}))
	r := do(t, h, "GET", "/", map[string]string{"Accept-Encoding": "gzip"})
	if got := string(body(t, r)); got != strings.Repeat("a", 2000)+strings.Repeat("b", 2000) {
		t.Fatal("flushed body corrupted")
	}
}

func TestRealSizeSaving(t *testing.T) {
	r := do(t, Gzip(jsonHandler()), "GET", "/", map[string]string{"Accept-Encoding": "gzip"})
	raw, _ := io.ReadAll(r.Body)
	t.Logf("original %d bytes -> gzip %d bytes", len(big), len(raw))
	if len(raw) >= len(big)/2 {
		t.Fatal("expected at least 50% saving on repetitive JSON")
	}
}
