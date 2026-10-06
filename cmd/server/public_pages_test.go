package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testGate() *authGate {
	return &authGate{
		enabled: true,
		user:    "u",
		pass:    "p",
		key:     []byte("test-key"),
		ttl:     time.Hour,
		fails:   map[string]*failRecord{},
	}
}

func TestPublicPagesAreOpenAndRestStaysGated(t *testing.T) {
	g := testGate()
	h := g.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("DASHBOARD"))
	}))

	get := func(path string, withSession bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if withSession {
			req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: g.sign(time.Now().Add(time.Hour).Unix())})
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	for _, path := range []string{"/", "/privacy", "/terms"} {
		rec := get(path, false)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s signed out: got %d, want 200", path, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "DASHBOARD") {
			t.Fatalf("%s must not serve the dashboard to a signed-out visitor", path)
		}
	}
	if body := get("/", false).Body.String(); !strings.Contains(body, `href="/privacy"`) || !strings.Contains(body, `href="/terms"`) {
		t.Fatal("homepage must link to /privacy and /terms")
	}

	// Signed-in users still get the dashboard at "/".
	if body := get("/", true).Body.String(); body != "DASHBOARD" {
		t.Fatalf("signed-in / should reach the dashboard, got %q", body)
	}

	// Everything else (including the OAuth callback) is still gated exactly as before.
	if rec := get("/api/oauth/google/callback", false); rec.Code != http.StatusUnauthorized {
		t.Fatalf("oauth callback signed out: got %d, want 401", rec.Code)
	}
	if rec := get("/app.js", false); rec.Code != http.StatusFound {
		t.Fatalf("/app.js signed out: got %d, want 302 to /login", rec.Code)
	}
}

func TestLoginDisabledStillServesDashboardAtRoot(t *testing.T) {
	g := &authGate{enabled: false, fails: map[string]*failRecord{}}
	h := g.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("DASHBOARD"))
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Body.String() != "DASHBOARD" {
		t.Fatalf("login disabled: / should be the dashboard, got %q", rec.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/privacy", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Privacy Policy") {
		t.Fatalf("login disabled: /privacy should still be served, got %d", rec.Code)
	}
}
