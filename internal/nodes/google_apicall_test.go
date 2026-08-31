package nodes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"microflow/internal/engine"
)

// TestGoogleAPICallClassifiesPermanentVsTransient locks in the error
// classification "Read Used Topics" (and every other Google Sheets/
// YouTube/Gmail node) relies on: a 401/403 (bad/missing credential) is
// marked Permanent so the engine's retry loop stops immediately with a
// clear configuration error, while a 429/5xx (rate limit / transient
// server error) is left retryable so RetryOnFail/MaxTries can do its
// job.
func TestGoogleAPICallClassifiesPermanentVsTransient(t *testing.T) {
	cases := []struct {
		name          string
		status        int
		wantPermanent bool
	}{
		{"unauthorized", http.StatusUnauthorized, true},
		{"forbidden", http.StatusForbidden, true},
		{"too many requests", http.StatusTooManyRequests, false},
		{"internal server error", http.StatusInternalServerError, false},
		{"bad gateway", http.StatusBadGateway, false},
		{"service unavailable", http.StatusServiceUnavailable, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"error":{"code":` + http.StatusText(tc.status) + `,"message":"denied"}}`))
			}))
			defer srv.Close()

			err := googleAPICall(context.Background(), "GET", srv.URL, "fake-token-should-never-leak", nil, nil)
			if err == nil {
				t.Fatal("expected an error for a non-2xx response")
			}
			if got := engine.IsPermanent(err); got != tc.wantPermanent {
				t.Errorf("status %d: IsPermanent = %v, want %v (err: %v)", tc.status, got, tc.wantPermanent, err)
			}
			if strings.Contains(err.Error(), "fake-token-should-never-leak") {
				t.Error("error message leaked the bearer token")
			}
		})
	}
}

// TestGoogleAPICallSuccessUnaffected verifies the classification change
// doesn't touch the existing success path at all.
func TestGoogleAPICallSuccessUnaffected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	var into struct {
		OK bool `json:"ok"`
	}
	if err := googleAPICall(context.Background(), "GET", srv.URL, "tok", nil, &into); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !into.OK {
		t.Error("expected decoded response body")
	}
}
