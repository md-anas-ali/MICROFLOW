package nodes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"microflow/internal/yta"
)

func TestParseISODuration(t *testing.T) {
	for in, want := range map[string]int{"PT45S": 45, "PT1M5S": 65, "PT3M": 180, "PT1H": 3600, "P1D": -1} {
		if got := parseISODuration(in); got != want {
			t.Errorf("%s: got %d want %d", in, got, want)
		}
	}
}

func TestScrubRemovesTokens(t *testing.T) {
	s := ytaScrub("Get https://x?access_token=ya29.SECRETVALUE&a=1 failed Bearer abc.def-ghi")
	if strings.Contains(s, "SECRETVALUE") || strings.Contains(s, "abc.def") {
		t.Fatalf("token leaked: %s", s)
	}
}

func TestClientRetriesTransientAndNotPermanent(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(401)
			return
		}
		switch {
		case strings.HasPrefix(r.URL.Path, "/flaky"):
			if atomic.AddInt32(&hits, 1) == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(429)
				return
			}
			w.Write([]byte(`{"ok":true}`))
		case strings.HasPrefix(r.URL.Path, "/denied"):
			atomic.AddInt32(&hits, 100)
			w.WriteHeader(403)
			w.Write([]byte(`{"error":{"message":"nope","errors":[{"reason":"insufficientPermissions"}]}}`))
		}
	}))
	defer srv.Close()
	c := newYTAClient("tok", "", 10)
	var out struct{ OK bool }
	if err := c.get(context.Background(), srv.URL+"/flaky", &out); err != nil || !out.OK {
		t.Fatalf("flaky call should succeed after retry: %v", err)
	}
	if c.used != 2 {
		t.Fatalf("expected 2 budgeted attempts, got %d", c.used)
	}
	atomic.StoreInt32(&hits, 0)
	err := c.get(context.Background(), srv.URL+"/denied", nil)
	ae, ok := err.(*ytaAPIError)
	if !ok || ae.Reason != "insufficientPermissions" || atomic.LoadInt32(&hits) != 100 {
		t.Fatalf("403 must be permanent (one attempt) and classified: %v hits=%d", err, hits)
	}
}

func TestClientBudgetAndCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) }))
	defer srv.Close()
	c := newYTAClient("tok", "", 1)
	if err := c.get(context.Background(), srv.URL, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.get(context.Background(), srv.URL, nil); err != errYTABudget {
		t.Fatalf("want budget error, got %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := newYTAClient("tok", "", 5).get(ctx, srv.URL, nil); err == nil {
		t.Fatal("cancelled context must abort")
	}
}

func TestFetchVideosFiltersAndAnalyticsDowngrade(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/channels"):
			w.Write([]byte(`{"items":[{"contentDetails":{"relatedPlaylists":{"uploads":"UU1"}}}]}`))
		case strings.HasSuffix(r.URL.Path, "/playlistItems"):
			w.Write([]byte(`{"items":[{"contentDetails":{"videoId":"s1"}},{"contentDetails":{"videoId":"long1"}},{"contentDetails":{"videoId":"priv1"}}]}`))
		case strings.HasSuffix(r.URL.Path, "/videos"):
			w.Write([]byte(`{"items":[
			 {"id":"s1","snippet":{"title":"a","publishedAt":"2026-10-01T10:00:00Z"},"contentDetails":{"duration":"PT42S"},"status":{"privacyStatus":"public"},"statistics":{"viewCount":"1200","likeCount":"60","commentCount":"4"}},
			 {"id":"long1","snippet":{"title":"b","publishedAt":"2026-10-01T10:00:00Z"},"contentDetails":{"duration":"PT12M"},"status":{"privacyStatus":"public"},"statistics":{"viewCount":"9"}},
			 {"id":"priv1","snippet":{"title":"c","publishedAt":"2026-10-01T10:00:00Z"},"contentDetails":{"duration":"PT30S"},"status":{"privacyStatus":"private"},"statistics":{"viewCount":"0"}}]}`))
		default: // analytics API
			w.WriteHeader(403)
			w.Write([]byte(`{"error":{"message":"scope","errors":[{"reason":"insufficientPermissions"}]}}`))
		}
	}))
	defer srv.Close()
	oldD, oldA := ytaDataAPI, ytaAnalyticsAPI
	ytaDataAPI, ytaAnalyticsAPI = srv.URL+"/youtube/v3", srv.URL+"/v2/reports"
	defer func() { ytaDataAPI, ytaAnalyticsAPI = oldD, oldA }()

	c := newYTAClient("tok", "", 50)
	cfg := ytaConfig{MaxVideos: 10, LookbackDays: 90, EarlyMax: 5, CurveMax: 2, Evo: yta.EvoConfig{EvalDays: 7}}
	var f ytaFetch
	meta := map[string]yta.VideoMeta{"s1": {Category: "space"}}
	if err := c.fetchVideos(context.Background(), cfg, meta, now, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Videos) != 1 || f.Videos[0].ID != "s1" || f.Videos[0].Views != 1200 || f.Videos[0].Meta.Category != "space" {
		t.Fatalf("only the public Short should remain: %+v", f.Videos)
	}
	c.fetchAnalytics(context.Background(), cfg, now, &f)
	if f.HaveAnalytics || f.Videos[0].Analytics != nil {
		t.Fatal("analytics must be dropped after a 403")
	}
	if len(f.Notes) == 0 || !strings.Contains(f.Notes[0], "yt-analytics.readonly") {
		t.Fatalf("expected an actionable note, got %v", f.Notes)
	}
}
