package yta

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func mkVids(n int, now time.Time) []Video {
	var vs []Video
	for i := 0; i < n; i++ {
		vs = append(vs, Video{ID: string(rune('a' + i)), Title: "t", PublishedAt: now.AddDate(0, 0, -3-i*2), DurationSec: 30, Views: float64(100 + i), Likes: 5, Comments: 1})
	}
	return vs
}

func TestRecommendNeverNilAndJSONIsArray(t *testing.T) {
	recs := Recommend(Summary{})
	if recs == nil {
		t.Fatal("Recommend returned nil")
	}
	b, _ := json.Marshal(recs)
	if string(b) != "[]" {
		t.Fatalf("want [] got %s", b)
	}
	if AdvisoryText(recs) == "" {
		t.Fatal("advisory text empty")
	}
}

func TestCoverageExplainsTargetsVsHistory(t *testing.T) {
	now := time.Now().UTC()
	s := Diagnose(mkVids(33, now), now, Config{})
	c := s.Coverage
	if c.Fetched != 33 || c.Targets != 8 || c.History != 25 {
		t.Fatalf("coverage %+v", c)
	}
	if !strings.Contains(c.Explanation, "comparison history") {
		t.Fatal("missing explanation")
	}
	if recs := Recommend(s); len(recs) == 0 {
		t.Fatal("expected a fallback recommendation")
	}
}

func TestZeroOrMissingMetricsAreUnavailable(t *testing.T) {
	now := time.Now().UTC()
	v := Video{ID: "x", PublishedAt: now.AddDate(0, 0, -5), Views: 100, LikesHidden: true, CommentsOff: true,
		Analytics: &VideoAnalytics{Views: 0, HasAvgViewPct: true, AvgViewPct: 0, HasHook: true, HookRetention: 0}}
	for _, m := range []Metric{MLikeRate, MCommentRate, MAvgViewPct, MHookRetention} {
		if _, ok := v.Value(m, now); ok {
			t.Errorf("%s should be unavailable", m)
		}
	}
}

func TestTargetsShrinkToKeepPeers(t *testing.T) {
	now := time.Now().UTC()
	s := Diagnose(mkVids(10, now), now, Config{TargetVideos: 12})
	if s.Coverage.History < 5 {
		t.Fatalf("history starved: %+v", s.Coverage)
	}
}
