package yta

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

// DurationBand groups Shorts of comparable length: viewers behave very
// differently on a 15 s and a 55 s Short, so length is a benchmarking factor.
func DurationBand(sec int) string {
	switch {
	case sec <= 0:
		return ""
	case sec <= 20:
		return "<=20s"
	case sec <= 35:
		return "21-35s"
	case sec <= 50:
		return "36-50s"
	default:
		return "51s+"
	}
}

// ---- metric sanity -----------------------------------------------------------

// ValidateAnalytics returns the reasons a video's private analytics look
// implausible. Values that fail are treated as UNAVAILABLE by the caller, never
// repaired or guessed. (averageViewPercentage may legitimately exceed 100 for
// looping Shorts, so only NaN/Inf/negative values are rejected there.)
func ValidateAnalytics(a *VideoAnalytics) []string {
	if a == nil {
		return nil
	}
	var bad []string
	chk := func(name string, v float64) {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			bad = append(bad, name+" is NaN/negative")
		}
	}
	chk("views", a.Views)
	chk("minutesWatched", a.MinutesWatched)
	chk("averageViewDuration", a.AvgViewSec)
	if a.HasAvgViewPct {
		chk("averageViewPercentage", a.AvgViewPct)
	}
	chk("likes", a.Likes)
	chk("comments", a.Comments)
	if a.HasEngaged {
		chk("engagedViews", a.EngagedViews)
		if a.Views > 0 && a.EngagedViews > a.Views*1.01 {
			bad = append(bad, "engagedViews exceeds views")
		}
	}
	if a.HasHook && (a.HookRetention < 0 || a.HookRetention > 5 || math.IsNaN(a.HookRetention)) {
		bad = append(bad, "hook retention out of range")
	}
	return bad
}

// ---- content-feature analysis ---------------------------------------------------

var (
	reDigit    = regexp.MustCompile(`\d`)
	reQuestion = regexp.MustCompile(`(?i)\?|^(why|how|what|who|when|where|is|are|can|does|did)\b`)
)

// FeatureFinding is an ASSOCIATION between a title/length feature and
// performance among this channel's own videos. It is observational: it
// suggests what to test, it does not prove a cause.
type FeatureFinding struct {
	Feature   string  `json:"feature"`
	With      int     `json:"videosWith"`
	Without   int     `json:"videosWithout"`
	MedianW   float64 `json:"medianWith"`
	MedianWo  float64 `json:"medianWithout"`
	Relative  float64 `json:"relativeDifference"`
	Dominance float64 `json:"dominance"`
	Metric    Metric  `json:"metric"`
	Verdict   string  `json:"verdict"` // better | worse | no_clear_difference | insufficient
}

// FeatureInsights compares title/length features on the fair volume metric
// (first-window views when measured, otherwise views per day). A group needs
// at least minGroup videos or the verdict is "insufficient".
func FeatureInsights(videos []Video, now time.Time, minGroup int) []FeatureFinding {
	if minGroup < 4 {
		minGroup = 5
	}
	metric := MEarlyViews
	n := 0
	for _, v := range videos {
		if v.HasEarly {
			n++
		}
	}
	if n < 2*minGroup {
		metric = MViewsPerDay
	}
	type feat struct {
		name string
		has  func(Video) (bool, bool) // value, applicable
	}
	feats := []feat{
		{"title_contains_number", func(v Video) (bool, bool) { return reDigit.MatchString(v.Title), v.Title != "" }},
		{"title_is_question", func(v Video) (bool, bool) { return reQuestion.MatchString(strings.TrimSpace(v.Title)), v.Title != "" }},
		{"title_longer_than_50_chars", func(v Video) (bool, bool) { return len([]rune(v.Title)) > 50, v.Title != "" }},
		{"duration_over_40s", func(v Video) (bool, bool) { return v.DurationSec > 40, v.DurationSec > 0 }},
	}
	var out []FeatureFinding
	for _, f := range feats {
		var a, b []float64
		for _, v := range videos {
			if metric == MViewsPerDay && v.AgeDays(now) < MinMatureAgeDays {
				continue
			}
			has, ok := f.has(v)
			val, vok := v.Value(metric, now)
			if !ok || !vok {
				continue
			}
			if has {
				a = append(a, val)
			} else {
				b = append(b, val)
			}
		}
		ff := FeatureFinding{Feature: f.name, With: len(a), Without: len(b), Metric: metric, Verdict: "insufficient"}
		if len(a) >= minGroup && len(b) >= minGroup {
			ff.MedianW, ff.MedianWo = Median(a), Median(b)
			if ff.MedianWo != 0 {
				ff.Relative = (ff.MedianW - ff.MedianWo) / math.Abs(ff.MedianWo)
			}
			ff.Dominance = Dominance(a, b)
			switch {
			case ff.Relative >= 0.15 && ff.Dominance >= 0.65:
				ff.Verdict = "better"
			case ff.Relative <= -0.15 && ff.Dominance <= 0.35:
				ff.Verdict = "worse"
			default:
				ff.Verdict = "no_clear_difference"
			}
		}
		out = append(out, ff)
	}
	return out
}

// ---- topic ranking & content fatigue ----------------------------------------------

// TopicScore ranks one content category for the NEXT Short.
type TopicScore struct {
	Category    string  `json:"category"`
	Videos      int     `json:"videos"`
	RelMedian   float64 `json:"medianRelativeToChannel"` // 1.0 = channel median
	Trend       string  `json:"trend"`                   // rising | steady | declining | insufficient
	RecentShare float64 `json:"shareOfLastTenVideos"`
	Fatigued    bool    `json:"fatigued"`
	Evidence    string  `json:"evidenceLevel"`
	Rank        int     `json:"rank"`
	Note        string  `json:"note"`
}

// RankTopics ranks the categories MICROFLOW has recorded for its own videos.
// It can only rank categories it has published in; it does not invent new
// topics (topic discovery stays with the workflow's research nodes). Fatigue
// = a category that dominates recent output while its newer videos fall
// clearly below its own earlier ones.
func RankTopics(videos []Video, now time.Time) []TopicScore {
	metric := MEarlyViews
	have := 0
	for _, v := range videos {
		if v.HasEarly {
			have++
		}
	}
	if have < 8 {
		metric = MViewsPerDay
	}
	sorted := append([]Video(nil), videos...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].PublishedAt.After(sorted[j].PublishedAt) })
	lastTen := map[string]int{}
	for i, v := range sorted {
		if i >= 10 {
			break
		}
		if v.Meta.Category != "" {
			lastTen[v.Meta.Category]++
		}
	}
	by := map[string][]Video{} // newest first
	var all []float64
	for _, v := range sorted {
		if v.Meta.Category == "" || v.AgeDays(now) < MinMatureAgeDays {
			continue
		}
		if _, ok := v.Value(metric, now); !ok {
			continue
		}
		by[v.Meta.Category] = append(by[v.Meta.Category], v)
		val, _ := v.Value(metric, now)
		all = append(all, val)
	}
	overall := Median(all)
	var out []TopicScore
	for cat, vs := range by {
		ts := TopicScore{Category: cat, Videos: len(vs), Trend: "insufficient", Evidence: LevelInsufficient, RecentShare: float64(lastTen[cat]) / 10}
		var vals []float64
		for _, v := range vs {
			x, _ := v.Value(metric, now)
			vals = append(vals, x)
		}
		if len(vs) >= 3 && overall > 0 {
			ts.RelMedian = Median(vals) / overall
			ts.Evidence = LevelWeak
			if len(vs) >= 6 {
				ts.Evidence = LevelModerate
			}
		}
		if len(vs) >= 6 {
			half := len(vals) / 2
			recent, older := vals[:half], vals[half:]
			rel := 0.0
			if m := Median(older); m != 0 {
				rel = (Median(recent) - m) / math.Abs(m)
			}
			dom := Dominance(recent, older)
			switch {
			case rel <= -0.2 && dom <= 0.35:
				ts.Trend = "declining"
			case rel >= 0.2 && dom >= 0.65:
				ts.Trend = "rising"
			default:
				ts.Trend = "steady"
			}
		}
		ts.Fatigued = ts.Trend == "declining" && ts.RecentShare >= 0.3
		switch {
		case ts.Evidence == LevelInsufficient:
			ts.Note = "fewer than 3 mature videos: not ranked on performance"
		case ts.Fatigued:
			ts.Note = "dominates recent output while its newer videos underperform its own earlier ones: rest this category"
		case ts.Trend == "declining":
			ts.Note = "newer videos are weaker than earlier ones, but the category is not dominating recent output"
		}
		out = append(out, ts)
	}
	score := func(t TopicScore) float64 {
		if t.Evidence == LevelInsufficient {
			return -1 // unproven categories sort last, never first
		}
		s := t.RelMedian
		if t.Fatigued {
			s -= 0.5
		}
		if t.Trend == "rising" {
			s += 0.1
		}
		return s
	}
	sort.Slice(out, func(i, j int) bool {
		if si, sj := score(out[i]), score(out[j]); si != sj {
			return si > sj
		}
		return out[i].Category < out[j].Category
	})
	for i := range out {
		out[i].Rank = i + 1
	}
	return out
}

// ---- regression detection ------------------------------------------------------------

// Regression flags a recent drop versus the channel's own older history.
type Regression struct {
	Metric    Metric  `json:"metric"`
	Recent    float64 `json:"recentMedian"`
	History   float64 `json:"historyMedian"`
	Relative  float64 `json:"relativeChange"`
	Dominance float64 `json:"dominance"`
	NRecent   int     `json:"recentVideos"`
	NHistory  int     `json:"historyVideos"`
}

// DetectRegression compares the newest `recentN` mature videos with all older
// ones. It reports only a clear, consistent drop (>=15% and pairwise
// dominance <=0.3) and needs minHistory older videos, else nothing is claimed.
func DetectRegression(videos []Video, now time.Time, recentN, minHistory int) []Regression {
	if recentN < 3 {
		recentN = 5
	}
	if minHistory < 5 {
		minHistory = 8
	}
	sorted := append([]Video(nil), videos...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].PublishedAt.After(sorted[j].PublishedAt) })
	var mature []Video
	for _, v := range sorted {
		if v.AgeDays(now) >= MinMatureAgeDays {
			mature = append(mature, v)
		}
	}
	if len(mature) < recentN+minHistory {
		return nil
	}
	recent, hist := mature[:recentN], mature[recentN:]
	var out []Regression
	for _, m := range []Metric{MEarlyViews, MAvgViewPct, MLikeRate} {
		var r, h []float64
		for _, v := range recent {
			if x, ok := v.Value(m, now); ok {
				r = append(r, x)
			}
		}
		for _, v := range hist {
			if x, ok := v.Value(m, now); ok {
				h = append(h, x)
			}
		}
		if len(r) < 3 || len(h) < minHistory {
			continue
		}
		hm := Median(h)
		if hm == 0 {
			continue
		}
		rel := (Median(r) - hm) / math.Abs(hm)
		dom := Dominance(r, h)
		if rel <= -0.15 && dom <= 0.30 {
			out = append(out, Regression{Metric: m, Recent: Median(r), History: hm, Relative: rel, Dominance: dom, NRecent: len(r), NHistory: len(h)})
		}
	}
	return out
}

func (r Regression) String() string {
	return fmt.Sprintf("%s fell %.0f%% (recent median %.3g vs history %.3g, %d vs %d videos)", r.Metric, -r.Relative*100, r.Recent, r.History, r.NRecent, r.NHistory)
}
