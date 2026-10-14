// Package yta is MICROFLOW's deterministic YouTube-Shorts analytics core:
// fair age-window benchmarking, evidence-ranked diagnosis, single-variable
// experiments, closed-loop verification and guarded self-evolution.
//
// It performs NO I/O (no HTTP, no disk, no clock reads except through the
// `now` arguments), keeps no global state and uses only the standard
// library, so every rule below is unit-testable and the memory cost is
// bounded by the (small) number of videos passed in.
//
// Evidence levels ("insufficient" < "weak" < "moderate" < "strong") are
// ordinal labels derived from sample sizes, effect sizes and contradicting
// signals. They are deliberately NOT probabilities/percentages.
package yta

import (
	"math"
	"sort"
	"time"
)

// Metric names a per-video comparable quantity.
type Metric string

const (
	MViewsPerDay   Metric = "views_per_day"
	MAvgViewPct    Metric = "avg_view_pct"      // Analytics API averageViewPercentage
	MEngagedRate   Metric = "engaged_view_rate" // engagedViews / views (Analytics API)
	MLikeRate      Metric = "like_rate"         // likes / views
	MCommentRate   Metric = "comment_rate"      // comments / views
	MShareRate     Metric = "share_rate"        // shares / views (Analytics API only)
	MSubsPerKViews Metric = "subs_per_1k_views" // subscribersGained per 1000 views
	MHookRetention Metric = "hook_retention"    // audienceWatchRatio near 10% of runtime
	// MEarlyViews is views accumulated in the first EarlyDays days after
	// publication (Analytics API, one windowed query per video). Because every
	// video is measured over the SAME age window it is the fair volume metric.
	MEarlyViews Metric = "early_views"
)

// Evidence levels.
const (
	LevelInsufficient = "insufficient"
	LevelWeak         = "weak"
	LevelModerate     = "moderate"
	LevelStrong       = "strong"
)

// VideoAnalytics holds the private-analytics figures for one video. Any
// field the API did not return stays at its Has* == false state; nothing
// is ever fabricated or defaulted into a measurement.
type VideoAnalytics struct {
	Views          float64
	EngagedViews   float64
	HasEngaged     bool
	MinutesWatched float64
	AvgViewSec     float64
	AvgViewPct     float64
	HasAvgViewPct  bool
	Likes          float64
	Comments       float64
	Shares         float64
	HasShares      bool
	SubsGained     float64
	HookRetention  float64
	HasHook        bool
	// Curve is the shape of the audience-retention curve (where viewers left).
	// Transient: computed on every fetch, never persisted.
	Curve *Retention
}

// VideoMeta is the minimal per-video state MICROFLOW persists itself
// (the YouTube APIs cannot return our internal content category or the
// experiment arm a video belonged to).
type VideoMeta struct {
	Category     string `json:"c,omitempty"`
	ExperimentID string `json:"e,omitempty"`
	Arm          string `json:"a,omitempty"` // "treatment" | "baseline" | "adopted"
	Setting      string `json:"s,omitempty"`
	Variant      string `json:"v,omitempty"`
	At           string `json:"t,omitempty"`
	// Pre-publish quality context recorded by the Script Quality Gate so later
	// runs can test whether these heuristics actually predict results here.
	HookScore  int   `json:"hs,omitempty"`
	TitleScore int   `json:"ts,omitempty"`
	SceneWords []int `json:"sw,omitempty"`
}

// Video is one published Short with public (Data API) counters and, when the
// analytics scope is available, private analytics.
type Video struct {
	ID          string
	Title       string
	PublishedAt time.Time
	DurationSec int
	Views       float64
	Likes       float64
	Comments    float64
	Analytics   *VideoAnalytics
	Meta        VideoMeta
	// EarlyViews are the views in the first EarlyDays days (see MEarlyViews).
	EarlyViews float64
	HasEarly   bool
	// LikesHidden / CommentsOff are set when the Data API did not return the
	// counter at all (hidden likes, disabled comments). The zero value means
	// "available", so existing callers are unaffected. A missing counter is
	// UNAVAILABLE, never a measured zero.
	LikesHidden bool
	CommentsOff bool
}

// AgeDays is the age at `now`, never below a small positive floor so
// per-day rates stay finite.
func (v Video) AgeDays(now time.Time) float64 {
	d := now.Sub(v.PublishedAt).Hours() / 24
	if d < 0.25 {
		return 0.25
	}
	return d
}

// Value returns the metric for v and whether it is actually measurable.
func (v Video) Value(m Metric, now time.Time) (float64, bool) {
	a := v.Analytics
	switch m {
	case MViewsPerDay:
		// Lifetime public views over lifetime age: both cover the same span.
		// (Analytics views only cover the look-back window, so dividing them
		// by the full age would understate older videos.)
		views := v.Views
		if views <= 0 && a != nil && a.Views > 0 {
			views = a.Views
		}
		return views / v.AgeDays(now), true
	case MEarlyViews:
		if v.HasEarly {
			return v.EarlyViews, true
		}
	case MAvgViewPct:
		// 0 with real views is an API gap, not a measurement; so is a row with no views.
		if a != nil && a.HasAvgViewPct && a.Views > 0 && a.AvgViewPct > 0 {
			return a.AvgViewPct, true
		}
	case MEngagedRate:
		if a != nil && a.HasEngaged && a.Views > 0 {
			return a.EngagedViews / a.Views, true
		}
	case MLikeRate:
		if v.Views > 0 && !v.LikesHidden {
			return v.Likes / v.Views, true
		}
	case MCommentRate:
		if v.Views > 0 && !v.CommentsOff {
			return v.Comments / v.Views, true
		}
	case MShareRate:
		if a != nil && a.HasShares && a.Views > 0 {
			return a.Shares / a.Views, true
		}
	case MSubsPerKViews:
		if a != nil && a.Views > 0 {
			return a.SubsGained / a.Views * 1000, true
		}
	case MHookRetention:
		if a != nil && a.HasHook && a.Views > 0 && a.HookRetention > 0 {
			return a.HookRetention, true
		}
	}
	return 0, false
}

// AgeBucket groups videos of similar publication age. Comparing a 2-day-old
// Short with a 60-day-old one is unfair, so peers must share a bucket.
func AgeBucket(days float64) string {
	switch {
	case days < 1:
		return "0-1d"
	case days < 3:
		return "1-3d"
	case days < 7:
		return "3-7d"
	case days < 14:
		return "7-14d"
	case days < 30:
		return "14-30d"
	case days < 90:
		return "30-90d"
	default:
		return "90d+"
	}
}

// Median of xs (0 for empty). xs is not modified.
func Median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	c := append([]float64(nil), xs...)
	sort.Float64s(c)
	n := len(c)
	if n%2 == 1 {
		return c[n/2]
	}
	return (c[n/2-1] + c[n/2]) / 2
}

// RobustSpread is the scaled median absolute deviation, falling back to the
// mean absolute deviation when the MAD is zero (many identical values).
func RobustSpread(xs []float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	med := Median(xs)
	dev := make([]float64, len(xs))
	sum := 0.0
	for i, x := range xs {
		dev[i] = math.Abs(x - med)
		sum += dev[i]
	}
	if mad := Median(dev) * 1.4826; mad > 0 {
		return mad
	}
	return sum / float64(len(xs)) * 1.2533
}

// Comparison is the outcome of benchmarking one video on one metric.
type Comparison struct {
	Metric    Metric  `json:"metric"`
	Value     float64 `json:"value"`
	PeerMed   float64 `json:"peerMedian"`
	Z         float64 `json:"robustZ"`
	Peers     int     `json:"peers"`
	Scope     string  `json:"peerScope"` // "age+category" | "age" | "none"
	Bucket    string  `json:"ageBucket"`
	Verdict   string  `json:"verdict"` // below | typical | above | insufficient
	Available bool    `json:"available"`
}

// MinMatureAgeDays is the age before ratio metrics (retention, like rate...)
// are considered stable enough to compare.
const MinMatureAgeDays = 2.0

// peerWindow returns the comparison-window label for metric m and whether a
// video of the given age may take part in it at all.
//
//   - views_per_day: peers must be in the same publication-age bucket.
//   - early_views:   every video is measured over the same first-N-days
//     window, so all videos with that measurement are comparable.
//   - ratio metrics: age-insensitive once past MinMatureAgeDays.
func peerWindow(m Metric, v Video, now time.Time) (label string, ok bool) {
	age := v.AgeDays(now)
	switch m {
	case MViewsPerDay:
		return AgeBucket(age), true
	case MEarlyViews:
		return "first-window", v.HasEarly
	default:
		return "mature-2d+", age >= MinMatureAgeDays
	}
}

// Benchmark compares target against peers on metric. Peers share the
// target's comparison window (see peerWindow); if at least minPeers of them
// also share its content category the narrower set is used. Fewer than
// minPeers comparable videos => verdict "insufficient" (never a guess).
func Benchmark(target Video, all []Video, m Metric, now time.Time, minPeers int) Comparison {
	c := Comparison{Metric: m, Scope: "none", Verdict: "insufficient"}
	label, ok := peerWindow(m, target, now)
	c.Bucket = label
	if !ok {
		return c
	}
	tv, ok := target.Value(m, now)
	if !ok {
		return c
	}
	c.Available, c.Value = true, tv
	// Peer sets from narrowest to widest; the narrowest one with at least
	// minPeers videos is used. Length matters: a 15 s and a 55 s Short are
	// not comparable audiences.
	band := DurationBand(target.DurationSec)
	var catBand, cat, bandOnly, window []float64
	for _, p := range all {
		if p.ID == target.ID {
			continue
		}
		pl, pok := peerWindow(m, p, now)
		if !pok || pl != label {
			continue
		}
		pv, ok := p.Value(m, now)
		if !ok {
			continue
		}
		window = append(window, pv)
		sameCat := target.Meta.Category != "" && p.Meta.Category == target.Meta.Category
		sameBand := band != "" && DurationBand(p.DurationSec) == band
		if sameCat {
			cat = append(cat, pv)
		}
		if sameBand {
			bandOnly = append(bandOnly, pv)
		}
		if sameCat && sameBand {
			catBand = append(catBand, pv)
		}
	}
	peers, scope := window, "age"
	switch {
	case len(catBand) >= minPeers:
		peers, scope = catBand, "age+category+length"
	case len(cat) >= minPeers:
		peers, scope = cat, "age+category"
	case len(bandOnly) >= minPeers:
		peers, scope = bandOnly, "age+length"
	}
	c.Scope = scope
	c.Peers = len(peers)
	if len(peers) < minPeers {
		c.Scope = "none"
		return c
	}
	c.PeerMed = Median(peers)
	sp := RobustSpread(peers)
	// Floor the spread at 5% of the peer median: identical peers would
	// otherwise give a zero spread and hide an arbitrarily large deviation.
	if floor := 0.05 * math.Abs(c.PeerMed); sp < floor {
		sp = floor
	}
	if sp > 0 {
		c.Z = (tv - c.PeerMed) / sp
	}
	switch {
	case c.Z <= -1:
		c.Verdict = "below"
	case c.Z >= 1:
		c.Verdict = "above"
	default:
		c.Verdict = "typical"
	}
	return c
}
