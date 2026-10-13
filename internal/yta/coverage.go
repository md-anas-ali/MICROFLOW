package yta

import (
	"fmt"
	"sort"
	"time"
)

// Coverage explains how many of the fetched videos were analysed and why the
// rest were used only as comparison history (or excluded). It exists so the
// report never silently shows "33 fetched, 8 analysed" without a reason.
type Coverage struct {
	Fetched      int      `json:"fetched"`
	Targets      int      `json:"analysedAsTargets"`
	History      int      `json:"usedAsComparisonHistory"`
	TooRecent    int      `json:"tooRecentToJudge"`
	Categorised  int      `json:"withRecordedCategory"`
	WithAnalytic int      `json:"withPrivateAnalytics"`
	WithEarly    int      `json:"withFirstWindowViews"`
	Available    []Avail  `json:"metricAvailability"`
	Explanation  string   `json:"explanation"`
	Warnings     []string `json:"warnings,omitempty"`
}

// Avail is how many of the analysed (target) videos had a usable value for a metric.
type Avail struct {
	Metric    Metric `json:"metric"`
	Usable    int    `json:"usable"`
	OutOf     int    `json:"outOf"`
	MissingBy string `json:"missingBecause,omitempty"`
}

// BandScore compares Shorts length bands on views per day (mature videos).
type BandScore struct {
	Band      string  `json:"lengthBand"`
	Videos    int     `json:"videos"`
	RelMedian float64 `json:"medianRelativeToChannel"`
}

// Standout is a best/worst own video on the fair volume metric.
type Standout struct {
	ID     string  `json:"id"`
	Title  string  `json:"title"`
	Rel    float64 `json:"relativeToMedian"`
	Metric Metric  `json:"metric"`
	Best   bool    `json:"best"`
}

var availReasons = map[Metric]string{
	MAvgViewPct:    "Analytics API returned no row, zero views in the look-back window, or 0 percentage",
	MHookRetention: "retention curve not fetched (only the newest MICROFLOW_YTA_RETENTION_CURVES videos) or curve had no point near 10% of runtime",
	MEngagedRate:   "engagedViews not accepted/returned by the Analytics API",
	MShareRate:     "shares not returned by the Analytics API",
	MEarlyViews:    "video younger than the evaluation window, or beyond MICROFLOW_YTA_EARLY_VIDEOS",
	MLikeRate:      "likes hidden or zero public views",
	MCommentRate:   "comments disabled or zero public views",
}

func buildCoverage(videos, targets []Video, now time.Time) Coverage {
	c := Coverage{Fetched: len(videos), Targets: len(targets)}
	isT := map[string]bool{}
	for _, t := range targets {
		isT[t.ID] = true
	}
	for _, v := range videos {
		if v.Meta.Category != "" {
			c.Categorised++
		}
		if v.Analytics != nil {
			c.WithAnalytic++
		}
		if v.HasEarly {
			c.WithEarly++
		}
		switch {
		case isT[v.ID]:
		case v.AgeDays(now) < MinMatureAgeDays:
			c.TooRecent++
		default:
			c.History++
		}
	}
	for _, m := range []Metric{MEarlyViews, MViewsPerDay, MAvgViewPct, MHookRetention, MEngagedRate, MLikeRate, MCommentRate, MShareRate, MSubsPerKViews} {
		a := Avail{Metric: m, OutOf: len(targets)}
		for _, t := range targets {
			if _, ok := t.Value(m, now); ok {
				a.Usable++
			}
		}
		if a.Usable < a.OutOf {
			a.MissingBy = availReasons[m]
		}
		c.Available = append(c.Available, a)
	}
	c.Explanation = fmt.Sprintf("%d Shorts were fetched. Only the newest %d mature ones (>= %.0f days old) are the analysed TARGETS; the other %d older ones are not re-judged, they are the comparison history the targets are benchmarked against (a video is never compared with itself or with other recent videos). %d are too recent to judge fairly.",
		c.Fetched, c.Targets, MinMatureAgeDays, c.History, c.TooRecent)
	if c.Fetched > 0 && c.Categorised*2 < c.Fetched {
		c.Warnings = append(c.Warnings, fmt.Sprintf("only %d of %d videos have a recorded topic category (recorded at publish time), so topic analysis covers a minority of the channel", c.Categorised, c.Fetched))
	}
	if c.Fetched > 0 && c.WithAnalytic == 0 {
		c.Warnings = append(c.Warnings, "no private analytics matched any video: retention, hook and engagement analysis is unavailable (public counters only)")
	}
	for _, a := range c.Available {
		if a.OutOf > 0 && a.Usable == 0 && a.Metric != MEngagedRate && a.Metric != MShareRate {
			c.Warnings = append(c.Warnings, fmt.Sprintf("%s unavailable for all analysed videos (%s)", a.Metric, a.MissingBy))
		}
	}
	return c
}

// lengthBands scores length bands on views/day for videos old enough to judge.
func lengthBands(videos []Video, now time.Time) []BandScore {
	by := map[string][]float64{}
	var all []float64
	for _, v := range videos {
		if v.AgeDays(now) < MinMatureAgeDays || DurationBand(v.DurationSec) == "" {
			continue
		}
		x, _ := v.Value(MViewsPerDay, now)
		by[DurationBand(v.DurationSec)] = append(by[DurationBand(v.DurationSec)], x)
		all = append(all, x)
	}
	overall := Median(all)
	var out []BandScore
	if overall <= 0 {
		return out
	}
	for b, xs := range by {
		if len(xs) >= 3 {
			out = append(out, BandScore{Band: b, Videos: len(xs), RelMedian: Median(xs) / overall})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RelMedian > out[j].RelMedian })
	return out
}

// standouts returns up to n best and n worst videos on the fair volume
// metric (first-window views when measured, else age-bucketed views/day among
// videos of similar age is approximated by views/day for mature videos).
func standouts(videos []Video, now time.Time, n int) []Standout {
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
	type kv struct {
		v Video
		x float64
	}
	var xs []kv
	var vals []float64
	for _, v := range videos {
		if metric == MViewsPerDay && v.AgeDays(now) < MinMatureAgeDays {
			continue
		}
		if x, ok := v.Value(metric, now); ok {
			xs = append(xs, kv{v, x})
			vals = append(vals, x)
		}
	}
	med := Median(vals)
	if len(xs) < 2*n || med <= 0 {
		return nil
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i].x > xs[j].x })
	var out []Standout
	for i := 0; i < n; i++ {
		out = append(out, Standout{ID: xs[i].v.ID, Title: xs[i].v.Title, Rel: xs[i].x / med, Metric: metric, Best: true})
	}
	for i := len(xs) - n; i < len(xs); i++ {
		out = append(out, Standout{ID: xs[i].v.ID, Title: xs[i].v.Title, Rel: xs[i].x / med, Metric: metric})
	}
	return out
}
