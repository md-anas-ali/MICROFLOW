package yta

import (
	"fmt"
	"sort"
	"time"
)

// Cause identifiers (stable strings used in reports and experiment mapping).
const (
	CauseWeakHook         = "weak_hook"
	CauseMidVideoDrop     = "mid_video_pacing"
	CauseLowDistribution  = "low_distribution_or_packaging"
	CauseLowConversion    = "low_engagement_conversion"
	CauseTopicMismatch    = "topic_category_mismatch"
	CauseContentFatigue   = "content_fatigue"
	CauseImmatureData     = "data_not_mature_enough"
	CauseNoComparableData = "no_comparable_data"
)

// Signal summarises one metric across the recent target videos.
type Signal struct {
	Metric  Metric  `json:"metric"`
	Targets int     `json:"comparedVideos"` // targets with a usable benchmark
	Below   int     `json:"below"`
	Above   int     `json:"above"`
	MedianZ float64 `json:"medianRobustZ"`
	Peers   int     `json:"typicalPeerCount"`
}

func (s Signal) usable() bool { return s.Targets >= 3 }

// lowish: most compared videos are below peers and the typical z is clearly negative.
func (s Signal) low() bool {
	return s.usable() && s.Below*2 >= s.Targets && s.MedianZ <= -1
}

// fine: metric is NOT a problem (contradicts a "this is the cause" claim).
func (s Signal) fine() bool {
	return s.usable() && s.MedianZ > -0.5 && s.Below*2 < s.Targets
}

// Diagnosis is one candidate cause with its evidence ledger.
type Diagnosis struct {
	Cause         string   `json:"cause"`
	Level         string   `json:"evidenceLevel"`
	Supporting    []string `json:"supportingEvidence"`
	Contradicting []string `json:"contradictingEvidence"`
	Missing       []string `json:"missingData"`
	Setting       string   `json:"experimentSetting,omitempty"` // evolvable setting that addresses it, if any
	score         int
}

// Config tunes the diagnosis; zero values fall back to defaults.
type Config struct {
	MinPeers     int
	TargetVideos int // how many of the newest eligible videos are examined
}

func (c Config) minPeers() int {
	if c.MinPeers < 3 {
		return 5
	}
	return c.MinPeers
}
func (c Config) targets() int {
	if c.TargetVideos < 3 {
		return 8
	}
	return c.TargetVideos
}

// Summary is the aggregate benchmark picture plus ranked diagnoses.
type Summary struct {
	Examined  int         `json:"examinedVideos"`
	Signals   []Signal    `json:"signals"`
	Diagnoses []Diagnosis `json:"diagnoses"`
	Notes     []string    `json:"notes,omitempty"`

	Regressions []Regression     `json:"regressions,omitempty"`
	Features    []FeatureFinding `json:"contentFeatures,omitempty"`
	Topics      []TopicScore     `json:"topicRanking,omitempty"`

	Retention *RetentionPattern `json:"retentionPattern,omitempty"`

	Coverage  Coverage    `json:"coverage"`
	Bands     []BandScore `json:"lengthBands,omitempty"`
	Standouts []Standout  `json:"standouts,omitempty"`
}

var diagMetrics = []Metric{MEarlyViews, MViewsPerDay, MAvgViewPct, MHookRetention, MEngagedRate, MLikeRate, MCommentRate, MShareRate, MSubsPerKViews}

// Diagnose benchmarks the newest mature videos against the channel's own
// history and ranks possible causes of weak performance. It only reports a
// cause as supported when comparable evidence exists; otherwise the level
// stays "insufficient" and the missing data is named.
func Diagnose(videos []Video, now time.Time, cfg Config) Summary {
	var sum Summary
	// Context analyses that do not depend on having 3 mature targets.
	sum.Regressions = DetectRegression(videos, now, 5, 8)
	sum.Features = FeatureInsights(videos, now, 5)
	sum.Topics = RankTopics(videos, now)
	sum.Retention = AggregateRetention(videos)
	var targets []Video
	sorted := append([]Video(nil), videos...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].PublishedAt.After(sorted[j].PublishedAt) })
	for _, v := range sorted {
		if v.AgeDays(now) >= MinMatureAgeDays {
			targets = append(targets, v)
		}
		if len(targets) >= cfg.targets() {
			break
		}
	}
	// Keep enough older videos to benchmark against: shrink the target set
	// rather than starve the peer pool (insufficient peers => no verdicts).
	if hist := len(videos) - len(targets); hist < cfg.minPeers() && len(targets) > 3 {
		keep := len(videos) - cfg.minPeers()
		if keep < 3 {
			keep = 3
		}
		if keep < len(targets) {
			targets = targets[:keep]
		}
	}
	sum.Examined = len(targets)
	sum.Coverage = buildCoverage(videos, targets, now)
	sum.Bands = lengthBands(videos, now)
	sum.Standouts = standouts(videos, now, 3)
	if len(targets) < 3 {
		sum.Notes = append(sum.Notes, fmt.Sprintf("only %d video(s) older than %.0f days: not enough history to benchmark", len(targets), MinMatureAgeDays))
		sum.Diagnoses = []Diagnosis{{Cause: CauseImmatureData, Level: LevelInsufficient,
			Missing: []string{"at least 3 recent videos old enough to compare, plus peers"}}}
		return sum
	}

	// Recent targets are compared with OLDER history only. If the pool
	// contained the other recent videos, a channel-wide recent drop would make
	// itself look "typical" (the peers would be equally bad).
	isTarget := map[string]bool{}
	for _, t := range targets {
		isTarget[t.ID] = true
	}
	history := make([]Video, 0, len(videos))
	for _, v := range videos {
		if !isTarget[v.ID] {
			history = append(history, v)
		}
	}

	sig := map[Metric]Signal{}
	for _, m := range diagMetrics {
		var zs []float64
		s := Signal{Metric: m}
		for _, t := range targets {
			c := Benchmark(t, history, m, now, cfg.minPeers())
			if c.Verdict == "insufficient" {
				continue
			}
			s.Targets++
			zs = append(zs, c.Z)
			s.Peers += c.Peers
			switch c.Verdict {
			case "below":
				s.Below++
			case "above":
				s.Above++
			}
		}
		if s.Targets > 0 {
			s.MedianZ = Median(zs)
			s.Peers /= s.Targets
			sum.Signals = append(sum.Signals, s)
		}
		sig[m] = s
	}

	// Prefer the same-age-window volume metric; fall back to views/day.
	vol := sig[MEarlyViews]
	volName := "first-window views"
	if !vol.usable() {
		vol, volName = sig[MViewsPerDay], "views per day (age-bucketed)"
	}
	ret, hook, eng := sig[MAvgViewPct], sig[MHookRetention], sig[MEngagedRate]

	var out []Diagnosis
	describe := func(label string, s Signal) string {
		return fmt.Sprintf("%s: %d of %d compared videos below peers (median robust z %.2f)", label, s.Below, s.Targets, s.MedianZ)
	}

	// 1. weak hook
	d := Diagnosis{Cause: CauseWeakHook, Setting: SettingHookStyle}
	if hook.low() {
		d.Supporting = append(d.Supporting, describe("hook retention", hook))
		d.score += 2
	}
	if ret.low() {
		d.Supporting = append(d.Supporting, describe("average view percentage", ret))
		d.score++
	}
	// The retention-curve shape is a second, independent look at the opening: it
	// reads where the audience actually left, not only a single point at 10%.
	if rp := sum.Retention; rp != nil && rp.Videos >= 4 {
		if rp.MedianHookLeak >= 35 && float64(rp.LeakyHooks)/float64(rp.Videos) >= 0.5 {
			d.Supporting = append(d.Supporting, fmt.Sprintf("retention curves: the median video loses %.0f points of its audience by 10%% of the runtime (%d of %d curves lose over 35)", rp.MedianHookLeak, rp.LeakyHooks, rp.Videos))
			d.score++
		} else if rp.LeakyHooks == 0 {
			d.Contradicting = append(d.Contradicting, fmt.Sprintf("retention curves show no heavy opening loss (median %.0f points by 10%% of the runtime)", rp.MedianHookLeak))
		}
	}
	if hook.fine() {
		d.Contradicting = append(d.Contradicting, "hook retention is in line with peers")
		d.score -= 2
	}
	if !hook.usable() {
		d.Missing = append(d.Missing, "per-video retention curve (hook retention) not available")
	}
	if !ret.usable() {
		d.Missing = append(d.Missing, "average view percentage not available (needs the YouTube Analytics scope)")
	}
	out = append(out, d)

	// 2. mid-video pacing: overall retention low but the opening holds
	d = Diagnosis{Cause: CauseMidVideoDrop, Setting: SettingPacing}
	if ret.low() && hook.fine() {
		d.Supporting = append(d.Supporting, describe("average view percentage", ret), "opening retention is normal, so viewers leave later in the video")
		d.score += 3
	} else if ret.low() && !hook.usable() {
		d.Supporting = append(d.Supporting, describe("average view percentage", ret))
		d.score++
		d.Missing = append(d.Missing, "retention curve needed to tell a weak opening from a mid-video drop")
	}
	if ret.fine() {
		d.Contradicting = append(d.Contradicting, "average view percentage is in line with peers")
		d.score -= 2
	}
	if !ret.usable() {
		d.Missing = append(d.Missing, "average view percentage not available")
	}
	out = append(out, d)

	// 3. distribution / packaging: people who see it are fine, few see it
	d = Diagnosis{Cause: CauseLowDistribution, Setting: SettingTitleStyle}
	if vol.low() {
		d.Supporting = append(d.Supporting, describe(volName, vol))
		d.score += 2
		if ret.fine() {
			d.Supporting = append(d.Supporting, "watch-through is normal, so the problem is more likely reach than content")
			d.score++
		}
	}
	if vol.fine() {
		d.Contradicting = append(d.Contradicting, volName+" is in line with peers")
		d.score -= 2
	}
	if vol.low() && ret.low() {
		d.Contradicting = append(d.Contradicting, "retention is also low, so low views may be a consequence of weak watch-through rather than packaging")
		d.score--
	}
	d.Missing = append(d.Missing, "impressions and click-through rate are not exposed by the YouTube APIs, so packaging cannot be isolated directly")
	out = append(out, d)

	// 4. engagement conversion
	d = Diagnosis{Cause: CauseLowConversion}
	conv := 0
	for _, m := range []Metric{MLikeRate, MCommentRate, MShareRate, MSubsPerKViews} {
		if s := sig[m]; s.low() {
			d.Supporting = append(d.Supporting, describe(string(m), s))
			conv++
		}
	}
	d.score += conv
	if conv > 0 && (ret.fine() || eng.fine()) {
		d.Supporting = append(d.Supporting, "viewers watch normally but interact less than peers")
		d.score++
	}
	if conv > 0 && ret.low() {
		d.Contradicting = append(d.Contradicting, "low interaction may simply follow low watch-through")
		d.score--
	}
	if conv == 0 && (sig[MLikeRate].usable()) {
		d.Contradicting = append(d.Contradicting, "like rate is in line with peers")
		d.score -= 2
	}
	out = append(out, d)

	// 5. topic/category mismatch
	if d := topicDiagnosis(videos, now); d != nil {
		out = append(out, *d)
	}

	// 6. content fatigue (recommend-only: no automatic setting exists for it)
	fd := Diagnosis{Cause: CauseContentFatigue}
	for _, t := range sum.Topics {
		if t.Fatigued {
			fd.Supporting = append(fd.Supporting, fmt.Sprintf("category %q: %.0f%% of the last 10 videos, trend %s, newer videos weaker than its earlier ones", t.Category, t.RecentShare*100, t.Trend))
			fd.score += 2
			if t.Evidence == LevelModerate {
				fd.score++
			}
		}
	}
	if len(fd.Supporting) > 0 {
		out = append(out, fd)
	}

	for i := range out {
		out[i].Level = level(out[i])
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].score > out[j].score })
	sum.Diagnoses = out
	return sum
}

// level maps the evidence ledger to an ordinal label.
func level(d Diagnosis) string {
	switch {
	case len(d.Supporting) == 0 || d.score <= 0:
		return LevelInsufficient
	case d.score == 1 || d.score == 2:
		return LevelWeak
	case d.score >= 4 && len(d.Contradicting) == 0:
		return LevelStrong
	default:
		return LevelModerate
	}
}

func topicDiagnosis(videos []Video, now time.Time) *Diagnosis {
	by := map[string][]float64{}
	var all []float64
	for _, v := range videos {
		if v.Meta.Category == "" {
			continue
		}
		val, ok := v.Value(MEarlyViews, now)
		if !ok {
			continue
		}
		by[v.Meta.Category] = append(by[v.Meta.Category], val)
		all = append(all, val)
	}
	d := &Diagnosis{Cause: CauseTopicMismatch}
	enough := 0
	for _, xs := range by {
		if len(xs) >= 3 {
			enough++
		}
	}
	if enough < 2 {
		d.Missing = append(d.Missing, "needs at least 2 content categories with 3+ videos each (category is recorded by MICROFLOW at publish time)")
		return d
	}
	overall := Median(all)
	var cats []string
	for c := range by {
		cats = append(cats, c)
	}
	sort.Strings(cats)
	for _, c := range cats {
		xs := by[c]
		if len(xs) < 3 || overall <= 0 {
			continue
		}
		r := Median(xs) / overall
		switch {
		case r < 0.6:
			d.Supporting = append(d.Supporting, fmt.Sprintf("category %q median first-window views are %.0f%% of the channel median (%d videos)", c, r*100, len(xs)))
			d.score += 2
		case r > 1.4:
			d.Contradicting = append(d.Contradicting, fmt.Sprintf("category %q outperforms the channel median (%d videos)", c, len(xs)))
		}
	}
	if len(d.Supporting) == 0 {
		return nil
	}
	if len(d.Contradicting) > 0 {
		d.Contradicting = append(d.Contradicting, "performance differs by category, which supports re-weighting rather than a single fix")
	}
	return d
}
