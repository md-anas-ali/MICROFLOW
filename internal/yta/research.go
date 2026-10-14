package yta

// Competitor / viral-topic analysis. Raw view counts rank channel size, not
// ideas: a 400k video on a channel whose median is 30k is worth studying, the
// same video on a channel that normally gets 400k is a normal Tuesday. So every
// competitor video is scored as a MULTIPLE OF ITS OWN CHANNEL'S MEDIAN, which
// needs several videos per channel to mean anything (the logic here says so
// rather than ranking on noise). Adapted from youtube-agent-skill (MIT).
//
// Pure logic: the HTTP fetching lives in internal/nodes and passes plain
// values in. Nothing here is persisted except the small Research summary that
// rides inside the existing analytics report.

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// MarketVideo is one public video collected from a competitor or a chart.
type MarketVideo struct {
	Channel     string
	Title       string
	Views       float64
	PublishedAt time.Time
	DurationSec int
}

// Outlier is a video that beat its own channel's median.
type Outlier struct {
	Channel  string  `json:"channel"`
	Title    string  `json:"title"`
	Views    int     `json:"views"`
	Median   int     `json:"channelMedianViews"`
	Multiple float64 `json:"multipleOfMedian"`
	Formula  string  `json:"formula"`
	AgeDays  int     `json:"ageDays"`
}

// FormulaCount counts one wording pattern among outliers.
type FormulaCount struct {
	Formula string `json:"formula"`
	Count   int    `json:"count"`
}

// Research is the compact market picture stored with the analytics report.
type Research struct {
	Source      string         `json:"source"` // competitors | trending | none
	Channels    int            `json:"channels"`
	Videos      int            `json:"videos"`
	Outliers    []Outlier      `json:"outliers,omitempty"`
	Formulas    []FormulaCount `json:"formulas,omitempty"`
	SkippedThin []string       `json:"skippedThinChannels,omitempty"`
	Evidence    string         `json:"evidenceLevel"`
	TopicText   string         `json:"topicText,omitempty"` // injected into the script prompt (subjects only)
	Note        string         `json:"note,omitempty"`
}

const (
	minVideosPerChannel = 4
	minOutlierAgeDays   = 3.0
)

func cleanTitleLine(s string) string {
	s = cleanText(s)
	if r := []rune(s); len(r) > 90 {
		s = string(r[:90])
	}
	return s
}

// AnalyzeCompetitors ranks collected competitor videos by how far each beat
// its own channel's median. minMultiple defaults to 1.8.
func AnalyzeCompetitors(vids []MarketVideo, minMultiple float64, now time.Time) Research {
	if minMultiple < 1.2 {
		minMultiple = 1.8
	}
	r := Research{Source: "competitors", Evidence: LevelInsufficient}
	by := map[string][]MarketVideo{}
	for _, v := range vids {
		if v.Title == "" {
			continue
		}
		by[v.Channel] = append(by[v.Channel], v)
	}
	r.Videos = len(vids)
	var chans []string
	for c := range by {
		chans = append(chans, c)
	}
	sort.Strings(chans)
	var out []Outlier
	for _, c := range chans {
		vs := by[c]
		var views []float64
		for _, v := range vs {
			if now.Sub(v.PublishedAt).Hours()/24 >= minOutlierAgeDays {
				views = append(views, v.Views)
			}
		}
		if len(views) < minVideosPerChannel {
			r.SkippedThin = append(r.SkippedThin, fmt.Sprintf("%s (%d)", c, len(views)))
			continue
		}
		r.Channels++
		med := Median(views)
		if med <= 0 {
			continue
		}
		for _, v := range vs {
			age := now.Sub(v.PublishedAt).Hours() / 24
			if age < minOutlierAgeDays {
				continue
			}
			m := v.Views / med
			if m >= minMultiple {
				f, _ := ClassifyFormula(v.Title)
				out = append(out, Outlier{Channel: c, Title: cleanTitleLine(v.Title), Views: int(v.Views), Median: int(med),
					Multiple: float64(int(m*100)) / 100, Formula: f, AgeDays: int(age)})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Multiple > out[j].Multiple })
	if len(out) > 5 {
		out = out[:5]
	}
	r.Outliers = out
	r.Formulas = countFormulas(out)
	switch {
	case r.Channels >= 3 && len(out) >= 3:
		r.Evidence = LevelModerate
	case r.Channels >= 1 && len(out) >= 1:
		r.Evidence = LevelWeak
	}
	if r.Evidence != LevelInsufficient {
		var titles []string
		for _, o := range out {
			if len(titles) < 3 {
				titles = append(titles, fmt.Sprintf("%q (%.1fx its channel's median)", o.Title, o.Multiple))
			}
		}
		r.TopicText = "MARKET SIGNALS (competitor Shorts that clearly beat their own channel's normal views; inspiration for the SUBJECT or angle only, never copy wording, never reuse their topic): " + strings.Join(titles, "; ") + "."
	} else {
		r.Note = "no competitor video cleared the outlier threshold (or too few videos per channel)"
	}
	return r
}

// AnalyzeTrending summarises a chart (e.g. most-popular in a category). There
// is no per-channel median to compare against, so this is weaker evidence: it
// only says what is popular right now, ranked by views per day.
func AnalyzeTrending(vids []MarketVideo, now time.Time) Research {
	r := Research{Source: "trending", Evidence: LevelInsufficient, Videos: len(vids)}
	type kv struct {
		v   MarketVideo
		vpd float64
	}
	var xs []kv
	for _, v := range vids {
		age := now.Sub(v.PublishedAt).Hours() / 24
		if age < 0.25 {
			age = 0.25
		}
		if v.Title != "" {
			xs = append(xs, kv{v, v.Views / age})
		}
	}
	if len(xs) < 5 {
		r.Note = "fewer than 5 chart videos returned; not used"
		return r
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i].vpd > xs[j].vpd })
	var top []Outlier
	for i := 0; i < len(xs) && i < 5; i++ {
		f, _ := ClassifyFormula(xs[i].v.Title)
		top = append(top, Outlier{Channel: xs[i].v.Channel, Title: cleanTitleLine(xs[i].v.Title), Views: int(xs[i].v.Views), Formula: f,
			AgeDays: int(now.Sub(xs[i].v.PublishedAt).Hours() / 24)})
	}
	r.Outliers = top
	r.Formulas = countFormulas(top)
	r.Evidence = LevelWeak
	var titles []string
	for _, o := range top[:minInt(3, len(top))] {
		titles = append(titles, fmt.Sprintf("%q", o.Title))
	}
	r.TopicText = "TRENDING NOW IN THE CATEGORY (popular on YouTube today; weak signal, inspiration for the subject only, never copy wording or reuse the topic): " + strings.Join(titles, "; ") + "."
	r.Note = "chart data has no per-channel baseline, so this is popularity, not proof of what works for a small channel"
	return r
}

func countFormulas(out []Outlier) []FormulaCount {
	m := map[string]int{}
	for _, o := range out {
		m[o.Formula]++
	}
	var fc []FormulaCount
	for f, n := range m {
		fc = append(fc, FormulaCount{f, n})
	}
	sort.Slice(fc, func(i, j int) bool {
		if fc[i].Count != fc[j].Count {
			return fc[i].Count > fc[j].Count
		}
		return fc[i].Formula < fc[j].Formula
	})
	if len(fc) > 3 {
		fc = fc[:3]
	}
	return fc
}

// RecommendFromResearch turns market findings into recommendations. They are
// observations about OTHER channels: always weak evidence for this one, always
// phrased as something to test.
func RecommendFromResearch(r Research) []Recommendation {
	out := []Recommendation{}
	if r.Evidence == LevelInsufficient || len(r.Outliers) == 0 {
		return out
	}
	if len(r.Formulas) > 0 && r.Formulas[0].Formula != "Unclassified" && r.Formulas[0].Count >= 2 {
		out = append(out, Recommendation{Area: "hook", Cause: "competitor_pattern", Evidence: LevelWeak,
			Action:  fmt.Sprintf("%d of the %d strongest %s videos use the %q wording pattern. Try it on a few of your own Shorts; do not assume it transfers.", r.Formulas[0].Count, len(r.Outliers), r.Source, r.Formulas[0].Formula),
			TestHow: "Use the hook_style experiment or compare against concurrent baseline videos before adopting."})
	}
	if r.TopicText != "" {
		o := r.Outliers[0]
		out = append(out, Recommendation{Area: "topic", Cause: "viral_topic_signal", Evidence: LevelWeak,
			Action:  fmt.Sprintf("Top market outlier: %q (%.1fx its channel's median). Look for an unexplored angle on the same kind of subject rather than repeating it.", o.Title, o.Multiple),
			TestHow: "Already passed to the script prompt as a subject signal only; judge by your own first-window views."})
	}
	return out
}
