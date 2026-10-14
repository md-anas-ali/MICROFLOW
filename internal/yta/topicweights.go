package yta

import (
	"regexp"
	"strings"
	"time"
)

var reRoundSuffix = regexp.MustCompile(`\s*\(round \d+:.*$`)

// CategoryRoot reduces the idea string the workflow records at publish time
// ("space & astronomy: black holes", "history: the Rosetta Stone — a myth
// about it (round 2: ...)") to its broad category, so performance can be
// compared across ideas instead of every idea being its own group of one.
func CategoryRoot(c string) string {
	c = reRoundSuffix.ReplaceAllString(c, "")
	if i := strings.Index(c, ":"); i > 0 {
		c = c[:i]
	}
	return strings.ToLower(strings.TrimSpace(c))
}

// TopicWeights turns the channel's own category performance into gentle
// selection weights for the NEXT topic. 1.0 means "no evidence, no opinion".
// Weights stay inside [0.4, 1.8] on purpose: a category is never excluded and
// never dominates, so the channel keeps exploring while it leans toward what
// has demonstrably worked. Only categories with at least 3 mature videos get a
// weight; fatigued categories are rested.
func TopicWeights(videos []Video, now time.Time) map[string]float64 {
	rooted := make([]Video, len(videos))
	copy(rooted, videos)
	for i := range rooted {
		rooted[i].Meta.Category = CategoryRoot(rooted[i].Meta.Category)
	}
	out := map[string]float64{}
	for _, t := range RankTopics(rooted, now) {
		if t.Category == "" || t.Evidence == LevelInsufficient {
			continue
		}
		w := 1.0
		switch {
		case t.RelMedian >= 1.5:
			w = 1.8
		case t.RelMedian >= 1.2:
			w = 1.4
		case t.RelMedian < 0.6:
			w = 0.5
		case t.RelMedian < 0.8:
			w = 0.7
		}
		if t.Evidence == LevelWeak && w > 1 { // 3-5 videos: lean, do not commit
			w = 1 + (w-1)/2
		}
		if t.Trend == "rising" {
			w += 0.2
		}
		if t.Fatigued {
			w = 0.4
		}
		if w < 0.4 {
			w = 0.4
		}
		if w > 1.8 {
			w = 1.8
		}
		out[t.Category] = float64(int(w*100+0.5)) / 100
	}
	return out
}
