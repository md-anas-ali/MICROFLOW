package yta

import (
	"fmt"
	"strings"
)

// Recommendation is a concrete, deterministic content suggestion tied to a
// diagnosed cause. Only causes with evidence above "insufficient" produce
// recommendations; they are suggestions to test, never claims of fact.
type Recommendation struct {
	Area           string `json:"area"` // topic | hook | script | pacing | title | description | ending
	Action         string `json:"action"`
	Cause          string `json:"cause"`
	Evidence       string `json:"evidenceLevel"`
	Experimentable string `json:"experimentSetting,omitempty"`
	TestHow        string `json:"howToTest"`
}

// Recommend maps ranked diagnoses to recommendations (best-supported first).
func Recommend(sum Summary) []Recommendation {
	var out []Recommendation
	for _, d := range sum.Diagnoses {
		if d.Level == LevelInsufficient {
			continue
		}
		add := func(area, action, test string) {
			out = append(out, Recommendation{Area: area, Action: action, Cause: d.Cause, Evidence: d.Level, Experimentable: d.Setting, TestHow: test})
		}
		switch d.Cause {
		case CauseWeakHook:
			add("hook", "Open scene 1 with a concrete, specific claim, number or question about the topic; remove any setup before the first fact.", "Change only the hook style; compare average view percentage against concurrent baseline videos.")
		case CauseMidVideoDrop:
			add("pacing", "Shorten scenes 2-4 and make each reveal escalate; cut filler words so something new is revealed in every scene.", "Change only voice length per scene; compare average view percentage.")
			add("script", "Move the strongest reveal earlier and keep the final scene short.", "Evaluate after the pacing experiment; do not change both at once.")
		case CauseLowDistribution:
			add("title", "Test a different title pattern (number first, curiosity gap, How/Why) while keeping it accurate.", "Change only the title style; compare first-window views against concurrent baseline videos.")
			add("description", "Put the primary keyword early in the description and keep it specific.", "Test only after the title experiment concludes.")
			add("topic", "Favor categories that already beat the channel median for first-window views.", "Recommendation only; category weights are not changed automatically.")
		case CauseLowConversion:
			add("ending", "Make the closing follow/comment request specific to the video's topic (for example ask a question the viewer can answer).", "Recommendation only; no automatic setting exists for this.")
		case CauseContentFatigue:
			add("topic", "Rest the fatigued category for the next several Shorts and rotate to higher-ranked categories.", "Recommendation only; compare the category's next videos after the rest period.")
		case CauseTopicMismatch:
			add("topic", "Re-weight future topics toward categories with higher first-window views and drop the weakest category for a trial period.", "Recommendation only; change category weights manually, one category at a time.")
		}
	}
	// Observational content-feature associations (suggest what to test, not proven causes).
	for _, f := range sum.Features {
		switch f.Verdict {
		case "better":
			out = append(out, Recommendation{Area: "title", Cause: "observed_association", Evidence: LevelWeak,
				Action:  fmt.Sprintf("Own videos with %q earned %.0f%% more (median %s, %d vs %d videos). Treat as a hypothesis to test, not a proven cause.", f.Feature, f.Relative*100, f.Metric, f.With, f.Without),
				TestHow: "Change only this feature in a controlled experiment before relying on it."})
		case "worse":
			out = append(out, Recommendation{Area: "title", Cause: "observed_association", Evidence: LevelWeak,
				Action:  fmt.Sprintf("Own videos with %q earned %.0f%% less (median %s, %d vs %d videos). Treat as a hypothesis, not a proven cause.", f.Feature, -f.Relative*100, f.Metric, f.With, f.Without),
				TestHow: "Change only this feature in a controlled experiment before relying on it."})
		}
	}
	// Next-topic ranking (only categories with evidence).
	var names []string
	for _, t := range sum.Topics {
		if t.Evidence != LevelInsufficient && !t.Fatigued && len(names) < 3 {
			names = append(names, fmt.Sprintf("%s (x%.2f of channel median)", t.Category, t.RelMedian))
		}
	}
	if len(names) > 0 {
		out = append(out, Recommendation{Area: "topic", Cause: "topic_ranking", Evidence: LevelWeak,
			Action:  "Next Short: prefer " + strings.Join(names, "; ") + ". Ranking covers only categories already published.",
			TestHow: "Recommendation only; category weights are not changed automatically."})
	}
	return out
}
