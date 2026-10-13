package yta

import (
	"fmt"
	"strings"
)

// Brief is the short end-of-run report: problem, evidence, recommendations,
// changes, results and uncertainty. It contains no secrets, IDs of tokens or
// raw API payloads.
type Brief struct {
	Mode            string   `json:"mode"`
	Problem         string   `json:"problem"`
	Evidence        []string `json:"evidence"`
	Recommendations []string `json:"recommendations"`
	Changes         []string `json:"changes"`
	Results         []string `json:"results"`
	Uncertainty     []string `json:"uncertainty"`
	Summary         string   `json:"summary"`
}

func firstN[T any](xs []T, n int) []T {
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}

// BuildBrief condenses one analysis into the per-run report.
func BuildBrief(sum Summary, recs []Recommendation, plan Plan, events []Event, recommendOnly bool, notes []string) Brief {
	b := Brief{Mode: "auto-evolve (allow-listed settings only)"}
	if recommendOnly {
		b.Mode = "recommend-only (no setting is changed automatically)"
	}
	b.Problem = "No cause can be claimed from the available data."
	var top *Diagnosis
	for i := range sum.Diagnoses {
		if sum.Diagnoses[i].Level != LevelInsufficient {
			top = &sum.Diagnoses[i]
			break
		}
	}
	if top != nil {
		b.Problem = fmt.Sprintf("Most likely cause: %s (%s evidence).", top.Cause, top.Level)
		for _, e := range firstN(top.Supporting, 3) {
			b.Evidence = append(b.Evidence, "For: "+e)
		}
		for _, e := range firstN(top.Contradicting, 2) {
			b.Evidence = append(b.Evidence, "Against: "+e)
		}
		b.Uncertainty = append(b.Uncertainty, firstN(top.Missing, 3)...)
	} else if len(sum.Diagnoses) > 0 {
		b.Uncertainty = append(b.Uncertainty, firstN(sum.Diagnoses[0].Missing, 3)...)
	}
	for _, r := range sum.Regressions {
		b.Problem += " Regression: " + r.String() + "."
	}
	for _, r := range firstN(recs, 4) {
		b.Recommendations = append(b.Recommendations, r.Area+": "+r.Action)
	}
	for _, e := range events {
		switch e.Kind {
		case "adopted", "rollback", "setting_frozen", "adoption_skipped":
			b.Changes = append(b.Changes, e.Detail)
		default:
			b.Results = append(b.Results, e.Detail)
		}
	}
	switch {
	case plan.Started && plan.Proposed != nil:
		b.Changes = append(b.Changes, "Started experiment "+plan.Proposed.ID+" (one variable: "+plan.Proposed.Setting+").")
	case plan.Proposed != nil:
		b.Changes = append(b.Changes, "Proposed (not started): "+plan.Proposed.Setting+"="+plan.Proposed.Variant+" - "+plan.Reason)
	default:
		if plan.Reason != "" {
			b.Changes = append(b.Changes, "No new experiment: "+plan.Reason)
		}
	}
	if len(b.Results) == 0 {
		b.Results = append(b.Results, "No experiment concluded in this run.")
	}
	if len(sum.Topics) > 0 && sum.Topics[0].Evidence != LevelInsufficient {
		b.Results = append(b.Results, fmt.Sprintf("Top-ranked topic category: %s.", sum.Topics[0].Category))
	}
	b.Uncertainty = append(b.Uncertainty, firstN(notes, 3)...)
	if recommendOnly {
		b.Uncertainty = append(b.Uncertainty, "Recommend-only: nothing was changed; set MICROFLOW_YTA_EVOLVE_ALLOW to permit specific content settings.")
	}
	b.Summary = strings.Join([]string{b.Problem, strings.Join(firstN(b.Evidence, 2), " "), strings.Join(firstN(b.Changes, 1), " ")}, " ")
	if len(b.Summary) > 600 {
		b.Summary = b.Summary[:600]
	}
	b.Summary = strings.TrimSpace(b.Summary)
	return b
}
