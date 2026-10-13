package yta

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func tv(id string, age float64, early float64, cat, title string, dur int) Video {
	v := vid(id, age, early, 60, cat)
	v.Title, v.DurationSec = title, dur
	return v
}

func TestBenchmarkIsLengthAware(t *testing.T) {
	var peers []Video
	for i := 0; i < 6; i++ {
		peers = append(peers, tv(fmt.Sprintf("s%d", i), 20, 1000+float64(i)*10, "", "", 18))
		peers = append(peers, tv(fmt.Sprintf("l%d", i), 20, 100+float64(i)*10, "", "", 55))
	}
	c := Benchmark(tv("t", 5, 1000, "", "", 19), peers, MEarlyViews, t0, 5)
	if c.Scope != "age+length" || c.Verdict == "above" {
		t.Fatalf("a 19s Short must be compared with other short-length videos: %+v", c)
	}
}

func TestRegressionDetection(t *testing.T) {
	var vs []Video
	for i := 0; i < 10; i++ {
		vs = append(vs, vid(fmt.Sprintf("h%d", i), float64(20+i*3), 1000+float64(i)*8, 70, ""))
	}
	for i := 0; i < 5; i++ {
		vs = append(vs, vid(fmt.Sprintf("r%d", i), float64(3+i), 500+float64(i)*5, 70, ""))
	}
	r := DetectRegression(vs, t0, 5, 8)
	if len(r) != 1 || r[0].Metric != MEarlyViews || r[0].Relative > -0.15 {
		t.Fatalf("want one early-views regression, got %+v", r)
	}
	if got := DetectRegression(vs[:6], t0, 5, 8); got != nil {
		t.Fatalf("too little history must claim nothing: %+v", got)
	}
}

func TestTopicFatigueAndRanking(t *testing.T) {
	var vs []Video
	for i := 0; i < 10; i++ { // newest 10 videos are all "space"; the newest five are much weaker
		early := 1000.0 + float64(i)
		if i < 5 {
			early = 200 + float64(i)
		}
		vs = append(vs, vid(fmt.Sprintf("sp%d", i), float64(3+i), early, 60, "space"))
	}
	for i := 0; i < 6; i++ {
		vs = append(vs, vid(fmt.Sprintf("hi%d", i), float64(40+i), 800+float64(i), 60, "history"))
	}
	topics := RankTopics(vs, t0)
	if len(topics) != 2 {
		t.Fatalf("unexpected ranking: %+v", topics)
	}
	var space TopicScore
	for _, x := range topics {
		if x.Category == "space" {
			space = x
		}
	}
	if !space.Fatigued || space.Trend != "declining" || topics[0].Category != "history" {
		t.Fatalf("space should be fatigued and ranked below history: %+v", topics)
	}
	// unproven categories are never ranked first
	few := RankTopics([]Video{vid("a", 5, 9999, 60, "x"), vid("b", 6, 10, 60, "y")}, t0)
	for _, f := range few {
		if f.Evidence != LevelInsufficient {
			t.Fatalf("1 video must not be ranked on performance: %+v", f)
		}
	}
}

func TestFeatureInsightsNeedsSampleSize(t *testing.T) {
	var vs []Video
	for i := 0; i < 6; i++ {
		vs = append(vs, tv(fmt.Sprintf("n%d", i), 20, 2000+float64(i)*5, "", "7 facts about space", 30))
		vs = append(vs, tv(fmt.Sprintf("m%d", i), 20, 1000+float64(i)*5, "", "Space facts", 30))
	}
	var num FeatureFinding
	for _, f := range FeatureInsights(vs, t0, 5) {
		if f.Feature == "title_contains_number" {
			num = f
		}
	}
	if num.Verdict != "better" {
		t.Fatalf("%+v", num)
	}
	for _, f := range FeatureInsights(vs[:6], t0, 5) {
		if f.Verdict != "insufficient" {
			t.Fatalf("3 videos per group must be insufficient: %+v", f)
		}
	}
}

func TestValidateAnalytics(t *testing.T) {
	if bad := ValidateAnalytics(&VideoAnalytics{Views: 10, AvgViewPct: 130, HasAvgViewPct: true}); len(bad) != 0 {
		t.Fatalf("loops may exceed 100%%: %v", bad)
	}
	for _, a := range []*VideoAnalytics{{Views: -1}, {Views: math.NaN()}, {Views: 10, EngagedViews: 50, HasEngaged: true}} {
		if len(ValidateAnalytics(a)) == 0 {
			t.Fatalf("should reject %+v", a)
		}
	}
}

func TestIntegritySealRepairAndAuditBound(t *testing.T) {
	st := State{Settings: map[string]string{SettingPacing: "tight"}}
	st.Seal()
	if !st.IntegrityOK() {
		t.Fatal("fresh seal must verify")
	}
	st.Settings[SettingPacing] = "dense" // out-of-band edit
	if st.IntegrityOK() {
		t.Fatal("tamper must be detected")
	}
	st.Settings["bogus"] = "x"
	st.Active = &Experiment{ID: "e", Setting: SettingHookStyle, Variant: "nope", MinPerArm: 4}
	st.Changes = []Change{{Setting: SettingPacing, Status: "pending_verification"}, {Setting: SettingPacing, Status: "pending_verification"}, {Setting: "", Status: "weird"}}
	fixed := st.Repair(t0)
	if _, ok := st.Settings["bogus"]; ok || st.Active != nil || len(st.Changes) != 2 || st.Changes[0].Status != "closed_by_repair" || len(fixed) < 3 {
		t.Fatalf("repair incomplete: %+v %v", st, fixed)
	}
	for i := 0; i < 200; i++ {
		st.AddAudit("k", strings.Repeat("x", 500), EvoConfig{}, t0)
		st.AddRun(RunRecord{})
	}
	if len(st.Audit) != maxAuditKept || len(st.Runs) != maxRunsKept || len(st.Audit[0].Detail) > 220 {
		t.Fatalf("history not bounded: %d %d", len(st.Audit), len(st.Runs))
	}
	if !strings.Contains(st.Audit[0].Policy, "recommend-only") {
		t.Fatalf("policy snapshot missing: %q", st.Audit[0].Policy)
	}
}

func TestBriefStatesUncertaintyAndMode(t *testing.T) {
	sum := Summary{Diagnoses: []Diagnosis{{Cause: CauseWeakHook, Level: LevelModerate, Supporting: []string{"a"}, Contradicting: []string{"b"}, Missing: []string{"c"}}},
		Regressions: []Regression{{Metric: MEarlyViews, Relative: -0.4, NRecent: 5, NHistory: 9}}}
	b := BuildBrief(sum, []Recommendation{{Area: "hook", Action: "x"}}, Plan{Reason: "r"}, []Event{{Kind: "rollback", Detail: "rolled"}}, true, nil)
	if !strings.Contains(b.Problem, "weak_hook") || !strings.Contains(b.Problem, "Regression") || len(b.Changes) < 2 {
		t.Fatalf("%+v", b)
	}
	joined := strings.Join(b.Uncertainty, " ")
	if !strings.Contains(joined, "c") || !strings.Contains(joined, "Recommend-only") || !strings.Contains(b.Mode, "recommend-only") {
		t.Fatalf("uncertainty/mode missing: %+v", b)
	}
	none := BuildBrief(Summary{}, nil, Plan{}, nil, true, nil)
	if !strings.Contains(none.Problem, "No cause can be claimed") {
		t.Fatalf("%+v", none)
	}
	_ = time.Now
}
