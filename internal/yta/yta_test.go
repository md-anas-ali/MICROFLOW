package yta

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

func vid(id string, ageDays float64, early, pct float64, cat string) Video {
	return Video{ID: id, PublishedAt: t0.Add(-time.Duration(ageDays * 24 * float64(time.Hour))),
		Views: early, Likes: early / 50, EarlyViews: early, HasEarly: true,
		Analytics: &VideoAnalytics{Views: early, AvgViewPct: pct, HasAvgViewPct: true},
		Meta:      VideoMeta{Category: cat}}
}

func history(n int, early, pct float64) []Video {
	var vs []Video
	for i := 0; i < n; i++ {
		// small deterministic spread so MAD is non-zero
		vs = append(vs, vid(fmt.Sprintf("h%d", i), float64(20+i*3), early+float64(i%5)*10, pct+float64(i%3), "history"))
	}
	return vs
}

func TestBenchmarkInsufficientPeers(t *testing.T) {
	c := Benchmark(vid("a", 5, 100, 50, ""), history(2, 1000, 70), MEarlyViews, t0, 5)
	if c.Verdict != "insufficient" {
		t.Fatalf("want insufficient, got %+v", c)
	}
}

func TestBenchmarkBelowAndAgeFairness(t *testing.T) {
	all := history(12, 1000, 70)
	c := Benchmark(vid("a", 5, 200, 40, ""), all, MEarlyViews, t0, 5)
	if c.Verdict != "below" || c.Scope != "age" {
		t.Fatalf("want below/age, got %+v", c)
	}
	// views_per_day must only compare within an age bucket: all history is >=20 days old.
	c = Benchmark(vid("a", 5, 200, 40, ""), all, MViewsPerDay, t0, 5)
	if c.Verdict != "insufficient" {
		t.Fatalf("different age buckets must not be compared, got %+v", c)
	}
}

func TestDiagnoseDoesNotClaimWithoutEvidence(t *testing.T) {
	s := Diagnose(history(2, 1000, 70), t0, Config{})
	if len(s.Diagnoses) != 1 || s.Diagnoses[0].Cause != CauseImmatureData || s.Diagnoses[0].Level != LevelInsufficient {
		t.Fatalf("unexpected: %+v", s.Diagnoses)
	}
}

func lowRetentionSet() []Video {
	all := history(15, 1000, 70)
	for i := 0; i < 5; i++ {
		all = append(all, vid(fmt.Sprintf("r%d", i), float64(3+i), 1000+float64(i)*5, 35+float64(i), "history"))
	}
	return all
}

func TestDiagnoseMidVideoDropWhenOnlyRetentionLow(t *testing.T) {
	s := Diagnose(lowRetentionSet(), t0, Config{})
	top := s.Diagnoses[0]
	if top.Cause == CauseLowDistribution || top.Level == LevelInsufficient {
		t.Fatalf("views are normal, retention low: got top %+v", top)
	}
	for _, d := range s.Diagnoses {
		if d.Cause == CauseLowDistribution && d.Level != LevelInsufficient {
			t.Fatalf("distribution must not be claimed when views are in line: %+v", d)
		}
	}
}

func TestParseAllowRejectsUnknown(t *testing.T) {
	a, unk := ParseAllow("hook_style, bogus, OAuth_scopes")
	if !a[SettingHookStyle] || len(a) != 1 || len(unk) != 2 {
		t.Fatalf("allow=%v unknown=%v", a, unk)
	}
}

func TestNextRecommendOnlyWhenNotAllowed(t *testing.T) {
	var st State
	sum := Diagnose(lowRetentionSet(), t0, Config{})
	p := st.Next(sum, true, t0, EvoConfig{Allow: map[string]bool{}})
	if p.Started || st.Active != nil {
		t.Fatalf("must not start without permission: %+v", p)
	}
	if p.Proposed == nil {
		t.Fatalf("expected a proposal: %+v", p)
	}
}

func TestAssignIdempotentAndAlternating(t *testing.T) {
	st := State{Active: &Experiment{ID: "e1", Setting: SettingHookStyle, Variant: "question", Metric: MAvgViewPct, CreatedAt: t0}}
	cfg := EvoConfig{Allow: map[string]bool{SettingHookStyle: true}}
	g1 := st.AssignFor("x1", cfg, t0)
	g1b := st.AssignFor("x1", cfg, t0)
	g2 := st.AssignFor("x2", cfg, t0)
	if g1.Arm != "treatment" || g1b.Arm != g1.Arm || g2.Arm != "baseline" {
		t.Fatalf("%v %v %v", g1.Arm, g1b.Arm, g2.Arm)
	}
	if g1.Text == "" || g2.Text != "" {
		t.Fatalf("only treatment gets text: %q / %q", g1.Text, g2.Text)
	}
	if g := (&State{Active: st.Active}).AssignFor("x9", EvoConfig{}, t0); g.Arm != "none" || g.Text != "" {
		t.Fatalf("not allowed => no guidance: %+v", g)
	}
}

func TestPinnedOnlyAfterAnalyze(t *testing.T) {
	st := State{Active: &Experiment{ID: "e1", Setting: SettingHookStyle, Variant: "question", Metric: MAvgViewPct, CreatedAt: t0}}
	cfg := EvoConfig{Allow: map[string]bool{SettingHookStyle: true}}
	if _, ok := st.Pinned("never-analyzed"); ok {
		t.Fatal("register must not attribute an arm when analyze did not run")
	}
	g := st.AssignFor("run1", cfg, t0)
	p, ok := st.Pinned("run1")
	if !ok || p.Arm != g.Arm || p.Text != g.Text {
		t.Fatalf("pinned mismatch %+v vs %+v", p, g)
	}
}

func armVideos(exp string, arm string, n int, pct float64) []Video {
	var vs []Video
	for i := 0; i < n; i++ {
		v := vid(fmt.Sprintf("%s%d", arm, i), 10, 500, pct+float64(i), "")
		v.Meta = VideoMeta{ExperimentID: exp, Arm: arm}
		vs = append(vs, v)
	}
	return vs
}

func TestVerifyAdoptsImprovementThenRollsBackIfWorse(t *testing.T) {
	cfg := EvoConfig{Allow: map[string]bool{SettingHookStyle: true}, MinPerArm: 4, EvalDays: 7}
	st := State{Active: &Experiment{ID: "e1", Setting: SettingHookStyle, Variant: "question", Metric: MAvgViewPct, CreatedAt: t0.Add(-20 * 24 * time.Hour)}}
	vs := append(armVideos("e1", "treatment", 5, 70), armVideos("e1", "baseline", 5, 40)...)
	ev := st.Verify(vs, t0, cfg)
	if st.Settings[SettingHookStyle] != "question" || st.Active != nil || len(ev) < 2 {
		t.Fatalf("not adopted: %+v %+v", st.Settings, ev)
	}
	ch := st.Changes[0]
	if ch.Status != "pending_verification" || ch.From != "" {
		t.Fatalf("rollback info missing: %+v", ch)
	}
	// adopted videos perform much worse than the baseline median -> rollback
	var adopted []Video
	for i := 0; i < 4; i++ {
		v := vid(fmt.Sprintf("ad%d", i), 10, 500, 20+float64(i), "")
		v.Meta = VideoMeta{Arm: "adopted", Variant: "hook_style=question"}
		adopted = append(adopted, v)
	}
	ev = st.Verify(adopted, t0, cfg)
	if _, still := st.Settings[SettingHookStyle]; still || st.Changes[0].Status != "rolled_back" {
		t.Fatalf("rollback failed: %+v %+v %+v", st.Settings, st.Changes, ev)
	}
	if !st.blocked(SettingHookStyle, "question", t0) {
		t.Fatal("rolled-back variant must be blocked from repeating")
	}
}

func TestVerifyWorseBlocksAndInconclusiveWithoutData(t *testing.T) {
	cfg := EvoConfig{Allow: map[string]bool{SettingPacing: true}, MinPerArm: 4, EvalDays: 7}
	st := State{Active: &Experiment{ID: "e2", Setting: SettingPacing, Variant: "tight", Metric: MAvgViewPct, CreatedAt: t0.Add(-5 * 24 * time.Hour)}}
	if ev := st.Verify(armVideos("e2", "treatment", 2, 50), t0, cfg); len(ev) != 0 || st.Active == nil {
		t.Fatalf("too little data must keep running, got %+v", ev)
	}
	vs := append(armVideos("e2", "treatment", 5, 30), armVideos("e2", "baseline", 5, 60)...)
	st.Verify(vs, t0, cfg)
	if st.Active != nil || st.History[0].Result != "worse" || !st.blocked(SettingPacing, "tight", t0) {
		t.Fatalf("worse not handled: %+v", st)
	}
	if st.Settings[SettingPacing] != "" {
		t.Fatal("a worse variant must never be adopted")
	}
}

func TestChangeRateFreeze(t *testing.T) {
	st := State{Changes: []Change{{Setting: SettingPacing, At: t0.Add(-2 * 24 * time.Hour)}, {Setting: SettingPacing, At: t0.Add(-5 * 24 * time.Hour)}}}
	if !st.changeRateExceeded(SettingPacing, t0) || st.changeRateExceeded(SettingHookStyle, t0) {
		t.Fatal("rate limiter wrong")
	}
}

func TestStateIsBounded(t *testing.T) {
	var st State
	for i := 0; i < 1000; i++ {
		st.RecordVideo(fmt.Sprintf("v%d", i), VideoMeta{At: fmt.Sprintf("%06d", i)})
	}
	if len(st.Videos) != maxVideosKept {
		t.Fatalf("videos not capped: %d", len(st.Videos))
	}
	if _, ok := st.Videos["v999"]; !ok {
		t.Fatal("newest must be kept")
	}
}

func TestBenchmarkIdenticalPeersStillDetectDeviation(t *testing.T) {
	var peers []Video
	for i := 0; i < 8; i++ {
		peers = append(peers, vid(fmt.Sprintf("p%d", i), 30, 1000, 70, ""))
	}
	c := Benchmark(vid("x", 5, 500, 40, ""), peers, MAvgViewPct, t0, 5)
	if c.Verdict != "below" {
		t.Fatalf("tight peer cluster must not hide a large deviation: %+v", c)
	}
	same := Benchmark(vid("y", 5, 500, 70, ""), peers, MAvgViewPct, t0, 5)
	if same.Verdict != "typical" {
		t.Fatalf("equal value must be typical: %+v", same)
	}
}

func TestWorstCaseStateSizeIsBounded(t *testing.T) {
	st := State{Settings: map[string]string{SettingHookStyle: "question", SettingPacing: "tight", SettingTitleStyle: "how_why"},
		Active: &Experiment{ID: "exp-20261013-hook_style-contradiction", Setting: SettingHookStyle, Variant: "question", Metric: MAvgViewPct, Hypothesis: strings.Repeat("h", 300), CreatedAt: t0}}
	for i := 0; i < 1000; i++ {
		st.RecordVideo(fmt.Sprintf("vid%08d", i), VideoMeta{Category: "category-name-long", ExperimentID: "exp-20261013-hook_style-contradiction", Arm: "treatment", Setting: SettingHookStyle, Variant: "contradiction", At: t0.Format(time.RFC3339)})
		st.AddAudit("experiment_concluded", strings.Repeat("d", 400), EvoConfig{Allow: map[string]bool{SettingHookStyle: true, SettingPacing: true, SettingTitleStyle: true}}, t0)
		st.AddRun(RunRecord{At: t0, Status: "ok", Mode: "full (Data API + Analytics API)", Errors: map[string]int{"quota": 3, "timeout": 2, "oauth": 1}, Calls: 99})
		st.History = append(st.History, Outcome{ExperimentID: "e", Setting: SettingHookStyle, Variant: "contradiction", Metric: MAvgViewPct, Result: "improved", At: t0})
		st.Changes = append(st.Changes, Change{ID: "chg-20261013T150405-hook_style", Setting: SettingHookStyle, From: "question", To: "contradiction", At: t0, Status: "verified"})
		st.Blocks = append(st.Blocks, Block{Setting: SettingHookStyle, Variant: "contradiction", Until: t0.Add(time.Hour), Reason: strings.Repeat("r", 60)})
		st.Assign = map[string]Assignment{}
		for j := 0; j < maxAssignKept; j++ {
			st.Assign[fmt.Sprintf("exec-%d-%d", i, j)] = Assignment{Arm: "treatment", ExperimentID: "exp-20261013-hook_style-contradiction", Variant: "contradiction", Adopted: []string{"a=b", "c=d", "e=f"}, At: t0}
		}
		st.trim()
	}
	st.Seal()
	b, _ := json.Marshal(st)
	t.Logf("worst-case persisted state: %d bytes", len(b))
	if len(b) > 150_000 {
		t.Fatalf("state can grow too large: %d bytes", len(b))
	}
}
