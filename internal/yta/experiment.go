package yta

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Evolvable settings. These are the ONLY things MICROFLOW may change on its
// own, and only when named in the operator's allow-list. Credentials, OAuth
// scopes and security settings are never touched by this package.
const (
	SettingHookStyle  = "hook_style"
	SettingTitleStyle = "title_style"
	SettingPacing     = "pacing"
)

// variantLibrary holds the deterministic instruction text each variant
// injects into the script prompt. Every instruction is compatible with the
// existing prompt rules (5 scenes, title < 60 chars with a number, ...).
var variantLibrary = map[string][]struct{ Name, Text string }{
	SettingHookStyle: {
		{"question", "Scene 1 must be one surprising question about the topic (max 12 words)."},
		{"bold_claim", "Scene 1 must be one bold, specific claim stated as fact (max 12 words)."},
		{"number_stat", "Scene 1 must open with a concrete number or measurement (max 12 words)."},
		{"contradiction", "Scene 1 must contradict a common assumption about the topic (max 12 words)."},
	},
	SettingTitleStyle: {
		{"number_first", "The TITLE must start with its number (still under 60 characters)."},
		{"curiosity_gap", "The TITLE must open a curiosity gap without revealing the answer (still under 60 characters, still include a number)."},
		{"how_why", "The TITLE must start with 'How' or 'Why' (still under 60 characters, still include a number)."},
	},
	SettingPacing: {
		{"tight", "VOICE: 14-18 words per scene, shortest natural sentence, one idea per scene."},
		{"dense", "VOICE: 20-24 words per scene, one fact-rich sentence per scene."},
	},
}

// VariantText returns the injection text for setting=variant ("" if unknown).
func VariantText(setting, variant string) string {
	for _, v := range variantLibrary[setting] {
		if v.Name == variant {
			return v.Text
		}
	}
	return ""
}

// ValidVariant reports whether variant is a known value of setting. Used to
// validate every automatic change before it is applied.
func ValidVariant(setting, variant string) bool { return VariantText(setting, variant) != "" }

// Experiment is one controlled, single-variable test. Baseline and treatment
// videos are produced CONCURRENTLY (alternating assignment), so time-of-
// week and trend effects hit both arms equally.
type Experiment struct {
	ID         string    `json:"id"`
	Setting    string    `json:"setting"`
	Variant    string    `json:"variant"`
	Metric     Metric    `json:"metric"`
	Hypothesis string    `json:"hypothesis"`
	CreatedAt  time.Time `json:"createdAt"`
	MinPerArm  int       `json:"minPerArm"`
	EvalDays   int       `json:"evalDays"`
	MaxDays    int       `json:"maxDays"`
	Next       int       `json:"next"`
}

// Outcome is a verified experiment result kept for history.
type Outcome struct {
	ExperimentID    string    `json:"experimentId"`
	Setting         string    `json:"setting"`
	Variant         string    `json:"variant"`
	Metric          Metric    `json:"metric"`
	Result          string    `json:"result"` // improved | worse | no_effect | inconclusive
	BaselineMedian  float64   `json:"baselineMedian"`
	TreatmentMedian float64   `json:"treatmentMedian"`
	RelChange       float64   `json:"relativeChange"`
	Dominance       float64   `json:"dominance"` // share of treatment>baseline pairs
	NBaseline       int       `json:"nBaseline"`
	NTreatment      int       `json:"nTreatment"`
	At              time.Time `json:"at"`
}

// Change is one automatic settings change with the rollback record.
type Change struct {
	ID             string    `json:"id"`
	Setting        string    `json:"setting"`
	From           string    `json:"from"`
	To             string    `json:"to"`
	ExperimentID   string    `json:"experimentId"`
	Metric         Metric    `json:"metric"`
	BaselineMedian float64   `json:"baselineMedian"`
	Status         string    `json:"status"` // pending_verification | verified | rolled_back
	At             time.Time `json:"at"`
	ClosedAt       time.Time `json:"closedAt,omitempty"`
}

// Block prevents repeating a harmful or useless change.
type Block struct {
	Setting string    `json:"setting"`
	Variant string    `json:"variant,omitempty"` // "" = the whole setting is frozen
	Until   time.Time `json:"until"`
	Reason  string    `json:"reason"`
}

// Assignment pins the arm chosen for one workflow execution so retries of the
// analytics node never flip a video into the other arm.
type Assignment struct {
	Arm          string    `json:"arm"`
	ExperimentID string    `json:"experimentId,omitempty"`
	Setting      string    `json:"setting,omitempty"`
	Variant      string    `json:"variant,omitempty"`
	Adopted      []string  `json:"adopted,omitempty"`
	At           time.Time `json:"at"`
}

// State is everything MICROFLOW persists for this module. It is intentionally
// small: video history and metrics are re-fetched from the YouTube APIs.
type State struct {
	V        int                   `json:"v"`
	LastRun  time.Time             `json:"lastRun,omitempty"`
	QuotaDay string                `json:"quotaDay,omitempty"`
	QuotaUse int                   `json:"quotaUse,omitempty"`
	Settings map[string]string     `json:"settings,omitempty"`
	Videos   map[string]VideoMeta  `json:"videos,omitempty"`
	Active   *Experiment           `json:"active,omitempty"`
	History  []Outcome             `json:"history,omitempty"`
	Changes  []Change              `json:"changes,omitempty"`
	Blocks   []Block               `json:"blocks,omitempty"`
	Assign   map[string]Assignment `json:"assign,omitempty"`
	Report   map[string]any        `json:"report,omitempty"` // last compact report, reused between runs
	Sum      string                `json:"sum,omitempty"`    // integrity seal over the decision-critical fields
	Audit    []AuditEntry          `json:"audit,omitempty"`  // bounded decision history
	Runs     []RunRecord           `json:"runs,omitempty"`   // bounded per-run health history

	// Set on load only (never persisted): the state failed its integrity
	// check or had to be repaired, so this run must stay recommend-only.
	Tainted bool     `json:"-"`
	Repairs []string `json:"-"`
}

// Limits bounding persisted state (rule: keep only the minimum).
const (
	maxVideosKept  = 300
	maxHistoryKept = 20
	maxChangesKept = 20
	maxAssignKept  = 24
	maxBlocksKept  = 30
)

// EvoConfig holds the operator-controlled evolution policy.
type EvoConfig struct {
	Allow     map[string]bool // settings that may be changed automatically
	MinPerArm int
	EvalDays  int
	MaxDays   int
	MinEffect float64 // minimum relative change to call a difference (default 0.10)
	MinDomin  float64 // minimum pairwise dominance to call improved (default 0.70)
}

func (c EvoConfig) minPerArm() int {
	if c.MinPerArm < 3 {
		return 4
	}
	return c.MinPerArm
}
func (c EvoConfig) evalDays() int {
	if c.EvalDays < 2 {
		return 7
	}
	return c.EvalDays
}
func (c EvoConfig) maxDays() int {
	if c.MaxDays < c.evalDays()*2 {
		return 45
	}
	return c.MaxDays
}
func (c EvoConfig) minEffect() float64 {
	if c.MinEffect <= 0 {
		return 0.10
	}
	return c.MinEffect
}
func (c EvoConfig) minDomin() float64 {
	if c.MinDomin <= 0.5 {
		return 0.70
	}
	return c.MinDomin
}

// ParseAllow turns "hook_style, title_style" into a validated allow-set;
// unknown names are returned separately so they can be reported, never
// silently honoured.
func ParseAllow(raw string) (allow map[string]bool, unknown []string) {
	allow = map[string]bool{}
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(strings.ToLower(p))
		if p == "" {
			continue
		}
		if _, ok := variantLibrary[p]; ok {
			allow[p] = true
		} else {
			unknown = append(unknown, p)
		}
	}
	return
}

// ---- blocks ---------------------------------------------------------------

func (s *State) blocked(setting, variant string, now time.Time) bool {
	for _, b := range s.Blocks {
		if b.Setting == setting && now.Before(b.Until) && (b.Variant == "" || b.Variant == variant) {
			return true
		}
	}
	return false
}

func (s *State) addBlock(setting, variant string, d time.Duration, reason string, now time.Time) {
	s.Blocks = append(s.Blocks, Block{Setting: setting, Variant: variant, Until: now.Add(d), Reason: reason})
	s.trim()
}

func (s *State) pruneBlocks(now time.Time) {
	kept := s.Blocks[:0]
	for _, b := range s.Blocks {
		if now.Before(b.Until) {
			kept = append(kept, b)
		}
	}
	s.Blocks = kept
}

// changeRateExceeded: more than 2 automatic changes to one setting within 14
// days means the loop is thrashing; freeze instead of changing again.
func (s *State) changeRateExceeded(setting string, now time.Time) bool {
	n := 0
	for _, c := range s.Changes {
		if c.Setting == setting && now.Sub(c.At) < 14*24*time.Hour {
			n++
		}
	}
	return n >= 2
}

func (s *State) pendingVerification() *Change {
	for i := range s.Changes {
		if s.Changes[i].Status == "pending_verification" {
			return &s.Changes[i]
		}
	}
	return nil
}

// ---- arm assignment & guidance ---------------------------------------------

// Guidance is what the workflow injects into the script prompt.
type Guidance struct {
	Arm          string   `json:"arm"` // treatment | baseline | adopted | none
	ExperimentID string   `json:"experimentId,omitempty"`
	Setting      string   `json:"setting,omitempty"`
	Variant      string   `json:"variant,omitempty"`
	Adopted      []string `json:"adopted,omitempty"` // "setting=value" currently adopted
	Text         string   `json:"text"`
}

// AssignFor returns (and pins) the guidance for one execution. It is
// idempotent per execKey: a retry of the same execution gets exactly what
// was delivered the first time, so a video can never move between arms.
// Adopted settings apply to every video; the single experiment variable
// applies only to the treatment arm.
func (s *State) AssignFor(execKey string, cfg EvoConfig, now time.Time) Guidance {
	if a, ok := s.Assign[execKey]; ok {
		return guidanceFrom(a)
	}
	a := Assignment{Arm: "none", At: now}
	names := make([]string, 0, len(s.Settings))
	for k := range s.Settings {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		v := s.Settings[k]
		if !cfg.Allow[k] || !ValidVariant(k, v) {
			continue // never apply something that is not (or no longer) permitted/valid
		}
		a.Adopted = append(a.Adopted, k+"="+v)
	}
	if len(a.Adopted) > 0 {
		a.Arm = "adopted"
	}
	if s.Active != nil && cfg.Allow[s.Active.Setting] {
		arm := "treatment"
		if s.Active.Next%2 == 1 {
			arm = "baseline"
		}
		s.Active.Next++
		a.Arm, a.ExperimentID, a.Setting = arm, s.Active.ID, s.Active.Setting
		if arm == "treatment" {
			a.Variant = s.Active.Variant
		}
	}
	if s.Assign == nil {
		s.Assign = map[string]Assignment{}
	}
	s.Assign[execKey] = a
	if len(s.Assign) > maxAssignKept {
		s.trimAssign()
	}
	return guidanceFrom(a)
}

// Pinned returns the guidance that analyze delivered for this execution,
// without creating one. ok is false if analyze never ran (or failed), in
// which case the video must NOT be attributed to any arm.
func (s *State) Pinned(execKey string) (Guidance, bool) {
	a, ok := s.Assign[execKey]
	if !ok {
		return Guidance{Arm: "none"}, false
	}
	return guidanceFrom(a), true
}

func guidanceFrom(a Assignment) Guidance {
	g := Guidance{Arm: a.Arm, ExperimentID: a.ExperimentID, Setting: a.Setting, Variant: a.Variant, Adopted: a.Adopted}
	var lines []string
	for _, kv := range a.Adopted {
		if p := strings.SplitN(kv, "=", 2); len(p) == 2 {
			if t := VariantText(p[0], p[1]); t != "" {
				lines = append(lines, t)
			}
		}
	}
	if a.Variant != "" {
		if t := VariantText(a.Setting, a.Variant); t != "" {
			lines = append(lines, t)
		}
	}
	if len(lines) > 0 {
		g.Text = "PERFORMANCE GUIDANCE (from channel analytics; all earlier rules still apply): " + strings.Join(lines, " ")
	}
	return g
}

func (s *State) trimAssign() {
	// The map is only an idempotency cache: keep the newest half.
	type kv struct {
		k string
		t time.Time
	}
	all := make([]kv, 0, len(s.Assign))
	for k, v := range s.Assign {
		all = append(all, kv{k, v.At})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].t.Before(all[j].t) })
	for _, e := range all[:len(all)-maxAssignKept/2] {
		delete(s.Assign, e.k)
	}
}

// RecordVideo stores the minimal per-video context for later benchmarking.
func (s *State) RecordVideo(id string, m VideoMeta) {
	if id == "" {
		return
	}
	if s.Videos == nil {
		s.Videos = map[string]VideoMeta{}
	}
	s.Videos[id] = m
	if len(s.Videos) > maxVideosKept {
		type kv struct {
			id string
			at string
		}
		var all []kv
		for k, v := range s.Videos {
			all = append(all, kv{k, v.At})
		}
		sort.Slice(all, func(i, j int) bool { return all[i].at < all[j].at })
		for _, e := range all[:len(all)-maxVideosKept] {
			delete(s.Videos, e.id)
		}
	}
}

// ---- evaluation ------------------------------------------------------------

// Dominance is the share of (a,b) pairs with a > b (ties count half).
func Dominance(a, b []float64) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	w := 0.0
	for _, x := range a {
		for _, y := range b {
			switch {
			case x > y:
				w++
			case x == y:
				w += 0.5
			}
		}
	}
	return w / float64(len(a)*len(b))
}

// Event is a human-readable record of a decision (for monitoring).
type Event struct {
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

// Verify closes the loop: it evaluates the active experiment and any pending
// post-adoption verification using freshly re-fetched metrics, applies
// guarded changes and returns the decisions taken.
func (s *State) Verify(videos []Video, now time.Time, cfg EvoConfig) []Event {
	var ev []Event
	s.pruneBlocks(now)
	mature := func(v Video) bool { return v.AgeDays(now) >= float64(cfg.evalDays()) }

	// 1. experiment evaluation
	if e := s.Active; e != nil {
		var tr, bs []float64
		for _, v := range videos {
			if v.Meta.ExperimentID != e.ID || !mature(v) {
				continue
			}
			val, ok := v.Value(e.Metric, now)
			if !ok {
				continue
			}
			switch v.Meta.Arm {
			case "treatment":
				tr = append(tr, val)
			case "baseline":
				bs = append(bs, val)
			}
		}
		enough := len(tr) >= cfg.minPerArm() && len(bs) >= cfg.minPerArm()
		expired := now.Sub(e.CreatedAt) > time.Duration(cfg.maxDays())*24*time.Hour
		if enough || expired {
			o := Outcome{ExperimentID: e.ID, Setting: e.Setting, Variant: e.Variant, Metric: e.Metric,
				NBaseline: len(bs), NTreatment: len(tr), At: now, Result: "inconclusive"}
			if enough {
				o.BaselineMedian, o.TreatmentMedian = Median(bs), Median(tr)
				if o.BaselineMedian != 0 {
					o.RelChange = (o.TreatmentMedian - o.BaselineMedian) / absf(o.BaselineMedian)
				}
				o.Dominance = Dominance(tr, bs)
				switch {
				case o.RelChange >= cfg.minEffect() && o.Dominance >= cfg.minDomin():
					o.Result = "improved"
				case o.RelChange <= -cfg.minEffect() && o.Dominance <= 1-cfg.minDomin():
					o.Result = "worse"
				default:
					o.Result = "no_effect"
				}
			}
			ev = append(ev, s.conclude(o, now, cfg)...)
		}
	}

	// 2. verification of a previously adopted change
	if c := s.pendingVerification(); c != nil {
		token := c.Setting + "=" + c.To
		var vals []float64
		for _, v := range videos {
			if v.Meta.Arm != "adopted" || !hasToken(v.Meta.Variant, token) || !mature(v) {
				continue
			}
			if val, ok := v.Value(c.Metric, now); ok {
				vals = append(vals, val)
			}
		}
		if len(vals) >= cfg.minPerArm() {
			med := Median(vals)
			rel := 0.0
			if c.BaselineMedian != 0 {
				rel = (med - c.BaselineMedian) / absf(c.BaselineMedian)
			}
			c.ClosedAt = now
			if rel <= -cfg.minEffect() {
				c.Status = "rolled_back"
				s.Settings[c.Setting] = c.From
				if c.From == "" {
					delete(s.Settings, c.Setting)
				}
				s.addBlock(c.Setting, c.To, 45*24*time.Hour, "post-adoption verification worse than baseline", now)
				ev = append(ev, Event{"rollback", fmt.Sprintf("%s=%s rolled back to %q: median %s %.3f vs baseline %.3f (%+.0f%%)", c.Setting, c.To, c.From, c.Metric, med, c.BaselineMedian, rel*100)})
			} else {
				c.Status = "verified"
				ev = append(ev, Event{"verified", fmt.Sprintf("%s=%s held up after adoption (median %s %.3f vs baseline %.3f)", c.Setting, c.To, c.Metric, med, c.BaselineMedian)})
			}
		}
	}
	s.trim()
	return ev
}

func (s *State) conclude(o Outcome, now time.Time, cfg EvoConfig) []Event {
	e := s.Active
	s.Active = nil
	s.History = append(s.History, o)
	ev := []Event{{"experiment_concluded", fmt.Sprintf("%s %s=%s -> %s (n=%d/%d, rel %+.0f%%, dominance %.2f)", o.ExperimentID, o.Setting, o.Variant, o.Result, o.NTreatment, o.NBaseline, o.RelChange*100, o.Dominance)}}
	switch o.Result {
	case "improved":
		if !cfg.Allow[e.Setting] || s.changeRateExceeded(e.Setting, now) || s.blocked(e.Setting, e.Variant, now) {
			ev = append(ev, Event{"adoption_skipped", "improvement found but adoption not permitted/safe now (allow-list, change-rate or block)"})
			break
		}
		if !ValidVariant(e.Setting, e.Variant) {
			break
		}
		if s.Settings == nil {
			s.Settings = map[string]string{}
		}
		from := s.Settings[e.Setting]
		s.Settings[e.Setting] = e.Variant
		s.Changes = append(s.Changes, Change{ID: "chg-" + now.UTC().Format("20060102T150405") + "-" + e.Setting,
			Setting: e.Setting, From: from, To: e.Variant, ExperimentID: e.ID, Metric: e.Metric,
			BaselineMedian: o.BaselineMedian, Status: "pending_verification", At: now})
		ev = append(ev, Event{"adopted", fmt.Sprintf("%s: %q -> %q (rollback value kept: %q)", e.Setting, from, e.Variant, from)})
	case "worse":
		s.addBlock(e.Setting, e.Variant, 30*24*time.Hour, "experiment showed worse performance", now)
		worse := 0
		for i := len(s.History) - 1; i >= 0 && s.History[i].Setting == e.Setting && s.History[i].Result == "worse"; i-- {
			worse++
		}
		if worse >= 2 {
			s.addBlock(e.Setting, "", 60*24*time.Hour, "two consecutive worse experiments", now)
			ev = append(ev, Event{"setting_frozen", e.Setting + " frozen for 60 days after repeated harm"})
		}
	case "no_effect":
		s.addBlock(e.Setting, e.Variant, 14*24*time.Hour, "no measurable effect", now)
	}
	return ev
}

func (s *State) trim() {
	if n := len(s.Blocks); n > maxBlocksKept {
		// keep the blocks that last longest
		sort.SliceStable(s.Blocks, func(i, j int) bool { return s.Blocks[i].Until.After(s.Blocks[j].Until) })
		s.Blocks = append([]Block(nil), s.Blocks[:maxBlocksKept]...)
	}
	if n := len(s.History); n > maxHistoryKept {
		s.History = append([]Outcome(nil), s.History[n-maxHistoryKept:]...)
	}
	if n := len(s.Changes); n > maxChangesKept {
		s.Changes = append([]Change(nil), s.Changes[n-maxChangesKept:]...)
	}
}

// ---- planning --------------------------------------------------------------

// Plan is the outcome of choosing the next experiment.
type Plan struct {
	Proposed *Experiment `json:"proposed,omitempty"`
	Started  bool        `json:"started"`
	Reason   string      `json:"reason"`
}

// Next picks at most ONE new experiment (single variable) addressing the
// best-supported evolvable cause. It never starts one while another is
// running or a change awaits verification, and never without at least
// "weak" evidence. When the setting is not in the allow-list the experiment
// is only proposed, never started.
func (s *State) Next(sum Summary, haveAnalytics bool, now time.Time, cfg EvoConfig) Plan {
	if s.Active != nil {
		return Plan{Reason: "experiment " + s.Active.ID + " is still running"}
	}
	if s.pendingVerification() != nil {
		return Plan{Reason: "waiting for post-adoption verification before changing anything else"}
	}
	for _, d := range sum.Diagnoses {
		if d.Setting == "" || d.Level == LevelInsufficient {
			continue
		}
		metric := MEarlyViews
		switch d.Setting {
		case SettingHookStyle, SettingPacing:
			if !haveAnalytics {
				continue
			}
			metric = MAvgViewPct
		case SettingTitleStyle:
			if !haveAnalytics {
				metric = MViewsPerDay
			}
		}
		if s.blocked(d.Setting, "", now) || s.changeRateExceeded(d.Setting, now) {
			continue
		}
		variant := s.pickVariant(d.Setting, now)
		if variant == "" {
			continue
		}
		e := &Experiment{
			ID: fmt.Sprintf("exp-%s-%s-%s", now.UTC().Format("20060102"), d.Setting, variant), Setting: d.Setting, Variant: variant,
			Metric: metric, CreatedAt: now, MinPerArm: cfg.minPerArm(), EvalDays: cfg.evalDays(), MaxDays: cfg.maxDays(),
			Hypothesis: fmt.Sprintf("Changing only %s to %q improves %s versus the current default; suspected cause: %s (%s evidence).", d.Setting, variant, metric, d.Cause, d.Level),
		}
		if !cfg.Allow[d.Setting] {
			return Plan{Proposed: e, Reason: d.Setting + " is not in the evolve allow-list: recommendation only, nothing changed"}
		}
		s.Active = e
		return Plan{Proposed: e, Started: true, Reason: "started single-variable experiment"}
	}
	return Plan{Reason: "no diagnosis with sufficient evidence maps to a permitted, unblocked setting"}
}

func (s *State) pickVariant(setting string, now time.Time) string {
	cur := s.Settings[setting]
	for _, v := range variantLibrary[setting] {
		if v.Name == cur || s.blocked(setting, v.Name, now) {
			continue
		}
		tested := false
		for _, h := range s.History {
			if h.Setting == setting && h.Variant == v.Name && (h.Result == "worse" || h.Result == "no_effect") && now.Sub(h.At) < 60*24*time.Hour {
				tested = true
			}
		}
		if !tested {
			return v.Name
		}
	}
	return ""
}

func absf(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func hasToken(list, token string) bool {
	for _, p := range strings.Split(list, ",") {
		if p == token {
			return true
		}
	}
	return false
}

// AdoptedVariant renders the adopted-settings list stored on a video's meta.
func AdoptedVariant(adopted []string) string { return strings.Join(adopted, ",") }
