package yta

// Audience-retention curve analysis. A retention curve answers a different
// question from average view percentage: not "how much was watched" but
// "WHERE did people leave". It reports three different problems with three
// different fixes (adapted from youtube-agent-skill, MIT):
//
//	HOOK LEAK  what was lost in the opening
//	CLIFFS     short steep drops - a specific moment people left at
//	SLIDE      the steady bleed across the middle (pacing, not content)
//
// With the per-scene word counts recorded at publish time, each cliff is
// mapped to the scene being spoken at that moment, which makes the finding
// actionable ("scene 3 loses viewers in 4 of 6 videos") instead of abstract.

import (
	"fmt"
	"math"
	"sort"
)

// CurvePoint is one point of the audience-retention curve: X is the elapsed
// share of the video (0..1), Y the audience watch ratio (1.0 = everyone).
type CurvePoint struct{ X, Y float64 }

// Cliff is one steep drop.
type Cliff struct {
	AtRatio float64 `json:"atRatio"`
	Lost    float64 `json:"lostPercentPoints"`
	AtSec   float64 `json:"atSecond,omitempty"`
	Scene   int     `json:"scene,omitempty"` // 1-based, 0 = unknown
}

// Retention is the shape of one video's curve.
type Retention struct {
	Points      int     `json:"points"`
	StartPct    float64 `json:"startPct"`
	EndPct      float64 `json:"endPct"`
	HookLeakPct float64 `json:"hookLeakPct"` // percentage points lost by 10% of the runtime
	SlidePerTen float64 `json:"slidePerTenPct"`
	Cliffs      []Cliff `json:"cliffs,omitempty"`
}

// SceneAt maps an elapsed ratio to a 1-based scene index from the per-scene
// word counts (equal speaking speed assumed). 0 when the counts are unusable.
func SceneAt(ratio float64, sceneWords []int) int {
	total := 0
	for _, w := range sceneWords {
		if w <= 0 {
			return 0
		}
		total += w
	}
	if total == 0 || len(sceneWords) == 0 {
		return 0
	}
	acc := 0.0
	for i, w := range sceneWords {
		acc += float64(w) / float64(total)
		if ratio < acc || i == len(sceneWords)-1 {
			return i + 1
		}
	}
	return 0
}

// AnalyzeCurve reads one curve. ok is false when it has too few points to say
// anything. durSec may be 0 (then cliffs carry no seconds).
func AnalyzeCurve(pts []CurvePoint, durSec int, sceneWords []int) (Retention, bool) {
	var r Retention
	clean := make([]CurvePoint, 0, len(pts))
	for _, p := range pts {
		if p.X >= 0 && p.X <= 1.0001 && !math.IsNaN(p.Y) && p.Y >= 0 {
			clean = append(clean, p)
		}
	}
	sort.Slice(clean, func(i, j int) bool { return clean[i].X < clean[j].X })
	if len(clean) < 12 {
		return r, false
	}
	r.Points = len(clean)
	ys := make([]float64, len(clean))
	for i, p := range clean {
		ys[i] = p.Y * 100
	}
	r.StartPct, r.EndPct = ys[0], ys[len(ys)-1]
	at := func(x float64) float64 { // value at the nearest point
		best, bd := ys[0], 2.0
		for i, p := range clean {
			if d := math.Abs(p.X - x); d < bd {
				best, bd = ys[i], d
			}
		}
		return best
	}
	r.HookLeakPct = math.Max(0, r.StartPct-at(0.10))

	// Window drops (~5% of the runtime each) are far steadier than single-step
	// drops on a noisy 100-point curve.
	w := maxInt(2, len(clean)/20)
	type win struct {
		i    int
		lost float64
	}
	var wins []win
	var mid []float64
	for i := 0; i+w < len(clean); i++ {
		d := ys[i] - ys[i+w]
		wins = append(wins, win{i, d})
		if clean[i].X >= 0.10 {
			mid = append(mid, math.Max(0, d))
		}
	}
	if len(mid) > 0 {
		// median bleed per 5%-window, scaled to per 10% of the runtime
		r.SlidePerTen = Median(mid) * 2
	}
	typical := Median(mid)
	sort.Slice(wins, func(a, b int) bool { return wins[a].lost > wins[b].lost })
	var picked []int
	for _, c := range wins {
		if len(r.Cliffs) >= 3 {
			break
		}
		// a cliff is clearly steeper than the typical bleed and large in absolute terms;
		// the opening is reported as the hook leak, not as a cliff
		if c.lost < 4 || c.lost < 2.5*math.Max(typical, 0.5) || clean[c.i].X < 0.10 {
			continue
		}
		near := false
		for _, p := range picked {
			if absInt(p-c.i) < w*2 {
				near = true
				break
			}
		}
		if near {
			continue
		}
		picked = append(picked, c.i)
		cl := Cliff{AtRatio: math.Round(clean[c.i].X*1000) / 1000, Lost: math.Round(c.lost*10) / 10}
		if durSec > 0 {
			cl.AtSec = math.Round(clean[c.i].X*float64(durSec)*10) / 10
		}
		cl.Scene = SceneAt(clean[c.i].X, sceneWords)
		r.Cliffs = append(r.Cliffs, cl)
	}
	return r, true
}

func absInt(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

// RetentionPattern aggregates the curves of the videos that have one.
type RetentionPattern struct {
	Videos         int         `json:"videosWithCurve"`
	MedianHookLeak float64     `json:"medianHookLeakPct"`
	LeakyHooks     int         `json:"videosWithHookLeakOver35"`
	MedianSlide    float64     `json:"medianSlidePerTenPct"`
	CliffScenes    map[int]int `json:"videosWhoseBiggestCliffIsInScene,omitempty"`
	SceneKnown     int         `json:"videosWithSceneMapping"`
	TopScene       int         `json:"topCliffScene,omitempty"`
	TopSceneShare  float64     `json:"topCliffSceneShare,omitempty"`
}

// AggregateRetention summarises the retention curves of videos. nil when no
// video has one.
func AggregateRetention(videos []Video) *RetentionPattern {
	p := &RetentionPattern{CliffScenes: map[int]int{}}
	var leaks, slides []float64
	for _, v := range videos {
		c := v.Analytics
		if c == nil || c.Curve == nil {
			continue
		}
		p.Videos++
		leaks = append(leaks, c.Curve.HookLeakPct)
		slides = append(slides, c.Curve.SlidePerTen)
		if c.Curve.HookLeakPct > 35 {
			p.LeakyHooks++
		}
		if len(c.Curve.Cliffs) > 0 && c.Curve.Cliffs[0].Scene > 0 {
			p.SceneKnown++
			p.CliffScenes[c.Curve.Cliffs[0].Scene]++
		}
	}
	if p.Videos == 0 {
		return nil
	}
	p.MedianHookLeak = math.Round(Median(leaks)*10) / 10
	p.MedianSlide = math.Round(Median(slides)*10) / 10
	best, bestN := 0, 0
	for s, n := range p.CliffScenes {
		if n > bestN || (n == bestN && s < best) {
			best, bestN = s, n
		}
	}
	if p.SceneKnown > 0 {
		p.TopScene = best
		p.TopSceneShare = math.Round(float64(bestN)/float64(p.SceneKnown)*100) / 100
	}
	if len(p.CliffScenes) == 0 {
		p.CliffScenes = nil
	}
	return p
}

// RecommendFromRetention converts the aggregate into evidence-labelled advice.
// Levels follow the sample: fewer than 4 curves says nothing; 4-5 is weak; 6+
// with a consistent finding is moderate. Never "strong": a retention curve
// shows where people left, not why.
func RecommendFromRetention(p *RetentionPattern) []Recommendation {
	out := []Recommendation{}
	if p == nil || p.Videos < 4 {
		return out
	}
	level := LevelWeak
	if p.Videos >= 6 {
		level = LevelModerate
	}
	if p.MedianHookLeak >= 35 && float64(p.LeakyHooks)/float64(p.Videos) >= 0.5 {
		out = append(out, Recommendation{Area: "hook", Cause: "retention_curve_hook_leak", Evidence: level,
			Action:  fmt.Sprintf("Viewers leave in the opening: the median video loses %.0f points of its audience by 10%% of the runtime (%d of %d videos lose over 35). Make the first line state the most surprising concrete fact immediately and cut any setup.", p.MedianHookLeak, p.LeakyHooks, p.Videos),
			TestHow: "Change only the hook style; compare hook retention and average view percentage against concurrent baseline videos.", Experimentable: SettingHookStyle})
	}
	if p.TopScene >= 2 && p.SceneKnown >= 4 && p.TopSceneShare >= 0.5 {
		l := LevelWeak
		if p.SceneKnown >= 6 && p.TopSceneShare >= 0.6 {
			l = LevelModerate
		}
		out = append(out, Recommendation{Area: "pacing", Cause: "retention_curve_cliff_scene", Evidence: l,
			Action:  fmt.Sprintf("The biggest single drop falls in scene %d in %.0f%% of the %d videos that could be mapped. Make that scene shorter, move its reveal one scene earlier, or add a new fact before it so something new lands there.", p.TopScene, p.TopSceneShare*100, p.SceneKnown),
			TestHow: "Change only that scene's length or position; check whether the cliff moves or shrinks on the next videos.", Experimentable: SettingPacing})
	}
	if p.MedianSlide >= 12 {
		out = append(out, Recommendation{Area: "pacing", Cause: "retention_curve_slide", Evidence: level,
			Action:  fmt.Sprintf("Steady bleed across the middle: about %.0f points per tenth of the runtime. That is pacing, not one bad moment: shorten scenes and cut filler words rather than rewriting content.", p.MedianSlide),
			TestHow: "Use the pacing experiment (tight vs dense).", Experimentable: SettingPacing})
	}
	return out
}
