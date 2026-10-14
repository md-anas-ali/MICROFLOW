package yta

// Deterministic pre-publish quality engine for one Short: hook scoring, title +
// thumbnail pairing lint, script pacing review and SEO/metadata clean-up.
//
// The scoring ideas (five hook properties with a weakest-link verdict, the
// title/thumbnail pairing lint) are adapted from the open-source
// youtube-agent-skill project (MIT licence). They are re-implemented here in
// plain Go: no AI call, no network, no new dependency, no persisted data.
//
// HONESTY NOTE. These are heuristics measured on wording, not on audience
// behaviour. A low score is a reason to look again; a high score is not a
// promise of views. The numbers are used to (a) choose between alternative
// wordings the script writer already produced and (b) be recorded next to the
// real results of the video so later analytics runs can test whether they
// actually predict anything on THIS channel (see FeatureInsights).

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"unicode"
)

// ---- shared helpers ---------------------------------------------------------------

var (
	reWordTok   = regexp.MustCompile(`[a-z0-9'%$.]+`)
	reAlphaWord = regexp.MustCompile(`[a-z0-9']+`)
	reNumTok    = regexp.MustCompile(`\d[\d,.]*`)
	reKeyWord   = regexp.MustCompile(`[a-z]{4,}`)
)

func lcWords(t string) []string { return reWordTok.FindAllString(strings.ToLower(t), -1) }

var stopWords = map[string]bool{}

func init() {
	for _, w := range strings.Fields(`the a an of for to in on and or is are was were be been with this that it as at by from
		you your i my we our they them he she but so if then than there here what which who how when where why not no yes do does did
		just really very like about into over out up down can could will would should have has had get got make made go going went one two`) {
		stopWords[w] = true
	}
}

func keywordSet(texts ...string) map[string]bool {
	out := map[string]bool{}
	for _, t := range texts {
		for _, w := range reKeyWord.FindAllString(strings.ToLower(t), -1) {
			if !stopWords[w] {
				out[w] = true
			}
		}
	}
	return out
}

func numberSet(texts ...string) map[string]bool {
	out := map[string]bool{}
	for _, t := range texts {
		for _, n := range reNumTok.FindAllString(t, -1) {
			out[strings.Trim(n, ".,")] = true
		}
	}
	return out
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func cleanText(s string) string {
	s = strings.NewReplacer("<", "", ">", "", "\r", " ", "\n", " ", "\t", " ").Replace(s)
	return strings.Join(strings.Fields(s), " ")
}

// bannedOpeners are generic viral phrases the script prompt already forbids;
// the review flags them again because free models ignore the rule often.
var bannedPhrases = []string{"did you know", "here's a fact", "here is a fact", "you won't believe", "you wont believe",
	"this will blow your mind", "wait for it", "mind blown", "crazy fact", "let that sink in", "will shock you", "scientists say"}

func hasBannedPhrase(t string) string {
	l := strings.ToLower(t)
	for _, p := range bannedPhrases {
		if strings.Contains(l, p) {
			return p
		}
	}
	return ""
}

// ---- hook scoring --------------------------------------------------------------------

var (
	hookFiller = toSet("basically actually literally just really very so kind sort like guys hey welcome today video subscribe channel")
	hookVague  = toSet("amazing incredible insane crazy huge massive game changer secret powerful ultimate best revolutionary mind blowing unbelievable")

	reConcrete  = regexp.MustCompile(`(?i)\b(\d[\d,.]*\s?(%|k|m|x|s|h)?|\$\d|\d+\s?(second|minute|hour|day|week|month|year)s?)\b|\d%`)
	reYou       = regexp.MustCompile(`(?i)\b(you|your|you're|youre|yourself)\b`)
	reStake     = regexp.MustCompile(`(?i)\b(lose|lost|wasting|waste|quit|fail|broke|cost|risk|before|stop|never|die|dying|dead|killed|vanished|disappeared|banned|forbidden)\b`)
	reCuriosity = regexp.MustCompile(`(?i)\b(why|how|what|which|until|before|but|nobody|almost|except|reason|actually|secret|hidden|really)\b`)
	reClosed    = regexp.MustCompile(`(?i)\b(because|so that|which means)\b`)
)

func toSet(s string) map[string]bool {
	m := map[string]bool{}
	for _, w := range strings.Fields(s) {
		m[w] = true
	}
	return m
}

// HookScore is the five-property score of one hook line (0-100 each).
type HookScore struct {
	Text        string `json:"text"`
	Specificity int    `json:"specificity"`
	Address     int    `json:"address"`
	Stakes      int    `json:"stakes"`
	Curiosity   int    `json:"curiosity"`
	Brevity     int    `json:"brevity"`
	Verdict     int    `json:"verdict"`
	Band        string `json:"band"` // STRONG | WORKABLE | WEAK
	Formula     string `json:"formula"`
	Weakest     string `json:"weakest"`
	Fix         string `json:"fix"`
}

var hookFix = map[string]string{
	"specificity": "swap one adjective for a number, a name or a date",
	"address":     "speak to the viewer directly (you / your) early in the line",
	"stakes":      "name what it costs, risks or ends",
	"curiosity":   "leave a gap the next scene closes instead of answering inside the line",
	"brevity":     "keep scene 1 to roughly 5-14 spoken words",
}

// ScoreHook scores a Shorts hook (the first spoken line). Verdict is 60% the
// mean of the five properties and 40% the weakest one: a hook with one dead
// property leaks at that property and an average hides it.
func ScoreHook(text string) HookScore {
	t := cleanText(text)
	h := HookScore{Text: t}
	h.Specificity = hookSpecificity(t)
	h.Address = hookAddress(t)
	h.Stakes = hookStakes(t)
	h.Curiosity = hookCuriosity(t)
	h.Brevity = hookBrevity(t)
	vals := map[string]int{"specificity": h.Specificity, "address": h.Address, "stakes": h.Stakes, "curiosity": h.Curiosity, "brevity": h.Brevity}
	names := []string{"specificity", "address", "stakes", "curiosity", "brevity"}
	sum, min := 0, 101
	for _, n := range names {
		sum += vals[n]
		if vals[n] < min {
			min, h.Weakest = vals[n], n
		}
	}
	h.Verdict = int(math.Round(0.6*float64(sum)/5 + 0.4*float64(min)))
	switch {
	case h.Verdict >= 72:
		h.Band = "STRONG"
	case h.Verdict >= 55:
		h.Band = "WORKABLE"
	default:
		h.Band = "WEAK"
	}
	h.Formula, _ = ClassifyFormula(t)
	h.Fix = hookFix[h.Weakest]
	return h
}

func hookSpecificity(t string) int {
	w := lcWords(t)
	if len(w) == 0 {
		return 0
	}
	nums := len(reConcrete.FindAllString(t, -1))
	vague, filler := 0, 0
	for _, x := range w {
		if hookVague[x] {
			vague++
		}
		if hookFiller[x] {
			filler++
		}
	}
	s := 34 + nums*22 - vague*16 - filler*5
	if hasBannedPhrase(t) != "" {
		s -= 30 // the exact openers viewers have heard a thousand times
	}
	caps := 0
	for i, f := range strings.Fields(t) {
		if i == 0 {
			continue
		}
		if r := []rune(f); len(r) > 0 && unicode.IsUpper(r[0]) {
			caps++
		}
	}
	s += int(math.Min(18, float64(6*caps)))
	return clampInt(s, 0, 100)
}

func hookAddress(t string) int {
	n := len(reYou.FindAllString(t, -1))
	f := strings.Fields(t)
	if len(f) > 6 {
		f = f[:6]
	}
	first := 0
	if reYou.MatchString(strings.Join(f, " ")) {
		first = 30
	}
	// A fact channel rarely says "you" in scene 1; a concrete subject is the
	// equivalent pull, so a specific named thing earns a floor.
	floor := 0
	if hookSpecificity(t) >= 60 {
		floor = 45
	}
	return clampInt(maxInt(26+n*20+first, floor), 0, 100)
}

func hookStakes(t string) int {
	n := len(reStake.FindAllString(t, -1))
	add := 0
	if reConcrete.MatchString(t) {
		add = 14
	}
	return clampInt(22+n*26+add, 0, 100)
}

func hookCuriosity(t string) int {
	n := len(reCuriosity.FindAllString(t, -1))
	q := 0
	if strings.HasSuffix(strings.TrimSpace(t), "?") {
		q = 18
	}
	closed := 0
	if reClosed.MatchString(t) {
		closed = -18
	}
	return clampInt(24+n*17+q+closed, 0, 100)
}

func hookBrevity(t string) int {
	n := len(lcWords(t))
	switch {
	case n == 0:
		return 0
	case n >= 5 && n <= 14:
		return 100
	case n < 5:
		return maxInt(30, 100-(5-n)*14)
	default:
		return maxInt(10, 100-(n-14)*8)
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ---- formula classification ----------------------------------------------------------

var formulaRe [][]*regexp.Regexp

func initFormulaRe() {
	if formulaRe != nil {
		return
	}
	formulaRe = make([][]*regexp.Regexp, len(hookFormulas))
	for i, f := range hookFormulas {
		for _, p := range f.Patterns {
			if re, err := regexp.Compile("(?i)" + p); err == nil {
				formulaRe[i] = append(formulaRe[i], re)
			}
		}
	}
}

// ClassifyFormula names the wording pattern of a hook or title ("Unclassified"
// when nothing matches, which usually means a summary rather than a hook) and
// how many of that formula's patterns matched. A judgement about the words on
// screen, not a claim about why a video worked.
func ClassifyFormula(text string) (string, int) {
	initFormulaRe()
	best, hits := "Unclassified", 0
	for i, f := range hookFormulas {
		n := 0
		for _, re := range formulaRe[i] {
			if re.MatchString(text) {
				n++
			}
		}
		if n > hits {
			best, hits = f.Name, n
		}
	}
	return best, hits
}

// ---- title + thumbnail lint ----------------------------------------------------------

// Issue is one lint finding.
type Issue struct {
	Code string `json:"code"`
	Msg  string `json:"message"`
}

// TitleLint is the lint result of a title (optionally with thumbnail text).
type TitleLint struct {
	Title  string   `json:"title"`
	Chars  int      `json:"chars"`
	Score  int      `json:"score"`
	Issues []Issue  `json:"issues"`
	Good   []string `json:"good,omitempty"`
	Notes  []string `json:"notes,omitempty"` // informational, not scored
}

const (
	titleDesktop = 60
	titleMobile  = 40
	titleHard    = 100
)

var titleVague = toSet("amazing incredible insane crazy huge massive ultimate best powerful secret revolutionary mindblowing epic perfect complete everything")

// LintTitle checks a title and, when given, the thumbnail text that will sit
// next to it. The pairing is the unit: repeating the title on the thumbnail
// wastes half the click surface.
func LintTitle(title, thumb string) TitleLint {
	t := cleanText(title)
	n := len([]rune(t))
	r := TitleLint{Title: t, Chars: n}
	add := func(code, msg string) { r.Issues = append(r.Issues, Issue{code, msg}) }
	switch {
	case n == 0:
		add("empty", "title is empty")
	case n > titleHard:
		add("length", fmt.Sprintf("%d characters - YouTube's hard limit is %d", n, titleHard))
	case n > titleDesktop:
		add("length", fmt.Sprintf("%d characters - search and the feed cut near %d", n, titleDesktop))
	default:
		r.Good = append(r.Good, fmt.Sprintf("%d characters, inside the %d-character cut", n, titleDesktop))
	}
	if n > titleMobile {
		r.Notes = append(r.Notes, fmt.Sprintf("a small mobile feed may show only the first ~%d characters: keep the subject at the front", titleMobile))
	}
	caps := 0
	for _, w := range strings.Fields(t) {
		if len([]rune(w)) > 2 && w == strings.ToUpper(w) && strings.ToLower(w) != w {
			caps++
		}
	}
	if caps > 2 {
		add("shouting", fmt.Sprintf("%d all-caps words - two is the ceiling before it reads as spam", caps))
	}
	var vague []string
	for _, w := range reAlphaWord.FindAllString(strings.ToLower(t), -1) {
		if titleVague[w] {
			vague = append(vague, w)
		}
	}
	if len(vague) > 0 {
		add("vague", strings.Join(dedupeStrings(vague), ", ")+" - swap for a number, a name or a date")
	}
	if nums := reNumTok.FindAllString(t, -1); len(nums) > 0 {
		r.Good = append(r.Good, "carries a concrete figure")
	} else {
		add("no-number", "no number, date or name - the most reliable single fix")
	}
	if strings.HasSuffix(t, "?") {
		r.Good = append(r.Good, "open question in the title")
	}
	front := 0
	for i, w := range reAlphaWord.FindAllString(strings.ToLower(t), -1) {
		if i >= 3 {
			break
		}
		if !stopWords[w] {
			front++
		}
	}
	if n > 0 && front == 0 {
		add("front-load", "the first three words are all filler - move the subject forward")
	}
	if h := hasBannedPhrase(t); h != "" {
		add("cliche", fmt.Sprintf("generic viral phrase %q", h))
	}
	if th := cleanText(thumb); th != "" {
		tw := keywordSetLoose(t)
		hw := keywordSetLoose(th)
		var shared []string
		for w := range hw {
			if tw[w] {
				shared = append(shared, w)
			}
		}
		sort.Strings(shared)
		if len(shared) > 0 {
			add("duplicate", "thumbnail repeats the title on "+strings.Join(shared, ", ")+" - it should say what the title does not")
		} else {
			r.Good = append(r.Good, "thumbnail and title carry different words")
		}
		if k := len(strings.Fields(th)); k > 4 {
			add("thumb-length", fmt.Sprintf("%d words on the thumbnail - three is the ceiling at feed size", k))
		}
	}
	r.Score = clampInt(100-14*len(r.Issues)+4*len(r.Good), 0, 100)
	return r
}

func keywordSetLoose(s string) map[string]bool {
	out := map[string]bool{}
	for _, w := range reAlphaWord.FindAllString(strings.ToLower(s), -1) {
		if !stopWords[w] && len(w) > 1 {
			out[w] = true
		}
	}
	return out
}

func dedupeStrings(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// ---- thumbnail text ---------------------------------------------------------------------

var reThumbBad = regexp.MustCompile(`[^A-Z0-9 '?!%$&+-]`)

// CleanThumbText normalises overlay text for the thumbnail: upper case, at
// most three words, at most 22 characters, and only characters that are safe
// to hand to a shell/ffmpeg without escaping.
func CleanThumbText(s string) string {
	s = reThumbBad.ReplaceAllString(strings.ToUpper(cleanText(s)), "")
	f := strings.Fields(s)
	if len(f) > 3 {
		f = f[:3]
	}
	s = strings.Join(f, " ")
	for len(s) > 22 && len(f) > 1 {
		f = f[:len(f)-1]
		s = strings.Join(f, " ")
	}
	if len(s) > 22 {
		s = s[:22]
	}
	return strings.TrimSpace(s)
}

func deriveThumbText(title, topic string) string {
	tw := keywordSetLoose(title)
	var picks []string
	for _, w := range regexp.MustCompile(`[a-z0-9]+`).FindAllString(strings.ToLower(topic), -1) {
		if len(w) >= 4 && !stopWords[w] && !tw[w] {
			picks = append(picks, w)
		}
		if len(picks) == 2 {
			break
		}
	}
	return CleanThumbText(strings.Join(picks, " "))
}

// ---- script review ------------------------------------------------------------------------

// ScriptReport is a pacing and completeness review of the spoken script.
type ScriptReport struct {
	SceneWords []int    `json:"sceneWords"`
	TotalWords int      `json:"totalWords"`
	EstSeconds float64  `json:"estimatedSeconds"`
	Score      int      `json:"score"`
	Issues     []string `json:"issues"`
}

var (
	reCTA     = regexp.MustCompile(`(?i)\b(follow|subscribe|comment|share|tell me|let me know|more like this)\b`)
	reAnyNum  = regexp.MustCompile(`\d`)
	spokenWPS = 2.9 // words per second at the production TTS rate (about 175 wpm)
)

// ReviewScript checks pacing, concreteness and the closing request. It cannot
// judge truth: fact verification is a separate step in the workflow.
func ReviewScript(voices []string) ScriptReport {
	r := ScriptReport{Issues: []string{}}
	minW, maxW := 1<<30, 0
	hasNum := false
	for i, v := range voices {
		n := len(strings.Fields(v))
		r.SceneWords = append(r.SceneWords, n)
		r.TotalWords += n
		if n < minW {
			minW = n
		}
		if n > maxW {
			maxW = n
		}
		if reAnyNum.MatchString(v) {
			hasNum = true
		}
		if p := hasBannedPhrase(v); p != "" {
			r.Issues = append(r.Issues, fmt.Sprintf("scene %d uses the generic phrase %q", i+1, p))
		}
		switch {
		case i == 0 && n > 14:
			r.Issues = append(r.Issues, fmt.Sprintf("scene 1 (the hook) has %d words; 5-14 holds attention best", n))
		case i > 0 && (n < 12 || n > 26):
			r.Issues = append(r.Issues, fmt.Sprintf("scene %d has %d words; 14-24 keeps the pace even", i+1, n))
		}
	}
	if len(voices) >= 3 && minW > 0 && float64(maxW)/float64(minW) > 2.0 {
		r.Issues = append(r.Issues, "scene lengths are very uneven; the longest is more than twice the shortest")
	}
	if len(voices) > 0 && !hasNum {
		r.Issues = append(r.Issues, "no concrete number or measurement anywhere in the script")
	}
	if len(voices) > 0 && !reCTA.MatchString(voices[len(voices)-1]) {
		r.Issues = append(r.Issues, "the closing scene has no follow/comment request")
	}
	r.EstSeconds = math.Round(float64(r.TotalWords)/spokenWPS*10) / 10
	if r.EstSeconds > 58 {
		r.Issues = append(r.Issues, fmt.Sprintf("estimated runtime %.0fs is close to or over the 60s Shorts limit", r.EstSeconds))
	} else if len(voices) > 0 && r.EstSeconds < 22 {
		r.Issues = append(r.Issues, fmt.Sprintf("estimated runtime %.0fs is short; 30-45s usually carries more watch time", r.EstSeconds))
	}
	r.Score = clampInt(100-12*len(r.Issues), 0, 100)
	return r
}

// ---- SEO / metadata ---------------------------------------------------------------------

// MetaIn / MetaOut: deterministic metadata clean-up (no invented facts).
type MetaIn struct {
	Title, Topic, Description string
	Tags                      []string
}

type MetaOut struct {
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	Hashtags    []string `json:"hashtags"`
	Changes     []string `json:"changes"`
}

var (
	reHashTok  = regexp.MustCompile(`#\S+`)
	reTagBad   = regexp.MustCompile(`[^a-z0-9 '-]`)
	reTopicTok = regexp.MustCompile(`[a-z0-9]+`)
)

func shortenTitle(t string, limit int) string {
	r := []rune(t)
	if len(r) <= limit {
		return t
	}
	cut := string(r[:limit])
	if i := strings.LastIndex(cut, " "); i > limit/2 {
		cut = cut[:i]
	}
	cut = strings.TrimRight(cut, " ,;:-–—&+")
	words := strings.Fields(cut)
	for len(words) > 2 && stopWords[strings.ToLower(words[len(words)-1])] {
		words = words[:len(words)-1]
	}
	return strings.Join(words, " ")
}

// OptimizeMeta tidies the title, description, tags and hashtags for search
// and feed display. It only trims, reorders, de-duplicates and adds words that
// already exist in the topic slug, so it cannot introduce a false claim.
func OptimizeMeta(in MetaIn) MetaOut {
	out := MetaOut{Changes: []string{}}
	// title
	title := cleanText(in.Title)
	if title != in.Title && strings.TrimSpace(in.Title) != title {
		out.Changes = append(out.Changes, "title: removed markup/extra whitespace")
	}
	if n := len([]rune(title)); n > titleDesktop {
		if s := shortenTitle(title, titleDesktop); len([]rune(s)) >= 20 {
			out.Changes = append(out.Changes, fmt.Sprintf("title: shortened from %d to %d characters at a word boundary", n, len([]rune(s))))
			title = s
		}
	}
	if n := len([]rune(title)); n > titleHard {
		title = string([]rune(title)[:titleHard])
		out.Changes = append(out.Changes, "title: hard-cut to 100 characters")
	}
	out.Title = title

	// topic keywords (from the slug the writer produced for this very video)
	var topicWords []string
	for _, w := range reTopicTok.FindAllString(strings.ToLower(in.Topic), -1) {
		if len(w) >= 3 && !stopWords[w] {
			topicWords = append(topicWords, w)
		}
	}
	topicWords = dedupeStrings(topicWords)
	if len(topicWords) > 4 {
		topicWords = topicWords[:4]
	}

	// tags
	seen := map[string]bool{}
	var tags []string
	push := func(t string) {
		t = strings.TrimSpace(reTagBad.ReplaceAllString(strings.ToLower(strings.TrimLeft(strings.TrimSpace(t), "#")), ""))
		t = strings.Join(strings.Fields(t), " ")
		if len(t) < 2 || len(t) > 30 || seen[t] {
			return
		}
		seen[t] = true
		tags = append(tags, t)
	}
	if len(topicWords) > 0 {
		push(strings.Join(topicWords, " "))
	}
	for _, t := range in.Tags {
		push(t)
	}
	// tags that mention a topic word first (stable), then the rest
	sort.SliceStable(tags, func(i, j int) bool {
		return tagHasAny(tags[i], topicWords) && !tagHasAny(tags[j], topicWords)
	})
	var kept []string
	total := 0
	for _, t := range tags {
		if total+len(t)+1 > 450 {
			break
		}
		kept = append(kept, t)
		total += len(t) + 1
	}
	if len(kept) != len(in.Tags) {
		out.Changes = append(out.Changes, fmt.Sprintf("tags: cleaned/de-duplicated/ordered (%d -> %d)", len(in.Tags), len(kept)))
	}
	out.Tags = kept

	// hashtags: #shorts plus up to four single-word topical tags. YouTube shows
	// the first three above the title and ignores ALL hashtags past fifteen.
	tagsForHash := []string{}
	for _, t := range kept {
		if len(strings.Fields(t)) > 2 {
			continue // long phrases make unreadable hashtags
		}
		h := strings.ReplaceAll(t, " ", "")
		h = strings.ReplaceAll(strings.ReplaceAll(h, "'", ""), "-", "")
		if h != "" && h != "shorts" && len(h) <= 24 && !containsStr(tagsForHash, h) {
			tagsForHash = append(tagsForHash, h)
		}
		if len(tagsForHash) == 4 {
			break
		}
	}
	out.Hashtags = append([]string{"shorts"}, tagsForHash...)

	// description
	desc := strings.TrimSpace(reHashTok.ReplaceAllString(cleanText(in.Description), ""))
	desc = strings.Join(strings.Fields(desc), " ")
	if desc == "" {
		desc = title
		out.Changes = append(out.Changes, "description: was empty, used the title")
	}
	if desc == "" {
		desc = "Short"
	}
	head := strings.ToLower(desc)
	if len(head) > 120 {
		head = head[:120]
	}
	if len(topicWords) > 0 && !tagHasAny(head, topicWords) {
		phrase := strings.Join(topicWords[:minInt(2, len(topicWords))], " ")
		desc = capitalizeFirst(phrase) + ": " + desc
		out.Changes = append(out.Changes, "description: led with the topic keywords so search sees them in the first 120 characters")
	}
	if !strings.ContainsAny(desc[len(desc)-1:], ".!?") {
		desc += "."
	}
	var hs []string
	for _, h := range out.Hashtags {
		hs = append(hs, "#"+h)
	}
	full := desc + "\n\n" + strings.Join(hs, " ")
	for len(full) > 4800 && len(desc) > 50 {
		desc = strings.TrimSpace(desc[:len(desc)-50])
		full = desc + "\n\n" + strings.Join(hs, " ")
	}
	out.Description = full
	return out
}

func tagHasAny(s string, words []string) bool {
	for _, w := range words {
		if strings.Contains(s, w) {
			return true
		}
	}
	return false
}

func containsStr(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func capitalizeFirst(s string) string {
	r := []rune(s)
	if len(r) == 0 {
		return s
	}
	r[0] = unicode.ToUpper(r[0])
	return string(r)
}

// ---- one-call improvement of a finished script -------------------------------------------------

// ScriptIn is everything the gate needs about one finished script.
type ScriptIn struct {
	Title, Topic, Hook, Description string
	Tags                            []string
	Voices                          []string // spoken line of each scene, in order
	HookOptions, TitleOptions       []string // alternatives produced by the writer in the same response
	ThumbText                       string
}

// ImproveOpts controls what may be changed.
type ImproveOpts struct {
	KeepHook  bool // a hook_style experiment/adopted setting owns scene 1
	KeepTitle bool // a title_style experiment/adopted setting owns the title
	MinGain   int  // minimum score gain before an alternative replaces the original
	Apply     bool // false = report only
}

// HookCandidate is one alternative that was scored.
type HookCandidate struct {
	Text     string `json:"text"`
	Score    int    `json:"score"`
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

// ThumbReport reviews the thumbnail overlay text.
type ThumbReport struct {
	Text   string   `json:"text"`
	Issues []string `json:"issues,omitempty"`
}

// QualityReport is the evidence for every decision the gate took.
type QualityReport struct {
	Status         string          `json:"status"`
	Overall        int             `json:"overall"`
	Hook           HookScore       `json:"hook"`
	HookBefore     int             `json:"hookScoreBefore"`
	HookReplaced   bool            `json:"hookReplaced"`
	HookNote       string          `json:"hookNote,omitempty"`
	HookCandidates []HookCandidate `json:"hookCandidates,omitempty"`
	Title          TitleLint       `json:"title"`
	TitleBefore    int             `json:"titleScoreBefore"`
	TitleReplaced  bool            `json:"titleReplaced"`
	TitleNote      string          `json:"titleNote,omitempty"`
	Thumb          ThumbReport     `json:"thumbnail"`
	Script         ScriptReport    `json:"script"`
	Meta           []string        `json:"metadataChanges"`
	Warnings       []string        `json:"warnings"`
}

// ScriptOut is the (possibly improved) script plus its report.
type ScriptOut struct {
	Title, Hook, Description, ThumbText string
	Tags                                []string
	Voices                              []string
	Report                              QualityReport
}

func normHookCandidate(s string) string {
	s = cleanText(s)
	s = strings.Trim(s, "\"“”")
	if s == "" {
		return s
	}
	s = capitalizeFirst(s)
	if !strings.ContainsAny(s[len(s)-1:], ".!?") {
		if reCuriosity.MatchString(strings.Fields(s)[0]) || regexp.MustCompile(`(?i)^(why|how|what|who|when|where|which|is|are|can|does|did|could)\b`).MatchString(s) {
			s += "?"
		} else {
			s += "."
		}
	}
	return s
}

// ImproveScript scores the hook, title, thumbnail text, pacing and metadata of
// one finished script and, when allowed, swaps in a better alternative that
// the writer itself produced. Safety rules for every swap:
//   - it may not introduce a number that is not already in the script/title
//     (a new statistic would be an unverified claim);
//   - it must share a topic keyword with the script (stay on subject);
//   - it must beat the original by MinGain points (no churn for noise);
//   - when a hook/title experiment owns that element it is reported, not changed.
func ImproveScript(in ScriptIn, o ImproveOpts) ScriptOut {
	if o.MinGain <= 0 {
		o.MinGain = 8
	}
	out := ScriptOut{Title: cleanText(in.Title), Hook: cleanText(in.Hook), Description: in.Description,
		Tags: append([]string(nil), in.Tags...), Voices: append([]string(nil), in.Voices...)}
	rep := QualityReport{Status: "ok", Meta: []string{}, Warnings: []string{}}
	if len(out.Voices) > 0 && strings.TrimSpace(out.Voices[0]) != "" {
		out.Hook = cleanText(out.Voices[0]) // the spoken scene-1 line IS the hook
	}

	corpus := append([]string{out.Title, in.Description, in.Topic}, out.Voices...)
	nums := numberSet(corpus...)
	subject := keywordSet(in.Topic, out.Title, strings.Join(out.Voices, " "))

	passesSafety := func(c string) (bool, string) {
		for _, n := range reNumTok.FindAllString(c, -1) {
			if !nums[strings.Trim(n, ".,")] {
				return false, "introduces a number that is not in the script"
			}
		}
		if len(subject) > 0 {
			ok := false
			for w := range keywordSet(c) {
				if subject[w] {
					ok = true
					break
				}
			}
			if !ok {
				return false, "shares no subject keyword with the script"
			}
		}
		if p := hasBannedPhrase(c); p != "" {
			return false, "uses the generic phrase " + p
		}
		return true, ""
	}

	// ---- hook
	cur := ScoreHook(out.Hook)
	rep.HookBefore = cur.Verdict
	hookGain := o.MinGain
	if hasBannedPhrase(out.Hook) != "" && hookGain > 3 {
		hookGain = 3 // a generic opener is a known problem: accept a smaller improvement
	}
	best := cur
	bestText := ""
	seen := map[string]bool{strings.ToLower(out.Hook): true}
	for _, raw := range in.HookOptions {
		c := normHookCandidate(raw)
		if c == "" || seen[strings.ToLower(c)] {
			continue
		}
		seen[strings.ToLower(c)] = true
		hc := HookCandidate{Text: c}
		if n := len(lcWords(c)); n < 4 || n > 14 {
			hc.Reason = fmt.Sprintf("%d words (needs 4-14)", n)
		} else if ok, why := passesSafety(c); !ok {
			hc.Reason = why
		} else {
			s := ScoreHook(c)
			hc.Score = s.Verdict
			if s.Verdict >= cur.Verdict+hookGain && s.Verdict > best.Verdict {
				best, bestText = s, c
			}
		}
		if hc.Score == 0 && hc.Reason == "" {
			hc.Score = ScoreHook(c).Verdict
		}
		rep.HookCandidates = append(rep.HookCandidates, hc)
	}
	switch {
	case bestText == "":
		rep.HookNote = "kept the original hook"
		if cur.Band == "WEAK" {
			rep.Warnings = append(rep.Warnings, "hook scored WEAK and no safe, better alternative was available: "+cur.Fix)
		}
	case o.KeepHook:
		rep.HookNote = "a better alternative exists but a hook_style setting owns scene 1; left unchanged"
	case !o.Apply:
		rep.HookNote = "report-only mode: a better alternative exists but was not applied"
	default:
		for i := range rep.HookCandidates {
			if rep.HookCandidates[i].Text == bestText {
				rep.HookCandidates[i].Accepted = true
			}
		}
		out.Hook = bestText
		if len(out.Voices) > 0 {
			out.Voices[0] = bestText
		}
		rep.HookReplaced = true
		rep.HookNote = fmt.Sprintf("replaced scene 1: %d -> %d", cur.Verdict, best.Verdict)
		cur = best
	}
	rep.Hook = cur

	// ---- title (+ thumbnail pairing)
	thumb := CleanThumbText(in.ThumbText)
	meta0 := OptimizeMeta(MetaIn{Title: out.Title, Topic: in.Topic, Description: in.Description, Tags: in.Tags})
	out.Title = meta0.Title
	curT := LintTitle(out.Title, thumb)
	rep.TitleBefore = curT.Score
	bestT, bestTitle := curT, ""
	seenT := map[string]bool{strings.ToLower(out.Title): true}
	for _, raw := range in.TitleOptions {
		c := shortenTitle(cleanText(raw), titleDesktop)
		if c == "" || seenT[strings.ToLower(c)] {
			continue
		}
		seenT[strings.ToLower(c)] = true
		if ok, _ := passesSafety(c); !ok {
			continue
		}
		l := LintTitle(c, thumb)
		if l.Score >= curT.Score+o.MinGain && l.Score > bestT.Score {
			bestT, bestTitle = l, c
		}
	}
	switch {
	case bestTitle == "":
		rep.TitleNote = "kept the original title"
	case o.KeepTitle:
		rep.TitleNote = "a better title exists but a title_style setting owns the title; left unchanged"
	case !o.Apply:
		rep.TitleNote = "report-only mode: a better title exists but was not applied"
	default:
		out.Title = bestTitle
		rep.TitleReplaced = true
		rep.TitleNote = fmt.Sprintf("replaced title: %d -> %d", curT.Score, bestT.Score)
		curT = bestT
	}
	// thumbnail text must say something the title does not
	if thumb == "" {
		thumb = deriveThumbText(out.Title, in.Topic)
	}
	if thumb != "" {
		curT = LintTitle(out.Title, thumb)
		for _, is := range curT.Issues {
			if is.Code == "duplicate" || is.Code == "thumb-length" {
				rep.Thumb.Issues = append(rep.Thumb.Issues, is.Msg)
			}
		}
		if len(rep.Thumb.Issues) > 0 {
			if alt := deriveThumbText(out.Title, in.Topic); alt != "" && alt != thumb {
				thumb = alt
				rep.Thumb.Issues = append(rep.Thumb.Issues, "replaced the overlay text with topic words the title does not use")
				curT = LintTitle(out.Title, thumb)
			} else {
				thumb = "" // better no overlay than a duplicate of the title
				rep.Thumb.Issues = append(rep.Thumb.Issues, "no overlay text: nothing distinct from the title was available")
				curT = LintTitle(out.Title, "")
			}
		}
	}
	out.ThumbText = thumb
	rep.Thumb.Text = thumb
	rep.Title = curT

	// ---- script + metadata
	rep.Script = ReviewScript(out.Voices)
	m := OptimizeMeta(MetaIn{Title: out.Title, Topic: in.Topic, Description: in.Description, Tags: in.Tags})
	rep.Meta = append(rep.Meta, meta0.Changes...)
	for _, c := range m.Changes {
		if !containsStr(rep.Meta, c) {
			rep.Meta = append(rep.Meta, c)
		}
	}
	if o.Apply {
		out.Description, out.Tags = m.Description, m.Tags
	} else {
		out.Title = cleanText(in.Title)
		rep.Meta = append(rep.Meta, "report-only mode: metadata changes were not applied")
	}
	if !o.Apply {
		out.Hook, out.ThumbText = cleanText(in.Hook), thumb
		out.Voices = append([]string(nil), in.Voices...)
	}
	for _, s := range rep.Script.Issues {
		rep.Warnings = append(rep.Warnings, s)
	}
	rep.Overall = int(math.Round(0.4*float64(rep.Hook.Verdict) + 0.3*float64(rep.Title.Score) + 0.3*float64(rep.Script.Score)))
	out.Report = rep
	return out
}
