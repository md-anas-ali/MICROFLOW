package nodes

// YouTube analytics integration (MICROFLOW v3).
//
// It extends the existing YouTube node with resource "analytics":
//
//	operation "analyze"  fetch -> benchmark -> diagnose -> verify -> plan -> guidance
//	operation "register" record the video that was just uploaded (category, arm)
//
// Everything is opt-in per node, reuses the node's existing Google
// credential resolution (so account/service separation is unchanged), the
// existing per-workflow static-data store (no new database) and the pure
// logic in internal/yta. The node never fails the Shorts pipeline because
// of an analytics problem: errors degrade to "public data only" or to
// "skipped" and are reported in the output.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"microflow/internal/engine"
	"microflow/internal/expr"
	"microflow/internal/model"
	"microflow/internal/yta"
)

const (
	ytaStateKey      = "ytAnalytics"
	_ytaDataAPI      = "https://www.googleapis.com/youtube/v3"
	_ytaAnalyticsAPI = "https://youtubeanalytics.googleapis.com/v2/reports"
	ytaMaxShortSec   = 180 // YouTube Shorts may be up to 3 minutes
	ytaMaxBodyBytes  = 4 << 20
	ytaMinCallGap    = 200 * time.Millisecond
	ytaAttempts      = 3
	ytaPerCallTimout = 25 * time.Second
)

// API base URLs are variables only so tests can point them at httptest.
var (
	ytaDataAPI      = _ytaDataAPI
	ytaAnalyticsAPI = _ytaAnalyticsAPI
)

// ---- configuration -----------------------------------------------------------

type ytaConfig struct {
	Enabled        bool
	MinInterval    time.Duration
	LookbackDays   int
	MaxVideos      int
	EarlyMax       int
	CurveMax       int
	CallBudget     int
	Timeout        time.Duration
	Diag           yta.Config
	Evo            yta.EvoConfig
	UnknownAllowed []string
	// Market research + topic steering (advisory inputs to the NEXT topic).
	ResearchEvery time.Duration
	MarketSignals bool
	TopicWeights  bool
}

func ytaEnvInt(rc *engine.RunContext, key string, def, min, max int) int {
	v, err := strconv.Atoi(strings.TrimSpace(rc.Env(key)))
	if err != nil || v < min {
		return def
	}
	if v > max {
		return max
	}
	return v
}

func loadYTAConfig(rc *engine.RunContext) ytaConfig {
	c := ytaConfig{
		Enabled:      !strings.EqualFold(strings.TrimSpace(rc.Env("MICROFLOW_YTA_ENABLED")), "false"),
		MinInterval:  time.Duration(ytaEnvInt(rc, "MICROFLOW_YTA_MIN_INTERVAL_HOURS", 12, 0, 168)) * time.Hour,
		LookbackDays: ytaEnvInt(rc, "MICROFLOW_YTA_LOOKBACK_DAYS", 90, 14, 365),
		MaxVideos:    ytaEnvInt(rc, "MICROFLOW_YTA_MAX_VIDEOS", 60, 10, 200),
		EarlyMax:     ytaEnvInt(rc, "MICROFLOW_YTA_EARLY_VIDEOS", 25, 0, 60),
		CurveMax:     ytaEnvInt(rc, "MICROFLOW_YTA_RETENTION_CURVES", 20, 0, 40),
		CallBudget:   ytaEnvInt(rc, "MICROFLOW_YTA_DAILY_CALL_BUDGET", 150, 10, 2000),
		Timeout:      time.Duration(ytaEnvInt(rc, "MICROFLOW_YTA_TIMEOUT_SECONDS", 90, 15, 600)) * time.Second,
	}
	// TargetVideos: how many of the newest mature Shorts are judged (the rest are
	// the comparison history). Default 12 keeps >= 5 history peers when ~20+ videos exist.
	c.Diag = yta.Config{MinPeers: ytaEnvInt(rc, "MICROFLOW_YTA_MIN_PEERS", 5, 3, 30), TargetVideos: ytaEnvInt(rc, "MICROFLOW_YTA_TARGET_VIDEOS", 12, 3, 40)}
	c.Evo = yta.EvoConfig{
		MinPerArm: ytaEnvInt(rc, "MICROFLOW_YTA_MIN_PER_ARM", 4, 3, 30),
		EvalDays:  ytaEnvInt(rc, "MICROFLOW_YTA_EVAL_DAYS", 7, 2, 60),
		MaxDays:   ytaEnvInt(rc, "MICROFLOW_YTA_EXPERIMENT_MAX_DAYS", 45, 14, 180),
	}
	// Default: every evolvable content setting is allowed when the variable is not
	// set at all. Setting it to an empty string keeps recommend-only mode.
	allowRaw, allowSet := rc.LookupEnv("MICROFLOW_YTA_EVOLVE_ALLOW")
	if !allowSet {
		allowRaw = "hook_style,title_style,pacing"
	}
	c.Evo.Allow, c.UnknownAllowed = yta.ParseAllow(allowRaw)
	c.ResearchEvery = time.Duration(ytaEnvInt(rc, "MICROFLOW_YTA_RESEARCH_HOURS", 24, 1, 168)) * time.Hour
	c.MarketSignals = ytaQualityEnvBool(rc, "MICROFLOW_YTA_MARKET_SIGNALS", true)
	c.TopicWeights = ytaQualityEnvBool(rc, "MICROFLOW_YTA_TOPIC_WEIGHTS", true)
	return c
}

// ---- state persistence ---------------------------------------------------------

func loadYTAState(data map[string]any, now time.Time) yta.State {
	var st yta.State
	if raw, ok := data[ytaStateKey]; ok && raw != nil {
		b, merr := json.Marshal(raw)
		if merr != nil || json.Unmarshal(b, &st) != nil {
			// Corrupt state: keep a bounded fingerprint for diagnosis, start clean,
			// and stay recommend-only for this run instead of silently trusting nothing.
			sum := sha256.Sum256(b)
			head := string(b)
			if len(head) > 300 {
				head = head[:300]
			}
			data["ytAnalyticsCorrupt"] = map[string]any{"at": now.Format(time.RFC3339), "bytes": len(b), "sha256": hex.EncodeToString(sum[:8]), "head": ytaScrub(head)}
			st = yta.State{Tainted: true, Repairs: []string{"state was unreadable and was reset (fingerprint kept under ytAnalyticsCorrupt)"}}
		} else {
			mismatch := !st.IntegrityOK()
			fixed := st.Repair(now)
			if mismatch {
				fixed = append([]string{"integrity seal mismatch (state changed outside MICROFLOW or was damaged)"}, fixed...)
			}
			if len(fixed) > 0 {
				st.Tainted, st.Repairs = true, fixed
			}
		}
	}
	st.V = 1
	return st
}

func saveYTAState(data map[string]any, st yta.State) error {
	st.Seal()
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	data[ytaStateKey] = m
	return nil
}

// ytaRead loads state without writing anything back (no reseal, no repair persisted).
func ytaRead(ctx context.Context, rc *engine.RunContext, fn func(st *yta.State)) error {
	if rc.StaticData == nil {
		return errors.New("persistent workflow static-data store is not configured")
	}
	return rc.StaticData.WithLock(ctx, rc.Workflow.ID, func(data map[string]any) (map[string]any, error) {
		if data == nil {
			data = map[string]any{}
		}
		st := loadYTAState(data, time.Now().UTC())
		fn(&st)
		return data, nil
	})
}

func ytaUpdate(ctx context.Context, rc *engine.RunContext, fn func(st *yta.State) error) error {
	if rc.StaticData == nil {
		return errors.New("persistent workflow static-data store is not configured")
	}
	return rc.StaticData.WithLock(ctx, rc.Workflow.ID, func(data map[string]any) (map[string]any, error) {
		if data == nil {
			data = map[string]any{}
		}
		st := loadYTAState(data, time.Now().UTC())
		if err := fn(&st); err != nil {
			return data, err
		}
		if err := saveYTAState(data, st); err != nil {
			return data, err
		}
		return data, nil
	})
}

// ---- API client with quota, retry/backoff, rate limit ----------------------------

type ytaAPIError struct {
	Status int
	Reason string
	Msg    string
}

func (e *ytaAPIError) Error() string {
	return fmt.Sprintf("youtube api status %d (%s): %s", e.Status, e.Reason, e.Msg)
}

var errYTABudget = errors.New("daily analytics call budget exhausted")

type ytaClient struct {
	hc     *http.Client
	token  string
	opID   string
	budget int // remaining calls today
	used   int
	errs   []string
	last   time.Time
	// classes counts failed requests by cause (monitoring only).
	classes map[string]int
}

func newYTAClient(token, opID string, remaining int) *ytaClient {
	return &ytaClient{hc: &http.Client{Timeout: ytaPerCallTimout}, token: token, opID: opID, budget: remaining}
}

// get performs one budgeted, rate-limited GET with retries on transient
// failures (network, 429, 5xx). 401/403/400 are never retried.
func (c *ytaClient) get(ctx context.Context, u string, into any) error {
	err := c.doGet(ctx, u, into)
	if err != nil {
		if c.classes == nil {
			c.classes = map[string]int{}
		}
		c.classes[ytaClassify(err)]++
	}
	return err
}

// ytaClassify buckets a failure for monitoring (OAuth, quota, timeout...).
func ytaClassify(err error) string {
	var ae *ytaAPIError
	switch {
	case errors.Is(err, errYTABudget):
		return "budget"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "Client.Timeout") || strings.Contains(err.Error(), "timeout"):
		return "timeout"
	case errors.As(err, &ae):
		switch {
		case ae.Status == 401:
			return "oauth"
		case ae.Status == 429, ae.Reason == "quotaExceeded", ae.Reason == "rateLimitExceeded", ae.Reason == "dailyLimitExceeded", ae.Reason == "userRateLimitExceeded":
			return "quota"
		case ae.Status == 403:
			return "permission"
		case ae.Status == 400:
			return "bad_request"
		case ae.Status >= 500:
			return "server"
		}
	case strings.HasPrefix(err.Error(), "network error"):
		return "network"
	}
	return "other"
}

func (c *ytaClient) doGet(ctx context.Context, u string, into any) error {
	var lastErr error
	for attempt := 0; attempt < ytaAttempts; attempt++ {
		if c.budget <= 0 {
			return errYTABudget
		}
		if wait := ytaMinCallGap - time.Since(c.last); wait > 0 {
			if err := ytaSleep(ctx, wait); err != nil {
				return err
			}
		}
		c.budget--
		c.used++
		c.last = time.Now()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.token)
		if c.opID != "" {
			req.Header.Set("X-MicroFlow-Operation-ID", c.opID)
		}
		resp, err := c.hc.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = fmt.Errorf("network error: %s", ytaScrub(err.Error()))
		} else {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, ytaMaxBodyBytes))
			resp.Body.Close()
			if resp.StatusCode < 300 {
				if into == nil {
					return nil
				}
				return json.Unmarshal(body, into)
			}
			apiErr := parseYTAError(resp.StatusCode, body)
			if resp.StatusCode != 429 && resp.StatusCode < 500 {
				return apiErr // permanent for this request
			}
			lastErr = apiErr
			if ra, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && ra > 0 && ra <= 10 {
				if err := ytaSleep(ctx, time.Duration(ra)*time.Second); err != nil {
					return err
				}
				continue
			}
		}
		if attempt < ytaAttempts-1 {
			backoff := time.Duration(1<<attempt) * time.Second
			backoff += time.Duration(rand.Int63n(int64(400 * time.Millisecond)))
			if err := ytaSleep(ctx, backoff); err != nil {
				return err
			}
		}
	}
	return lastErr
}

func ytaSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

var ytaSecretLike = regexp.MustCompile(`(?i)(bearer\s+|access_token=|refresh_token=|key=)[A-Za-z0-9._~+/=-]+`)

// ytaScrub removes anything token-like and bounds the length of text that
// may reach logs or workflow output.
func ytaScrub(s string) string {
	s = ytaSecretLike.ReplaceAllString(s, "${1}[redacted]")
	if len(s) > 240 {
		s = s[:240]
	}
	return s
}

func parseYTAError(status int, body []byte) *ytaAPIError {
	var e struct {
		Error struct {
			Message string `json:"message"`
			Status  string `json:"status"`
			Errors  []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &e)
	reason := e.Error.Status
	if len(e.Error.Errors) > 0 && e.Error.Errors[0].Reason != "" {
		reason = e.Error.Errors[0].Reason
	}
	return &ytaAPIError{Status: status, Reason: reason, Msg: ytaScrub(e.Error.Message)}
}

// ---- fetching ---------------------------------------------------------------------

type ytaFetch struct {
	Videos        []yta.Video
	HaveAnalytics bool
	HaveEngaged   bool
	Audience      map[string]any
	Notes         []string
	Partial       bool
	// Market research (competitor outliers / category chart) and category weights.
	Research   *yta.Research
	ResearchAt string
	Weights    map[string]float64
}

var isoDur = regexp.MustCompile(`^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$`)

func parseISODuration(s string) int {
	m := isoDur.FindStringSubmatch(s)
	if m == nil {
		return -1
	}
	n := func(x string) int { v, _ := strconv.Atoi(x); return v }
	return n(m[1])*3600 + n(m[2])*60 + n(m[3])
}

func (c *ytaClient) fetchVideos(ctx context.Context, cfg ytaConfig, meta map[string]yta.VideoMeta, now time.Time, f *ytaFetch) error {
	var ch struct {
		Items []struct {
			ContentDetails struct {
				RelatedPlaylists struct {
					Uploads string `json:"uploads"`
				} `json:"relatedPlaylists"`
			} `json:"contentDetails"`
		} `json:"items"`
	}
	if err := c.get(ctx, ytaDataAPI+"/channels?part=contentDetails&mine=true", &ch); err != nil {
		return fmt.Errorf("channels.list: %w", err)
	}
	if len(ch.Items) == 0 || ch.Items[0].ContentDetails.RelatedPlaylists.Uploads == "" {
		return errors.New("channels.list returned no uploads playlist for the connected account")
	}
	uploads := ch.Items[0].ContentDetails.RelatedPlaylists.Uploads

	var ids []string
	page := ""
	for len(ids) < cfg.MaxVideos {
		u := ytaDataAPI + "/playlistItems?part=contentDetails&maxResults=50&playlistId=" + url.QueryEscape(uploads)
		if page != "" {
			u += "&pageToken=" + url.QueryEscape(page)
		}
		var pl struct {
			Items []struct {
				ContentDetails struct {
					VideoID string `json:"videoId"`
				} `json:"contentDetails"`
			} `json:"items"`
			NextPageToken string `json:"nextPageToken"`
		}
		if err := c.get(ctx, u, &pl); err != nil {
			return fmt.Errorf("playlistItems.list: %w", err)
		}
		for _, it := range pl.Items {
			if it.ContentDetails.VideoID != "" && len(ids) < cfg.MaxVideos {
				ids = append(ids, it.ContentDetails.VideoID)
			}
		}
		if page = pl.NextPageToken; page == "" {
			break
		}
	}

	for i := 0; i < len(ids); i += 50 {
		end := min(i+50, len(ids))
		var vl struct {
			Items []struct {
				ID      string `json:"id"`
				Snippet struct {
					Title       string `json:"title"`
					PublishedAt string `json:"publishedAt"`
				} `json:"snippet"`
				ContentDetails struct {
					Duration string `json:"duration"`
				} `json:"contentDetails"`
				Status struct {
					PrivacyStatus string `json:"privacyStatus"`
				} `json:"status"`
				Statistics struct {
					ViewCount    string `json:"viewCount"`
					LikeCount    string `json:"likeCount"`
					CommentCount string `json:"commentCount"`
				} `json:"statistics"`
			} `json:"items"`
		}
		u := ytaDataAPI + "/videos?part=snippet,statistics,contentDetails,status&id=" + url.QueryEscape(strings.Join(ids[i:end], ","))
		if err := c.get(ctx, u, &vl); err != nil {
			return fmt.Errorf("videos.list: %w", err)
		}
		for _, v := range vl.Items {
			if v.Status.PrivacyStatus != "public" {
				continue // private/scheduled/unlisted videos carry no comparable performance
			}
			pub, err := time.Parse(time.RFC3339, v.Snippet.PublishedAt)
			if err != nil || pub.After(now) {
				continue
			}
			dur := parseISODuration(v.ContentDetails.Duration)
			if dur <= 0 || dur > ytaMaxShortSec {
				continue // not a Short (heuristic: <= 3 minutes)
			}
			atoi := func(s string) float64 { x, _ := strconv.ParseFloat(s, 64); return x }
			// A counter the API omits (hidden likes, disabled comments) is UNAVAILABLE, not a measured zero.
			f.Videos = append(f.Videos, yta.Video{ID: v.ID, Title: v.Snippet.Title, PublishedAt: pub, DurationSec: dur,
				Views: atoi(v.Statistics.ViewCount), Likes: atoi(v.Statistics.LikeCount), Comments: atoi(v.Statistics.CommentCount),
				LikesHidden: strings.TrimSpace(v.Statistics.LikeCount) == "", CommentsOff: strings.TrimSpace(v.Statistics.CommentCount) == "",
				Meta: meta[v.ID]})
		}
	}
	sort.Slice(f.Videos, func(i, j int) bool { return f.Videos[i].PublishedAt.After(f.Videos[j].PublishedAt) })
	return nil
}

type ytaReport struct {
	Headers []struct {
		Name string `json:"name"`
	} `json:"columnHeaders"`
	Rows [][]any `json:"rows"`
}

func (r ytaReport) col(name string) int {
	for i, h := range r.Headers {
		if h.Name == name {
			return i
		}
	}
	return -1
}

func (c *ytaClient) report(ctx context.Context, q url.Values, into *ytaReport) error {
	q.Set("ids", "channel==MINE")
	return c.get(ctx, ytaAnalyticsAPI+"?"+q.Encode(), into)
}

func num(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}

func ytaDate(t time.Time) string { return t.UTC().Format("2006-01-02") }

// fetchAnalytics adds private analytics. Any API refusal downgrades to
// public-data-only with a named reason; it never aborts the run.
func (c *ytaClient) fetchAnalytics(ctx context.Context, cfg ytaConfig, now time.Time, f *ytaFetch) {
	if len(f.Videos) == 0 {
		return
	}
	start := ytaDate(now.AddDate(0, 0, -cfg.LookbackDays))
	end := ytaDate(now)
	byID := map[string]int{}
	for i := range f.Videos {
		byID[f.Videos[i].ID] = i
	}
	base := []string{"views", "estimatedMinutesWatched", "averageViewDuration", "averageViewPercentage", "likes", "comments", "shares", "subscribersGained"}
	withEngaged := append(append([]string{}, base...), "engagedViews")

	for i := 0; i < len(f.Videos); i += 50 {
		end50 := min(i+50, len(f.Videos))
		var ids []string
		for _, v := range f.Videos[i:end50] {
			ids = append(ids, v.ID)
		}
		var rep ytaReport
		var asked []string
		try := func(metrics []string) error {
			asked = metrics
			rep = ytaReport{}
			return c.report(ctx, url.Values{"startDate": {start}, "endDate": {end}, "dimensions": {"video"},
				"metrics": {strings.Join(metrics, ",")}, "filters": {"video==" + strings.Join(ids, ",")}, "maxResults": {"50"}}, &rep)
		}
		err := try(withEngaged)
		var ae *ytaAPIError
		if errors.As(err, &ae) && ae.Status == 400 {
			// engagedViews may not be accepted for this channel/report: retry without it.
			f.Notes = append(f.Notes, "engagedViews metric not accepted by the API; continuing without it")
			err = try(base)
		} else if err == nil {
			f.HaveEngaged = true
		}
		if err != nil {
			f.noteAnalyticsErr(err, c)
			return
		}
		f.HaveAnalytics = true
		var missing []string
		for _, m := range asked {
			if rep.col(m) < 0 {
				missing = append(missing, m)
			}
		}
		if len(missing) > 0 && !f.hasNote("lacked columns") {
			f.Notes = append(f.Notes, "Analytics API response lacked columns: "+strings.Join(missing, ",")+"; those metrics are treated as unavailable")
		}
		vi := rep.col("video")
		for _, row := range rep.Rows {
			if vi < 0 || vi >= len(row) {
				continue
			}
			idx, ok := byID[fmt.Sprint(row[vi])]
			if !ok {
				continue
			}
			a := &yta.VideoAnalytics{}
			get := func(name string) (float64, bool) {
				if k := rep.col(name); k >= 0 && k < len(row) {
					return num(row[k]), true
				}
				return 0, false
			}
			a.Views, _ = get("views")
			a.MinutesWatched, _ = get("estimatedMinutesWatched")
			a.AvgViewSec, _ = get("averageViewDuration")
			a.AvgViewPct, a.HasAvgViewPct = get("averageViewPercentage")
			a.Likes, _ = get("likes")
			a.Comments, _ = get("comments")
			a.Shares, a.HasShares = get("shares")
			a.SubsGained, _ = get("subscribersGained")
			if f.HaveEngaged {
				a.EngagedViews, a.HasEngaged = get("engagedViews")
			}
			f.Videos[idx].Analytics = a
		}
	}

	// Same-age-window views (fair volume metric): newest eligible videos first.
	early := cfg.Evo
	window := early.EvalDays
	if window < 2 {
		window = 7
	}
	n := 0
	for i := range f.Videos {
		v := &f.Videos[i]
		if n >= cfg.EarlyMax || v.AgeDays(now) < float64(window) {
			continue
		}
		var rep ytaReport
		s := ytaDate(v.PublishedAt)
		e := ytaDate(v.PublishedAt.AddDate(0, 0, window-1))
		if err := c.report(ctx, url.Values{"startDate": {s}, "endDate": {e}, "metrics": {"views"}, "filters": {"video==" + v.ID}}, &rep); err != nil {
			if errors.Is(err, errYTABudget) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				f.Partial = true
				f.Notes = append(f.Notes, "stopped fetching first-window views: "+err.Error())
				break
			}
			f.noteAnalyticsErr(err, c)
			break
		}
		if len(rep.Rows) > 0 && len(rep.Rows[0]) > 0 {
			v.EarlyViews, v.HasEarly = num(rep.Rows[0][0]), true
		}
		n++
	}

	// Retention curves (hook retention) for the newest mature videos.
	n = 0
	for i := range f.Videos {
		v := &f.Videos[i]
		if n >= cfg.CurveMax || v.AgeDays(now) < yta.MinMatureAgeDays || v.Analytics == nil {
			continue
		}
		var rep ytaReport
		err := c.report(ctx, url.Values{"startDate": {ytaDate(v.PublishedAt)}, "endDate": {end}, "dimensions": {"elapsedVideoTimeRatio"},
			"metrics": {"audienceWatchRatio"}, "filters": {"video==" + v.ID}}, &rep)
		if err != nil {
			if errors.Is(err, errYTABudget) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				f.Partial = true
				break
			}
			f.noteAnalyticsErr(err, c)
			break
		}
		if h, ok := hookRetention(rep); ok {
			v.Analytics.HookRetention, v.Analytics.HasHook = h, true
		}
		if pts := curvePoints(rep); len(pts) > 0 {
			if cv, ok := yta.AnalyzeCurve(pts, v.DurationSec, v.Meta.SceneWords); ok {
				v.Analytics.Curve = &cv
			}
		}
		n++
	}

	// Channel-level audience context (informational; not used as a cause).
	f.Audience = map[string]any{}
	var rep ytaReport
	if err := c.report(ctx, url.Values{"startDate": {start}, "endDate": {end}, "dimensions": {"ageGroup,gender"}, "metrics": {"viewerPercentage"}, "sort": {"-viewerPercentage"}, "maxResults": {"3"}}, &rep); err == nil {
		var top []string
		for _, r := range rep.Rows {
			if len(r) >= 3 {
				top = append(top, fmt.Sprintf("%v/%v %.1f%%", r[0], r[1], num(r[2])))
			}
		}
		f.Audience["topViewerGroups"] = top
	} else if !errors.Is(err, errYTABudget) {
		f.Notes = append(f.Notes, "audience demographics unavailable: "+ytaScrub(err.Error()))
	}
	rep = ytaReport{}
	if err := c.report(ctx, url.Values{"startDate": {start}, "endDate": {end}, "dimensions": {"insightTrafficSourceType"}, "metrics": {"views"}, "sort": {"-views"}, "maxResults": {"4"}}, &rep); err == nil {
		total := 0.0
		for _, r := range rep.Rows {
			if len(r) >= 2 {
				total += num(r[1])
			}
		}
		var top []string
		for _, r := range rep.Rows {
			if len(r) >= 2 && total > 0 {
				top = append(top, fmt.Sprintf("%v %.0f%% of views", r[0], num(r[1])/total*100))
			}
		}
		f.Audience["topTrafficSources"] = top
	}
}

// curvePoints converts an elapsedVideoTimeRatio/audienceWatchRatio report
// into curve points (nil when the columns are missing).
func curvePoints(r ytaReport) []yta.CurvePoint {
	ri, vi := r.col("elapsedVideoTimeRatio"), r.col("audienceWatchRatio")
	if ri < 0 || vi < 0 {
		return nil
	}
	pts := make([]yta.CurvePoint, 0, len(r.Rows))
	for _, row := range r.Rows {
		if ri < len(row) && vi < len(row) {
			pts = append(pts, yta.CurvePoint{X: num(row[ri]), Y: num(row[vi])})
		}
	}
	return pts
}

// hookRetention reads audienceWatchRatio at ~10% of the runtime.
func hookRetention(r ytaReport) (float64, bool) {
	ri, vi := r.col("elapsedVideoTimeRatio"), r.col("audienceWatchRatio")
	if ri < 0 || vi < 0 {
		return 0, false
	}
	best, bestD, found := 0.0, 1.0, false
	for _, row := range r.Rows {
		if ri >= len(row) || vi >= len(row) {
			continue
		}
		d := num(row[ri]) - 0.10
		if d < 0 {
			d = -d
		}
		if d < bestD {
			best, bestD, found = num(row[vi]), d, true
		}
	}
	return best, found && bestD <= 0.03
}

func (f *ytaFetch) hasNote(sub string) bool {
	for _, n := range f.Notes {
		if strings.Contains(n, sub) {
			return true
		}
	}
	return false
}

func (f *ytaFetch) noteAnalyticsErr(err error, c *ytaClient) {
	var ae *ytaAPIError
	switch {
	case errors.As(err, &ae) && (ae.Status == 401 || ae.Status == 403):
		f.Notes = append(f.Notes, "YouTube Analytics API refused access ("+ae.Reason+"): the connected account needs the yt-analytics.readonly scope and the YouTube Analytics API enabled; continuing with public Data API metrics only")
		// access problem: private analytics are unusable for the rest of this run
		for i := range f.Videos {
			f.Videos[i].Analytics = nil
		}
		f.HaveAnalytics = false
	default:
		f.Notes = append(f.Notes, "analytics query failed: "+ytaScrub(err.Error()))
		f.Partial = true
	}
	c.errs = append(c.errs, ytaScrub(err.Error()))
}

// ---- node entry points ----------------------------------------------------------------

func (e *YouTubeExecutor) executeAnalytics(ctx context.Context, rc *engine.RunContext, node *model.Node, token string, input model.NodeOutput) (model.NodeOutput, error) {
	op := node.ParamString("operation", "analyze")
	items := flatten(input)
	if len(items) == 0 {
		items = []model.Item{{JSON: map[string]any{}}}
	}
	cfg := loadYTAConfig(rc)
	execID := "adhoc"
	if rc.Execution != nil && rc.Execution.ID != "" {
		execID = rc.Execution.ID
	}

	switch op {
	case "quality":
		return ytaQuality(ctx, rc, node, execID, items)
	case "register":
		return ytaRegister(ctx, rc, node, cfg, execID, items)
	default:
		out := make([]model.Item, 0, len(items))
		res := ytaAnalyze(ctx, rc, cfg, token, execID)
		for _, it := range items {
			j := map[string]any{}
			for k, v := range it.JSON {
				j[k] = v
			}
			j["ytAnalytics"] = res
			out = append(out, model.Item{JSON: j, Binary: it.Binary})
		}
		return model.NodeOutput{out}, nil
	}
}

func ytaRegister(ctx context.Context, rc *engine.RunContext, node *model.Node, cfg ytaConfig, execID string, items []model.Item) (model.NodeOutput, error) {
	var out []model.Item
	for _, it := range items {
		ec := rc.ExprContext(it.JSON)
		videoID := ""
		for _, k := range []string{"videoId", "id"} {
			if s, ok := it.JSON[k].(string); ok && s != "" {
				videoID = s
				break
			}
		}
		catV, _ := expr.EvalValue(node.ParamString("category", ""), ec)
		category := strings.TrimSpace(fmt.Sprint(catV))
		if category == "<nil>" {
			category = ""
		}
		// Pre-publish quality context from the Script Quality Gate (optional).
		evalInt := func(name string) int {
			v, err := expr.EvalValue(node.ParamString(name, ""), ec)
			if err != nil {
				return 0
			}
			n, _ := strconv.Atoi(strings.TrimSpace(fmt.Sprint(v)))
			if n < 0 || n > 100 {
				return 0
			}
			return n
		}
		hs, ts := evalInt("hookScore"), evalInt("titleScore")
		var sw []int
		if v, err := expr.EvalValue(node.ParamString("sceneWords", ""), ec); err == nil {
			for _, p := range strings.Split(fmt.Sprint(v), ",") {
				if n, e := strconv.Atoi(strings.TrimSpace(p)); e == nil && n > 0 && n < 200 {
					sw = append(sw, n)
				}
			}
			if len(sw) > 12 {
				sw = nil
			}
		}
		j := map[string]any{}
		for k, v := range it.JSON {
			j[k] = v
		}
		if videoID == "" {
			j["ytRegister"] = map[string]any{"status": "skipped", "reason": "no video id on the incoming item"}
			out = append(out, model.Item{JSON: j, Binary: it.Binary})
			continue
		}
		var meta yta.VideoMeta
		err := ytaUpdate(ctx, rc, func(st *yta.State) error {
			meta = yta.VideoMeta{Category: category, At: time.Now().UTC().Format(time.RFC3339)}
			meta.HookScore, meta.TitleScore, meta.SceneWords = hs, ts, sw
			// Attribute an arm ONLY if analyze pinned one for this execution, i.e.
			// the guidance really reached the script prompt.
			if g, ok := st.Pinned(execID); ok {
				meta.ExperimentID = g.ExperimentID
				switch g.Arm {
				case "treatment", "baseline":
					meta.Arm, meta.Setting, meta.Variant = g.Arm, g.Setting, g.Variant
				case "adopted":
					meta.Arm, meta.Variant = "adopted", yta.AdoptedVariant(g.Adopted)
				}
			}
			st.RecordVideo(videoID, meta)
			return nil
		})
		if err != nil {
			// recording is best-effort: never fail an already-uploaded video
			log.Printf("youtube analytics register: %s", ytaScrub(err.Error()))
			j["ytRegister"] = map[string]any{"status": "error", "error": ytaScrub(err.Error())}
		} else {
			j["ytRegister"] = map[string]any{"status": "ok", "arm": meta.Arm, "experimentId": meta.ExperimentID, "category": category}
		}
		out = append(out, model.Item{JSON: j, Binary: it.Binary})
	}
	return model.NodeOutput{out}, nil
}

func ytaAnalyze(ctx context.Context, rc *engine.RunContext, cfg ytaConfig, token, execID string) map[string]any {
	began := time.Now()
	var ms0 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	cpu0 := ytaCPUms()
	var client *ytaClient
	res := map[string]any{"status": "ok"}
	var errs []string
	// Health record of every run (including failed ones) for monitoring history.
	defer func() {
		if rc.StaticData == nil || res["status"] == "disabled" || ctx.Err() != nil {
			return
		}
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		rec := yta.RunRecord{At: time.Now().UTC(), Status: fmt.Sprint(res["status"]), Mode: fmt.Sprint(res["mode"]),
			DurationMs: time.Since(began).Milliseconds(), HeapMB: float64(ms.HeapAlloc) / (1 << 20), CPUMs: -1,
			Cached: res["reusedLastAnalysis"] == true}
		if c := ytaCPUms(); c >= 0 && cpu0 >= 0 {
			rec.CPUMs = c - cpu0
		}
		if client != nil {
			rec.Calls, rec.Errors = client.used, client.classes
		}
		rctx, c2 := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer c2()
		_ = ytaUpdate(rctx, rc, func(st *yta.State) error { st.AddRun(rec); return nil })
	}()
	fail := func(status, msg string) map[string]any {
		res["status"] = status
		res["guidance"] = yta.Guidance{Arm: "none"}
		res["guidanceText"] = ""
		res["guidanceMode"] = "none"
		errs = append(errs, ytaScrub(msg))
		res["errors"] = errs
		return res
	}
	if !cfg.Enabled {
		res["status"] = "disabled"
		res["guidance"] = yta.Guidance{Arm: "none"}
		res["guidanceText"] = ""
		res["guidanceMode"] = "none"
		return res
	}
	if len(cfg.UnknownAllowed) > 0 {
		errs = append(errs, "ignored unknown names in MICROFLOW_YTA_EVOLVE_ALLOW: "+strings.Join(cfg.UnknownAllowed, ","))
	}
	if rc.StaticData == nil {
		return fail("skipped", "persistent static-data store not configured")
	}
	runCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	now := time.Now().UTC()

	// Phase A: snapshot state (short lock) and decide whether a refresh is due.
	var snap yta.State
	if err := ytaRead(runCtx, rc, func(st *yta.State) {
		snap = *st
		snap.Videos = map[string]yta.VideoMeta{}
		for k, v := range st.Videos {
			snap.Videos[k] = v
		}
	}); err != nil {
		if ctx.Err() != nil {
			return fail("cancelled", ctx.Err().Error())
		}
		return fail("skipped", "state load failed: "+err.Error())
	}
	today := now.Format("2006-01-02")
	used := 0
	if snap.QuotaDay == today {
		used = snap.QuotaUse
	}
	fresh := !snap.LastRun.IsZero() && now.Sub(snap.LastRun) < cfg.MinInterval && snap.Report != nil

	var fetch ytaFetch
	if !fresh {
		client = newYTAClient(token, rc.CurrentOperationID, cfg.CallBudget-used)
		if err := client.fetchVideos(runCtx, cfg, snap.Videos, now, &fetch); err != nil {
			var ae *ytaAPIError
			switch {
			case ctx.Err() != nil:
				return fail("cancelled", ctx.Err().Error())
			case errors.Is(err, errYTABudget):
				if snap.Report == nil {
					// Never decide anything (e.g. close an experiment) on an empty fetch.
					return fail("degraded", "daily API call budget exhausted and no previous analysis exists")
				}
				fresh = true
				errs = append(errs, "daily API call budget exhausted; reusing last analysis")
			case errors.As(err, &ae) && (ae.Status == 401 || ae.Status == 403):
				return fail("degraded", "YouTube Data API refused access ("+ae.Reason+"); reconnect the YouTube account or check the API is enabled and quota not exceeded")
			default:
				fresh = snap.Report != nil
				errs = append(errs, "fetch failed: "+ytaScrub(err.Error()))
				if snap.Report == nil {
					return fail("degraded", "could not fetch channel data: "+err.Error())
				}
			}
		} else {
			client.fetchAnalytics(runCtx, cfg, now, &fetch)
			errs = append(errs, client.errs...)
			// Real-metric validation: implausible private metrics are treated as
			// UNAVAILABLE (never repaired or guessed).
			bad := 0
			for i := range fetch.Videos {
				if issues := yta.ValidateAnalytics(fetch.Videos[i].Analytics); len(issues) > 0 {
					fetch.Videos[i].Analytics = nil
					bad++
				}
			}
			if bad > 0 {
				fetch.Notes = append(fetch.Notes, fmt.Sprintf("%d video(s) returned implausible analytics values and were treated as unavailable", bad))
			}
		}
	}

	// Market research: refreshed at most every MICROFLOW_YTA_RESEARCH_HOURS, otherwise
	// the previous summary is reused (no API calls). Failure never affects the run.
	if !fresh && client != nil {
		var prev *yta.Research
		prevAt := time.Time{}
		if snap.Report != nil {
			if b, err := json.Marshal(snap.Report["research"]); err == nil && string(b) != "null" {
				var r yta.Research
				if json.Unmarshal(b, &r) == nil && r.Source != "" {
					prev = &r
				}
			}
			if ts, ok := snap.Report["researchAt"].(string); ok {
				prevAt, _ = time.Parse(time.RFC3339, ts)
			}
		}
		if prev != nil && now.Sub(prevAt) < cfg.ResearchEvery {
			fetch.Research, fetch.ResearchAt = prev, prevAt.Format(time.RFC3339)
		} else if cfg.MarketSignals {
			r, notes := client.fetchMarket(runCtx, rc, now)
			fetch.Notes = append(fetch.Notes, notes...)
			if r.Source != "none" || prev == nil {
				fetch.Research, fetch.ResearchAt = &r, now.Format(time.RFC3339)
			} else {
				fetch.Research, fetch.ResearchAt = prev, prevAt.Format(time.RFC3339)
			}
		}
		if cfg.TopicWeights {
			fetch.Weights = yta.TopicWeights(fetch.Videos, now)
		}
	}

	// Phase C: apply decisions atomically on the freshest state.
	var guidance yta.Guidance
	var events []yta.Event
	var plan yta.Plan
	var summary yta.Summary
	var recs []yta.Recommendation
	var brief yta.Brief
	var repairs []string
	var advisory string
	recommendOnly := len(cfg.Evo.Allow) == 0
	cached := fresh
	err := ytaUpdate(runCtx, rc, func(st *yta.State) error {
		st.Settings = ensureMap(st.Settings)
		evo := cfg.Evo
		if st.Tainted {
			// State failed its integrity check or needed repair: no automatic
			// change and no adopted/experiment guidance in this run.
			evo.Allow = map[string]bool{}
			repairs = st.Repairs
			st.AddAudit("integrity_repair", strings.Join(st.Repairs, "; "), evo, now)
			recommendOnly = true
		}
		if client != nil {
			if st.QuotaDay != today {
				st.QuotaDay, st.QuotaUse = today, 0
			}
			st.QuotaUse += client.used
		}
		if !cached {
			// re-attach the freshest per-video meta (register may have run meanwhile)
			for i := range fetch.Videos {
				fetch.Videos[i].Meta = st.Videos[fetch.Videos[i].ID]
			}
			summary = yta.Diagnose(fetch.Videos, now, cfg.Diag)
			recs = yta.Recommend(summary)
			if fetch.Research != nil {
				recs = append(recs, yta.RecommendFromResearch(*fetch.Research)...)
			}
			events = st.Verify(fetch.Videos, now, evo)
			plan = st.Next(summary, fetch.HaveAnalytics, now, evo)
			for _, e := range events {
				st.AddAudit(e.Kind, e.Detail, evo, now)
			}
			if plan.Proposed != nil {
				kind := "experiment_proposed"
				if plan.Started {
					kind = "experiment_started"
				}
				st.AddAudit(kind, plan.Proposed.ID+": "+plan.Reason, evo, now)
			}
			for _, r := range summary.Regressions {
				st.AddAudit("regression_detected", r.String(), evo, now)
			}
			notes := append(append([]string{}, fetch.Notes...), unavailableMetrics...)
			brief = yta.BuildBrief(summary, recs, plan, events, recommendOnly, notes)
			st.LastRun = now
			advisory = yta.AdvisoryText(recs)
			st.Report = compactReport(summary, recs, plan, fetch, brief, advisory)
		}
		guidance = st.AssignFor(execID, evo, now)
		return nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return fail("cancelled", ctx.Err().Error())
		}
		return fail("degraded", "could not persist analytics state: "+err.Error())
	}

	if cached {
		var st yta.State
		_ = ytaRead(runCtx, rc, func(s *yta.State) { st = *s })
		if st.Report != nil {
			if v, ok := st.Report["recommendations"]; !ok || v == nil {
				st.Report["recommendations"] = []any{} // old reports persisted null
			}
			if t, ok := st.Report["advisoryText"].(string); ok {
				advisory = t
			}
		}
		res["report"] = st.Report
		res["brief"] = st.Report["brief"]
		res["reusedLastAnalysis"] = true
	} else {
		res["report"] = map[string]any{
			"summary": summary, "recommendations": recs, "experimentPlan": plan, "mode": modeName(fetch),
			"audience": fetch.Audience, "notes": fetch.Notes, "partial": fetch.Partial, "videosFetched": len(fetch.Videos),
			"coverage": summary.Coverage, "advisoryText": advisory,
			"research": fetch.Research, "topicWeights": fetch.Weights, "retentionPattern": summary.Retention,
		}
		res["decisions"] = events
		res["brief"] = brief
		res["mode"] = modeName(fetch)
	}
	// Advisory inputs for the NEXT topic (kill switches: MICROFLOW_YTA_MARKET_SIGNALS /
	// MICROFLOW_YTA_TOPIC_WEIGHTS = off). Both are subject-level only and bounded: market
	// text names subjects worth an unexplored angle, weights lean 0.4x-1.8x and never
	// exclude a category. They never touch hook/title/pacing, so experiments stay clean.
	res["researchText"], res["topicWeights"] = "", map[string]float64{}
	if cached {
		var st yta.State
		_ = ytaRead(runCtx, rc, func(s *yta.State) { st = *s })
		if cfg.MarketSignals {
			if rm, ok := st.Report["research"].(map[string]any); ok {
				if t, ok := rm["topicText"].(string); ok {
					res["researchText"] = t
				}
			}
		}
		if cfg.TopicWeights {
			if wm, ok := st.Report["topicWeights"].(map[string]any); ok {
				out := map[string]float64{}
				for k, v := range wm {
					if f, ok := v.(float64); ok {
						out[k] = f
					}
				}
				res["topicWeights"] = out
			}
		}
	} else {
		if cfg.MarketSignals && fetch.Research != nil {
			res["researchText"] = fetch.Research.TopicText
		}
		if cfg.TopicWeights && fetch.Weights != nil {
			res["topicWeights"] = fetch.Weights
		}
	}
	res["policy"] = cfg.Evo.Snapshot()
	res["unavailableMetrics"] = unavailableMetrics
	if len(repairs) > 0 {
		res["integrityRepairs"] = repairs
	}
	res["guidance"] = guidance
	// guidanceText is never empty on a healthy run. "applied" text (adopted
	// setting or experiment arm) is injected into the script prompt by the
	// workflow; "advisory" text (recommend-only) is reported but NOT injected.
	if guidance.Text != "" {
		res["guidanceText"], res["guidanceMode"] = guidance.Text, "applied"
	} else {
		if advisory == "" {
			advisory = yta.AdvisoryText(nil)
		}
		res["guidanceText"], res["guidanceMode"] = advisory, "advisory"
	}
	res["errors"] = errs

	var ms1 runtime.MemStats
	runtime.ReadMemStats(&ms1)
	mon := map[string]any{
		"durationMs":     time.Since(began).Milliseconds(),
		"heapAllocMB":    float64(ms1.HeapAlloc) / (1 << 20),
		"heapDeltaMB":    (float64(ms1.HeapAlloc) - float64(ms0.HeapAlloc)) / (1 << 20),
		"sysMB":          float64(ms1.Sys) / (1 << 20),
		"cachedAnalysis": cached,
		"goroutines":     runtime.NumGoroutine(),
		"recommendOnly":  recommendOnly,
	}
	if c := ytaCPUms(); c >= 0 && cpu0 >= 0 {
		mon["cpuMs"] = c - cpu0
	}
	if client != nil {
		mon["apiCalls"] = client.used
		mon["apiErrors"] = len(client.errs)
		mon["errorClasses"] = client.classes
		mon["quotaUsedToday"] = used + client.used
		mon["dailyCallBudget"] = cfg.CallBudget
	}
	res["monitor"] = mon
	log.Printf("youtube analytics: status=%v arm=%s experiment=%s calls=%v durationMs=%v heapMB=%.1f cached=%v errors=%d",
		res["status"], guidance.Arm, guidance.ExperimentID, mon["apiCalls"], mon["durationMs"], mon["heapAllocMB"], cached, len(errs))
	// transient data (fetched videos, report rows) is dropped here: nothing was written to disk and
	// only the bounded State above is persisted.
	fetch = ytaFetch{}
	return res
}

// unavailableMetrics are things MICROFLOW deliberately does NOT report, because
// no documented YouTube API metric for them has been verified here. They are
// never estimated.
var unavailableMetrics = []string{
	"viewed-vs-swiped-away: not requested; no documented Analytics API metric for it has been verified (Studio-only unless confirmed). engagedViews is used instead when the API returns it",
	"impressions and click-through rate: not exposed by the YouTube APIs",
	"visual/video quality: no API metric exists; only title, length and metadata features are analysed",
}

func ensureMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func modeName(f ytaFetch) string {
	if f.HaveAnalytics {
		return "full (Data API + Analytics API)"
	}
	return "public-data-only (Data API; Analytics API unavailable)"
}

// compactReport is the small summary persisted between runs.
func compactReport(sum yta.Summary, recs []yta.Recommendation, plan yta.Plan, f ytaFetch, brief yta.Brief, advisory string) map[string]any {
	diag := sum.Diagnoses
	if len(diag) > 3 {
		diag = diag[:3]
	}
	if len(recs) > 5 {
		recs = recs[:5]
	}
	if recs == nil {
		recs = []yta.Recommendation{}
	}
	b, _ := json.Marshal(map[string]any{
		"summary":  map[string]any{"examinedVideos": sum.Examined, "diagnoses": diag, "notes": sum.Notes, "coverage": sum.Coverage, "retentionPattern": sum.Retention},
		"research": f.Research, "researchAt": f.ResearchAt, "topicWeights": f.Weights,
		"advisoryText":    advisory,
		"recommendations": recs, "experimentPlan": plan, "mode": modeName(f), "videosFetched": len(f.Videos), "brief": brief,
	})
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return m
}
