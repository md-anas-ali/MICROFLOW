package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/yta"
)

type ytaMemStatic struct {
	mu   sync.Mutex
	data map[string]any
}

func (m *ytaMemStatic) WithLock(ctx context.Context, _ string, fn func(map[string]any) (map[string]any, error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// round-trip like the Postgres store (JSON), so non-serialisable state would fail here
	var cp map[string]any
	b, _ := json.Marshal(m.data)
	_ = json.Unmarshal(b, &cp)
	out, err := fn(cp)
	if err != nil {
		return err
	}
	m.data = out
	return nil
}

func fakeYouTube(t *testing.T, now time.Time) *httptest.Server {
	const n = 30
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case strings.HasSuffix(r.URL.Path, "/channels"):
			w.Write([]byte(`{"items":[{"contentDetails":{"relatedPlaylists":{"uploads":"UU1"}}}]}`))
		case strings.HasSuffix(r.URL.Path, "/playlistItems"):
			var items []string
			for i := 0; i < n; i++ {
				items = append(items, fmt.Sprintf(`{"contentDetails":{"videoId":"v%d"}}`, i))
			}
			w.Write([]byte(`{"items":[` + strings.Join(items, ",") + `]}`))
		case strings.HasSuffix(r.URL.Path, "/videos"):
			var items []string
			for i := 0; i < n; i++ {
				pub := now.Add(-time.Duration(3+i*2) * 24 * time.Hour).Format(time.RFC3339)
				items = append(items, fmt.Sprintf(`{"id":"v%d","snippet":{"title":"t%d","publishedAt":"%s"},"contentDetails":{"duration":"PT40S"},"status":{"privacyStatus":"public"},"statistics":{"viewCount":"%d","likeCount":"%d","commentCount":"3"}}`, i, i, pub, 1000+i*7, 40+i%4))
			}
			w.Write([]byte(`{"items":[` + strings.Join(items, ",") + `]}`))
		default: // analytics
			switch q.Get("dimensions") {
			case "video":
				ids := strings.Split(strings.TrimPrefix(q.Get("filters"), "video=="), ",")
				var rows []string
				for _, id := range ids {
					var k int
					fmt.Sscanf(id, "v%d", &k)
					pct := 72.0 + float64(k%5) // history is healthy
					if k < 6 {
						pct = 35 + float64(k) // the newest six videos hold viewers badly
					}
					rows = append(rows, fmt.Sprintf(`["%s",%d,100,30,%.1f,40,3,5,2,%d]`, id, 1000+k*7, pct, 900+k*7))
				}
				w.Write([]byte(`{"columnHeaders":[{"name":"video"},{"name":"views"},{"name":"estimatedMinutesWatched"},{"name":"averageViewDuration"},{"name":"averageViewPercentage"},{"name":"likes"},{"name":"comments"},{"name":"shares"},{"name":"subscribersGained"},{"name":"engagedViews"}],"rows":[` + strings.Join(rows, ",") + `]}`))
			case "elapsedVideoTimeRatio":
				var k int
				fmt.Sscanf(strings.TrimPrefix(q.Get("filters"), "video==v"), "%d", &k)
				v := 0.9
				if k < 6 {
					v = 0.55
				}
				w.Write([]byte(fmt.Sprintf(`{"columnHeaders":[{"name":"elapsedVideoTimeRatio"},{"name":"audienceWatchRatio"}],"rows":[[0.09,%.2f],[0.1,%.2f],[0.11,%.2f]]}`, v, v, v)))
			case "ageGroup,gender":
				w.Write([]byte(`{"columnHeaders":[{"name":"ageGroup"},{"name":"gender"},{"name":"viewerPercentage"}],"rows":[["age25-34","male",40.5]]}`))
			case "insightTrafficSourceType":
				w.Write([]byte(`{"columnHeaders":[{"name":"insightTrafficSourceType"},{"name":"views"}],"rows":[["SHORTS",800],["YT_SEARCH",200]]}`))
			default: // first-window views
				w.Write([]byte(`{"columnHeaders":[{"name":"views"}],"rows":[[900]]}`))
			}
		}
	}))
}

func TestAnalyzeRegisterLoopEndToEnd(t *testing.T) {
	now := time.Now().UTC()
	srv := fakeYouTube(t, now)
	defer srv.Close()
	oldD, oldA := ytaDataAPI, ytaAnalyticsAPI
	ytaDataAPI, ytaAnalyticsAPI = srv.URL+"/youtube/v3", srv.URL+"/v2/reports"
	defer func() { ytaDataAPI, ytaAnalyticsAPI = oldD, oldA }()

	store := &ytaMemStatic{data: map[string]any{"unrelated": "kept"}}
	rc := &engine.RunContext{
		Workflow: &model.Workflow{ID: "wf1"}, Execution: &model.Execution{ID: "exec-1"}, StaticData: store,
		ServiceEnv: map[string]string{"MICROFLOW_YTA_EVOLVE_ALLOW": "hook_style,pacing"},
	}
	cfg := loadYTAConfig(rc)
	res := ytaAnalyze(context.Background(), rc, cfg, "tok", "exec-1")
	if res["status"] != "ok" {
		t.Fatalf("status %v errors %v", res["status"], res["errors"])
	}
	rep := res["report"].(map[string]any)
	sum := rep["summary"].(yta.Summary)
	if sum.Examined < 3 || len(sum.Diagnoses) == 0 {
		t.Fatalf("no diagnosis: %+v", sum)
	}
	top := sum.Diagnoses[0]
	if top.Level == yta.LevelInsufficient || top.Cause != yta.CauseWeakHook {
		t.Fatalf("expected weak_hook (both opening and overall retention are low vs older history), got %+v", top)
	}
	if b, ok := res["brief"].(yta.Brief); !ok || b.Summary == "" || len(b.Uncertainty) == 0 || !strings.Contains(b.Mode, "auto-evolve") {
		t.Fatalf("per-run brief missing: %+v", res["brief"])
	}
	if res["policy"] == "" || res["unavailableMetrics"] == nil {
		t.Fatal("policy snapshot / unavailable-metrics statement missing")
	}
	g := res["guidance"].(yta.Guidance)
	if g.Arm != "treatment" || !strings.HasPrefix(g.Text, "PERFORMANCE GUIDANCE") {
		t.Fatalf("first execution should be the treatment arm: %+v", g)
	}
	if g.Setting != "hook_style" {
		t.Fatal("title_style is not in the allow-list and must never be applied")
	}
	// idempotent re-run for the same execution reuses the pinned arm and the cached analysis
	res2 := ytaAnalyze(context.Background(), rc, cfg, "tok", "exec-1")
	if res2["guidance"].(yta.Guidance).Text != g.Text || res2["reusedLastAnalysis"] != true {
		t.Fatalf("retry must be idempotent: %+v", res2)
	}
	// second execution alternates to baseline
	g2 := ytaAnalyze(context.Background(), rc, cfg, "tok", "exec-2")["guidance"].(yta.Guidance)
	if g2.Arm != "baseline" || g2.Text != "" {
		t.Fatalf("second execution must be baseline without guidance: %+v", g2)
	}

	// register: attributes the arm only because analyze pinned it
	node := &model.Node{Name: "Record", Parameters: map[string]any{"category": "space"}}
	out, err := ytaRegister(context.Background(), rc, node, cfg, "exec-1", []model.Item{{JSON: map[string]any{"id": "NEWVID"}}})
	if err != nil {
		t.Fatal(err)
	}
	item := out[0][0].JSON
	if item["id"] != "NEWVID" || item["ytRegister"].(map[string]any)["arm"] != "treatment" {
		t.Fatalf("register output wrong: %+v", item)
	}
	out, _ = ytaRegister(context.Background(), rc, node, cfg, "never-analyzed", []model.Item{{JSON: map[string]any{"id": "ORPHAN"}}})
	if arm := out[0][0].JSON["ytRegister"].(map[string]any)["arm"]; arm != "" {
		t.Fatalf("a video whose run had no analytics guidance must not get an arm, got %v", arm)
	}

	// persisted state is small, bounded and leaves other workflow static data untouched
	if store.data["unrelated"] != "kept" {
		t.Fatal("other static data was clobbered")
	}
	b, _ := json.Marshal(store.data[ytaStateKey])
	if len(b) > 20000 {
		t.Fatalf("state too large: %d bytes", len(b))
	}
	if strings.Contains(string(b), "tok") && strings.Contains(string(b), "Bearer") {
		t.Fatal("token must never be persisted")
	}
	st := stateOf(t, store)
	started := false
	for _, a := range st.Audit {
		if a.Kind == "experiment_started" && a.Policy != "" {
			started = true
		}
	}
	if !started || len(st.Runs) == 0 {
		t.Fatalf("audit history / run records missing: audit=%d runs=%d", len(st.Audit), len(st.Runs))
	}
	t.Logf("persisted analytics state: %d bytes", len(b))
}

func TestAnalyzeDisabledAndNoStore(t *testing.T) {
	rc := &engine.RunContext{Workflow: &model.Workflow{ID: "w"}, Execution: &model.Execution{ID: "e"}, ServiceEnv: map[string]string{"MICROFLOW_YTA_ENABLED": "false"}}
	if r := ytaAnalyze(context.Background(), rc, loadYTAConfig(rc), "tok", "e"); r["status"] != "disabled" {
		t.Fatalf("%v", r["status"])
	}
	rc.ServiceEnv = nil
	if r := ytaAnalyze(context.Background(), rc, loadYTAConfig(rc), "tok", "e"); r["status"] != "skipped" {
		t.Fatalf("%v", r["status"])
	}
}
