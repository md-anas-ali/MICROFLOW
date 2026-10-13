package nodes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/yta"
)

func withFakeAPI(t *testing.T, h http.Handler) func() {
	srv := httptest.NewServer(h)
	oldD, oldA := ytaDataAPI, ytaAnalyticsAPI
	ytaDataAPI, ytaAnalyticsAPI = srv.URL+"/youtube/v3", srv.URL+"/v2/reports"
	return func() { ytaDataAPI, ytaAnalyticsAPI = oldD, oldA; srv.Close() }
}

func TestServerErrorsExhaustRetriesAndAreClassified(t *testing.T) {
	var hits int32
	defer withFakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(503)
	}))()
	c := newYTAClient("tok", "", 20)
	err := c.get(context.Background(), ytaDataAPI+"/x", nil)
	if err == nil || atomic.LoadInt32(&hits) != ytaAttempts || c.classes["server"] != 1 {
		t.Fatalf("want %d attempts then a classified server error, got hits=%d err=%v classes=%v", ytaAttempts, hits, err, c.classes)
	}
}

func TestClassifyQuotaOAuthTimeout(t *testing.T) {
	cases := map[string]error{
		"quota":       &ytaAPIError{Status: 403, Reason: "quotaExceeded"},
		"oauth":       &ytaAPIError{Status: 401},
		"permission":  &ytaAPIError{Status: 403, Reason: "insufficientPermissions"},
		"bad_request": &ytaAPIError{Status: 400},
		"timeout":     context.DeadlineExceeded,
		"budget":      errYTABudget,
	}
	for want, err := range cases {
		if got := ytaClassify(err); got != want {
			t.Errorf("%v: got %s want %s", err, got, want)
		}
	}
}

func TestClientTimeoutIsBoundedAndClassified(t *testing.T) {
	defer withFakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(400 * time.Millisecond):
		case <-r.Context().Done():
		}
	}))()
	c := newYTAClient("tok", "", 20)
	c.hc.Timeout = 80 * time.Millisecond
	began := time.Now()
	err := c.get(context.Background(), ytaDataAPI+"/slow", nil)
	if err == nil || c.classes["timeout"] != 1 || time.Since(began) > 10*time.Second {
		t.Fatalf("slow API must time out and be classified: err=%v classes=%v took=%v", err, c.classes, time.Since(began))
	}
}

func newRC(env map[string]string, store *ytaMemStatic) *engine.RunContext {
	return &engine.RunContext{Workflow: &model.Workflow{ID: "wf"}, Execution: &model.Execution{ID: "e"}, StaticData: store, ServiceEnv: env}
}

func stateOf(t *testing.T, s *ytaMemStatic) yta.State {
	t.Helper()
	var st yta.State
	b, _ := json.Marshal(s.data[ytaStateKey])
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestDataAPIQuotaExceededDegradesWithoutChangingDecisions(t *testing.T) {
	defer withFakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		w.Write([]byte(`{"error":{"message":"quota","errors":[{"reason":"quotaExceeded"}]}}`))
	}))()
	store := &ytaMemStatic{data: map[string]any{}}
	rc := newRC(map[string]string{"MICROFLOW_YTA_EVOLVE_ALLOW": "hook_style"}, store)
	res := ytaAnalyze(context.Background(), rc, loadYTAConfig(rc), "tok", "e1")
	if res["status"] != "degraded" || res["guidanceText"] != "" {
		t.Fatalf("must degrade with no guidance: %v %v", res["status"], res["guidanceText"])
	}
	st := stateOf(t, store)
	if st.Active != nil || len(st.Settings) != 0 || len(st.Changes) != 0 {
		t.Fatalf("a failed fetch must not change any decision: %+v", st)
	}
	if len(st.Runs) != 1 || st.Runs[0].Errors["quota"] < 1 || st.Runs[0].Status != "degraded" {
		t.Fatalf("failed run must still be recorded with its error class: %+v", st.Runs)
	}
}

func TestBudgetExhaustedWithNoHistoryNeverActsOnEmptyData(t *testing.T) {
	defer withFakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{}`)) }))()
	store := &ytaMemStatic{data: map[string]any{ytaStateKey: map[string]any{"quotaDay": time.Now().UTC().Format("2006-01-02"), "quotaUse": 150}}}
	rc := newRC(map[string]string{}, store)
	res := ytaAnalyze(context.Background(), rc, loadYTAConfig(rc), "tok", "e1")
	if res["status"] != "degraded" || !strings.Contains(strings.Join(res["errors"].([]string), " "), "budget") {
		t.Fatalf("%v %v", res["status"], res["errors"])
	}
}

func TestCorruptStateIsRecoveredInRecommendOnlyMode(t *testing.T) {
	now := time.Now().UTC()
	srv := fakeYouTube(t, now)
	defer srv.Close()
	oldD, oldA := ytaDataAPI, ytaAnalyticsAPI
	ytaDataAPI, ytaAnalyticsAPI = srv.URL+"/youtube/v3", srv.URL+"/v2/reports"
	defer func() { ytaDataAPI, ytaAnalyticsAPI = oldD, oldA }()

	store := &ytaMemStatic{data: map[string]any{ytaStateKey: "garbage-not-an-object"}}
	rc := newRC(map[string]string{"MICROFLOW_YTA_EVOLVE_ALLOW": "hook_style,pacing", "MICROFLOW_YTA_EARLY_VIDEOS": "3", "MICROFLOW_YTA_RETENTION_CURVES": "3"}, store)
	res := ytaAnalyze(context.Background(), rc, loadYTAConfig(rc), "tok", "e1")
	g := res["guidance"].(yta.Guidance)
	if res["status"] != "ok" || g.Arm != "none" || g.Text != "" || res["integrityRepairs"] == nil {
		t.Fatalf("corrupt state must run recommend-only and report the repair: %v %+v %v", res["status"], g, res["integrityRepairs"])
	}
	if _, ok := store.data["ytAnalyticsCorrupt"]; !ok {
		t.Fatal("a bounded fingerprint of the corrupt state must be kept")
	}
	st := stateOf(t, store)
	if st.Active != nil || st.Sum == "" || len(st.Audit) == 0 {
		t.Fatalf("recovered state must be sealed, audited and have no experiment: %+v", st)
	}
	// next run is healthy again (state was resealed)
	res2 := ytaAnalyze(context.Background(), rc, loadYTAConfig(rc), "tok", "e2")
	if res2["integrityRepairs"] != nil {
		t.Fatalf("state should be healthy after recovery: %v", res2["integrityRepairs"])
	}
}

func TestOutOfBandStateEditForcesRecommendOnly(t *testing.T) {
	now := time.Now().UTC()
	srv := fakeYouTube(t, now)
	defer srv.Close()
	oldD, oldA := ytaDataAPI, ytaAnalyticsAPI
	ytaDataAPI, ytaAnalyticsAPI = srv.URL+"/youtube/v3", srv.URL+"/v2/reports"
	defer func() { ytaDataAPI, ytaAnalyticsAPI = oldD, oldA }()

	store := &ytaMemStatic{data: map[string]any{}}
	rc := newRC(map[string]string{"MICROFLOW_YTA_EVOLVE_ALLOW": "hook_style,pacing", "MICROFLOW_YTA_EARLY_VIDEOS": "3", "MICROFLOW_YTA_RETENTION_CURVES": "3"}, store)
	ytaAnalyze(context.Background(), rc, loadYTAConfig(rc), "tok", "e1")
	st := store.data[ytaStateKey].(map[string]any)
	st["settings"] = map[string]any{"pacing": "tight"} // someone edited the settings by hand
	res := ytaAnalyze(context.Background(), rc, loadYTAConfig(rc), "tok", "e2")
	g := res["guidance"].(yta.Guidance)
	if g.Arm != "none" || g.Text != "" || res["integrityRepairs"] == nil {
		t.Fatalf("an edit that breaks the seal must not be applied automatically: %+v %v", g, res["integrityRepairs"])
	}
}

func TestMissingAnalyticsColumnsAreReportedNotGuessed(t *testing.T) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	defer withFakeAPI(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"columnHeaders":[{"name":"video"},{"name":"views"}],"rows":[["s1",500]]}`))
	}))()
	f := ytaFetch{Videos: []yta.Video{{ID: "s1", PublishedAt: now.AddDate(0, 0, -3), DurationSec: 30}}}
	c := newYTAClient("tok", "", 50)
	c.fetchAnalytics(context.Background(), ytaConfig{LookbackDays: 30, Evo: yta.EvoConfig{EvalDays: 7}}, now, &f)
	a := f.Videos[0].Analytics
	if a == nil || a.HasAvgViewPct || a.HasEngaged || a.HasShares {
		t.Fatalf("metrics the API did not return must stay unavailable: %+v", a)
	}
	if !f.hasNote("lacked columns") {
		t.Fatalf("missing columns must be reported: %v", f.Notes)
	}
}
