package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"microflow/internal/store"
	"microflow/internal/tenant"
)

// --- fakes (ServiceStore is embedded so only what the Environment
// handlers touch needs implementing) ---

type envTestVault struct{ vals map[string]map[string]string }

func (v *envTestVault) PutGlobal(context.Context, string, string, bool) error { return nil }
func (v *envTestVault) DeleteGlobal(context.Context, string) error            { return nil }
func (v *envTestVault) PutService(_ context.Context, sid, k, val string, _ bool) error {
	if v.vals[sid] == nil {
		v.vals[sid] = map[string]string{}
	}
	v.vals[sid][k] = val
	return nil
}
func (v *envTestVault) DeleteService(_ context.Context, sid, k string) error {
	delete(v.vals[sid], k)
	return nil
}
func (v *envTestVault) GetService(_ context.Context, sid, k string) (string, bool, error) {
	val, ok := v.vals[sid][k]
	return val, ok, nil
}
func (v *envTestVault) DeleteAllService(_ context.Context, sid string) (int, error) {
	n := len(v.vals[sid])
	delete(v.vals, sid)
	return n, nil
}

type envTestSvcStore struct {
	ServiceStore
	svcs map[string]*tenant.Service
	env  *envTestVault
}

func (f *envTestSvcStore) GetService(_ context.Context, id string) (*tenant.Service, error) {
	if s, ok := f.svcs[id]; ok {
		return s, nil
	}
	return nil, errors.New("not found")
}
func (f *envTestSvcStore) ListServiceEnv(_ context.Context, id string) ([]store.EnvInfo, error) {
	out := []store.EnvInfo{}
	for k := range f.env.vals[id] {
		out = append(out, store.EnvInfo{Key: k, IsSecret: true})
	}
	return out, nil
}

func newEnvTestServer(t *testing.T, withReauth bool) (*Server, *envTestVault) {
	t.Helper()
	s, _, _, _ := newTestServer(t)
	ev := &envTestVault{vals: map[string]map[string]string{}}
	ss := &envTestSvcStore{
		svcs: map[string]*tenant.Service{
			"svc-a": {ID: "svc-a", Name: "Alpha"},
			"svc-b": {ID: "svc-b", Name: "Beta"},
		},
		env: ev,
	}
	s.WithTenancy(ss, ev, nil)
	if withReauth {
		s.WithReauth(func(_ *http.Request, pw string) (bool, time.Duration) { return pw == "correct-pw", 0 })
	}
	return s, ev
}

func TestParseDotEnv(t *testing.T) {
	in := "# comment\n\nA=1\nexport B=two words # trailing\nC=\"quoted # kept\\n\"\nD='single # kept'\nA=override\nbad line\n1BAD=x\nE=\"open\n"
	entries, issues, dupes := parseDotEnv(in)
	got := map[string]string{}
	for _, e := range entries {
		got[e.Key] = e.Value
	}
	want := map[string]string{"A": "override", "B": "two words", "C": "quoted # kept\n", "D": "single # kept"}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	if dupes != 1 {
		t.Errorf("dupes = %d, want 1", dupes)
	}
	if len(issues) != 3 {
		t.Errorf("issues = %v, want 3 (bad line, invalid name, unterminated quote)", issues)
	}
}

func TestServiceEnvAddEditRules(t *testing.T) {
	s, ev := newEnvTestServer(t, false)
	if rr := doReq(s, "POST", "/api/services/svc-a/env", "svc-a", `{"key":"K","value":"v1","mode":"create"}`); rr.Code != 200 {
		t.Fatalf("create: %d %s", rr.Code, rr.Body.String())
	}
	if rr := doReq(s, "POST", "/api/services/svc-a/env", "svc-a", `{"key":"K","value":"v2","mode":"create"}`); rr.Code != http.StatusConflict {
		t.Errorf("duplicate create: %d, want 409", rr.Code)
	}
	if ev.vals["svc-a"]["K"] != "v1" {
		t.Errorf("duplicate create overwrote the value")
	}
	if rr := doReq(s, "POST", "/api/services/svc-a/env", "svc-a", `{"key":"NOPE","value":"x","mode":"update"}`); rr.Code != http.StatusNotFound {
		t.Errorf("update of missing key: %d, want 404", rr.Code)
	}
	if rr := doReq(s, "POST", "/api/services/svc-a/env", "svc-a", `{"key":"K","value":"v3","mode":"update"}`); rr.Code != 200 || ev.vals["svc-a"]["K"] != "v3" {
		t.Errorf("update failed: %d", rr.Code)
	}
}

func TestServiceEnvListNeverContainsValues(t *testing.T) {
	s, ev := newEnvTestServer(t, false)
	ev.vals["svc-a"] = map[string]string{"K": "super-secret-value"}
	rr := doReq(s, "GET", "/api/services/svc-a/env", "svc-a", "")
	if rr.Code != 200 || strings.Contains(rr.Body.String(), "super-secret-value") {
		t.Fatalf("list leaked or failed: %d %s", rr.Code, rr.Body.String())
	}
	rr = doReq(s, "GET", "/api/services/svc-a/env/K", "svc-a", "")
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), "super-secret-value") || rr.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("reveal: %d %q cache=%q", rr.Code, rr.Body.String(), rr.Header().Get("Cache-Control"))
	}
}

func TestServiceEnvImport(t *testing.T) {
	s, ev := newEnvTestServer(t, false)
	ev.vals["svc-a"] = map[string]string{"KEEP": "old"}
	body := `{"content":"KEEP=new\nFRESH=1\n# c\n\nFRESH=2\nbad"}`
	rr := doReq(s, "POST", "/api/services/svc-a/env/import", "svc-a", body)
	if rr.Code != 200 {
		t.Fatalf("import: %d %s", rr.Code, rr.Body.String())
	}
	if ev.vals["svc-a"]["KEEP"] != "old" || ev.vals["svc-a"]["FRESH"] != "2" {
		t.Errorf("state after import: %v", ev.vals["svc-a"])
	}
	if len(ev.vals["svc-b"]) != 0 {
		t.Errorf("import leaked into another Service")
	}
	if strings.Contains(rr.Body.String(), "old") || strings.Contains(rr.Body.String(), "new") {
		t.Errorf("import response echoes values: %s", rr.Body.String())
	}
	rr = doReq(s, "POST", "/api/services/svc-a/env/import", "svc-a", `{"content":"KEEP=new","overwrite":true}`)
	if rr.Code != 200 || ev.vals["svc-a"]["KEEP"] != "new" {
		t.Errorf("overwrite import failed: %d %v", rr.Code, ev.vals["svc-a"])
	}
}

func TestServiceEnvIsolation(t *testing.T) {
	s, ev := newEnvTestServer(t, true)
	ev.vals["svc-a"] = map[string]string{"K": "a-val"}
	ev.vals["svc-b"] = map[string]string{"K": "b-val"}
	// A request in Service B's context can't touch Service A's Environment.
	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/api/services/svc-a/env", ""},
		{"GET", "/api/services/svc-a/env/K", ""},
		{"POST", "/api/services/svc-a/env", `{"key":"K","value":"x"}`},
		{"POST", "/api/services/svc-a/env/import", `{"content":"K=x","overwrite":true}`},
		{"DELETE", "/api/services/svc-a/env/K", ""},
		{"POST", "/api/services/svc-a/env/delete-all", `{"confirmName":"Alpha","password":"correct-pw"}`},
	} {
		if rr := doReq(s, tc.method, tc.path, "svc-b", tc.body); rr.Code != http.StatusNotFound {
			t.Errorf("%s %s as svc-b: %d, want 404", tc.method, tc.path, rr.Code)
		}
	}
	if ev.vals["svc-a"]["K"] != "a-val" || ev.vals["svc-b"]["K"] != "b-val" {
		t.Fatalf("cross-service request modified data: %v", ev.vals)
	}
}

func TestDeleteAllServiceEnvNeedsBothSteps(t *testing.T) {
	// no re-auth wired => refused (fail closed)
	s0, ev0 := newEnvTestServer(t, false)
	ev0.vals["svc-a"] = map[string]string{"K": "v"}
	if rr := doReq(s0, "POST", "/api/services/svc-a/env/delete-all", "svc-a", `{"confirmName":"Alpha","password":"correct-pw"}`); rr.Code != http.StatusNotImplemented || len(ev0.vals["svc-a"]) != 1 {
		t.Fatalf("without reauth: %d, vals=%v", rr.Code, ev0.vals)
	}

	s, ev := newEnvTestServer(t, true)
	ev.vals["svc-a"] = map[string]string{"K": "v", "L": "w"}
	ev.vals["svc-b"] = map[string]string{"K": "keep"}
	path := "/api/services/svc-a/env/delete-all"
	if rr := doReq(s, "POST", path, "svc-a", `{"confirmName":"wrong","password":"correct-pw"}`); rr.Code != http.StatusBadRequest {
		t.Errorf("wrong confirmation: %d, want 400", rr.Code)
	}
	if rr := doReq(s, "POST", path, "svc-a", `{"confirmName":"Alpha","password":"nope"}`); rr.Code != http.StatusForbidden {
		t.Errorf("wrong password: %d, want 403", rr.Code)
	}
	if rr := doReq(s, "POST", path, "svc-a", `{"confirmName":"Alpha"}`); rr.Code != http.StatusForbidden {
		t.Errorf("missing password: %d, want 403", rr.Code)
	}
	if len(ev.vals["svc-a"]) != 2 {
		t.Fatalf("rejected requests deleted data: %v", ev.vals["svc-a"])
	}
	if rr := doReq(s, "POST", path, "svc-a", `{"confirmName":"Alpha","password":"correct-pw"}`); rr.Code != 200 {
		t.Fatalf("valid delete: %d %s", rr.Code, rr.Body.String())
	}
	if len(ev.vals["svc-a"]) != 0 || ev.vals["svc-b"]["K"] != "keep" {
		t.Errorf("after delete: %v", ev.vals)
	}
}
