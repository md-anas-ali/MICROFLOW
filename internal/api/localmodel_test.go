package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"microflow/internal/localmodel"
)

type fakeLM struct{ calls []string }

func (f *fakeLM) Status() localmodel.Status {
	return localmodel.Status{Supported: true, Process: "stopped"}
}
func (f *fakeLM) Install() error   { f.calls = append(f.calls, "install"); return nil }
func (f *fakeLM) CancelInstall()   { f.calls = append(f.calls, "cancel") }
func (f *fakeLM) Uninstall() error { f.calls = append(f.calls, "uninstall"); return nil }
func (f *fakeLM) Start() error {
	f.calls = append(f.calls, "start")
	return errors.New("install first")
}
func (f *fakeLM) Stop() error { f.calls = append(f.calls, "stop"); return nil }
func (f *fakeLM) SetEnabled(on bool) error {
	if on {
		f.calls = append(f.calls, "enable")
	} else {
		f.calls = append(f.calls, "disable")
	}
	return nil
}

func TestLocalModelRoutes(t *testing.T) {
	f := &fakeLM{}
	h := New(nil, nil, nil, nil, nil).WithLocalModel(f).Handler()
	do := func(method, path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w
	}
	if w := do("GET", "/api/local-model/status"); w.Code != 200 {
		t.Fatalf("status %d", w.Code)
	} else {
		var st localmodel.Status
		if json.Unmarshal(w.Body.Bytes(), &st) != nil || !st.Supported {
			t.Fatal("bad status body")
		}
	}
	for _, a := range []string{"install", "cancel-install", "stop", "enable", "disable", "uninstall"} {
		if w := do("POST", "/api/local-model/"+a); w.Code != 200 {
			t.Fatalf("%s -> %d", a, w.Code)
		}
	}
	if w := do("POST", "/api/local-model/start"); w.Code != http.StatusConflict {
		t.Fatalf("start error should be 409, got %d", w.Code)
	}
	if w := do("POST", "/api/local-model/bogus"); w.Code != 404 {
		t.Fatalf("bogus -> %d", w.Code)
	}
	if w := do("GET", "/api/local-model/install"); w.Code == 200 {
		t.Fatal("GET must not trigger actions")
	}
	if len(f.calls) != 7 {
		t.Fatalf("calls: %v", f.calls)
	}
}
