package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func doReq(s *Server, method, path, service, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if service != "" {
		req.Header.Set("X-Microflow-Service", service)
	}
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	return rr
}

// A workflow belonging to Service A must be invisible (404, not 403) to
// requests that name Service B, on every workflow-scoped endpoint; the
// owning Service and header-less legacy clients keep working.
func TestWorkflowRoutesEnforceServiceOwnership(t *testing.T) {
	s, wfs, _, _ := newTestServer(t)
	wf := testWorkflow("wf-a")
	wf.ServiceID = "svc-a"
	wfs.wfs["wf-a"] = wf

	for _, tc := range []struct{ method, path string }{
		{"GET", "/api/workflows/wf-a"},
		{"GET", "/api/workflows/wf-a/export"},
		{"GET", "/api/workflows/wf-a/credentials"},
		{"POST", "/api/workflows/wf-a/execute"},
		{"POST", "/api/workflows/wf-a/save"},
		{"DELETE", "/api/workflows/wf-a"},
	} {
		body := ""
		if tc.method == "POST" {
			body = `{"name":"x"}`
		}
		if rr := doReq(s, tc.method, tc.path, "svc-b", body); rr.Code != http.StatusNotFound {
			t.Errorf("%s %s as svc-b: status %d, want 404", tc.method, tc.path, rr.Code)
		}
	}
	// nothing was deleted/overwritten by the rejected requests
	if got := wfs.wfs["wf-a"]; got == nil || got.ServiceID != "svc-a" {
		t.Fatalf("workflow was modified by cross-service requests: %+v", got)
	}
	if rr := doReq(s, "GET", "/api/workflows/wf-a", "svc-a", ""); rr.Code != http.StatusOK {
		t.Errorf("owner should read its workflow, got %d", rr.Code)
	}
	if rr := doReq(s, "GET", "/api/workflows/wf-a", "", ""); rr.Code != http.StatusOK {
		t.Errorf("legacy header-less client should still work, got %d", rr.Code)
	}
}

// Re-saving an existing workflow can never move it to another Service,
// even if the body or header claims a different one.
func TestSaveCannotReassignService(t *testing.T) {
	s, wfs, _, _ := newTestServer(t)
	wf := testWorkflow("wf-a")
	wf.ServiceID = "svc-a"
	wfs.wfs["wf-a"] = wf
	rr := doReq(s, "POST", "/api/workflows/wf-a/save", "svc-a", `{"name":"renamed","serviceId":"svc-b"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("owner save failed: %d %s", rr.Code, rr.Body.String())
	}
	if wfs.wfs["wf-a"].ServiceID != "svc-a" {
		t.Fatalf("service reassigned to %q", wfs.wfs["wf-a"].ServiceID)
	}
}
