// Tests for the credentials endpoints added on top of setcred's
// existing vault.Put path. Uses in-memory fakes for WorkflowStore and
// vault.Store (no Postgres needed) plus a real *vault.Vault, so the
// encryption/decryption path itself is exercised for real -- only the
// persistence backend is faked.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"microflow/internal/model"
	"microflow/internal/runner"
	"microflow/internal/store"
	"microflow/internal/vault"
)

// fakeWorkflowStore is a minimal in-memory WorkflowStore for tests.
type fakeWorkflowStore struct {
	workflows map[string]*model.Workflow
}

func (f *fakeWorkflowStore) SaveWorkflow(ctx context.Context, wf *model.Workflow) error {
	f.workflows[wf.ID] = wf
	return nil
}

func (f *fakeWorkflowStore) LoadWorkflow(ctx context.Context, id string) (*model.Workflow, error) {
	wf, ok := f.workflows[id]
	if !ok {
		return nil, errNotFound
	}
	return wf, nil
}

func (f *fakeWorkflowStore) ListWorkflows(ctx context.Context) ([]*model.Workflow, error) {
	var out []*model.Workflow
	for _, wf := range f.workflows {
		out = append(out, wf)
	}
	return out, nil
}

type notFoundErr string

func (e notFoundErr) Error() string { return string(e) }

const errNotFound = notFoundErr("workflow not found")

// fakeVaultStore implements vault.Store (GetEncrypted/PutEncrypted) AND
// exposes ListCredentials, so it doubles as this test's CredentialStore
// -- same two-interface split *store.Store has in production.
type fakeVaultStore struct {
	rows map[string][]byte // key = workflowID + "/" + logicalName
}

func newFakeVaultStore() *fakeVaultStore { return &fakeVaultStore{rows: map[string][]byte{}} }

func (f *fakeVaultStore) key(workflowID, logicalName string) string {
	return workflowID + "/" + logicalName
}

func (f *fakeVaultStore) GetEncrypted(ctx context.Context, workflowID, logicalName string) ([]byte, error) {
	ct, ok := f.rows[f.key(workflowID, logicalName)]
	if !ok {
		return nil, errNotFound
	}
	return ct, nil
}

func (f *fakeVaultStore) PutEncrypted(ctx context.Context, workflowID, logicalName string, ciphertext []byte) error {
	f.rows[f.key(workflowID, logicalName)] = ciphertext
	return nil
}

func (f *fakeVaultStore) ListCredentials(ctx context.Context, workflowID string) ([]store.CredentialInfo, error) {
	var out []store.CredentialInfo
	prefix := workflowID + "/"
	for k := range f.rows {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, store.CredentialInfo{NodeName: k[len(prefix):]})
		}
	}
	return out, nil
}

const testMasterKey = "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=" // 32 raw bytes, base64 -- test-only key

func newTestServer(t *testing.T) (*Server, *fakeWorkflowStore, *fakeVaultStore) {
	t.Helper()
	wfStore := &fakeWorkflowStore{workflows: map[string]*model.Workflow{}}
	vStore := newFakeVaultStore()
	v, err := vault.New(vStore, testMasterKey)
	if err != nil {
		t.Fatalf("vault.New: %v", err)
	}
	// run is unused by the credentials endpoints, but Server requires
	// one; a nil runner is fine as long as tests never call /execute.
	var run *runner.Runner
	s := New(wfStore, run, vStore, v)
	return s, wfStore, vStore
}

func TestSaveCredentialHappyPath(t *testing.T) {
	s, wfStore, vStore := newTestServer(t)
	wfStore.workflows["wf1"] = &model.Workflow{
		ID:   "wf1",
		Name: "Test Workflow",
		Nodes: map[string]*model.Node{
			"YouTube Upload": {Name: "YouTube Upload", Type: model.TypeYouTube},
		},
	}

	body := `{"nodeName":"YouTube Upload","clientId":"cid","clientSecret":"csecret","refreshToken":"rtoken"}`
	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/credentials", bytes.NewBufferString(body))
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()

	s.handleSaveCredential(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("csecret")) || bytes.Contains(rec.Body.Bytes(), []byte("rtoken")) {
		t.Fatalf("response leaked a secret: %s", rec.Body.String())
	}

	// Confirm it actually went through vault.Put (real AES-GCM
	// ciphertext in the fake store), and that Resolve gets the same
	// plaintext back -- the same round trip OAuthResolver relies on.
	ct, ok := vStore.rows["wf1/YouTube Upload"]
	if !ok {
		t.Fatal("credential was not persisted")
	}
	if bytes.Contains(ct, []byte("csecret")) || bytes.Contains(ct, []byte("rtoken")) {
		t.Fatal("ciphertext contains plaintext secret bytes -- encryption did not happen")
	}

	got, err := vault.New(vStore, testMasterKey)
	if err != nil {
		t.Fatalf("vault.New: %v", err)
	}
	secrets, err := got.Resolve(context.Background(), "wf1", "YouTube Upload")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if secrets["clientId"] != "cid" || secrets["clientSecret"] != "csecret" || secrets["refreshToken"] != "rtoken" {
		t.Fatalf("resolved secrets don't match what was saved: %#v", secrets)
	}
}

func TestSaveCredentialRejectsUnknownNode(t *testing.T) {
	s, wfStore, _ := newTestServer(t)
	wfStore.workflows["wf1"] = &model.Workflow{ID: "wf1", Nodes: map[string]*model.Node{}}

	body := `{"nodeName":"Does Not Exist","clientId":"cid","clientSecret":"cs","refreshToken":"rt"}`
	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/credentials", bytes.NewBufferString(body))
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()

	s.handleSaveCredential(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown node, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestSaveCredentialRejectsNonGoogleNode(t *testing.T) {
	s, wfStore, _ := newTestServer(t)
	wfStore.workflows["wf1"] = &model.Workflow{
		ID: "wf1",
		Nodes: map[string]*model.Node{
			"HTTP Call": {Name: "HTTP Call", Type: model.TypeHTTPRequest},
		},
	}

	body := `{"nodeName":"HTTP Call","clientId":"cid","clientSecret":"cs","refreshToken":"rt"}`
	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/credentials", bytes.NewBufferString(body))
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()

	s.handleSaveCredential(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-Google node type, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestSaveCredentialRejectsMissingFields(t *testing.T) {
	s, wfStore, _ := newTestServer(t)
	wfStore.workflows["wf1"] = &model.Workflow{
		ID: "wf1",
		Nodes: map[string]*model.Node{
			"Gmail Send": {Name: "Gmail Send", Type: model.TypeGmail},
		},
	}

	body := `{"nodeName":"Gmail Send","clientId":"cid","clientSecret":"","refreshToken":"rt"}`
	req := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/credentials", bytes.NewBufferString(body))
	req.SetPathValue("id", "wf1")
	rec := httptest.NewRecorder()

	s.handleSaveCredential(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing clientSecret, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestListCredentialsNeverIncludesSecrets(t *testing.T) {
	s, wfStore, _ := newTestServer(t)
	wfStore.workflows["wf1"] = &model.Workflow{
		ID: "wf1",
		Nodes: map[string]*model.Node{
			"Sheets Node": {Name: "Sheets Node", Type: model.TypeGoogleSheets},
		},
	}

	saveBody := `{"nodeName":"Sheets Node","clientId":"cid","clientSecret":"topsecret","refreshToken":"reftoken"}`
	saveReq := httptest.NewRequest(http.MethodPost, "/api/workflows/wf1/credentials", bytes.NewBufferString(saveBody))
	saveReq.SetPathValue("id", "wf1")
	saveRec := httptest.NewRecorder()
	s.handleSaveCredential(saveRec, saveReq)
	if saveRec.Code != http.StatusOK {
		t.Fatalf("setup save failed: %d %s", saveRec.Code, saveRec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/workflows/wf1/credentials", nil)
	listReq.SetPathValue("id", "wf1")
	listRec := httptest.NewRecorder()
	s.handleListCredentials(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", listRec.Code, listRec.Body.String())
	}
	if bytes.Contains(listRec.Body.Bytes(), []byte("topsecret")) || bytes.Contains(listRec.Body.Bytes(), []byte("reftoken")) {
		t.Fatalf("list response leaked a secret: %s", listRec.Body.String())
	}

	var views []credentialView
	if err := json.Unmarshal(listRec.Body.Bytes(), &views); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(views) != 1 || views[0].NodeName != "Sheets Node" || views[0].NodeType != "googleSheets" {
		t.Fatalf("unexpected list result: %#v", views)
	}
}
