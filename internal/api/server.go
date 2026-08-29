// Package api is MicroFlow's HTTP API, consumed by the React editor.
// Endpoints: import, save, get, list, execute, list executions -- the
// spec's "Simple n8n-style UI" needs (workflow name/save/execute up
// top; execution panel driven by GET /executions/:id).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"microflow/internal/model"
	"microflow/internal/parser"
	"microflow/internal/runner"
	"microflow/internal/store"
	"microflow/internal/vault"
)

type WorkflowStore interface {
	SaveWorkflow(ctx context.Context, wf *model.Workflow) error
	LoadWorkflow(ctx context.Context, id string) (*model.Workflow, error)
	ListWorkflows(ctx context.Context) ([]*model.Workflow, error)
}

// CredentialStore is the read side of vault.Store (internal/store's
// Postgres implementation satisfies it too): metadata only, never
// ciphertext or plaintext secrets (rule 11/12).
type CredentialStore interface {
	ListCredentials(ctx context.Context, workflowID string) ([]store.CredentialInfo, error)
}

type Server struct {
	mux         *http.ServeMux
	workflows   WorkflowStore
	credentials CredentialStore
	run         *runner.Runner
	vault       *vault.Vault
}

// New wires the API to the shared runner/store/vault -- the exact same
// *vault.Vault instance cmd/server hands the OAuthResolver, so this
// package writes credentials through the identical encryption/storage
// path as cmd/setcred, never a parallel one.
func New(workflows WorkflowStore, run *runner.Runner, credentials CredentialStore, v *vault.Vault) *Server {
	s := &Server{mux: http.NewServeMux(), workflows: workflows, run: run, credentials: credentials, vault: v}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/workflows", s.handleList)
	s.mux.HandleFunc("POST /api/workflows/import", s.handleImport)
	s.mux.HandleFunc("POST /api/workflows/{id}/save", s.handleSave)
	s.mux.HandleFunc("GET /api/workflows/{id}", s.handleGet)
	s.mux.HandleFunc("GET /api/workflows/{id}/export", s.handleExport)
	s.mux.HandleFunc("POST /api/workflows/{id}/execute", s.handleExecute)
	s.mux.HandleFunc("GET /api/workflows/{id}/credentials", s.handleListCredentials)
	s.mux.HandleFunc("POST /api/workflows/{id}/credentials", s.handleSaveCredential)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	wfs, err := s.workflows.ListWorkflows(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, wfs)
}

// handleImport implements rule 1/26: parse, return the compatibility
// checklist, and DO NOT silently drop anything the parser didn't map.
func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	raw, err := readBody(r, 10*1024*1024)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	result, err := parser.Parse(raw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.workflows.SaveWorkflow(r.Context(), result.Workflow); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"workflow":    result.Workflow,
		"checklist":   result.Checklist,
		"unsupported": result.Unsupported(),
	})
}

// maxWorkflowSaveBytes mirrors handleImport's 10MB cap. handleSave
// previously decoded straight from r.Body with no limit at all -- a
// workflow-editor bug or a malicious client could stream an unbounded
// body into json.Decoder and grow heap without any ceiling.
const maxWorkflowSaveBytes = 10 * 1024 * 1024

func (s *Server) handleSave(w http.ResponseWriter, r *http.Request) {
	var wf model.Workflow
	limited := io.LimitReader(r.Body, maxWorkflowSaveBytes+1)
	if err := json.NewDecoder(limited).Decode(&wf); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	wf.ID = r.PathValue("id")
	if err := s.workflows.SaveWorkflow(r.Context(), &wf); err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, wf)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	wf, err := s.workflows.LoadWorkflow(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, wf)
}

// handleExport converts the saved workflow back to n8n's JSON export
// shape (internal/parser.Export) and returns it as a downloadable file,
// per the spec's "Export" requirement in the UI's Save/Import/Export list.
func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	wf, err := s.workflows.LoadWorkflow(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	raw, err := parser.Export(wf)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", `attachment; filename="`+sanitizeExportFilename(wf.Name)+`.json"`)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

// handleExecute runs a workflow manually via the shared runner (the
// exact same path the scheduler and webhook server use), starting from
// the given node (default: the first trigger found).
func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	wf, err := s.workflows.LoadWorkflow(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	startNode := r.URL.Query().Get("startNode")
	if startNode == "" {
		startNode = runner.FirstTriggerNode(wf)
	}
	if startNode == "" {
		writeErr(w, http.StatusBadRequest, errNoTrigger)
		return
	}

	seed := model.NodeOutput{{{JSON: map[string]any{}}}}
	ex, _ := s.run.RunFromNode(r.Context(), wf.ID, startNode, "manual", seed)
	// Note: RunFromNode's error return is the *workflow's* run error --
	// the HTTP call itself succeeded (we have a valid `ex` either way,
	// even on failure), so this always replies 200 with status in the body.
	writeJSON(w, http.StatusOK, ex)
}

// credentialNodeTypes are exactly the node types
// internal/nodes/google.go calls Creds.Resolve(workflowID, node.Name)
// for at runtime -- the same set cmd/setcred's googleNodeNames()
// filters on. Anything else is rejected before it ever reaches the vault.
func isGoogleCredentialNodeType(t model.NodeType) bool {
	switch t {
	case model.TypeGoogleSheets, model.TypeYouTube, model.TypeGmail:
		return true
	default:
		return false
	}
}

// credentialView is CredentialStore's result reshaped for the response:
// adds the node's type for display, still never touches secret bytes.
type credentialView struct {
	NodeName  string `json:"nodeName"`
	NodeType  string `json:"nodeType,omitempty"`
	UpdatedAt string `json:"updatedAt"`
}

// handleListCredentials lists which nodes in a workflow already have a
// Google credential saved, for a "saved accounts" list -- node name and
// last-updated time only, never client secret/refresh token (rule 11/12).
func (s *Server) handleListCredentials(w http.ResponseWriter, r *http.Request) {
	wfID := r.PathValue("id")
	wf, err := s.workflows.LoadWorkflow(r.Context(), wfID)
	if err != nil {
		writeErr(w, http.StatusNotFound, errors.New("workflow not found"))
		return
	}
	creds, err := s.credentials.ListCredentials(r.Context(), wfID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to list credentials"))
		return
	}
	out := make([]credentialView, 0, len(creds))
	for _, c := range creds {
		view := credentialView{NodeName: c.NodeName, UpdatedAt: c.UpdatedAt.Format(time.RFC3339)}
		if n, ok := wf.Nodes[c.NodeName]; ok {
			view.NodeType = string(n.Type)
		}
		out = append(out, view)
	}
	writeJSON(w, http.StatusOK, out)
}

// maxCredentialSaveBytes caps the request body for handleSaveCredential.
// OAuth client secrets/refresh tokens are short strings, so this is
// generous headroom, not an attempt to fit real-world data exactly.
const maxCredentialSaveBytes = 8 * 1024

type saveCredentialRequest struct {
	NodeName     string `json:"nodeName"`
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	RefreshToken string `json:"refreshToken"`
}

// handleSaveCredential is the HTTP equivalent of cmd/setcred, scoped to
// one already-existing Google node in one workflow (chosen from a
// dropdown by the frontend, per the "target one specific node" design):
// it validates the node, builds the same secret shape setcred does via
// vault.GoogleOAuthSecrets, and calls the identical *vault.Vault.Put the
// CLI and the server's OAuthResolver both use -- no parallel storage,
// no new encryption, no CLI spawned.
func (s *Server) handleSaveCredential(w http.ResponseWriter, r *http.Request) {
	wfID := r.PathValue("id")

	var req saveCredentialRequest
	limited := io.LimitReader(r.Body, maxCredentialSaveBytes+1)
	if err := json.NewDecoder(limited).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("invalid request body"))
		return
	}
	req.NodeName = strings.TrimSpace(req.NodeName)
	req.ClientID = strings.TrimSpace(req.ClientID)
	req.ClientSecret = strings.TrimSpace(req.ClientSecret)
	req.RefreshToken = strings.TrimSpace(req.RefreshToken)

	if req.NodeName == "" {
		writeErr(w, http.StatusBadRequest, errors.New("nodeName is required"))
		return
	}
	if req.ClientID == "" || req.ClientSecret == "" || req.RefreshToken == "" {
		// Deliberately doesn't echo back which field's value was given --
		// nothing from req is ever reflected into a response (rule 11/12).
		writeErr(w, http.StatusBadRequest, errors.New("clientId, clientSecret, and refreshToken are all required"))
		return
	}

	wf, err := s.workflows.LoadWorkflow(r.Context(), wfID)
	if err != nil {
		writeErr(w, http.StatusNotFound, errors.New("workflow not found"))
		return
	}
	node, ok := wf.Nodes[req.NodeName]
	if !ok {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("no node named %q in this workflow", req.NodeName))
		return
	}
	if !isGoogleCredentialNodeType(node.Type) {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("node %q is a %q node -- credentials can only be attached to googleSheets/youTube/gmail nodes", req.NodeName, node.Type))
		return
	}

	secrets := vault.GoogleOAuthSecrets(req.ClientID, req.ClientSecret, req.RefreshToken)
	if err := s.vault.Put(r.Context(), wfID, req.NodeName, secrets); err != nil {
		// Generic on purpose: never echo vault/cipher internals (rule 11/12).
		writeErr(w, http.StatusInternalServerError, errors.New("failed to save credential"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":   "ok",
		"nodeName": req.NodeName,
	})
}

func readBody(r *http.Request, max int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r.Body, max))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

var errNoTrigger = jsonErr("no trigger node found and no ?startNode given")

type jsonErr string

func (e jsonErr) Error() string { return string(e) }

func sanitizeExportFilename(name string) string {
	if name == "" {
		return "workflow"
	}
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == ' ':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
