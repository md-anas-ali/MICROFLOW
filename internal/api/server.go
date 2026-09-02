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
	"sort"
	"strconv"
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

// ExecutionLoader is the durable fallback GET /api/executions/{id}
// uses once an async execution's in-memory record (in *runner.Manager)
// has been evicted or was never async in the first place (scheduler/
// webhook runs still go through the synchronous Runner.RunFromNode
// path and are only ever visible here). *store.Store satisfies this.
type ExecutionLoader interface {
	GetExecution(ctx context.Context, id string) (*model.Execution, error)
}

// ExecutionHistoryStore supplies durable execution history for the live
// monitor/history page. *store.Store implements it.
type ExecutionHistoryStore interface {
	ListExecutions(ctx context.Context, limit int) ([]*model.Execution, error)
}

type Server struct {
	mux         *http.ServeMux
	workflows   WorkflowStore
	credentials CredentialStore
	run         *runner.Runner
	vault       *vault.Vault
	// accounts is nil-safe by design: every handler that touches it
	// checks for nil first and returns a clear 500 rather than a panic,
	// so a server wired up without central-credential storage (e.g. an
	// older test helper) still serves every other endpoint normally.
	accounts *vault.AccountVault

	// googleOAuth/googleAccounts back the "Google Connections" page
	// (Connect/Reconnect/Disconnect per service). Both are nil unless
	// EnableGoogleOAuth is called (main.go does this only when
	// GOOGLE_OAUTH_CLIENT_ID/SECRET/REDIRECT_URL are configured), so a
	// server without a registered OAuth app simply doesn't expose the
	// /api/google/* routes rather than panicking -- existing manual
	// credential paste endpoints above are unaffected either way.
	googleOAuth    *vault.GoogleOAuthApp
	googleAccounts *vault.GoogleServiceAccounts

	// manager and execLoader back the async execute/executions/events
	// endpoints (handleExecuteAsync, handleGetExecution,
	// handleCancelExecution, handleExecutionEvents). Both nil-safe by
	// design, same pattern as accounts above: a Server built with plain
	// New() (every existing test helper) keeps the old synchronous
	// /execute behavior and 501s the new endpoints, rather than
	// panicking. Call WithAsync to enable them.
	manager     *runner.Manager
	execLoader  ExecutionLoader
	execHistory ExecutionHistoryStore
}

// WithAsync enables the async execute/executions/events endpoints:
// mgr is the bounded worker-pool Manager handleExecuteAsync starts
// runs on; execs is the durable fallback handleGetExecution reads from
// once Manager evicts a finished execution's in-memory record. Returns
// s for chaining, mirroring runner.Runner.WithMemGuard's pattern.
// Both must be non-nil for the async endpoints to actually activate --
// passing a nil mgr is a no-op (existing sync behavior keeps working).
func (s *Server) WithAsync(mgr *runner.Manager, execs ExecutionLoader) *Server {
	s.manager = mgr
	s.execLoader = execs
	if history, ok := execs.(ExecutionHistoryStore); ok {
		s.execHistory = history
	}
	return s
}

// New wires the API to the shared runner/store/vault -- the exact same
// *vault.Vault instance cmd/server hands the OAuthResolver, so this
// package writes credentials through the identical encryption/storage
// path as cmd/setcred, never a parallel one. accounts is the central
// (account-scoped, not per-workflow) Google credential store backing
// the /api/credentials/google endpoints -- see internal/vault/central.go.
func New(workflows WorkflowStore, run *runner.Runner, credentials CredentialStore, v *vault.Vault, accounts *vault.AccountVault) *Server {
	s := &Server{mux: http.NewServeMux(), workflows: workflows, run: run, credentials: credentials, vault: v, accounts: accounts}
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

	// Async execution follow-up endpoints (spec sections M/N). Always
	// registered; each handler 501s cleanly if WithAsync was never
	// called, rather than the route simply not existing (see manager's
	// doc comment above).
	s.mux.HandleFunc("GET /api/executions", s.handleListExecutions)
	s.mux.HandleFunc("GET /api/executions/events", s.handleAllExecutionEvents)
	s.mux.HandleFunc("GET /api/executions/{id}", s.handleGetExecution)
	s.mux.HandleFunc("POST /api/executions/{id}/cancel", s.handleCancelExecution)
	s.mux.HandleFunc("GET /api/executions/{id}/events", s.handleExecutionEvents)

	s.mux.HandleFunc("GET /api/workflows/{id}/credentials", s.handleListCredentials)
	s.mux.HandleFunc("POST /api/workflows/{id}/credentials", s.handleSaveCredential)

	// Central Google credential: one saved account, shared automatically
	// by every googleSheets/youTube/gmail node in every workflow (see
	// internal/vault/central.go's CentralFallbackResolver). Deliberately
	// not scoped under /api/workflows/{id} -- it isn't a per-workflow
	// resource.
	s.mux.HandleFunc("GET /api/credentials/google", s.handleGetCentralCredential)
	s.mux.HandleFunc("POST /api/credentials/google", s.handleSaveCentralCredential)
	s.mux.HandleFunc("DELETE /api/credentials/google", s.handleDeleteCentralCredential)
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

// handleExecute starts a workflow run and returns immediately (spec
// section M: "The HTTP request MUST NEVER wait for workflow
// completion"). If the server was wired with WithAsync, it enqueues
// the run on the bounded Manager and replies 202 with
// {executionId, status:"queued"}; progress is then followed via
// GET /api/executions/{id} or the SSE stream at
// GET /api/executions/{id}/events. A server not wired with WithAsync
// (e.g. an older test helper -- see execute_test.go) falls back to the
// previous synchronous behavior unchanged, so existing callers/tests
// are unaffected.
func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	if s.manager != nil {
		s.handleExecuteAsync(w, r)
		return
	}
	s.handleExecuteSync(w, r)
}

type executeAcceptedResponse struct {
	ExecutionID string `json:"executionId"`
	Status      string `json:"status"`
}

func (s *Server) handleExecuteAsync(w http.ResponseWriter, r *http.Request) {
	wfID := r.PathValue("id")
	startNode := r.URL.Query().Get("startNode")
	seed := model.NodeOutput{{{JSON: map[string]any{}}}}

	execID, err := s.manager.Start(r.Context(), wfID, startNode, "manual", seed)
	if err != nil {
		if errors.Is(err, runner.ErrQueueFull) {
			// 429: bounded backpressure, not an unbounded queue (rule 19).
			writeErr(w, http.StatusTooManyRequests, err)
			return
		}
		// Everything else Manager.Start can return (workflow not found,
		// no trigger/startNode, unknown startNode) is a client error
		// about this specific request, not a server failure.
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusAccepted, executeAcceptedResponse{ExecutionID: execID, Status: string(model.StatusQueued)})
}

// handleExecuteSync is the original (pre-async) behavior: runs the
// workflow via the shared runner and blocks until it finishes. Kept as
// the fallback for a Server not wired with WithAsync.
func (s *Server) handleExecuteSync(w http.ResponseWriter, r *http.Request) {
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
	ex, err := s.run.RunFromNode(r.Context(), wf.ID, startNode, "manual", seed)
	// RunFromNode returns (non-nil execution, workflow-run-error) once a
	// run actually starts -- engine.Run always populates the execution
	// even on failure, so that case correctly replies 200 with the
	// status/error visible in the body for the exec panel.
	//
	// But RunFromNode can also fail *before* a run ever starts (its own
	// LoadWorkflow race, missing start node, or the per-execution scratch
	// dir failing to create -- e.g. a full/read-only disk). Those return
	// (nil, err) and were previously falling through to `writeJSON(200,
	// nil)`: an HTTP success with a JSON `null` body. The frontend then
	// dereferenced fields on that null (ex.nodeRuns, ex.status), which is
	// the silent-failure bug this fixes -- give it a real error status
	// instead of a fake success.
	if ex == nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, ex)
}

// handleListExecutions returns the newest executions, merging live Manager
// snapshots over durable history so queued/running records are immediately
// visible and completed records remain available after the in-memory cache
// expires.
func (s *Server) handleListExecutions(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 500 {
		limit = 500
	}

	byID := make(map[string]*model.Execution, limit)
	if s.execHistory != nil {
		history, err := s.execHistory.ListExecutions(r.Context(), limit)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		for _, ex := range history {
			if ex != nil {
				byID[ex.ID] = ex
			}
		}
	}
	if s.manager != nil {
		for _, ex := range s.manager.List(limit) {
			if ex != nil {
				byID[ex.ID] = ex
			}
		}
	}

	out := make([]*model.Execution, 0, len(byID))
	for _, ex := range byID {
		out = append(out, ex)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	queue := map[string]int{}
	if s.manager != nil {
		queue = s.manager.Stats()
	}
	writeJSON(w, http.StatusOK, map[string]any{"executions": out, "queue": queue})
}

// handleAllExecutionEvents is the global SSE stream used by the Executions
// page. Events are metadata only; clients refresh the bounded list when an
// execution changes instead of receiving workflow payloads over this stream.
func (s *Server) handleAllExecutionEvents(w http.ResponseWriter, r *http.Request) {
	if s.manager == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("async execution is not enabled on this server"))
		return
	}
	events, replay, unsubscribe := s.manager.SubscribeAll()
	defer unsubscribe()
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, errors.New("streaming is not supported by this server"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-store")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	seq := 0
	for _, ev := range replay {
		seq++
		if err := writeSSEEvent(w, flusher, seq, ev); err != nil {
			return
		}
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			seq++
			if err := writeSSEEvent(w, flusher, seq, ev); err != nil {
				return
			}
		case <-ticker.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// handleGetExecution returns the current state of one execution: the
// live in-memory record from Manager while it's queued/running/newly
// finished, falling back to the durable store once Manager has evicted
// it (see runner.finishedRetention) or for executions that were never
// started through this Manager at all (scheduler/webhook runs).
func (s *Server) handleGetExecution(w http.ResponseWriter, r *http.Request) {
	execID := r.PathValue("id")
	if s.manager != nil {
		if ex, ok := s.manager.Get(execID); ok {
			writeJSON(w, http.StatusOK, ex)
			return
		}
	}
	if s.execLoader == nil {
		writeErr(w, http.StatusNotFound, errors.New("execution not found"))
		return
	}
	ex, err := s.execLoader.GetExecution(r.Context(), execID)
	if err != nil {
		writeErr(w, http.StatusNotFound, errors.New("execution not found"))
		return
	}
	writeJSON(w, http.StatusOK, ex)
}

// handleCancelExecution requests cancellation of a queued or running
// execution. Only meaningful for async executions (a synchronous
// RunFromNode call has no separate cancel handle); returns 404 if the
// Manager has no live record (already finished, or never async).
func (s *Server) handleCancelExecution(w http.ResponseWriter, r *http.Request) {
	if s.manager == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("async execution is not enabled on this server"))
		return
	}
	execID := r.PathValue("id")
	if err := s.manager.Cancel(execID); err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelling"})
}

// writeSSEEvent writes one event as a plain default-"message" SSE
// frame (id + data only, deliberately no `event:` line): the frontend
// listens with a single EventSource.onmessage handler and reads
// ev.type from the decoded JSON payload itself, rather than needing a
// named addEventListener per runner.EventType. `data` is the
// JSON-encoded runner.Event -- metadata/NodeRunResult only, never raw
// media (see runner.Event's doc comment).
func writeSSEEvent(w http.ResponseWriter, flusher http.Flusher, seq int, ev runner.Event) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\ndata: %s\n\n", seq, payload); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// handleExecutionEvents streams live progress for one execution over
// SSE (spec section N). Replays a small bounded backlog first (so a
// client connecting a moment late still sees execution.created/early
// node events), then forwards new events as Manager's broadcaster
// publishes them, until the execution reaches a terminal state (the
// broadcaster closes the channel) or the client disconnects (request
// context cancelled). Payloads are metadata-only NodeRunResult
// snapshots -- never raw media (rule: "NEVER send video/audio/image/
// binary data through SSE"); the per-client channel is bounded
// (runner.subscriberBufSize) so a slow client cannot grow server
// memory (rule 10).
func (s *Server) handleExecutionEvents(w http.ResponseWriter, r *http.Request) {
	if s.manager == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("async execution is not enabled on this server"))
		return
	}
	execID := r.PathValue("id")
	events, replay, unsubscribe, ok := s.manager.Subscribe(execID)
	if !ok {
		writeErr(w, http.StatusNotFound, errors.New("execution not found or already finished"))
		return
	}
	defer unsubscribe()

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, errors.New("streaming not supported"))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	seq := 0
	for _, ev := range replay {
		if err := writeSSEEvent(w, flusher, seq, ev); err != nil {
			return
		}
		seq++
	}

	ctx := r.Context()
	// keepAlive prevents idle proxies/load balancers from closing a
	// long Wait node's connection; it's a comment, not a payload, so it
	// never counts as an "event" a client needs to parse.
	keepAlive := time.NewTicker(25 * time.Second)
	defer keepAlive.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-keepAlive.C:
			if _, err := fmt.Fprint(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case ev, chOK := <-events:
			if !chOK {
				return // broadcaster closed: execution reached a terminal state
			}
			if err := writeSSEEvent(w, flusher, seq, ev); err != nil {
				return
			}
			seq++
		}
	}
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

// centralCredentialStatus is what GET /api/credentials/google returns --
// whether an account is configured and when it was last saved, never
// secret material (rule 11/12), matching credentialView's shape above.
type centralCredentialStatus struct {
	Configured bool   `json:"configured"`
	UpdatedAt  string `json:"updatedAt,omitempty"`
}

func (s *Server) handleGetCentralCredential(w http.ResponseWriter, r *http.Request) {
	if s.accounts == nil {
		writeErr(w, http.StatusInternalServerError, errors.New("central credential storage is not configured on this server"))
		return
	}
	updatedAt, ok, err := s.accounts.Status(r.Context(), vault.CentralGoogleAccount)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to check central credential status"))
		return
	}
	out := centralCredentialStatus{Configured: ok}
	if ok {
		out.UpdatedAt = updatedAt.Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, out)
}

// handleSaveCentralCredential is the "log in once" endpoint: saves the
// single Google account credential every googleSheets/youTube/gmail
// node across every workflow will automatically use from now on (see
// CentralFallbackResolver). Same request shape, same field validation,
// and the same vault.GoogleOAuthSecrets encoding as the per-node
// handleSaveCredential above -- only the storage scope differs.
func (s *Server) handleSaveCentralCredential(w http.ResponseWriter, r *http.Request) {
	if s.accounts == nil {
		writeErr(w, http.StatusInternalServerError, errors.New("central credential storage is not configured on this server"))
		return
	}

	var req saveCredentialRequest
	limited := io.LimitReader(r.Body, maxCredentialSaveBytes+1)
	if err := json.NewDecoder(limited).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("invalid request body"))
		return
	}
	req.ClientID = strings.TrimSpace(req.ClientID)
	req.ClientSecret = strings.TrimSpace(req.ClientSecret)
	req.RefreshToken = strings.TrimSpace(req.RefreshToken)

	if req.ClientID == "" || req.ClientSecret == "" || req.RefreshToken == "" {
		// Deliberately doesn't echo back which field's value was given --
		// nothing from req is ever reflected into a response (rule 11/12).
		writeErr(w, http.StatusBadRequest, errors.New("clientId, clientSecret, and refreshToken are all required"))
		return
	}

	secrets := vault.GoogleOAuthSecrets(req.ClientID, req.ClientSecret, req.RefreshToken)
	if err := s.accounts.Put(r.Context(), vault.CentralGoogleAccount, secrets); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to save credential"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleDeleteCentralCredential(w http.ResponseWriter, r *http.Request) {
	if s.accounts == nil {
		writeErr(w, http.StatusInternalServerError, errors.New("central credential storage is not configured on this server"))
		return
	}
	if err := s.accounts.Delete(r.Context(), vault.CentralGoogleAccount); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to delete credential"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Google Connections (Connect/Reconnect/Disconnect per service) handlers
// live in google_connect.go, registered via EnableGoogleOAuth.

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
