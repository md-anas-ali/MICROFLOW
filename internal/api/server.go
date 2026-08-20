// Package api is MicroFlow's HTTP API, consumed by the React editor.
// Endpoints: import, save, get, list, execute, list executions -- the
// spec's "Simple n8n-style UI" needs (workflow name/save/execute up
// top; execution panel driven by GET /executions/:id).
package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"microflow/internal/model"
	"microflow/internal/parser"
	"microflow/internal/runner"
)

type WorkflowStore interface {
	SaveWorkflow(ctx context.Context, wf *model.Workflow) error
	LoadWorkflow(ctx context.Context, id string) (*model.Workflow, error)
	ListWorkflows(ctx context.Context) ([]*model.Workflow, error)
}

type Server struct {
	mux       *http.ServeMux
	workflows WorkflowStore
	run       *runner.Runner
}

func New(workflows WorkflowStore, run *runner.Runner) *Server {
	s := &Server{mux: http.NewServeMux(), workflows: workflows, run: run}
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
