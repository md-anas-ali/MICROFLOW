// Global Workflow: ONE upload that becomes the workflow of EVERY Service.
//
// POST /api/global-workflow (raw n8n/MicroFlow workflow JSON, same body as
// /api/workflows/import). For each existing Service the previous workflow(s)
// are removed and a fresh copy of the uploaded workflow (new id, so Services
// stay fully isolated) takes their place. Environment, credentials and
// settings of every Service are untouched.
//
// Safe by construction: nothing is changed if any affected workflow has a
// running/queued execution (409). The new copy is saved before the old one is
// deleted, so a Service is never left without a workflow on a failed step.
package api

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"microflow/internal/model"
	"microflow/internal/parser"
	"microflow/internal/store"
)

func newWorkflowUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (s *Server) handleGlobalWorkflow(w http.ResponseWriter, r *http.Request) {
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
	template, err := json.Marshal(result.Workflow)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to prepare workflow"))
		return
	}

	ctx := r.Context()
	svcs, err := s.services.ListServices(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to list services"))
		return
	}

	// Pre-flight: refuse the whole operation if anything is running.
	old := map[string][]*model.Workflow{}
	for _, sv := range svcs {
		wfs, err := s.services.ListWorkflowsByService(ctx, sv.ID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, errors.New("failed to list workflows"))
			return
		}
		for _, wf := range wfs {
			if s.run != nil && s.run.IsWorkflowActive(wf.ID) {
				writeErr(w, http.StatusConflict, fmt.Errorf("service %q has a running or queued workflow -- stop it first, nothing was changed", sv.Name))
				return
			}
		}
		old[sv.ID] = wfs
	}

	replaced := 0
	for _, sv := range svcs {
		var wf model.Workflow
		if err := json.Unmarshal(template, &wf); err != nil {
			writeErr(w, http.StatusInternalServerError, errors.New("failed to copy workflow"))
			return
		}
		wf.ID = newWorkflowUUID()
		wf.ServiceID = sv.ID
		if err := s.persistWorkflow(ctx, &wf); err != nil {
			writeErr(w, http.StatusInternalServerError, fmt.Errorf("service %q: failed to save workflow (earlier services already updated, this one kept its old workflow): %v", sv.Name, err))
			return
		}
		for _, o := range old[sv.ID] {
			s.persistMu.Lock()
			derr := s.workflows.DeleteWorkflow(ctx, o.ID)
			if derr == nil && s.onWorkflowChanged != nil {
				s.onWorkflowChanged(o.ID, nil)
			}
			s.persistMu.Unlock()
			if derr != nil && !errors.Is(derr, store.ErrWorkflowNotFound) {
				writeErr(w, http.StatusInternalServerError, fmt.Errorf("service %q: new workflow saved but old one could not be removed: %v", sv.Name, derr))
				return
			}
		}
		replaced++
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":      "ok",
		"name":        result.Workflow.Name,
		"services":    replaced,
		"checklist":   result.Checklist,
		"unsupported": result.Unsupported(),
	})
}
