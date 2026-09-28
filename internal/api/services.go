// Handlers backing Service (tenant) management, Global/Service
// Environment, and Run Service / Run All Services. Registered by
// WithTenancy so a Server built without it (every pre-existing test
// helper) exposes none of these routes, exactly like WithAsync/
// EnableGoogleOAuth's nil-safe pattern elsewhere in this package.
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"microflow/internal/runall"
	"microflow/internal/store"
	"microflow/internal/tenant"
)

func (s *Server) routesTenancy() {
	s.mux.HandleFunc("GET /api/services", s.handleListServices)
	s.mux.HandleFunc("POST /api/services", s.handleCreateService)
	s.mux.HandleFunc("GET /api/services/{id}", s.handleGetService)
	s.mux.HandleFunc("PATCH /api/services/{id}", s.handleRenameService)
	s.mux.HandleFunc("DELETE /api/services/{id}", s.handleDeleteService)
	s.mux.HandleFunc("GET /api/services/{id}/workflows", s.handleListServiceWorkflows)
	s.mux.HandleFunc("GET /api/services/{id}/executions", s.handleListServiceExecutions)
	s.mux.HandleFunc("POST /api/services/{id}/run", s.handleRunService)

	s.mux.HandleFunc("GET /api/global-env", s.handleListGlobalEnv)
	s.mux.HandleFunc("POST /api/global-env", s.handlePutGlobalEnv)
	s.mux.HandleFunc("DELETE /api/global-env/{key}", s.handleDeleteGlobalEnv)

	s.mux.HandleFunc("GET /api/services/{id}/env", s.handleListServiceEnv)
	s.mux.HandleFunc("POST /api/services/{id}/env", s.handlePutServiceEnv)
	s.mux.HandleFunc("DELETE /api/services/{id}/env/{key}", s.handleDeleteServiceEnv)

	s.mux.HandleFunc("POST /api/run-all", s.handleRunAll)
	s.mux.HandleFunc("GET /api/run-all", s.handleRunAllStatus)
	s.mux.HandleFunc("POST /api/run-all/cancel", s.handleRunAllCancel)
}

// --- Services ---

func (s *Server) handleListServices(w http.ResponseWriter, r *http.Request) {
	list, err := s.services.ListServices(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to list services"))
		return
	}
	writeJSON(w, http.StatusOK, list)
}

type createServiceRequest struct {
	Name string `json:"name"`
}

// nonSlugChars/slugify turn a Service's display name into a short,
// readable, URL/OAuth-state-safe id fragment (lowercase, digits and
// hyphens only -- the one character set every id in this codebase's
// URL paths already assumes is safe, see sanitizeExportFilename).
var nonSlugChars = regexp.MustCompile(`[^a-z0-9]+`)

func slugify(name string) string {
	s := nonSlugChars.ReplaceAllString(strings.ToLower(strings.TrimSpace(name)), "-")
	s = strings.Trim(s, "-")
	if s == "" {
		return "service"
	}
	if len(s) > 40 {
		s = s[:40]
	}
	return s
}

const maxServiceNameBytes = 200

// handleCreateService implements rule 3: unlimited logical Services,
// each with a unique id. The id is derived from the given name (for a
// short, readable URL/API path) with a random suffix to guarantee
// uniqueness even if two Services are given the same name.
func (s *Server) handleCreateService(w http.ResponseWriter, r *http.Request) {
	var req createServiceRequest
	limited := io.LimitReader(r.Body, maxServiceNameBytes+1024)
	if err := json.NewDecoder(limited).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("invalid request body"))
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		writeErr(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	if len(req.Name) > maxServiceNameBytes {
		writeErr(w, http.StatusBadRequest, errors.New("name is too long"))
		return
	}
	id := slugify(req.Name) + "-" + randSuffix()
	svc := &tenant.Service{ID: id, Name: req.Name}
	if err := s.services.CreateService(r.Context(), svc); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to create service"))
		return
	}
	writeJSON(w, http.StatusOK, svc)
}

// serviceDetail adds the workflow count to tenant.Service for the
// dashboard's Service list/detail view -- read-only derived data, not
// persisted.
type serviceDetail struct {
	*tenant.Service
	WorkflowCount int `json:"workflowCount"`
}

func (s *Server) handleGetService(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	svc, err := s.services.GetService(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, errors.New("service not found"))
		return
	}
	count, err := s.services.CountWorkflowsInService(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to load service"))
		return
	}
	writeJSON(w, http.StatusOK, serviceDetail{Service: svc, WorkflowCount: count})
}

type renameServiceRequest struct {
	Name string `json:"name"`
}

func (s *Server) handleRenameService(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req renameServiceRequest
	limited := io.LimitReader(r.Body, maxServiceNameBytes+1024)
	if err := json.NewDecoder(limited).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("invalid request body"))
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		writeErr(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	if err := s.services.RenameService(r.Context(), id, req.Name); err != nil {
		writeErr(w, http.StatusNotFound, errors.New("service not found"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleDeleteService implements the "protect from accidental
// destructive action" requirement two ways: (1) it requires the
// request to echo the Service's CURRENT name back as ?confirmName=,
// which the dashboard only ever does after the person has typed/
// confirmed it in a dialog -- a bare DELETE with no confirmation is
// rejected outright; (2) store.DeleteService itself refuses (409) if
// any workflow in the Service still has an in-flight execution.
func (s *Server) handleDeleteService(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == tenant.DefaultID {
		writeErr(w, http.StatusBadRequest, errors.New("the Default service cannot be deleted"))
		return
	}
	svc, err := s.services.GetService(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, errors.New("service not found"))
		return
	}
	confirm := r.URL.Query().Get("confirmName")
	if confirm == "" || confirm != svc.Name {
		writeErr(w, http.StatusBadRequest, errors.New("deleting a service is permanent -- pass ?confirmName=<the service's exact current name> to confirm"))
		return
	}
	if err := s.services.DeleteService(r.Context(), id); err != nil {
		if errors.Is(err, store.ErrServiceHasActiveExecutions) {
			writeErr(w, http.StatusConflict, errors.New("service has a running or queued execution and cannot be deleted"))
			return
		}
		if errors.Is(err, store.ErrServiceNotFound) {
			writeErr(w, http.StatusNotFound, errors.New("service not found"))
			return
		}
		writeErr(w, http.StatusInternalServerError, errors.New("failed to delete service"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "id": id})
}

func (s *Server) handleListServiceWorkflows(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireService(w, r, id) {
		return
	}
	wfs, err := s.services.ListWorkflowsByService(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to list workflows"))
		return
	}
	writeJSON(w, http.StatusOK, wfs)
}

func (s *Server) handleListServiceExecutions(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireService(w, r, id) {
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			limit = n
		}
	}
	execs, err := s.services.ListExecutionsByService(r.Context(), id, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to list executions"))
		return
	}
	writeJSON(w, http.StatusOK, execs)
}

// handleRunService runs every workflow belonging to this Service, in
// order, strictly sequentially, through the same durable Run-All
// machinery as Run All Services (see internal/runall) -- so a Service
// with several workflows behaves the same way running "all Services"
// does, just scoped to one.
func (s *Server) handleRunService(w http.ResponseWriter, r *http.Request) {
	if s.runAll == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("run-all is not enabled on this server"))
		return
	}
	id := r.PathValue("id")
	if !s.requireService(w, r, id) {
		return
	}
	jobID, err := s.runAll.StartOne(r.Context(), id, false)
	if err != nil {
		if errors.Is(err, runall.ErrAlreadyActive) {
			writeErr(w, http.StatusConflict, err)
			return
		}
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"jobId": jobID, "status": "queued"})
}

// --- Global / Service Environment ---
//
// Every handler below only ever writes or deletes a value, or lists
// metadata (key/isSecret/updatedAt) -- never reads a decrypted value
// back over HTTP (rule 13, matching every other credential endpoint in
// this package).

const maxEnvValueBytes = 64 * 1024

type envEntryRequest struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	IsSecret bool   `json:"isSecret"`
}

var validEnvKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func decodeEnvEntry(r *http.Request) (envEntryRequest, error) {
	var req envEntryRequest
	limited := io.LimitReader(r.Body, maxEnvValueBytes+1024)
	if err := json.NewDecoder(limited).Decode(&req); err != nil {
		return req, errors.New("invalid request body")
	}
	req.Key = strings.TrimSpace(req.Key)
	if req.Key == "" {
		return req, errors.New("key is required")
	}
	if !validEnvKey.MatchString(req.Key) {
		return req, errors.New("key must look like an environment variable name (letters, digits, underscore; not starting with a digit)")
	}
	if len(req.Value) > maxEnvValueBytes {
		return req, errors.New("value is too large")
	}
	return req, nil
}

func (s *Server) handleListGlobalEnv(w http.ResponseWriter, r *http.Request) {
	list, err := s.services.ListGlobalEnv(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to list global environment"))
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handlePutGlobalEnv(w http.ResponseWriter, r *http.Request) {
	req, err := decodeEnvEntry(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.envVault.PutGlobal(r.Context(), req.Key, req.Value, req.IsSecret); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to save global environment value"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "key": req.Key})
}

func (s *Server) handleDeleteGlobalEnv(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := s.envVault.DeleteGlobal(r.Context(), key); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to delete global environment value"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleListServiceEnv(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireService(w, r, id) {
		return
	}
	list, err := s.services.ListServiceEnv(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to list service environment"))
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handlePutServiceEnv(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireService(w, r, id) {
		return
	}
	req, err := decodeEnvEntry(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := s.envVault.PutService(r.Context(), id, req.Key, req.Value, req.IsSecret); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to save service environment value"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "key": req.Key})
}

func (s *Server) handleDeleteServiceEnv(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireService(w, r, id) {
		return
	}
	key := r.PathValue("key")
	if err := s.envVault.DeleteService(r.Context(), id, key); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to delete service environment value"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// --- Run All Services ---

type runAllRequest struct {
	StopOnFailure bool `json:"stopOnFailure"`
}

func (s *Server) handleRunAll(w http.ResponseWriter, r *http.Request) {
	if s.runAll == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("run-all is not enabled on this server"))
		return
	}
	var req runAllRequest
	if r.ContentLength > 0 {
		limited := io.LimitReader(r.Body, 4096)
		_ = json.NewDecoder(limited).Decode(&req) // best-effort; default false is fine
	}
	jobID, err := s.runAll.StartAll(r.Context(), req.StopOnFailure)
	if err != nil {
		if errors.Is(err, runall.ErrAlreadyActive) {
			writeErr(w, http.StatusConflict, err)
			return
		}
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"jobId": jobID, "status": "queued"})
}

func (s *Server) handleRunAllStatus(w http.ResponseWriter, r *http.Request) {
	if s.runAll == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("run-all is not enabled on this server"))
		return
	}
	st, err := s.runAll.Status(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to load run-all status"))
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) handleRunAllCancel(w http.ResponseWriter, r *http.Request) {
	if s.runAll == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("run-all is not enabled on this server"))
		return
	}
	if err := s.runAll.Cancel(r.Context()); err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelling"})
}

// randSuffix produces a short, URL-safe, non-security-sensitive
// uniqueness suffix for a Service id -- actual uniqueness is still
// enforced by the services table's primary key; this only makes a
// collision (and the resulting error asking the person to retry) rare
// in practice.
func randSuffix() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	seed := uint64(time.Now().UnixNano())
	out := make([]byte, 6)
	for i := range out {
		seed = seed*6364136223846793005 + 1442695040888963407
		out[i] = alphabet[(seed>>33)%uint64(len(alphabet))]
	}
	return string(out)
}
