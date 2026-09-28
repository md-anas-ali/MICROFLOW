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
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	s.mux.HandleFunc("GET /api/services/{id}/env/{key}", s.handleRevealServiceEnv)
	s.mux.HandleFunc("POST /api/services/{id}/env/import", s.handleImportServiceEnv)
	s.mux.HandleFunc("POST /api/services/{id}/env/delete-all", s.handleDeleteAllServiceEnv)

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
	// Mode is optional. "" keeps the original upsert behavior (Global
	// Environment page, older clients). "create" rejects an existing key
	// (409); "update" rejects a missing key (404) -- so Add can never
	// overwrite and Edit can never create a duplicate.
	Mode string `json:"mode"`
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
	if !s.requireEnvService(w, r, id) {
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
	if !s.requireEnvService(w, r, id) {
		return
	}
	req, err := decodeEnvEntry(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.Mode != "" && req.Mode != "create" && req.Mode != "update" {
		writeErr(w, http.StatusBadRequest, errors.New("mode must be create or update"))
		return
	}
	envWriteMu.Lock()
	defer envWriteMu.Unlock()
	if req.Mode != "" {
		exists, err := s.serviceEnvHasKey(r, id, req.Key)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, errors.New("failed to check service environment"))
			return
		}
		if req.Mode == "create" && exists {
			writeErr(w, http.StatusConflict, errors.New("a variable with this name already exists in this Service -- use Edit to change it"))
			return
		}
		if req.Mode == "update" && !exists {
			writeErr(w, http.StatusNotFound, errors.New("variable not found in this Service"))
			return
		}
	}
	if err := s.envVault.PutService(r.Context(), id, req.Key, req.Value, req.IsSecret); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to save service environment value"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "key": req.Key})
}

func (s *Server) handleDeleteServiceEnv(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireEnvService(w, r, id) {
		return
	}
	key := r.PathValue("key")
	if err := s.envVault.DeleteService(r.Context(), id, key); err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to delete service environment value"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// envWriteMu serializes Service Environment check-then-write sequences
// (Add's duplicate check, Edit's exists check, Import) so two concurrent
// requests can't both pass the check.
var envWriteMu sync.Mutex

// ReauthFunc re-verifies the logged-in person's password (the existing
// login gate; see cmd/server/auth.go). retryAfter > 0 means the caller
// is temporarily locked out by the same brute-force limiter as login.
type ReauthFunc func(r *http.Request, password string) (ok bool, retryAfter time.Duration)

// requireEnvService is requireService plus a defense-in-depth isolation
// check: when the dashboard's X-Microflow-Service header is present it
// must name the same Service as the URL, so a Service context can never
// be used to reach another Service's Environment. Mismatch => 404.
func (s *Server) requireEnvService(w http.ResponseWriter, r *http.Request, id string) bool {
	if want := r.Header.Get("X-Microflow-Service"); want != "" && want != id {
		writeErr(w, http.StatusNotFound, errors.New("service not found"))
		return false
	}
	return s.requireService(w, r, id)
}

func (s *Server) serviceEnvHasKey(r *http.Request, id, key string) (bool, error) {
	list, err := s.services.ListServiceEnv(r.Context(), id)
	if err != nil {
		return false, err
	}
	for _, e := range list {
		if e.Key == key {
			return true, nil
		}
	}
	return false, nil
}

// handleRevealServiceEnv returns ONE decrypted value of this Service,
// only when explicitly asked for by key (the dashboard's show/edit
// action). List responses stay metadata-only. Never cached, never logged.
func (s *Server) handleRevealServiceEnv(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireEnvService(w, r, id) {
		return
	}
	key := r.PathValue("key")
	if !validEnvKey.MatchString(key) {
		writeErr(w, http.StatusBadRequest, errors.New("invalid key"))
		return
	}
	val, found, err := s.envVault.GetService(r.Context(), id, key)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to read service environment value"))
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, errors.New("variable not found in this Service"))
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": val})
}

const (
	maxEnvImportBytes   = 256 * 1024
	maxEnvImportEntries = 500
)

type envImportRequest struct {
	Content   string `json:"content"`
	Overwrite bool   `json:"overwrite"`
}

type envKV struct{ Key, Value string }

// envImportIssue never carries line content (it may hold a secret) --
// only the line number and a fixed reason.
type envImportIssue struct {
	Line   int    `json:"line"`
	Reason string `json:"reason"`
}

// parseDotEnv parses .env text: KEY=VALUE per line; blank lines and
// #-comments ignored; optional "export " prefix; single/double quoted
// values; unquoted values lose a trailing " # comment". Duplicate keys
// inside the file: the last one wins (dotenv convention) and is counted.
// Multi-line values are not supported.
func parseDotEnv(content string) (entries []envKV, issues []envImportIssue, dupes int) {
	content = strings.TrimPrefix(content, "\ufeff")
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")
	var order []string
	vals := map[string]string{}
	for i, raw := range strings.Split(content, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") || strings.HasPrefix(line, "export\t") {
			line = strings.TrimSpace(line[len("export"):])
		}
		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			issues = append(issues, envImportIssue{Line: i + 1, Reason: "expected KEY=VALUE"})
			continue
		}
		key := strings.TrimSpace(line[:eq])
		if !validEnvKey.MatchString(key) {
			issues = append(issues, envImportIssue{Line: i + 1, Reason: "invalid variable name"})
			continue
		}
		val, ok := parseDotEnvValue(strings.TrimSpace(line[eq+1:]))
		if !ok {
			issues = append(issues, envImportIssue{Line: i + 1, Reason: "unterminated quote"})
			continue
		}
		if len(val) > maxEnvValueBytes {
			issues = append(issues, envImportIssue{Line: i + 1, Reason: "value is too large"})
			continue
		}
		if _, seen := vals[key]; seen {
			dupes++
		} else {
			order = append(order, key)
		}
		vals[key] = val
	}
	for _, k := range order {
		entries = append(entries, envKV{Key: k, Value: vals[k]})
	}
	return entries, issues, dupes
}

func parseDotEnvValue(v string) (string, bool) {
	if v == "" {
		return "", true
	}
	switch v[0] {
	case '"':
		var b strings.Builder
		for i := 1; i < len(v); i++ {
			c := v[i]
			if c == '\\' && i+1 < len(v) {
				i++
				switch v[i] {
				case 'n':
					b.WriteByte('\n')
				case 't':
					b.WriteByte('\t')
				case '"', '\\':
					b.WriteByte(v[i])
				default:
					b.WriteByte('\\')
					b.WriteByte(v[i])
				}
				continue
			}
			if c == '"' {
				return b.String(), true // anything after the closing quote (e.g. a comment) is ignored
			}
			b.WriteByte(c)
		}
		return "", false
	case '\'':
		end := strings.IndexByte(v[1:], '\'')
		if end < 0 {
			return "", false
		}
		return v[1 : 1+end], true
	}
	if i := strings.Index(v, " #"); i >= 0 {
		v = v[:i]
	}
	return strings.TrimSpace(v), true
}

// handleImportServiceEnv imports .env text into THIS Service only.
// Existing keys are kept unless overwrite=true. Imported values are
// stored as secrets (safe default; Edit can change that). The response
// reports counts and line numbers only -- never any value.
func (s *Server) handleImportServiceEnv(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireEnvService(w, r, id) {
		return
	}
	var req envImportRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, maxEnvImportBytes+4096)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("invalid request body"))
		return
	}
	if len(req.Content) > maxEnvImportBytes {
		writeErr(w, http.StatusBadRequest, errors.New(".env content is too large"))
		return
	}
	entries, issues, dupes := parseDotEnv(req.Content)
	if len(entries) == 0 {
		writeErr(w, http.StatusBadRequest, errors.New("no valid KEY=VALUE entries found"))
		return
	}
	if len(entries) > maxEnvImportEntries {
		writeErr(w, http.StatusBadRequest, errors.New("too many entries in one import"))
		return
	}
	envWriteMu.Lock()
	defer envWriteMu.Unlock()
	existing, err := s.services.ListServiceEnv(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to check service environment"))
		return
	}
	have := make(map[string]bool, len(existing))
	for _, e := range existing {
		have[e.Key] = true
	}
	added, updated, skipped := 0, 0, 0
	for _, kv := range entries {
		if have[kv.Key] && !req.Overwrite {
			skipped++
			continue
		}
		if err := s.envVault.PutService(r.Context(), id, kv.Key, kv.Value, true); err != nil {
			writeErr(w, http.StatusInternalServerError, errors.New("failed to save service environment value"))
			return
		}
		if have[kv.Key] {
			updated++
		} else {
			added++
		}
	}
	if issues == nil {
		issues = []envImportIssue{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"added": added, "updated": updated, "skipped": skipped,
		"duplicatesInFile": dupes, "invalid": issues,
	})
}

type envDeleteAllRequest struct {
	ConfirmName string `json:"confirmName"`
	Password    string `json:"password"`
}

// handleDeleteAllServiceEnv deletes this Service's whole Environment set
// (variables only). Two steps, both enforced HERE, not just in the UI:
//  1. confirmation -- confirmName must equal the Service's name;
//  2. the existing login gate's password re-check (s.reauth), rate
//     limited by the same limiter as login.
//
// Fails closed if no re-check is wired.
func (s *Server) handleDeleteAllServiceEnv(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.requireEnvService(w, r, id) {
		return
	}
	if s.reauth == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("deleting an Environment set is not enabled on this server"))
		return
	}
	var req envDeleteAllRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<10)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, errors.New("invalid request body"))
		return
	}
	svc, err := s.services.GetService(r.Context(), id)
	if err != nil || svc == nil {
		writeErr(w, http.StatusNotFound, errors.New("service not found"))
		return
	}
	if req.ConfirmName == "" || req.ConfirmName != svc.Name {
		writeErr(w, http.StatusBadRequest, errors.New("confirmation did not match -- nothing was deleted"))
		return
	}
	ok, wait := s.reauth(r, req.Password)
	if wait > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
		writeErr(w, http.StatusTooManyRequests, errors.New("too many failed attempts -- try again later"))
		return
	}
	if !ok {
		writeErr(w, http.StatusForbidden, errors.New("verification failed -- nothing was deleted"))
		return
	}
	envWriteMu.Lock()
	defer envWriteMu.Unlock()
	n, err := s.envVault.DeleteAllService(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("failed to delete service environment"))
		return
	}
	log.Printf("service environment set deleted: service=%s variables=%d", id, n) // count only, never keys/values
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "deleted": n})
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
