// Package mcp exposes MicroFlow to AI clients (Claude, Cursor, ...) over the
// Model Context Protocol, using the "Streamable HTTP" transport: one endpoint,
// POST /mcp, carrying JSON-RPC 2.0 messages.
//
// Design:
//   - Zero new dependencies (stdlib only).
//   - Every tool is a thin in-process call to the EXISTING /api/* handler, so
//     Service isolation, validation, queueing and redaction behave exactly
//     like the web UI. No engine/store/vault code is touched or duplicated.
//   - Auth is a dedicated bearer token (MICROFLOW_MCP_TOKEN). Without it the
//     endpoint is not registered at all (fail closed). The browser login
//     cookie is NOT accepted here and the MCP token is NOT valid for /api/*.
//   - MICROFLOW_MCP_READONLY=true removes the run/cancel tools (and the edit tools).
//   - MICROFLOW_MCP_ALLOW_EDIT=true adds update_node / revert_last_edit (edit.go).
//     Off by default; read-only always wins.
//   - No secrets are ever returned: workflow summaries omit node parameters,
//     get_node masks secret-looking keys, and execution details come from the
//     existing redacting debug report.
package mcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	serverName    = "microflow"
	serverVersion = "1.0.0"
	latestProto   = "2025-06-18"

	maxRequestBytes = 1 << 20 // 1 MiB of JSON-RPC per POST
	maxResultBytes  = 60_000  // cap on text returned to the model
	minTokenLen     = 16
)

var supportedProtos = map[string]bool{
	"2025-06-18": true,
	"2025-03-26": true,
	"2024-11-05": true,
}

// Config is read from the environment by ConfigFromEnv.
type Config struct {
	Token     string // MICROFLOW_MCP_TOKEN; empty = MCP disabled
	ReadOnly  bool   // MICROFLOW_MCP_READONLY
	AllowEdit bool   // MICROFLOW_MCP_ALLOW_EDIT (opt-in; ignored when ReadOnly)
}

// Enabled reports whether the /mcp endpoint should be registered.
func (c Config) Enabled() bool { return c.Token != "" }

// ConfigFromEnv returns an error (so main can refuse to start) for a token
// that is set but too short to be safe.
func ConfigFromEnv() (Config, error) {
	c := Config{Token: strings.TrimSpace(os.Getenv("MICROFLOW_MCP_TOKEN"))}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("MICROFLOW_MCP_READONLY"))) {
	case "1", "true", "yes", "on":
		c.ReadOnly = true
	}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("MICROFLOW_MCP_ALLOW_EDIT"))) {
	case "1", "true", "yes", "on":
		c.AllowEdit = true
	}
	if c.Token != "" && len(c.Token) < minTokenLen {
		return c, fmt.Errorf("MICROFLOW_MCP_TOKEN must be at least %d characters (use a long random value)", minTokenLen)
	}
	return c, nil
}

// Handler serves POST /mcp. api is the existing API handler (the one mounted
// at /api/); it is invoked in-process, never over the network.
type Handler struct {
	api   http.Handler
	cfg   Config
	token [32]byte
	hist  *editHistory
}

func NewHandler(api http.Handler, cfg Config) *Handler {
	return &Handler{api: api, cfg: cfg, token: sha256.Sum256([]byte(cfg.Token)), hist: newEditHistory()}
}

func (h *Handler) tokenOK(candidate string) bool {
	if candidate == "" {
		return false
	}
	got := sha256.Sum256([]byte(candidate))
	return subtle.ConstantTimeCompare(got[:], h.token[:]) == 1
}

// authorized accepts the token either as "Authorization: Bearer <token>" or,
// for clients that cannot send headers (e.g. the Claude "Add custom
// connector" form), as the last URL segment: /mcp/<token>. Same token, same
// constant-time check. Note the URL form puts the token in access logs.
func (h *Handler) authorized(r *http.Request) bool {
	const p = "Bearer "
	if v := r.Header.Get("Authorization"); len(v) > len(p) && strings.EqualFold(v[:len(p)], p) {
		if h.tokenOK(strings.TrimSpace(v[len(p):])) {
			return true
		}
	}
	if rest, ok := strings.CutPrefix(r.URL.Path, "/mcp/"); ok {
		return h.tokenOK(strings.Trim(rest, "/"))
	}
	return false
}

// ---------------- JSON-RPC ----------------

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="microflow-mcp"`)
		httpJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or missing bearer token"})
		return
	}
	switch r.Method {
	case http.MethodPost:
	case http.MethodDelete:
		w.WriteHeader(http.StatusNoContent) // stateless: nothing to terminate
		return
	default:
		// No server-initiated SSE stream; spec allows 405 for GET.
		w.Header().Set("Allow", "POST, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
	if err != nil || len(raw) > maxRequestBytes {
		httpJSON(w, http.StatusRequestEntityTooLarge, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"),
			Error: &rpcError{Code: -32600, Message: "request too large"}})
		return
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		httpJSON(w, http.StatusBadRequest, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"),
			Error: &rpcError{Code: -32700, Message: "empty body"}})
		return
	}

	// Batch or single.
	if raw[0] == '[' {
		var batch []rpcRequest
		if err := json.Unmarshal(raw, &batch); err != nil || len(batch) == 0 {
			httpJSON(w, http.StatusBadRequest, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"),
				Error: &rpcError{Code: -32700, Message: "parse error"}})
			return
		}
		var out []rpcResponse
		for _, req := range batch {
			if resp, ok := h.dispatch(r.Context(), req); ok {
				out = append(out, resp)
			}
		}
		if len(out) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		httpJSON(w, http.StatusOK, out)
		return
	}

	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		httpJSON(w, http.StatusBadRequest, rpcResponse{JSONRPC: "2.0", ID: json.RawMessage("null"),
			Error: &rpcError{Code: -32700, Message: "parse error"}})
		return
	}
	resp, ok := h.dispatch(r.Context(), req)
	if !ok {
		w.WriteHeader(http.StatusAccepted) // notification / response: no body
		return
	}
	httpJSON(w, http.StatusOK, resp)
}

// dispatch returns ok=false for notifications (no id), which get no reply.
func (h *Handler) dispatch(ctx context.Context, req rpcRequest) (rpcResponse, bool) {
	isNotification := len(req.ID) == 0
	reply := func(result any) (rpcResponse, bool) {
		if isNotification {
			return rpcResponse{}, false
		}
		return rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: result}, true
	}
	fail := func(code int, msg string) (rpcResponse, bool) {
		if isNotification {
			return rpcResponse{}, false
		}
		return rpcResponse{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{Code: code, Message: msg}}, true
	}

	if req.JSONRPC != "2.0" {
		return fail(-32600, "jsonrpc must be \"2.0\"")
	}

	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		proto := latestProto
		if supportedProtos[p.ProtocolVersion] {
			proto = p.ProtocolVersion
		}
		return reply(map[string]any{
			"protocolVersion": proto,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": serverName, "version": serverVersion},
			"instructions": "MicroFlow workflow runner. Use list_workflows to find workflow ids, " +
				"run_workflow to start one (it returns an executionId immediately), then " +
				"get_execution / get_execution_report to follow progress or diagnose failures." + h.editInstructions(),
		})
	case "ping":
		return reply(map[string]any{})
	case "tools/list":
		return reply(map[string]any{"tools": h.tools()})
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil || p.Name == "" {
			return fail(-32602, "invalid params: expected {name, arguments}")
		}
		return reply(h.callTool(ctx, p.Name, p.Arguments))
	default:
		if strings.HasPrefix(req.Method, "notifications/") {
			return rpcResponse{}, false
		}
		return fail(-32601, "method not found: "+req.Method)
	}
}

// ---------------- tools ----------------

type toolDef struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations map[string]any `json:"annotations,omitempty"`
	write       bool
	edit        bool // workflow-editing tool: needs Config.AllowEdit
}

func obj(required []string, props map[string]any) map[string]any {
	s := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func str(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }

var allTools = []toolDef{
	{
		Name:        "list_workflows",
		Description: "List workflows (id, name, service, active flag, node count). Optionally limit to one Service.",
		InputSchema: obj(nil, map[string]any{"serviceId": str("Only workflows of this Service id (optional).")}),
		Annotations: map[string]any{"readOnlyHint": true},
	},
	{
		Name:        "get_workflow",
		Description: "Summary of one workflow: its nodes (name, type, disabled) and how they connect. Node parameters are intentionally not returned.",
		InputSchema: obj([]string{"workflowId"}, map[string]any{
			"workflowId": str("Workflow id."), "serviceId": str("Service id the workflow belongs to (optional, enforces isolation)."),
		}),
		Annotations: map[string]any{"readOnlyHint": true},
	},
	{
		Name:        "list_executions",
		Description: "Newest executions with status, timing and error. Filter by workflowId or status; queue stats included.",
		InputSchema: obj(nil, map[string]any{
			"limit":      map[string]any{"type": "integer", "minimum": 1, "maximum": 100, "description": "Max rows (default 20)."},
			"workflowId": str("Only this workflow (optional)."),
			"status":     str("Only this status, e.g. queued, running, success, error, canceled (optional)."),
			"serviceId":  str("Only this Service (optional)."),
		}),
		Annotations: map[string]any{"readOnlyHint": true},
	},
	{
		Name:        "get_execution",
		Description: "Compact status of one execution: overall status, error, and one line per node run (status, attempt, duration, error).",
		InputSchema: obj([]string{"executionId"}, map[string]any{
			"executionId": str("Execution id."), "serviceId": str("Service id (optional)."),
		}),
		Annotations: map[string]any{"readOnlyHint": true},
	},
	{
		Name:        "get_execution_report",
		Description: "Detailed text debug report of an execution (secrets redacted by MicroFlow). scope=error (default) focuses on the failure; scope=full includes everything. Output is truncated if very large.",
		InputSchema: obj([]string{"executionId"}, map[string]any{
			"executionId": str("Execution id."),
			"scope":       map[string]any{"type": "string", "enum": []string{"error", "full"}, "description": "error (default) or full."},
			"serviceId":   str("Service id (optional)."),
		}),
		Annotations: map[string]any{"readOnlyHint": true},
	},
	{
		Name:        "run_workflow",
		Description: "Start a workflow now (manual mode). Returns immediately with an executionId; poll get_execution to follow it. Uses the same queue/limits as the UI.",
		InputSchema: obj([]string{"workflowId"}, map[string]any{
			"workflowId": str("Workflow id."),
			"startNode":  str("Start from this node name instead of the first trigger (optional)."),
			"serviceId":  str("Service id the workflow belongs to (optional, enforces isolation)."),
		}),
		write: true,
	},
	{
		Name:        "cancel_execution",
		Description: "Request cancellation of a queued or running execution.",
		InputSchema: obj([]string{"executionId"}, map[string]any{
			"executionId": str("Execution id."), "serviceId": str("Service id (optional)."),
		}),
		write: true,
	},
}

func (h *Handler) tools() []toolDef {
	out := make([]toolDef, 0, len(allTools))
	for _, t := range allTools {
		if t.write && h.cfg.ReadOnly {
			continue
		}
		if t.edit && !h.cfg.AllowEdit {
			continue
		}
		out = append(out, t)
	}
	return out
}

type toolResult struct {
	Content []map[string]string `json:"content"`
	IsError bool                `json:"isError,omitempty"`
}

func textResult(s string, isErr bool) toolResult {
	if len(s) > maxResultBytes {
		s = s[:maxResultBytes] + fmt.Sprintf("\n\n[truncated: output exceeded %d bytes]", maxResultBytes)
	}
	return toolResult{Content: []map[string]string{{"type": "text", "text": s}}, IsError: isErr}
}

func jsonResult(v any) toolResult {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return textResult("internal error: "+err.Error(), true)
	}
	return textResult(string(b), false)
}

type args struct {
	WorkflowID  string `json:"workflowId"`
	ExecutionID string `json:"executionId"`
	ServiceID   string `json:"serviceId"`
	StartNode   string `json:"startNode"`
	Scope       string `json:"scope"`
	Status      string `json:"status"`
	Limit       int    `json:"limit"`
}

func (h *Handler) callTool(ctx context.Context, name string, raw json.RawMessage) toolResult {
	var a args
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &a); err != nil {
			return textResult("invalid arguments: "+err.Error(), true)
		}
	}
	known := false
	for _, t := range h.tools() {
		if t.Name == name {
			known = true
			break
		}
	}
	if !known {
		return textResult("unknown tool: "+name, true)
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	switch name {
	case "list_workflows":
		return h.listWorkflows(ctx, a)
	case "get_workflow":
		if a.WorkflowID == "" {
			return textResult("workflowId is required", true)
		}
		return h.getWorkflow(ctx, a)
	case "list_executions":
		return h.listExecutions(ctx, a)
	case "get_execution":
		if a.ExecutionID == "" {
			return textResult("executionId is required", true)
		}
		return h.getExecution(ctx, a)
	case "get_execution_report":
		if a.ExecutionID == "" {
			return textResult("executionId is required", true)
		}
		scope := "error"
		if a.Scope == "full" {
			scope = "full"
		}
		q := url.Values{"format": {"text"}, "scope": {scope}}
		status, body, err := h.call(ctx, http.MethodGet, "/api/executions/"+url.PathEscape(a.ExecutionID)+"/debug-report?"+q.Encode(), a.ServiceID)
		return plainOrErr(status, body, err)
	case "run_workflow":
		if a.WorkflowID == "" {
			return textResult("workflowId is required", true)
		}
		path := "/api/workflows/" + url.PathEscape(a.WorkflowID) + "/execute"
		if a.StartNode != "" {
			path += "?" + url.Values{"startNode": {a.StartNode}}.Encode()
		}
		status, body, err := h.call(ctx, http.MethodPost, path, a.ServiceID)
		return plainOrErr(status, body, err)
	case "get_node", "update_node", "revert_last_edit":
		var ea editArgs
		if len(bytes.TrimSpace(raw)) > 0 {
			if err := json.Unmarshal(raw, &ea); err != nil {
				return textResult("invalid arguments: "+err.Error(), true)
			}
		}
		switch name {
		case "get_node":
			if ea.WorkflowID == "" || ea.NodeName == "" {
				return textResult("workflowId and nodeName are required", true)
			}
			return h.getNode(ctx, ea)
		case "update_node":
			return h.updateNode(ctx, ea)
		default:
			return h.revertLastEdit(ctx, ea)
		}
	case "cancel_execution":
		if a.ExecutionID == "" {
			return textResult("executionId is required", true)
		}
		status, body, err := h.call(ctx, http.MethodPost, "/api/executions/"+url.PathEscape(a.ExecutionID)+"/cancel", a.ServiceID)
		return plainOrErr(status, body, err)
	}
	return textResult("unknown tool: "+name, true)
}

// call invokes the existing API handler in-process.
func (h *Handler) call(ctx context.Context, method, path, serviceID string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, path, nil)
	if err != nil {
		return 0, nil, err
	}
	if serviceID != "" {
		req.Header.Set("X-Microflow-Service", serviceID)
	}
	req.RemoteAddr = "127.0.0.1:0"
	status, out := h.serve(req)
	return status, out, nil
}

// serve runs a prepared request through the API handler in-process.
func (h *Handler) serve(req *http.Request) (int, []byte) {
	rec := httptest.NewRecorder()
	h.api.ServeHTTP(rec, req)
	return rec.Code, rec.Body.Bytes()
}

func plainOrErr(status int, body []byte, err error) toolResult {
	if err != nil {
		return textResult(err.Error(), true)
	}
	if status >= 400 {
		return textResult(fmt.Sprintf("HTTP %d: %s", status, apiError(body)), true)
	}
	return textResult(strings.TrimSpace(string(body)), false)
}

func apiError(body []byte) string {
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		return e.Error
	}
	s := strings.TrimSpace(string(body))
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}

// getJSON runs a GET and decodes the body into v.
func (h *Handler) getJSON(ctx context.Context, path, serviceID string, v any) error {
	status, body, err := h.call(ctx, http.MethodGet, path, serviceID)
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("HTTP %d: %s", status, apiError(body))
	}
	if err := json.Unmarshal(body, v); err != nil {
		return errors.New("unexpected response from API")
	}
	return nil
}

// ---------------- tool bodies ----------------

type wfDoc struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	ServiceID string `json:"serviceId"`
	Active    bool   `json:"active"`
	Nodes     map[string]struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Disabled bool   `json:"disabled"`
	} `json:"nodes"`
	Connections map[string][]struct {
		SourceName  string `json:"sourceName"`
		SourceIndex int    `json:"sourceIndex"`
		TargetName  string `json:"targetName"`
		TargetIndex int    `json:"targetIndex"`
	} `json:"connections"`
	UpdatedAt string `json:"updatedAt"`
}

func (h *Handler) listWorkflows(ctx context.Context, a args) toolResult {
	path := "/api/workflows"
	if a.ServiceID != "" {
		path += "?" + url.Values{"serviceId": {a.ServiceID}}.Encode()
	}
	var wfs []wfDoc
	if err := h.getJSON(ctx, path, "", &wfs); err != nil {
		return textResult(err.Error(), true)
	}
	type row struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		ServiceID string `json:"serviceId"`
		Active    bool   `json:"active"`
		Nodes     int    `json:"nodeCount"`
		UpdatedAt string `json:"updatedAt,omitempty"`
	}
	rows := make([]row, 0, len(wfs))
	for _, w := range wfs {
		rows = append(rows, row{w.ID, w.Name, w.ServiceID, w.Active, len(w.Nodes), w.UpdatedAt})
	}
	return jsonResult(map[string]any{"workflows": rows})
}

func (h *Handler) getWorkflow(ctx context.Context, a args) toolResult {
	var w wfDoc
	if err := h.getJSON(ctx, "/api/workflows/"+url.PathEscape(a.WorkflowID), a.ServiceID, &w); err != nil {
		return textResult(err.Error(), true)
	}
	type node struct {
		Name     string `json:"name"`
		Type     string `json:"type"`
		Disabled bool   `json:"disabled,omitempty"`
	}
	nodes := make([]node, 0, len(w.Nodes))
	for _, n := range w.Nodes {
		nodes = append(nodes, node{n.Name, n.Type, n.Disabled})
	}
	type edge struct {
		From string `json:"from"`
		Out  int    `json:"output"`
		To   string `json:"to"`
		In   int    `json:"input"`
	}
	var edges []edge
	for _, cs := range w.Connections {
		for _, c := range cs {
			edges = append(edges, edge{c.SourceName, c.SourceIndex, c.TargetName, c.TargetIndex})
		}
	}
	return jsonResult(map[string]any{
		"id": w.ID, "name": w.Name, "serviceId": w.ServiceID, "active": w.Active,
		"nodes": nodes, "connections": edges, "updatedAt": w.UpdatedAt,
	})
}

type execDoc struct {
	ID         string `json:"id"`
	WorkflowID string `json:"workflowId"`
	ServiceID  string `json:"serviceId"`
	Mode       string `json:"mode"`
	Status     string `json:"status"`
	StartedAt  string `json:"startedAt"`
	FinishedAt string `json:"finishedAt"`
	Error      string `json:"error"`
	NodeRuns   []struct {
		NodeName string  `json:"nodeName"`
		Status   string  `json:"status"`
		Error    string  `json:"error"`
		Attempt  int     `json:"attempt"`
		Duration float64 `json:"durationMs"` // time.Duration marshals as ns
	} `json:"nodeRuns"`
}

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func (h *Handler) listExecutions(ctx context.Context, a args) toolResult {
	limit := a.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	// Fetch extra when filtering so the filter still yields up to `limit`.
	fetch := limit
	if a.WorkflowID != "" || a.Status != "" || a.ServiceID != "" {
		fetch = 500
	}
	var resp struct {
		Executions []execDoc      `json:"executions"`
		Queue      map[string]int `json:"queue"`
	}
	if err := h.getJSON(ctx, "/api/executions?limit="+strconv.Itoa(fetch), "", &resp); err != nil {
		return textResult(err.Error(), true)
	}
	type row struct {
		ID         string `json:"id"`
		WorkflowID string `json:"workflowId"`
		ServiceID  string `json:"serviceId,omitempty"`
		Mode       string `json:"mode"`
		Status     string `json:"status"`
		StartedAt  string `json:"startedAt"`
		FinishedAt string `json:"finishedAt,omitempty"`
		Error      string `json:"error,omitempty"`
	}
	rows := make([]row, 0, limit)
	for _, e := range resp.Executions {
		if a.WorkflowID != "" && e.WorkflowID != a.WorkflowID {
			continue
		}
		if a.ServiceID != "" && e.ServiceID != a.ServiceID {
			continue
		}
		if a.Status != "" && !strings.EqualFold(e.Status, a.Status) {
			continue
		}
		rows = append(rows, row{e.ID, e.WorkflowID, e.ServiceID, e.Mode, e.Status, e.StartedAt, e.FinishedAt, clip(e.Error, 300)})
		if len(rows) >= limit {
			break
		}
	}
	return jsonResult(map[string]any{"executions": rows, "queue": resp.Queue})
}

func (h *Handler) getExecution(ctx context.Context, a args) toolResult {
	var e execDoc
	if err := h.getJSON(ctx, "/api/executions/"+url.PathEscape(a.ExecutionID), a.ServiceID, &e); err != nil {
		return textResult(err.Error(), true)
	}
	type nr struct {
		Node       string `json:"node"`
		Status     string `json:"status"`
		Attempt    int    `json:"attempt,omitempty"`
		DurationMs int64  `json:"durationMs"`
		Error      string `json:"error,omitempty"`
	}
	runs := make([]nr, 0, len(e.NodeRuns))
	for _, r := range e.NodeRuns {
		runs = append(runs, nr{r.NodeName, r.Status, r.Attempt, int64(r.Duration / 1e6), clip(r.Error, 300)})
	}
	return jsonResult(map[string]any{
		"id": e.ID, "workflowId": e.WorkflowID, "serviceId": e.ServiceID, "mode": e.Mode,
		"status": e.Status, "startedAt": e.StartedAt, "finishedAt": e.FinishedAt,
		"error": clip(e.Error, 1000), "nodeRuns": runs,
		"hint": "Use get_execution_report for full failure details.",
	})
}

func httpJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
