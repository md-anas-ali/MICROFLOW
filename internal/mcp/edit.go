package mcp

// Editing tools for the MCP server: get_node, update_node, revert_last_edit.
//
// Safety model (all of it fails closed):
//   - update_node / revert_last_edit are registered ONLY when
//     MICROFLOW_MCP_ALLOW_EDIT=true, and never when MICROFLOW_MCP_READONLY=true
//     (read-only always wins).
//   - Changes go through the EXISTING POST /api/workflows/{id}/save handler,
//     so Service isolation, size caps and schedule re-sync behave exactly like
//     a save from the web UI. No store/engine code is touched.
//   - Exactly one node is changed per call; the rest of the workflow document
//     is round-tripped untouched (decoded as generic JSON, numbers preserved).
//   - Before every save the previous full workflow document is kept in an
//     in-memory ring (last historyPerWorkflow edits per workflow) so
//     revert_last_edit can undo it. The ring is lost on restart; for a
//     durable backup use the dashboard export.
//   - get_node masks values under secret-looking keys, and update_node
//     refuses any patch that contains the mask, so a masked value can never
//     be written back over a real secret.
//   - Node renames, node add/delete and connection edits are deliberately not
//     supported: those are structural changes to leave to the UI.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
)

const (
	redactMask         = "***REDACTED***"
	historyPerWorkflow = 20
	maxPatchBytes      = 512 << 10 // 512 KiB of patch per call
)

func init() {
	allTools = append(allTools,
		toolDef{
			Name: "get_node",
			Description: "Full configuration of ONE node (parameters, retry settings, disabled flag) so it can be diagnosed or edited. " +
				"Values under secret-looking keys (token, password, secret, apiKey, authorization, ...) are masked.",
			InputSchema: obj([]string{"workflowId", "nodeName"}, map[string]any{
				"workflowId": str("Workflow id."),
				"nodeName":   str("Exact node name."),
				"serviceId":  str("Service id the workflow belongs to (optional, enforces isolation)."),
			}),
			Annotations: map[string]any{"readOnlyHint": true},
		},
		toolDef{
			Name: "update_node",
			Description: "Change ONE node of a workflow and save it (same path as a UI save). " +
				"parameters is a shallow merge: each top-level key you pass replaces that key, a null value deletes it, keys you omit are untouched. " +
				"Also settable: disabled, retryOnFail, maxTries, waitBetweenTriesMs, continueOnFail, onErrorMode. " +
				"Cannot rename/add/delete nodes or edit connections. The previous version is kept so revert_last_edit can undo it. " +
				"Use dryRun=true first to preview the change without saving.",
			InputSchema: obj([]string{"workflowId", "nodeName"}, map[string]any{
				"workflowId": str("Workflow id."),
				"nodeName":   str("Exact name of the node to change."),
				"serviceId":  str("Service id the workflow belongs to (optional, enforces isolation)."),
				"parameters": map[string]any{"type": "object", "description": "Top-level parameter keys to set (null deletes a key)."},
				"disabled":   map[string]any{"type": "boolean"},
				"retryOnFail": map[string]any{"type": "boolean",
					"description": "Retry the node itself when it fails."},
				"maxTries":           map[string]any{"type": "integer", "minimum": 1, "maximum": 20},
				"waitBetweenTriesMs": map[string]any{"type": "integer", "minimum": 0, "maximum": 3600000},
				"continueOnFail":     map[string]any{"type": "boolean"},
				"onErrorMode": map[string]any{"type": "string",
					"enum": []string{"", "stopWorkflow", "continueRegularOutput", "continueErrorOutput"}},
				"dryRun": map[string]any{"type": "boolean", "description": "Show the diff, save nothing."},
			}),
			write: true,
			edit:  true,
		},
		toolDef{
			Name: "revert_last_edit",
			Description: "Undo the most recent update_node on a workflow by restoring the full workflow document saved just before it. " +
				"Only edits made since the server last started can be reverted.",
			InputSchema: obj([]string{"workflowId"}, map[string]any{
				"workflowId": str("Workflow id."),
				"serviceId":  str("Service id the workflow belongs to (optional, enforces isolation)."),
			}),
			write: true,
			edit:  true,
		},
	)
}

// editHistory keeps pre-edit snapshots, newest last.
type editHistory struct {
	mu   sync.Mutex
	snap map[string][]json.RawMessage // workflowId -> snapshots
	lock map[string]*sync.Mutex       // workflowId -> serialises read-modify-write
}

func newEditHistory() *editHistory {
	return &editHistory{snap: map[string][]json.RawMessage{}, lock: map[string]*sync.Mutex{}}
}

func (e *editHistory) wfLock(id string) *sync.Mutex {
	e.mu.Lock()
	defer e.mu.Unlock()
	l, ok := e.lock[id]
	if !ok {
		l = &sync.Mutex{}
		e.lock[id] = l
	}
	return l
}

func (e *editHistory) push(id string, raw json.RawMessage) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := append(e.snap[id], raw)
	if len(s) > historyPerWorkflow {
		s = s[len(s)-historyPerWorkflow:]
	}
	e.snap[id] = s
}

func (e *editHistory) pop(id string) (json.RawMessage, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.snap[id]
	if len(s) == 0 {
		return nil, false
	}
	raw := s[len(s)-1]
	e.snap[id] = s[:len(s)-1]
	return raw, true
}

// ---------------- helpers ----------------

type editArgs struct {
	WorkflowID         string         `json:"workflowId"`
	NodeName           string         `json:"nodeName"`
	ServiceID          string         `json:"serviceId"`
	Parameters         map[string]any `json:"parameters"`
	Disabled           *bool          `json:"disabled"`
	RetryOnFail        *bool          `json:"retryOnFail"`
	MaxTries           *int           `json:"maxTries"`
	WaitBetweenTriesMs *int           `json:"waitBetweenTriesMs"`
	ContinueOnFail     *bool          `json:"continueOnFail"`
	OnErrorMode        *string        `json:"onErrorMode"`
	DryRun             bool           `json:"dryRun"`
}

var secretKeyHints = []string{
	"token", "secret", "password", "passwd", "apikey", "api_key", "api-key",
	"authorization", "bearer", "credential", "privatekey", "private_key", "cookie", "clientsecret",
}

func looksSecret(key string) bool {
	k := strings.ToLower(key)
	for _, h := range secretKeyHints {
		if strings.Contains(k, h) {
			return true
		}
	}
	return false
}

// maskSecrets returns a deep copy of v with values under secret-looking keys
// replaced by redactMask.
func maskSecrets(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if looksSecret(k) {
				if val == nil {
					out[k] = nil
				} else {
					out[k] = redactMask
				}
				continue
			}
			out[k] = maskSecrets(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = maskSecrets(val)
		}
		return out
	default:
		return v
	}
}

func containsMask(v any) bool {
	switch t := v.(type) {
	case string:
		return strings.Contains(t, redactMask)
	case map[string]any:
		for _, val := range t {
			if containsMask(val) {
				return true
			}
		}
	case []any:
		for _, val := range t {
			if containsMask(val) {
				return true
			}
		}
	}
	return false
}

// fetchWorkflowRaw GETs the workflow document and decodes it generically,
// preserving numbers exactly.
func (h *Handler) fetchWorkflowRaw(ctx context.Context, id, serviceID string) (json.RawMessage, map[string]any, error) {
	status, body, err := h.call(ctx, http.MethodGet, "/api/workflows/"+url.PathEscape(id), serviceID)
	if err != nil {
		return nil, nil, err
	}
	if status >= 400 {
		return nil, nil, fmt.Errorf("HTTP %d: %s", status, apiError(body))
	}
	doc, err := decodeGeneric(body)
	if err != nil {
		return nil, nil, fmt.Errorf("unexpected response from API")
	}
	return json.RawMessage(bytes.Clone(body)), doc, nil
}

func decodeGeneric(b []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func nodeOf(doc map[string]any, name string) (map[string]any, error) {
	nodes, ok := doc["nodes"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("workflow has no nodes")
	}
	n, ok := nodes[name].(map[string]any)
	if !ok {
		names := make([]string, 0, len(nodes))
		for k := range nodes {
			names = append(names, k)
		}
		sort.Strings(names)
		hint := ""
		lower := strings.ToLower(name)
		for _, k := range names {
			if strings.Contains(strings.ToLower(k), lower) && lower != "" {
				hint += " " + fmt.Sprintf("%q", k)
			}
		}
		if hint != "" {
			return nil, fmt.Errorf("node %q not found; similar:%s", name, hint)
		}
		return nil, fmt.Errorf("node %q not found (use get_workflow to list node names)", name)
	}
	return n, nil
}

func (h *Handler) saveWorkflowRaw(ctx context.Context, id, serviceID string, doc any) error {
	b, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	return h.saveBytes(ctx, id, serviceID, b)
}

func (h *Handler) saveBytes(ctx context.Context, id, serviceID string, b []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "/api/workflows/"+url.PathEscape(id)+"/save", bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if serviceID != "" {
		req.Header.Set("X-Microflow-Service", serviceID)
	}
	req.RemoteAddr = "127.0.0.1:0"
	status, body := h.serve(req)
	if status >= 400 {
		return fmt.Errorf("save failed: HTTP %d: %s", status, apiError(body))
	}
	return nil
}

// ---------------- tool bodies ----------------

func (h *Handler) getNode(ctx context.Context, a editArgs) toolResult {
	_, doc, err := h.fetchWorkflowRaw(ctx, a.WorkflowID, a.ServiceID)
	if err != nil {
		return textResult(err.Error(), true)
	}
	n, err := nodeOf(doc, a.NodeName)
	if err != nil {
		return textResult(err.Error(), true)
	}
	return jsonResult(maskSecrets(n))
}

func (h *Handler) updateNode(ctx context.Context, a editArgs) toolResult {
	if a.WorkflowID == "" || a.NodeName == "" {
		return textResult("workflowId and nodeName are required", true)
	}
	if a.Parameters == nil && a.Disabled == nil && a.RetryOnFail == nil && a.MaxTries == nil &&
		a.WaitBetweenTriesMs == nil && a.ContinueOnFail == nil && a.OnErrorMode == nil {
		return textResult("nothing to change: pass parameters and/or disabled, retryOnFail, maxTries, waitBetweenTriesMs, continueOnFail, onErrorMode", true)
	}
	if a.Parameters != nil {
		if containsMask(a.Parameters) {
			return textResult("refused: the patch contains the redaction mask ("+redactMask+"). "+
				"Masked values from get_node are placeholders, not real secrets; omit those keys instead of sending them back.", true)
		}
		if b, _ := json.Marshal(a.Parameters); len(b) > maxPatchBytes {
			return textResult(fmt.Sprintf("patch too large (> %d bytes)", maxPatchBytes), true)
		}
	}
	if a.MaxTries != nil && (*a.MaxTries < 1 || *a.MaxTries > 20) {
		return textResult("maxTries must be between 1 and 20", true)
	}
	if a.WaitBetweenTriesMs != nil && (*a.WaitBetweenTriesMs < 0 || *a.WaitBetweenTriesMs > 3600000) {
		return textResult("waitBetweenTriesMs must be between 0 and 3600000", true)
	}
	if a.OnErrorMode != nil {
		switch *a.OnErrorMode {
		case "", "stopWorkflow", "continueRegularOutput", "continueErrorOutput":
		default:
			return textResult("onErrorMode must be one of: \"\", stopWorkflow, continueRegularOutput, continueErrorOutput", true)
		}
	}

	l := h.hist.wfLock(a.WorkflowID)
	l.Lock()
	defer l.Unlock()

	before, doc, err := h.fetchWorkflowRaw(ctx, a.WorkflowID, a.ServiceID)
	if err != nil {
		return textResult(err.Error(), true)
	}
	node, err := nodeOf(doc, a.NodeName)
	if err != nil {
		return textResult(err.Error(), true)
	}

	type change struct {
		Field string `json:"field"`
		From  any    `json:"from"`
		To    any    `json:"to"`
	}
	var changes []change

	if a.Parameters != nil {
		params, _ := node["parameters"].(map[string]any)
		if params == nil {
			params = map[string]any{}
		}
		keys := make([]string, 0, len(a.Parameters))
		for k := range a.Parameters {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			nv := a.Parameters[k]
			ov, existed := params[k]
			if nv == nil {
				if existed {
					delete(params, k)
					changes = append(changes, change{"parameters." + k, maskSecrets(ov), nil})
				}
				continue
			}
			params[k] = nv
			changes = append(changes, change{"parameters." + k, maskSecrets(ov), maskSecrets(nv)})
		}
		node["parameters"] = params
	}
	setBool := func(field string, v *bool) {
		if v == nil {
			return
		}
		old := node[field]
		if ob, ok := old.(bool); ok && ob == *v {
			return
		}
		node[field] = *v
		changes = append(changes, change{field, old, *v})
	}
	setInt := func(field string, v *int) {
		if v == nil {
			return
		}
		old := node[field]
		if on, ok := old.(json.Number); ok && on.String() == fmt.Sprint(*v) {
			return
		}
		node[field] = *v
		changes = append(changes, change{field, old, *v})
	}
	setBool("disabled", a.Disabled)
	setBool("retryOnFail", a.RetryOnFail)
	setInt("maxTries", a.MaxTries)
	setInt("waitBetweenTriesMs", a.WaitBetweenTriesMs)
	setBool("continueOnFail", a.ContinueOnFail)
	if a.OnErrorMode != nil {
		old := node["onErrorMode"]
		if os, ok := old.(string); !ok || os != *a.OnErrorMode {
			node["onErrorMode"] = *a.OnErrorMode
			changes = append(changes, change{"onErrorMode", old, *a.OnErrorMode})
		}
	}

	if len(changes) == 0 {
		return jsonResult(map[string]any{"saved": false, "node": a.NodeName, "note": "no change: node already has these values"})
	}
	if a.DryRun {
		return jsonResult(map[string]any{"saved": false, "dryRun": true, "node": a.NodeName, "changes": changes})
	}

	if err := h.saveWorkflowRaw(ctx, a.WorkflowID, a.ServiceID, doc); err != nil {
		return textResult(err.Error(), true)
	}
	h.hist.push(a.WorkflowID, before)
	return jsonResult(map[string]any{
		"saved": true, "node": a.NodeName, "changes": changes,
		"note": "Saved. Run the workflow to verify; call revert_last_edit to undo.",
	})
}

func (h *Handler) revertLastEdit(ctx context.Context, a editArgs) toolResult {
	if a.WorkflowID == "" {
		return textResult("workflowId is required", true)
	}
	l := h.hist.wfLock(a.WorkflowID)
	l.Lock()
	defer l.Unlock()

	// Confirm the workflow still exists and is visible to this Service
	// before touching anything.
	if _, _, err := h.fetchWorkflowRaw(ctx, a.WorkflowID, a.ServiceID); err != nil {
		return textResult(err.Error(), true)
	}
	snap, ok := h.hist.pop(a.WorkflowID)
	if !ok {
		return textResult("nothing to revert: no edit made through MCP since the server started", true)
	}
	if err := h.saveBytes(ctx, a.WorkflowID, a.ServiceID, snap); err != nil {
		h.hist.push(a.WorkflowID, snap) // keep it so the revert can be retried
		return textResult(err.Error(), true)
	}
	return jsonResult(map[string]any{"reverted": true, "workflowId": a.WorkflowID,
		"remainingUndo": h.hist.depth(a.WorkflowID)})
}

func (e *editHistory) depth(id string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.snap[id])
}

// editInstructions extends the server instructions only when editing is on,
// so clients that can't edit are not told about tools they don't have.
func (h *Handler) editInstructions() string {
	if h.cfg.ReadOnly || !h.cfg.AllowEdit {
		return ""
	}
	return " To fix a failing node: get_node to read it, update_node (dryRun first) to change it, " +
		"run_workflow (optionally with startNode) to verify, revert_last_edit to undo."
}
