// Package report builds a human-readable and a machine-readable debug
// report from a MicroFlow execution, for the "1-Click Full Execution
// Copy / Debug Report" feature.
//
// Design constraints (see cmd/ callers / API handlers for how these are
// enforced end to end):
//
//   - Read-only: Build never mutates the Execution or Workflow it is
//     given, and this package never touches the execution engine,
//     scheduler, or storage layer. It is purely a formatter over data
//     that already exists in model.Execution / model.Workflow.
//   - No fabrication: every field comes directly from the stored
//     Execution/Workflow, or is a value literally derivable from them
//     (e.g. "previous node" from the workflow's connection graph). A
//     field with no underlying data is rendered as the literal string
//     "None" -- never guessed, never left silently blank.
//   - Defense in depth on secrets: internal/engine.SecretRedactor
//     already redacts secret-shaped keys/values before an Execution is
//     ever persisted, so NodeRuns read out of the store are already
//     scrubbed. This package applies a second, independent masking
//     pass (maskSecrets / maskJSONValue) over every string it emits,
//     so a report is safe even if it's ever built from an Execution
//     that bypassed the engine's own redaction (e.g. a future caller,
//     a test double, or a manually constructed record).
package report

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"microflow/internal/model"
)

// none is the literal placeholder for "this field has no data" --
// used everywhere instead of omitting the field or guessing a value.
const none = "None"

// maxInlineBytes bounds how much of a single Input/Output/Error blob is
// inlined into the FULL/ERROR text reports before being truncated with
// a note pointing at the JSON export. This keeps "Copy Full Execution"
// clipboard-sized even for a workflow with a few huge HTTP/AI payloads,
// without ever dropping data from the JSON download (BuildJSON always
// serializes the untruncated value).
const maxInlineBytes = 20_000

// ---------------------------------------------------------------------
// Secret masking (second, independent layer -- see package doc).
// ---------------------------------------------------------------------

// secretKeyMask matches map/JSON key names that name a secret by
// convention. Deliberately broad, mirroring the spec's explicit list
// (Authorization, API_KEY, TOKEN, PASSWORD) plus the common variants.
var secretKeyMask = regexp.MustCompile(`(?i)^(authorization|api[_-]?key|apikey|token|access[_-]?token|refresh[_-]?token|auth[_-]?token|bearer[_-]?token|secret|client[_-]?secret|password|passwd|cookie|set-cookie)$`)

// secretValuePatterns catch secret-shaped values wherever they appear
// in free text (error messages, stack traces, headers echoed into a
// body) even when the surrounding key name doesn't say "secret" --
// e.g. an OpenAI-style "sk-..." key pasted into a URL or error string.
var secretValuePatterns = []*regexp.Regexp{
	regexp.MustCompile(`sk-[A-Za-z0-9_-]{10,}`),
	regexp.MustCompile(`(?i)bearer\s+[A-Za-z0-9._-]{10,}`),
	regexp.MustCompile(`(?i)(authorization|api[_-]?key|apikey|access[_-]?token|refresh[_-]?token|password|passwd)\s*[:=]\s*"?[^\s"&,}]{4,}"?`),
}

const redactedPlaceholder = "[REDACTED]"

// maskSecrets scrubs a free-text string (error message, stack trace,
// logs) for anything secret-shaped. It's intentionally independent of
// engine.SecretRedactor (which operates on already-known values from
// this specific run) -- this is a static, pattern-based backstop.
func maskSecrets(s string) string {
	if s == "" {
		return s
	}
	for _, re := range secretValuePatterns {
		s = re.ReplaceAllString(s, redactedPlaceholder)
	}
	return s
}

// maskJSONValue walks an arbitrary JSON-shaped value (as decoded by
// encoding/json: map[string]any, []any, or a scalar) and returns a
// masked deep copy: keys matching secretKeyMask are replaced outright;
// every string value (regardless of key) is also passed through
// maskSecrets so a leaked secret isn't missed just because its key
// wasn't named accordingly.
func maskJSONValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, sub := range val {
			if secretKeyMask.MatchString(strings.TrimSpace(k)) {
				out[k] = redactedPlaceholder
				continue
			}
			out[k] = maskJSONValue(sub)
		}
		return out
	case []any:
		out := make([]any, len(val))
		for i, sub := range val {
			out[i] = maskJSONValue(sub)
		}
		return out
	case string:
		return maskSecrets(val)
	default:
		return v
	}
}

func maskItems(items []model.Item) []model.Item {
	if items == nil {
		return nil
	}
	out := make([]model.Item, len(items))
	for i, it := range items {
		var json map[string]any
		if it.JSON != nil {
			if m, ok := maskJSONValue(any(it.JSON)).(map[string]any); ok {
				json = m
			}
		}
		out[i] = model.Item{JSON: json, Binary: it.Binary}
	}
	return out
}

func maskOutput(out model.NodeOutput) model.NodeOutput {
	if out == nil {
		return nil
	}
	masked := make(model.NodeOutput, len(out))
	for i, branch := range out {
		masked[i] = maskItems(branch)
	}
	return masked
}

// ---------------------------------------------------------------------
// Report data shapes.
// ---------------------------------------------------------------------

// NodeDebugInfo is every diagnostic field the feature spec requires
// for one executed node. Every field is either real data pulled from
// the Execution/Workflow, or the literal "None".
type NodeDebugInfo struct {
	Index    int    `json:"index"`
	NodeID   string `json:"nodeId"`
	NodeName string `json:"nodeName"`
	NodeType string `json:"nodeType"`
	Status   string `json:"status"`

	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	Duration  string `json:"duration"`

	Input  model.NodeOutput `json:"input,omitempty"`
	Output model.NodeOutput `json:"output,omitempty"`

	Error        string `json:"error"`
	ErrorType    string `json:"errorType"`
	ErrorMessage string `json:"errorMessage"`
	StackTrace   string `json:"stackTrace"`

	HTTPStatus string `json:"httpStatus"`

	RetryOnFail bool   `json:"retryOnFail"`
	MaxTries    string `json:"maxTries"`
	Attempt     string `json:"attempt"`

	PreviousNodes string `json:"previousNodes"`
	NextNodes     string `json:"nextNodes"`
}

// ExecutionSummary is the header/footer block of the full report.
type ExecutionSummary struct {
	ExecutionID     string `json:"executionId"`
	WorkflowID      string `json:"workflowId"`
	WorkflowName    string `json:"workflowName"`
	ExecutionStatus string `json:"executionStatus"`
	StartTime       string `json:"startTime"`
	EndTime         string `json:"endTime"`
	TotalDuration   string `json:"totalDuration"`

	TotalNodes      int `json:"totalNodes"`
	SuccessfulNodes int `json:"successfulNodes"`
	FailedNodes     int `json:"failedNodes"`
	CancelledNodes  int `json:"cancelledNodes"`
	// NotExecutedNodes covers workflow nodes that never ran this
	// execution (disabled, on an untaken IF/branch, or simply not
	// reached before the run stopped). MicroFlow's engine does not
	// record a distinct "skipped" status per node (see
	// internal/model.ExecutionStatus) -- rather than inventing one,
	// this is reported as a real, derived count: workflow node total
	// minus nodes that actually appear in NodeRuns.
	NotExecutedNodes int `json:"notExecutedNodes"`

	FailedNodeNames []string `json:"failedNodeNames"`
	MainError       string   `json:"mainError"`
}

// DebugReport is the full structured report: JSON-serializable as-is
// for the "Download Execution JSON" feature, and used to render the
// full/error text reports for the "Copy" buttons.
type DebugReport struct {
	GeneratedAt string           `json:"generatedAt"`
	Summary     ExecutionSummary `json:"summary"`
	Nodes       []NodeDebugInfo  `json:"nodes"`
}

// ---------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------

// Build turns one Execution (plus its Workflow, for node type/id/graph
// lookups) into a DebugReport. wf may be nil (e.g. the workflow was
// since deleted, or is otherwise unavailable) -- every field that
// would have come from wf degrades to "None" rather than failing the
// whole report; ex's own data (which is self-contained) is unaffected.
func Build(wf *model.Workflow, ex *model.Execution) *DebugReport {
	r := &DebugReport{GeneratedAt: time.Now().UTC().Format(time.RFC3339)}
	if ex == nil {
		r.Summary = ExecutionSummary{ExecutionID: none, WorkflowID: none, WorkflowName: none, ExecutionStatus: none, StartTime: none, EndTime: none, TotalDuration: none, MainError: none}
		return r
	}

	nodes := make([]NodeDebugInfo, 0, len(ex.NodeRuns))
	failedNames := make([]string, 0)
	successCount, failCount, cancelCount := 0, 0, 0

	for i, nr := range ex.NodeRuns {
		info := buildNodeInfo(wf, ex, i, nr)
		nodes = append(nodes, info)

		switch nr.Status {
		case model.StatusSuccess:
			successCount++
		case model.StatusError:
			failCount++
			failedNames = append(failedNames, nr.NodeName)
		case model.StatusCancelled:
			cancelCount++
		}
	}

	notExecuted := 0
	if wf != nil {
		ran := make(map[string]bool, len(ex.NodeRuns))
		for _, nr := range ex.NodeRuns {
			ran[nr.NodeName] = true
		}
		for name, n := range wf.Nodes {
			if n.Type == model.TypeStickyNote {
				continue // never executed by design, not a diagnostic-worthy gap
			}
			if !ran[name] {
				notExecuted++
			}
		}
	}

	workflowName := none
	if wf != nil && wf.Name != "" {
		workflowName = wf.Name
	}

	endTime := none
	totalDuration := none
	if ex.FinishedAt != nil {
		endTime = ex.FinishedAt.Format(time.RFC3339)
		totalDuration = ex.FinishedAt.Sub(ex.StartedAt).String()
	}

	mainError := none
	if ex.Error != "" {
		mainError = maskSecrets(ex.Error)
	} else if failCount > 0 {
		// The run-level Error can be empty even when a node failed
		// (e.g. continueOnFail absorbed it and the run kept going) --
		// fall back to the first failed node's own error rather than
		// reporting "None" when a real error exists in the data.
		for _, nr := range ex.NodeRuns {
			if nr.Status == model.StatusError && nr.Error != "" {
				mainError = maskSecrets(nr.Error)
				break
			}
		}
	}

	r.Summary = ExecutionSummary{
		ExecutionID:      valOr(ex.ID),
		WorkflowID:       valOr(ex.WorkflowID),
		WorkflowName:     workflowName,
		ExecutionStatus:  displayStatus(ex.Status),
		StartTime:        formatTimeOrNone(ex.StartedAt),
		EndTime:          endTime,
		TotalDuration:    totalDuration,
		TotalNodes:       len(ex.NodeRuns),
		SuccessfulNodes:  successCount,
		FailedNodes:      failCount,
		CancelledNodes:   cancelCount,
		NotExecutedNodes: notExecuted,
		FailedNodeNames:  failedNames,
		MainError:        mainError,
	}
	r.Nodes = nodes
	return r
}

func buildNodeInfo(wf *model.Workflow, ex *model.Execution, index int, nr model.NodeRunResult) NodeDebugInfo {
	info := NodeDebugInfo{
		Index:     index + 1,
		NodeName:  valOr(nr.NodeName),
		Status:    displayStatus(nr.Status),
		StartTime: formatTimeOrNone(nr.StartedAt),
		Duration:  none,
		Input:     maskOutput(nr.Input),
		Output:    maskOutput(nr.Output),
		Attempt:   itoaOrNone(nr.Attempt),
	}
	if !nr.StartedAt.IsZero() {
		info.EndTime = nr.StartedAt.Add(nr.Duration).Format(time.RFC3339)
		info.Duration = nr.Duration.String()
	} else {
		info.EndTime = none
	}

	// Node ID / type / retry config / graph position all come from the
	// Workflow definition (the Execution itself only knows the node's
	// name). If the workflow is unavailable, or no longer has a node
	// by this name (renamed/deleted since the run), every one of these
	// degrades to "None" rather than being guessed.
	info.NodeID = none
	info.NodeType = none
	info.MaxTries = none
	info.PreviousNodes = none
	info.NextNodes = none
	if wf != nil {
		if n, ok := wf.Nodes[nr.NodeName]; ok {
			info.NodeID = valOr(n.ID)
			info.NodeType = valOr(string(n.Type))
			info.RetryOnFail = n.RetryOnFail
			if n.MaxTries > 0 {
				info.MaxTries = strconv.Itoa(n.MaxTries)
			}
		}
		info.PreviousNodes = joinOrNone(previousNodeNames(wf, nr.NodeName))
		info.NextNodes = joinOrNone(nextNodeNames(wf, nr.NodeName))
	}

	// Error / error type / message / stack trace / HTTP status are all
	// derived from the single Error string MicroFlow stores per node
	// (there is no separate structured error field in the engine).
	info.Error = none
	info.ErrorType = none
	info.ErrorMessage = none
	info.StackTrace = none
	info.HTTPStatus = none
	if nr.Error != "" {
		masked := maskSecrets(nr.Error)
		info.Error = masked
		msg, stack := splitErrorAndStack(masked)
		info.ErrorMessage = msg
		if stack != "" {
			info.StackTrace = stack
		}
		info.ErrorType = classifyErrorType(masked)
	}
	if code := httpStatusFromOutput(nr.Output); code != "" {
		info.HTTPStatus = code
	} else if code := httpStatusFromError(maskSecrets(nr.Error)); code != "" {
		info.HTTPStatus = code
	}

	return info
}

// ---------------------------------------------------------------------
// small derivation helpers
// ---------------------------------------------------------------------

// displayStatus maps model.ExecutionStatus's internal values onto the
// vocabulary the debug report uses (SUCCESS/FAILED/CANCELLED/RUNNING/
// WAITING/QUEUED) -- notably StatusError -> "FAILED", matching how the
// UI/report describes a failed node, rather than the engine's internal
// "error" string. Never invents a status the data doesn't have.
func displayStatus(s model.ExecutionStatus) string {
	switch s {
	case model.StatusError:
		return "FAILED"
	case "":
		return none
	default:
		return strings.ToUpper(string(s))
	}
}

func valOr(s string) string {
	if strings.TrimSpace(s) == "" {
		return none
	}
	return s
}

func itoaOrNone(n int) string {
	if n <= 0 {
		return none
	}
	return strconv.Itoa(n)
}

func formatTimeOrNone(t time.Time) string {
	if t.IsZero() {
		return none
	}
	return t.Format(time.RFC3339)
}

func joinOrNone(names []string) string {
	if len(names) == 0 {
		return none
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func previousNodeNames(wf *model.Workflow, nodeName string) []string {
	var out []string
	for _, c := range wf.Incoming(nodeName) {
		out = append(out, c.SourceName)
	}
	return out
}

func nextNodeNames(wf *model.Workflow, nodeName string) []string {
	var out []string
	for _, c := range wf.Connections[nodeName] {
		out = append(out, c.TargetName)
	}
	return out
}

// stackFrameHint matches lines that look like a JS stack frame (goja's
// exception formatting, e.g. "at <eval>:3:9(3)") so a multi-line error
// string can be split into a first-line message plus a real stack
// trace, without ever inventing a stack trace that isn't there.
var stackFrameHint = regexp.MustCompile(`(?im)^\s*at\s+\S`)

func splitErrorAndStack(errText string) (message, stack string) {
	lines := strings.Split(errText, "\n")
	if len(lines) <= 1 {
		return errText, ""
	}
	for i := 1; i < len(lines); i++ {
		if stackFrameHint.MatchString(lines[i]) {
			return strings.TrimSpace(lines[0]), strings.TrimSpace(strings.Join(lines[i:], "\n"))
		}
	}
	// Multi-line but doesn't look like a stack trace (e.g. a wrapped
	// HTTP error body spanning lines) -- keep it all as the message,
	// no fabricated stack trace.
	return errText, ""
}

var errorTypeHints = []struct {
	pattern *regexp.Regexp
	label   string
}{
	{regexp.MustCompile(`(?i)configuration error \(HTTP (401|403)\)`), "AuthenticationError"},
	{regexp.MustCompile(`(?i)transient error \(HTTP (429|5\d\d)\)`), "TransientHTTPError"},
	{regexp.MustCompile(`(?i)timed out|timeout`), "TimeoutError"},
	{regexp.MustCompile(`(?i)cancelled|canceled|context canceled`), "CancellationError"},
	{regexp.MustCompile(`(?i)code node .*:`), "CodeExecutionError"},
	{regexp.MustCompile(`(?i)http request .*:`), "HTTPRequestError"},
	{regexp.MustCompile(`(?i)no executor registered|unsupported node`), "UnsupportedNodeError"},
}

// classifyErrorType gives a short label for the ERROR TYPE field based
// on MicroFlow's own known error-message shapes (see
// internal/nodes/http.go, internal/nodes/code.go, internal/engine).
// If nothing matches, "None" -- never a guessed label.
func classifyErrorType(errText string) string {
	for _, h := range errorTypeHints {
		if h.pattern.MatchString(errText) {
			return h.label
		}
	}
	return none
}

var httpStatusInErrorPattern = regexp.MustCompile(`HTTP (\d{3})`)

func httpStatusFromError(errText string) string {
	if m := httpStatusInErrorPattern.FindStringSubmatch(errText); m != nil {
		return m[1]
	}
	return ""
}

// httpStatusFromOutput looks for the "statusCode" field the httpRequest
// executor sets when a node has "Full Response" enabled (see
// internal/nodes/http.go). Not every httpRequest node has this
// (default response mode doesn't include it), so this is best-effort;
// falls back to httpStatusFromError, then "None".
func httpStatusFromOutput(out model.NodeOutput) string {
	for _, branch := range out {
		for _, item := range branch {
			if item.JSON == nil {
				continue
			}
			if v, ok := item.JSON["statusCode"]; ok {
				switch n := v.(type) {
				case float64:
					return strconv.Itoa(int(n))
				case int:
					return strconv.Itoa(n)
				case string:
					return n
				}
			}
		}
	}
	return ""
}

// ---------------------------------------------------------------------
// Text formatting
// ---------------------------------------------------------------------

const divider = "━━━━━━━━━━━━━━━━━━━━━━"

// FormatFullText renders the full node-by-node report ("Copy Full
// Execution").
func (r *DebugReport) FormatFullText() string {
	var b strings.Builder
	writeHeader(&b, r)
	for _, n := range r.Nodes {
		writeNodeBlock(&b, n, false)
	}
	writeSummary(&b, r)
	return b.String()
}

// FormatErrorText renders only failed/cancelled nodes plus the
// summary ("Copy Error Report") -- for pasting into a debugging
// conversation without the noise of every successful node.
func (r *DebugReport) FormatErrorText() string {
	var b strings.Builder
	b.WriteString("MICROFLOW — ERROR REPORT\n\n")
	fmt.Fprintf(&b, "Execution ID: %s\n", r.Summary.ExecutionID)
	fmt.Fprintf(&b, "Workflow: %s (%s)\n", r.Summary.WorkflowName, r.Summary.WorkflowID)
	fmt.Fprintf(&b, "Status: %s\n", r.Summary.ExecutionStatus)
	fmt.Fprintf(&b, "Main Error: %s\n\n", r.Summary.MainError)

	found := false
	for _, n := range r.Nodes {
		if n.Status == "SUCCESS" {
			continue
		}
		found = true
		writeNodeBlock(&b, n, true)
	}
	if !found {
		b.WriteString(divider + "\nNo failed, cancelled, or otherwise non-successful nodes in this execution.\n")
	}
	writeSummary(&b, r)
	return b.String()
}

func writeHeader(b *strings.Builder, r *DebugReport) {
	b.WriteString("MICROFLOW — FULL EXECUTION REPORT\n\n")
	fmt.Fprintf(b, "Execution ID: %s\n", r.Summary.ExecutionID)
	fmt.Fprintf(b, "Workflow: %s (%s)\n", r.Summary.WorkflowName, r.Summary.WorkflowID)
	fmt.Fprintf(b, "Status: %s\n", r.Summary.ExecutionStatus)
	fmt.Fprintf(b, "Started: %s\n", r.Summary.StartTime)
	fmt.Fprintf(b, "Finished: %s\n", r.Summary.EndTime)
	fmt.Fprintf(b, "Duration: %s\n", r.Summary.TotalDuration)
	b.WriteString("\n")
}

func writeNodeBlock(b *strings.Builder, n NodeDebugInfo, errorScope bool) {
	b.WriteString(divider + "\n")
	fmt.Fprintf(b, "NODE %d\n", n.Index)
	fmt.Fprintf(b, "Name: %s\n", n.NodeName)
	fmt.Fprintf(b, "Node ID: %s\n", n.NodeID)
	fmt.Fprintf(b, "Type: %s\n", n.NodeType)
	fmt.Fprintf(b, "Status: %s\n", n.Status)
	fmt.Fprintf(b, "Start: %s\n", n.StartTime)
	fmt.Fprintf(b, "End: %s\n", n.EndTime)
	fmt.Fprintf(b, "Duration: %s\n", n.Duration)
	fmt.Fprintf(b, "Attempt: %s\n", n.Attempt)
	fmt.Fprintf(b, "Retry On Fail: %t  |  Max Tries: %s\n", n.RetryOnFail, n.MaxTries)
	fmt.Fprintf(b, "Previous Node(s): %s\n", n.PreviousNodes)
	fmt.Fprintf(b, "Next Node(s): %s\n", n.NextNodes)
	fmt.Fprintf(b, "HTTP Status: %s\n", n.HTTPStatus)
	b.WriteString("\n")

	if !errorScope {
		b.WriteString("INPUT:\n")
		b.WriteString(truncatedJSON(n.Input))
		b.WriteString("\n\n")
	}

	b.WriteString("OUTPUT:\n")
	b.WriteString(truncatedJSON(n.Output))
	b.WriteString("\n\n")

	fmt.Fprintf(b, "ERROR:\n%s\n\n", n.Error)
	if n.Error != none {
		fmt.Fprintf(b, "ERROR TYPE: %s\n", n.ErrorType)
		fmt.Fprintf(b, "ERROR MESSAGE:\n%s\n\n", n.ErrorMessage)
		fmt.Fprintf(b, "STACK TRACE:\n%s\n\n", n.StackTrace)
	}
}

func writeSummary(b *strings.Builder, r *DebugReport) {
	b.WriteString(divider + "\n")
	b.WriteString("EXECUTION SUMMARY\n")
	fmt.Fprintf(b, "Total Nodes: %d\n", r.Summary.TotalNodes)
	fmt.Fprintf(b, "Successful: %d\n", r.Summary.SuccessfulNodes)
	fmt.Fprintf(b, "Failed: %d\n", r.Summary.FailedNodes)
	fmt.Fprintf(b, "Cancelled: %d\n", r.Summary.CancelledNodes)
	fmt.Fprintf(b, "Not Executed (disabled/unreached branches): %d\n\n", r.Summary.NotExecutedNodes)

	if len(r.Summary.FailedNodeNames) > 0 {
		fmt.Fprintf(b, "Failed Node(s):\n%s\n\n", strings.Join(r.Summary.FailedNodeNames, "\n"))
	} else {
		b.WriteString("Failed Node(s):\nNone\n\n")
	}
	fmt.Fprintf(b, "Main Error:\n%s\n", r.Summary.MainError)
}

// truncatedJSON pretty-prints v and, if it exceeds maxInlineBytes,
// truncates it with a note. Used only by the text reports -- BuildJSON
// (the raw download) always serializes the full, untruncated value.
func truncatedJSON(v any) string {
	if v == nil {
		return none
	}
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return none
	}
	if len(raw) == 0 || string(raw) == "null" {
		return none
	}
	if len(raw) <= maxInlineBytes {
		return string(raw)
	}
	return string(raw[:maxInlineBytes]) + fmt.Sprintf("\n... [truncated %d of %d bytes -- use \"Download Execution JSON\" for the full value]", len(raw)-maxInlineBytes, len(raw))
}

// ToJSON serializes the full, untruncated report for the "Download
// Execution JSON" feature.
func (r *DebugReport) ToJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}
