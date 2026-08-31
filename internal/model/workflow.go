// Package model defines MicroFlow's internal workflow representation.
// This is the target of the n8n compatibility layer (see internal/parser):
// n8n JSON -> parser -> model.Workflow -> engine.
package model

import (
	"encoding/json"
	"time"
)

// NodeType identifies which executor handles a node. MicroFlow does not
// reuse n8n's type strings internally, but the parser maps known n8n
// type strings onto these so the compatibility layer stays in one place.
type NodeType string

const (
	TypeManualTrigger   NodeType = "manualTrigger"
	TypeScheduleTrigger NodeType = "scheduleTrigger"
	TypeWebhookTrigger  NodeType = "webhookTrigger"
	TypeErrorTrigger    NodeType = "errorTrigger"
	TypeCode            NodeType = "code"
	TypeIf              NodeType = "if"
	TypeWait            NodeType = "wait"
	TypeNoOp            NodeType = "noOp"
	TypeSplitOut        NodeType = "splitOut"
	TypeSplitInBatches  NodeType = "splitInBatches"
	TypeHTTPRequest     NodeType = "httpRequest"
	TypeExecuteCommand  NodeType = "executeCommand"
	TypeReadWriteFile   NodeType = "readWriteFile"
	TypeGoogleSheets    NodeType = "googleSheets"
	TypeYouTube         NodeType = "youTube"
	TypeGmail           NodeType = "gmail"
	TypeStickyNote      NodeType = "stickyNote" // parsed but never executed
	TypeUnknown         NodeType = "unknown"    // parsed but flagged incompatible
)

// Node is one step in a workflow.
type Node struct {
	ID           string            `json:"id"`
	Name         string            `json:"name"` // n8n connections are name-addressed; we keep name as the primary key like n8n does
	Type         NodeType          `json:"type"`
	OriginalType string            `json:"originalType"` // raw n8n type string, kept for round-trip/export and diagnostics
	TypeVersion  float64           `json:"typeVersion"`
	Parameters   map[string]any    `json:"parameters"`
	Credentials  map[string]string `json:"credentials"` // logical credential name -> vault key, never raw secrets
	Position     [2]float64        `json:"position"`
	Disabled     bool              `json:"disabled"`
	// RetryOnFail / MaxTries / WaitBetweenTries mirror n8n's per-node retry
	// settings so a node's own retry policy (distinct from workflow-level
	// retry-loop patterns built out of IF+Wait+NoOp) is preserved.
	RetryOnFail        bool `json:"retryOnFail"`
	MaxTries           int  `json:"maxTries"`
	WaitBetweenTriesMs int  `json:"waitBetweenTriesMs"`
	ContinueOnFail     bool `json:"continueOnFail"`
	// OnErrorMode preserves n8n's raw onError setting ("continueRegularOutput"
	// | "continueErrorOutput" | "stopWorkflow" | ""), distinct from the
	// derived ContinueOnFail bool above. The engine needs the original
	// mode (not just "continue or not") to know whether a failed node's
	// error item should flow out on the normal output (continueRegularOutput,
	// mixed in with regular items) or on the dedicated second/error output
	// branch (continueErrorOutput, output index 1) -- these are genuinely
	// different n8n behaviors that collapsing to one bool would lose.
	OnErrorMode string `json:"onErrorMode"`
}

// Connection is a directed edge between two nodes, matching n8n's
// (name, outputIndex) -> (name, inputIndex) model. IF/Router nodes use
// output index 0/1 for true/false branches; most nodes have one output.
type Connection struct {
	SourceName  string `json:"sourceName"`
	SourceIndex int    `json:"sourceIndex"`
	TargetName  string `json:"targetName"`
	TargetIndex int    `json:"targetIndex"`
}

// Workflow is the full, parsed, executable representation of a workflow.
type Workflow struct {
	ID          string                  `json:"id"`
	Name        string                  `json:"name"`
	Active      bool                    `json:"active"`
	Nodes       map[string]*Node        `json:"nodes"`       // keyed by Name (n8n connections reference nodes by name)
	Connections map[string][]Connection `json:"connections"` // keyed by SourceName
	Settings    map[string]any          `json:"settings"`
	CreatedAt   time.Time               `json:"createdAt"`
	UpdatedAt   time.Time               `json:"updatedAt"`
}

// Incoming returns every connection that targets nodeName, used by the
// engine to know how many upstream branches must resolve before a node
// (e.g. a Merge-like consumer) can run.
func (w *Workflow) Incoming(nodeName string) []Connection {
	var out []Connection
	for _, conns := range w.Connections {
		for _, c := range conns {
			if c.TargetName == nodeName {
				out = append(out, c)
			}
		}
	}
	return out
}

// Item is a single unit of data flowing between nodes, matching n8n's
// { json, binary } item shape. Binary payloads are never held in Data
// directly for large media -- BinaryRef points at a spooled temp file
// (see internal/engine/binary.go) so large images/audio/video are never
// fully resident in RAM at once.
type Item struct {
	JSON   map[string]any       `json:"json"`
	Binary map[string]BinaryRef `json:"binary,omitempty"`
}

// BinaryRef is a pointer to binary data spooled on disk, not the bytes
// themselves. FileName is an absolute path under the run's scratch dir.
type BinaryRef struct {
	FileName     string `json:"fileName"`
	MimeType     string `json:"mimeType"`
	SizeBytes    int64  `json:"sizeBytes"`
	OriginalName string `json:"originalName,omitempty"`
}

// NodeOutput is what a node executor returns: n8n-style multiple output
// branches (index 0 = true/main, index 1 = false, etc.), each a slice of
// Items.
type NodeOutput [][]Item

// ExecutionStatus mirrors the panel states MicroFlow's UI shows.
type ExecutionStatus string

const (
	StatusRunning   ExecutionStatus = "running"
	StatusSuccess   ExecutionStatus = "success"
	StatusError     ExecutionStatus = "error"
	StatusCancelled ExecutionStatus = "cancelled"
	StatusWaiting   ExecutionStatus = "waiting"
)

// NodeRunResult is one node's logged execution, shown in the UI's
// Execution panel (status/input/output/error/logs/duration).
type NodeRunResult struct {
	NodeName  string          `json:"nodeName"`
	Status    ExecutionStatus `json:"status"`
	Input     NodeOutput      `json:"input,omitempty"`
	Output    NodeOutput      `json:"output,omitempty"`
	Error     string          `json:"error,omitempty"`
	Logs      []string        `json:"logs,omitempty"`
	StartedAt time.Time       `json:"startedAt"`
	Duration  time.Duration   `json:"durationMs"`
	Attempt   int             `json:"attempt"`
}

// Execution is one run of a workflow, start to finish (or to error/cancel).
type Execution struct {
	ID         string          `json:"id"`
	WorkflowID string          `json:"workflowId"`
	Mode       string          `json:"mode"` // manual | schedule | webhook | error
	Status     ExecutionStatus `json:"status"`
	StartedAt  time.Time       `json:"startedAt"`
	FinishedAt *time.Time      `json:"finishedAt,omitempty"`
	NodeRuns   []NodeRunResult `json:"nodeRuns"`
	Error      string          `json:"error,omitempty"`
}

func (n *Node) ParamString(key, def string) string {
	if v, ok := n.Parameters[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		b, _ := json.Marshal(v)
		return string(b)
	}
	return def
}
