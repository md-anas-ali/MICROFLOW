// Package parser implements the n8n compatibility layer: it reads a raw
// n8n workflow export and produces a model.Workflow that the engine can
// run. It is intentionally the ONLY place that knows about n8n's JSON
// shape and type-string vocabulary, so adding support for more n8n node
// types later means editing this file (and internal/nodes), not the
// engine.
package parser

import (
	"encoding/json"
	"fmt"

	"microflow/internal/model"
)

// rawN8NNode mirrors the fields n8n actually writes for a node. Unknown
// extra fields are ignored by encoding/json, so this does not need to be
// exhaustive of every n8n version -- but every field the sample workflow
// used (parameters, credentials, typeVersion, position, retry settings,
// continueOnFail, disabled) is captured.
type rawN8NNode struct {
	ID               string         `json:"id"`
	Name             string         `json:"name"`
	Type             string         `json:"type"`
	TypeVersion      float64        `json:"typeVersion"`
	Position         [2]float64     `json:"position"`
	Parameters       map[string]any `json:"parameters"`
	Credentials      map[string]any `json:"credentials"`
	Disabled         bool           `json:"disabled"`
	RetryOnFail      bool           `json:"retryOnFail"`
	MaxTries         int            `json:"maxTries"`
	WaitBetweenTries int            `json:"waitBetweenTries"`
	ContinueOnFail   bool           `json:"continueOnFail"`
	OnError          string         `json:"onError"` // n8n >=1.19 uses onError instead of continueOnFail
}

type rawN8NWorkflow struct {
	ID          string                       `json:"id"`
	Name        string                       `json:"name"`
	Active      bool                         `json:"active"`
	Nodes       []rawN8NNode                 `json:"nodes"`
	Connections map[string]rawN8NConnections `json:"connections"`
	Settings    map[string]any               `json:"settings"`
}

// n8n's connections shape is: { "<sourceName>": { "main": [ [ {node,type,index}, ... ], [ ... ] ] } }
// The outer array index is the SOURCE output index (0 = main/true, 1 = false for IF, etc).
type rawN8NConnections struct {
	Main [][]rawN8NConnTarget `json:"main"`
}

type rawN8NConnTarget struct {
	Node  string `json:"node"`
	Type  string `json:"type"`
	Index int    `json:"index"`
}

// typeMap translates n8n's fully-qualified type strings to MicroFlow's
// NodeType. Anything not listed here parses successfully but is tagged
// model.TypeUnknown so the compatibility checklist / import UI can flag
// it instead of silently dropping functionality (rule: never silently
// skip a required node).
var typeMap = map[string]model.NodeType{
	"n8n-nodes-base.manualTrigger":   model.TypeManualTrigger,
	"n8n-nodes-base.scheduleTrigger": model.TypeScheduleTrigger,
	"n8n-nodes-base.webhook":         model.TypeWebhookTrigger,
	"n8n-nodes-base.errorTrigger":    model.TypeErrorTrigger,
	"n8n-nodes-base.code":            model.TypeCode,
	"n8n-nodes-base.function":        model.TypeCode, // legacy alias
	"n8n-nodes-base.if":              model.TypeIf,
	"n8n-nodes-base.wait":            model.TypeWait,
	"n8n-nodes-base.noOp":            model.TypeNoOp,
	"n8n-nodes-base.splitOut":        model.TypeSplitOut,
	"n8n-nodes-base.splitInBatches":  model.TypeSplitInBatches,
	"n8n-nodes-base.httpRequest":     model.TypeHTTPRequest,
	"n8n-nodes-base.executeCommand":  model.TypeExecuteCommand,
	"n8n-nodes-base.readWriteFile":   model.TypeReadWriteFile,
	"n8n-nodes-base.googleSheets":    model.TypeGoogleSheets,
	"n8n-nodes-base.youTube":         model.TypeYouTube,
	"n8n-nodes-base.gmail":           model.TypeGmail,
	"n8n-nodes-base.stickyNote":      model.TypeStickyNote,
}

// CompatItem is one line of the compatibility checklist shown to the
// user on import, per spec section 1/26 ("show checklist before build").
type CompatItem struct {
	NodeName     string `json:"nodeName"`
	OriginalType string `json:"originalType"`
	Mapped       string `json:"mapped"`
	Supported    bool   `json:"supported"`
}

// ParseResult bundles the parsed workflow with its compatibility report.
type ParseResult struct {
	Workflow  *model.Workflow
	Checklist []CompatItem
}

// Parse converts raw n8n export bytes into a MicroFlow workflow plus a
// compatibility checklist. It does not execute anything and does not
// drop nodes it doesn't understand -- unsupported nodes are kept in the
// model (tagged Unknown) so the import UI can surface them rather than
// silently producing a workflow with missing functionality.
func Parse(raw []byte) (*ParseResult, error) {
	var rw rawN8NWorkflow
	if err := json.Unmarshal(raw, &rw); err != nil {
		return nil, fmt.Errorf("parser: invalid n8n export json: %w", err)
	}
	if len(rw.Nodes) == 0 {
		return nil, fmt.Errorf("parser: workflow has zero nodes")
	}

	wf := &model.Workflow{
		ID:          rw.ID,
		Name:        rw.Name,
		Active:      rw.Active,
		Nodes:       make(map[string]*model.Node, len(rw.Nodes)),
		Connections: make(map[string][]model.Connection, len(rw.Connections)),
		Settings:    rw.Settings,
	}

	var checklist []CompatItem
	for _, rn := range rw.Nodes {
		mapped, ok := typeMap[rn.Type]
		if !ok {
			mapped = model.TypeUnknown
		}
		checklist = append(checklist, CompatItem{
			NodeName:     rn.Name,
			OriginalType: rn.Type,
			Mapped:       string(mapped),
			Supported:    ok,
		})

		creds := map[string]string{}
		for logicalName := range rn.Credentials {
			// Only the logical credential *name* travels with the workflow.
			// The actual secret is looked up from the vault at execution
			// time by (workflow, logicalName) -- see internal/vault.
			creds[logicalName] = logicalName
		}

		maxTries := rn.MaxTries
		if maxTries == 0 && rn.RetryOnFail {
			maxTries = 3 // n8n default
		}
		waitMs := rn.WaitBetweenTries
		if waitMs == 0 {
			waitMs = 1000
		}

		// onErrorMode preserves which *kind* of continue-on-fail n8n asked
		// for (regular output vs dedicated error output) so the engine can
		// route a failed node's error item correctly instead of just
		// knowing "continue or not". A bare legacy continueOnFail:true
		// (pre-onError n8n versions) behaves like continueRegularOutput.
		onErrorMode := rn.OnError
		if onErrorMode == "" && rn.ContinueOnFail {
			onErrorMode = "continueRegularOutput"
		}

		wf.Nodes[rn.Name] = &model.Node{
			ID:                 rn.ID,
			Name:               rn.Name,
			Type:               mapped,
			OriginalType:       rn.Type,
			TypeVersion:        rn.TypeVersion,
			Parameters:         rn.Parameters,
			Credentials:        creds,
			Position:           rn.Position,
			Disabled:           rn.Disabled,
			RetryOnFail:        rn.RetryOnFail,
			MaxTries:           maxTries,
			WaitBetweenTriesMs: waitMs,
			ContinueOnFail:     rn.ContinueOnFail || rn.OnError == "continueRegularOutput" || rn.OnError == "continueErrorOutput",
			OnErrorMode:        onErrorMode,
		}
	}

	for sourceName, rc := range rw.Connections {
		for outIdx, targets := range rc.Main {
			for _, t := range targets {
				wf.Connections[sourceName] = append(wf.Connections[sourceName], model.Connection{
					SourceName:  sourceName,
					SourceIndex: outIdx,
					TargetName:  t.Node,
					TargetIndex: t.Index,
				})
			}
		}
	}

	return &ParseResult{Workflow: wf, Checklist: checklist}, nil
}

// Unsupported returns the checklist entries the user needs to look at
// before relying on this import for production use.
func (r *ParseResult) Unsupported() []CompatItem {
	var out []CompatItem
	for _, c := range r.Checklist {
		if !c.Supported && c.Mapped != string(model.TypeStickyNote) {
			out = append(out, c)
		}
	}
	return out
}
