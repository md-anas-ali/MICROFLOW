package parser

import (
	"encoding/json"
	"sort"

	"microflow/internal/model"
)

// exportN8NWorkflow mirrors rawN8NWorkflow but with plain json.RawMessage-
// friendly field ordering that matches what n8n itself writes, so the
// exported file opens cleanly in n8n if you ever need to go back.
type exportN8NWorkflow struct {
	Name        string                       `json:"name"`
	Active      bool                         `json:"active"`
	Nodes       []exportN8NNode              `json:"nodes"`
	Connections map[string]rawN8NConnections `json:"connections"`
	Settings    map[string]any               `json:"settings"`
}

type exportN8NNode struct {
	ID               string         `json:"id"`
	Name             string         `json:"name"`
	Type             string         `json:"type"`
	TypeVersion      float64        `json:"typeVersion"`
	Position         [2]float64     `json:"position"`
	Parameters       map[string]any `json:"parameters"`
	Credentials      map[string]any `json:"credentials,omitempty"`
	Disabled         bool           `json:"disabled,omitempty"`
	RetryOnFail      bool           `json:"retryOnFail,omitempty"`
	MaxTries         int            `json:"maxTries,omitempty"`
	WaitBetweenTries int            `json:"waitBetweenTries,omitempty"`
	ContinueOnFail   bool           `json:"continueOnFail,omitempty"`
	OnError          string         `json:"onError,omitempty"`
}

// Export converts a MicroFlow workflow back to n8n's export JSON shape.
// This is the reverse of Parse -- together they let a workflow round-trip
// n8n -> MicroFlow -> (edit) -> n8n. Node types MicroFlow doesn't
// natively map back to a known n8n type string fall back to
// OriginalType (which Parse always preserves, even for nodes tagged
// Unknown), so nothing silently turns into an empty/broken type string.
func Export(wf *model.Workflow) ([]byte, error) {
	out := exportN8NWorkflow{
		Name:        wf.Name,
		Active:      wf.Active,
		Connections: map[string]rawN8NConnections{},
		Settings:    wf.Settings,
	}

	// Sort-free but deterministic enough for practical use: iterate nodes
	// in a stable order by name so repeated exports of an unchanged
	// workflow produce byte-identical output (easier to diff/version).
	names := make([]string, 0, len(wf.Nodes))
	for name := range wf.Nodes {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		n := wf.Nodes[name]
		typeStr := n.OriginalType
		if typeStr == "" {
			typeStr = reverseTypeMap[n.Type]
		}
		creds := map[string]any{}
		for logicalName := range n.Credentials {
			// Export only the logical credential *name* n8n expects in
			// this field (an id/reference), never secret material --
			// same boundary the vault enforces at runtime.
			creds[logicalName] = map[string]string{"id": logicalName, "name": logicalName}
		}
		out.Nodes = append(out.Nodes, exportN8NNode{
			ID:               n.ID,
			Name:             n.Name,
			Type:             typeStr,
			TypeVersion:      n.TypeVersion,
			Position:         n.Position,
			Parameters:       n.Parameters,
			Credentials:      creds,
			Disabled:         n.Disabled,
			RetryOnFail:      n.RetryOnFail,
			MaxTries:         n.MaxTries,
			WaitBetweenTries: n.WaitBetweenTriesMs,
			ContinueOnFail:   n.ContinueOnFail,
			OnError:          n.OnErrorMode,
		})
	}

	for _, name := range names {
		conns := wf.Connections[name]
		if len(conns) == 0 {
			continue
		}
		maxIdx := 0
		for _, c := range conns {
			if c.SourceIndex > maxIdx {
				maxIdx = c.SourceIndex
			}
		}
		main := make([][]rawN8NConnTarget, maxIdx+1)
		for _, c := range conns {
			main[c.SourceIndex] = append(main[c.SourceIndex], rawN8NConnTarget{
				Node: c.TargetName, Type: "main", Index: c.TargetIndex,
			})
		}
		out.Connections[name] = rawN8NConnections{Main: main}
	}

	return json.MarshalIndent(out, "", "  ")
}

// reverseTypeMap is derived from typeMap so it can't drift out of sync;
// only used as a fallback when OriginalType is somehow empty (e.g. a
// node created fresh in the MicroFlow editor rather than imported).
var reverseTypeMap = buildReverseTypeMap()

func buildReverseTypeMap() map[model.NodeType]string {
	m := make(map[model.NodeType]string, len(typeMap))
	for n8nType, mfType := range typeMap {
		// first writer wins on collisions (e.g. function -> code alias);
		// fine since this is only a fallback path
		if _, exists := m[mfType]; !exists {
			m[mfType] = n8nType
		}
	}
	return m
}
