// Package nodes implements one executor per model.NodeType. Each file
// covers a family of node types; DefaultRegistry wires all of them for
// internal/engine.
package nodes

import (
	"microflow/internal/engine"
	"microflow/internal/model"
)

// DefaultRegistry returns every executor MicroFlow currently implements.
// Nodes NOT in this map cause the engine to hard-fail on import/execute
// rather than silently skip (see engine.Run) -- so extending coverage to
// a new n8n type means adding both a typeMap entry in internal/parser
// and an executor entry here.
func DefaultRegistry(deps Deps) map[model.NodeType]engine.NodeExecutor {
	return map[model.NodeType]engine.NodeExecutor{
		model.TypeManualTrigger:   &PassThroughExecutor{},
		model.TypeScheduleTrigger: &PassThroughExecutor{}, // scheduler seeds the run; node itself just passes data on
		model.TypeWebhookTrigger:  &PassThroughExecutor{}, // webhook server seeds the run body as input
		model.TypeErrorTrigger:    &PassThroughExecutor{},
		model.TypeNoOp:            &PassThroughExecutor{},

		model.TypeCode: &CodeExecutor{},

		model.TypeIf:             &IfExecutor{},
		model.TypeWait:           &WaitExecutor{},
		model.TypeSplitOut:       &SplitOutExecutor{},
		model.TypeSplitInBatches: &SplitInBatchesExecutor{},

		model.TypeHTTPRequest:    &HTTPRequestExecutor{Client: deps.HTTPClient},
		model.TypeExecuteCommand: &ExecuteCommandExecutor{AllowedBinaries: deps.AllowedBinaries, ScratchRoot: deps.ScratchRoot},
		model.TypeReadWriteFile:  &ReadWriteFileExecutor{ScratchRoot: deps.ScratchRoot},

		model.TypeGoogleSheets: &GoogleSheetsExecutor{Creds: deps.CredentialResolver},
		model.TypeYouTube:      &YouTubeExecutor{Creds: deps.CredentialResolver},
		model.TypeGmail:        &GmailExecutor{Creds: deps.CredentialResolver},
	}
}
