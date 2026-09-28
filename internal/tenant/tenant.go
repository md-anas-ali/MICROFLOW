// Package tenant defines MicroFlow's logical "Service" concept: an
// isolated workspace inside a single MicroFlow deployment, each with
// its own workflows, connected Google/YouTube/Gmail accounts,
// credentials, environment overrides, and execution history.
//
// Named "tenant" internally (not "service") to avoid colliding with
// the pre-existing "Google service" terminology used throughout
// internal/vault and internal/nodes (gmail/youtube/sheets). Every
// external surface (API JSON, HTTP routes, the dashboard) still calls
// this concept "Service", matching how the person using MicroFlow
// thinks about it -- only Go identifiers inside this codebase use
// "Tenant" to keep the two concepts unambiguous in code.
package tenant

import "time"

// DefaultID is the Service every pre-existing workflow is migrated
// into on first startup after this feature is introduced (see
// store schema migration), so an existing single-workflow deployment
// keeps working completely unchanged -- it simply has exactly one
// Service named "Default" that owns everything it already had.
const DefaultID = "default"

// Service is one isolated logical workspace. Deleting a Service is a
// destructive, deliberately-guarded operation (see api.handleDeleteService)
// that also removes every workflow/credential/env override scoped to it
// via ON DELETE CASCADE.
type Service struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
