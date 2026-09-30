package api

// Database Export / Import (see internal/store/backup.go for the format and
// the transactional full-replace logic). Both routes sit behind the existing
// login gate like every other /api/ route; Import additionally needs the
// login password re-verified (the same s.reauth check the destructive
// Environment action uses) and an explicit confirmation header, both
// enforced here, not just in the UI.
//
// Backups are portable: secrets are re-encrypted under a per-backup key held
// in the file on export and re-keyed to THIS installation's
// MICROFLOW_MASTER_KEY on import, so the two installations do not need to
// share a master key. Both actions need a typed confirmation phrase
// (X-Microflow-Confirm), enforced here and not just in the UI, to stop
// accidental clicks.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"microflow/internal/store"
)

// DatabaseBackupStore is implemented by *store.Store.
type DatabaseBackupStore interface {
	ExportBackup(ctx context.Context, open func(ciphertext []byte) ([]byte, error)) (*store.Backup, error)
	ImportBackup(ctx context.Context, p *store.ParsedBackup) (*store.ImportSummary, error)
}

// ImportConfirmPhrase / ExportConfirmPhrase must be sent in the
// X-Microflow-Confirm header of the respective request.
const (
	ImportConfirmPhrase = "IMPORT DATABASE"
	ExportConfirmPhrase = "EXPORT DATABASE"
)

// WithDatabaseBackup enables /api/database/export and /api/database/import.
// reload is called after a successful import commit to re-register schedules
// and webhooks from the freshly restored database (see cmd/server/main.go).
// Nil-safe like the other With* hooks: without it no routes are registered.
func (s *Server) WithDatabaseBackup(st DatabaseBackupStore, reload func(ctx context.Context) error) *Server {
	s.dbBackup = st
	s.dbReload = reload
	s.mux.HandleFunc("GET /api/database/export", s.handleDatabaseExport)
	s.mux.HandleFunc("POST /api/database/import", s.handleDatabaseImport)
	return s
}

func importMaxBytes() int64 {
	mb := int64(128)
	if v := os.Getenv("MICROFLOW_DB_IMPORT_MAX_MB"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			mb = n
		}
	}
	return mb << 20
}

func (s *Server) handleDatabaseExport(w http.ResponseWriter, r *http.Request) {
	if s.dbBackup == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("database backup is not enabled on this server"))
		return
	}
	if s.vault == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("database backup needs the credential vault, which is not enabled"))
		return
	}
	if r.Header.Get("X-Microflow-Confirm") != ExportConfirmPhrase {
		writeErr(w, http.StatusBadRequest, errors.New("confirmation missing -- nothing was exported"))
		return
	}
	b, err := s.dbBackup.ExportBackup(r.Context(), s.vault.Open)
	if err != nil {
		log.Printf("database export failed: %v", err)
		writeErr(w, http.StatusInternalServerError, errors.New("failed to export the database (a stored secret may not be decryptable with this installation's master key -- see the server log)"))
		return
	}
	name := "microflow-backup-" + b.ExportedAt.UTC().Format("20060102-150405") + ".json"
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	w.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(w).Encode(b); err != nil {
		log.Printf("database export: write failed: %v", err)
		return
	}
	log.Printf("database export: %v", b.Counts) // row counts only, never data
}

func (s *Server) handleDatabaseImport(w http.ResponseWriter, r *http.Request) {
	if s.dbBackup == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("database backup is not enabled on this server"))
		return
	}
	if s.reauth == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("database import is not enabled on this server"))
		return
	}
	if r.Header.Get("X-Microflow-Confirm") != ImportConfirmPhrase {
		writeErr(w, http.StatusBadRequest, errors.New("confirmation missing -- nothing was changed"))
		return
	}
	ok, wait := s.reauth(r, r.Header.Get("X-Microflow-Password"))
	if wait > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
		writeErr(w, http.StatusTooManyRequests, errors.New("too many failed attempts -- try again later"))
		return
	}
	if !ok {
		writeErr(w, http.StatusForbidden, errors.New("password verification failed -- nothing was changed"))
		return
	}

	// 1. VALIDATE (no database access).
	body := http.MaxBytesReader(w, r.Body, importMaxBytes())
	if s.vault == nil {
		writeErr(w, http.StatusNotImplemented, errors.New("database import needs the credential vault, which is not enabled"))
		return
	}
	// Secrets are decrypted with the key in the backup header and re-sealed with
	// THIS installation's master key here, in memory, before any DB access.
	parsed, err := store.ParseBackup(body, s.vault.Seal)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeErr(w, http.StatusRequestEntityTooLarge, fmt.Errorf("backup file is larger than the %d MB limit (MICROFLOW_DB_IMPORT_MAX_MB)", tooBig.Limit>>20))
			return
		}
		writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}

	// Hold the same locks a workflow save / Environment write take, so none
	// of them can interleave with the replace or the scheduler reload.
	s.persistMu.Lock()
	defer s.persistMu.Unlock()
	envWriteMu.Lock()
	defer envWriteMu.Unlock()

	// 2-4. STAGE -> REPLACE -> VERIFY -> COMMIT, atomically.
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()
	summary, err := s.dbBackup.ImportBackup(ctx, parsed)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrBackupActiveRuns):
			writeErr(w, http.StatusConflict, err)
		case errors.Is(err, store.ErrBackupInvalid):
			writeErr(w, http.StatusUnprocessableEntity, err)
		default:
			log.Printf("database import failed (rolled back): %v", err)
			writeErr(w, http.StatusInternalServerError, fmt.Errorf("import failed and was rolled back -- the existing database is unchanged: %v", err))
		}
		return
	}
	log.Printf("database import committed: %v", summary.Counts)

	// 5. RELOAD application state (schedules + webhook routes).
	resp := map[string]any{"status": "ok", "counts": summary.Counts, "reloaded": true}
	if len(summary.SkippedDeploymentKeys) > 0 {
		// Host/domain-specific values are never restored from a backup; say so
		// plainly so the operator sets this installation's own value.
		resp["skippedDeploymentKeys"] = summary.SkippedDeploymentKeys
		resp["notice"] = "GOOGLE_OAUTH_REDIRECT_URL / HOST_URL were not restored because they belong to the old domain. Set HOST_URL for THIS installation (Global/Service Environment or host env) to https://<your-domain> (or set GOOGLE_OAUTH_REDIRECT_URL to <your-domain>/api/oauth/google/callback) and register <your-domain>/api/oauth/google/callback in Google Cloud Console. Connected Google accounts keep working without it; it is only needed to Connect/Reconnect."
	}
	if s.dbReload != nil {
		if rerr := s.dbReload(context.WithoutCancel(r.Context())); rerr != nil {
			log.Printf("database import: reload after commit failed: %v", rerr)
			resp["reloaded"] = false
			resp["warning"] = "the import was committed but the running schedules could not be reloaded -- restart MicroFlow to finish applying it"
		}
	}
	writeJSON(w, http.StatusOK, resp)
}
