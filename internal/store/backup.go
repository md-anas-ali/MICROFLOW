package store

// Database backup (Export / Import).
//
// Scope: ONLY persistent application state that lives in the existing
// PostgreSQL database (DATABASE_URL) -- Services, Global/Service
// Environment, workflows (+ their static data), credentials / connected
// Google accounts, Run All schedules and the (legacy) schedules table. The
// runtime tables are deliberately NOT part of a backup: executions,
// execution_checkpoints and run_all_jobs describe runs that happened on
// one particular installation (see runtimeTables below).
//
// Secrets stay exactly as the existing vault stores them: AES-GCM
// ciphertext sealed with MICROFLOW_MASTER_KEY. The backup carries that
// ciphertext unchanged; import refuses (before touching anything) a backup
// whose secrets the target installation's master key cannot open.
//
// Import is FULL REPLACE in ONE transaction:
//
//	validate (pure, no DB) -> stage (temp tables inside the tx) ->
//	replace live tables -> verify counts -> COMMIT
//
// Any error at any step rolls the transaction back and the existing
// database is left untouched.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"microflow/internal/model"
	"microflow/internal/tenant"
)

const (
	// BackupFormat / BackupVersion identify a MicroFlow database backup.
	BackupFormat  = "microflow-db-backup"
	BackupVersion = 1
)

// ErrBackupActiveRuns is returned by ImportBackup while any execution or
// Run All sweep is queued/running/waiting: replacing workflows underneath a
// live run is never safe, so the import is refused and nothing changes.
var ErrBackupActiveRuns = errors.New("store: a workflow run or Run All sweep is still active -- wait for it to finish (or cancel it) and try the import again")

// ErrBackupInvalid wraps every validation failure (bad file, wrong format,
// checksum mismatch, broken references, undecryptable secrets ...).
var ErrBackupInvalid = errors.New("invalid backup")

func invalidf(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrBackupInvalid, fmt.Sprintf(format, a...))
}

// runtimeTables are intentionally excluded from backups (documented here so
// the boundary is explicit). Import clears them because they reference the
// workflows/services being replaced.
var runtimeTables = []string{"executions", "execution_checkpoints", "run_all_jobs"}

type backupCol struct {
	name string
	// req: the column is NOT NULL with no usable default -- must be present.
	req bool
	// def: SQL expression used when an (older) backup omits the column.
	def string
	// secret: bytea column holding vault ciphertext (checked against the
	// target master key during validation).
	secret bool
}

type backupTable struct {
	name  string
	cols  []backupCol
	order string // deterministic export order
	// pk column names, used for duplicate detection during validation.
	pk []string
}

func bkC(name string) backupCol         { return backupCol{name: name} }
func bkReq(name string) backupCol       { return backupCol{name: name, req: true} }
func bkDef(name, def string) backupCol  { return backupCol{name: name, def: def} }
func bkSecret(name string) backupCol    { return backupCol{name: name, req: true, secret: true} }
func bkNow(name string) backupCol       { return bkDef(name, "now()") }
func bkFlag(name, def string) backupCol { return bkDef(name, def) }

// backupTables lists every persistent table in FOREIGN-KEY DEPENDENCY ORDER
// (parents first). This list was derived from internal/store/schema.sql --
// see the table-by-table comments there.
var backupTables = []backupTable{
	{name: "services", pk: []string{"id"}, order: "created_at, id",
		cols: []backupCol{bkReq("id"), bkReq("name"), bkNow("created_at"), bkNow("updated_at")}},
	{name: "workflows", pk: []string{"id"}, order: "created_at, id",
		cols: []backupCol{bkReq("id"), bkReq("name"), bkDef("service_id", "'default'"), bkFlag("active", "false"), bkReq("definition"), bkNow("created_at"), bkNow("updated_at")}},
	{name: "workflow_static_data", pk: []string{"workflow_id"}, order: "workflow_id",
		cols: []backupCol{bkReq("workflow_id"), bkDef("data", "'{}'::jsonb"), bkNow("updated_at")}},
	{name: "credentials", pk: []string{"workflow_id", "logical_name"}, order: "workflow_id, logical_name",
		cols: []backupCol{bkReq("workflow_id"), bkReq("logical_name"), bkSecret("ciphertext"), bkNow("updated_at")}},
	{name: "google_account_credentials", pk: []string{"account"}, order: "account",
		cols: []backupCol{bkReq("account"), bkSecret("ciphertext"), bkNow("updated_at")}},
	{name: "schedules", pk: []string{"id"}, order: "id",
		cols: []backupCol{bkReq("id"), bkReq("workflow_id"), bkReq("node_name"), bkC("cron_expr"), bkC("interval_seconds"), bkC("next_run_at"), bkFlag("enabled", "true")}},
	{name: "global_env", pk: []string{"key"}, order: "key",
		cols: []backupCol{bkReq("key"), bkSecret("ciphertext"), bkFlag("is_secret", "true"), bkNow("updated_at")}},
	{name: "service_env", pk: []string{"service_id", "key"}, order: "service_id, key",
		cols: []backupCol{bkReq("service_id"), bkReq("key"), bkSecret("ciphertext"), bkFlag("is_secret", "true"), bkNow("updated_at")}},
	{name: "run_all_schedules", pk: []string{"id"}, order: "created_at, id",
		cols: []backupCol{bkReq("id"), bkDef("label", "''"), bkC("cron_expr"), bkC("interval_seconds"), bkFlag("stop_on_failure", "false"), bkFlag("enabled", "false"),
			bkNow("created_at"), bkNow("updated_at"), bkC("start_at"), bkC("end_at"), bkDef("max_runs", "0"), bkDef("run_count", "0")}},
}

func (t backupTable) colNames() []string {
	out := make([]string, len(t.cols))
	for i, col := range t.cols {
		out[i] = col.name
	}
	return out
}

// quoteIdent is only ever fed the constant identifiers above; "key" is a
// reserved-ish word in some contexts so every identifier is quoted.
func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func (t backupTable) quotedCols() string {
	q := make([]string, len(t.cols))
	for i, col := range t.cols {
		q[i] = quoteIdent(col.name)
	}
	return strings.Join(q, ", ")
}

// Backup is the portable file format.
type Backup struct {
	Format     string          `json:"format"`
	Version    int             `json:"version"`
	ExportedAt time.Time       `json:"exportedAt"`
	Counts     map[string]int  `json:"counts"`
	Checksum   string          `json:"checksum"` // hex sha256 of the compacted "tables" JSON
	Tables     json.RawMessage `json:"tables"`
}

func checksumOf(tablesJSON []byte) (string, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, tablesJSON); err != nil {
		return "", err
	}
	sum := sha256.Sum256(buf.Bytes())
	return hex.EncodeToString(sum[:]), nil
}

// ExportBackup reads every persistent table inside ONE read-only,
// repeatable-read transaction, so the file is a consistent snapshot even
// while the server keeps running. IDs, timestamps and relationships are
// preserved verbatim (rows are emitted as JSON objects keyed by column).
func (s *Store) ExportBackup(ctx context.Context) (*Backup, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var buf bytes.Buffer
	buf.WriteByte('{')
	counts := make(map[string]int, len(backupTables))
	for i, t := range backupTables {
		pairs := make([]string, 0, len(t.cols)*2)
		for _, col := range t.cols {
			pairs = append(pairs, "'"+col.name+"'", quoteIdent(col.name))
		}
		q := fmt.Sprintf(`SELECT COALESCE(jsonb_agg(jsonb_build_object(%s) ORDER BY %s), '[]'::jsonb)::text, count(*) FROM %s`,
			strings.Join(pairs, ", "), t.order, quoteIdent(t.name))
		var rows string
		var n int
		if err := tx.QueryRow(ctx, q).Scan(&rows, &n); err != nil {
			return nil, fmt.Errorf("export %s: %w", t.name, err)
		}
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(`"` + t.name + `":`)
		buf.WriteString(rows)
		counts[t.name] = n
	}
	buf.WriteByte('}')
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	tables := json.RawMessage(buf.Bytes())
	sum, err := checksumOf(tables)
	if err != nil {
		return nil, err
	}
	return &Backup{
		Format:     BackupFormat,
		Version:    BackupVersion,
		ExportedAt: time.Now().UTC(),
		Counts:     counts,
		Checksum:   sum,
		Tables:     tables,
	}, nil
}

// ParsedBackup is a backup that passed validation and is ready to import.
type ParsedBackup struct {
	rows   map[string][]byte // table -> JSON array text (validated)
	counts map[string]int
}

// Counts returns how many rows each table holds in the backup.
func (p *ParsedBackup) Counts() map[string]int {
	out := make(map[string]int, len(p.counts))
	for k, v := range p.counts {
		out[k] = v
	}
	return out
}

// ParseBackup is the VALIDATE step. It never touches the database:
//   - format/version, checksum, complete + known table set;
//   - required columns, primary-key uniqueness, referential integrity
//     (workflow -> service, static data/credentials/schedules -> workflow,
//     service_env -> service);
//   - every workflow definition parses as a model.Workflow with a matching id;
//   - every vault ciphertext can be opened by canOpen (the target
//     installation's MICROFLOW_MASTER_KEY).
func ParseBackup(r io.Reader, canOpen func(ciphertext []byte) bool) (*ParsedBackup, error) {
	var b Backup
	if err := json.NewDecoder(r).Decode(&b); err != nil {
		return nil, invalidf("not a readable MicroFlow backup file (%v)", err)
	}
	if b.Format != BackupFormat {
		return nil, invalidf("unrecognised file format %q", b.Format)
	}
	if b.Version < 1 || b.Version > BackupVersion {
		return nil, invalidf("backup version %d is not supported by this MicroFlow (supports up to %d)", b.Version, BackupVersion)
	}
	if len(b.Tables) == 0 {
		return nil, invalidf("backup contains no tables")
	}
	want, err := checksumOf(b.Tables)
	if err != nil {
		return nil, invalidf("backup data is damaged (%v)", err)
	}
	if !strings.EqualFold(want, b.Checksum) {
		return nil, invalidf("checksum mismatch -- the file is corrupted or was modified")
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b.Tables, &raw); err != nil {
		return nil, invalidf("backup tables are unreadable (%v)", err)
	}
	known := map[string]bool{}
	for _, t := range backupTables {
		known[t.name] = true
	}
	for name := range raw {
		if !known[name] {
			return nil, invalidf("unknown table %q -- the backup was made by a newer/different MicroFlow", name)
		}
	}

	p := &ParsedBackup{rows: map[string][]byte{}, counts: map[string]int{}}
	// keys[table] = set of primary keys, for reference checks below.
	keys := map[string]map[string]bool{}
	tableRows := map[string][]map[string]json.RawMessage{}

	for _, t := range backupTables {
		arr, ok := raw[t.name]
		if !ok {
			// Refusing is safer than silently treating a missing table as
			// "empty": a full replace would then wipe that data.
			return nil, invalidf("table %q is missing from the backup", t.name)
		}
		var rows []map[string]json.RawMessage
		if err := json.Unmarshal(arr, &rows); err != nil {
			return nil, invalidf("table %q is not a list of rows (%v)", t.name, err)
		}
		colKnown := map[string]bool{}
		for _, col := range t.cols {
			colKnown[col.name] = true
		}
		seen := map[string]bool{}
		for i, row := range rows {
			for k := range row {
				if !colKnown[k] {
					return nil, invalidf("%s row %d has unknown column %q", t.name, i+1, k)
				}
			}
			for _, col := range t.cols {
				v, present := row[col.name]
				isNull := !present || string(bytes.TrimSpace(v)) == "null"
				if col.req && isNull {
					return nil, invalidf("%s row %d is missing required column %q", t.name, i+1, col.name)
				}
				if col.secret && !isNull {
					ct, err := decodeBytea(v)
					if err != nil {
						return nil, invalidf("%s row %d: %s is not valid binary data", t.name, i+1, col.name)
					}
					if canOpen != nil && !canOpen(ct) {
						return nil, invalidf("%s row %d holds a secret this installation cannot decrypt -- the backup was made with a different MICROFLOW_MASTER_KEY; set the same MICROFLOW_MASTER_KEY here and retry", t.name, i+1)
					}
				}
			}
			var kp []string
			for _, pk := range t.pk {
				var sv string
				if err := json.Unmarshal(row[pk], &sv); err != nil || sv == "" {
					return nil, invalidf("%s row %d has an empty/invalid %q", t.name, i+1, pk)
				}
				kp = append(kp, sv)
			}
			key := strings.Join(kp, "\x00")
			if seen[key] {
				return nil, invalidf("%s contains a duplicate id %q", t.name, strings.Join(kp, "/"))
			}
			seen[key] = true
		}
		keys[t.name] = seen
		tableRows[t.name] = rows
		p.rows[t.name] = arr
		p.counts[t.name] = len(rows)
	}

	// Referential integrity (mirrors the schema's foreign keys).
	str := func(row map[string]json.RawMessage, col string) string {
		var s string
		_ = json.Unmarshal(row[col], &s)
		return s
	}
	serviceIDs := keys["services"]
	serviceIDs[tenant.DefaultID] = true // re-created on import if the file lacks it
	workflowIDs := keys["workflows"]
	for i, row := range tableRows["workflows"] {
		sid := str(row, "service_id")
		if sid == "" {
			sid = tenant.DefaultID
		}
		if !serviceIDs[sid] {
			return nil, invalidf("workflow %q references unknown service %q", str(row, "id"), sid)
		}
		var wf model.Workflow
		if err := json.Unmarshal(row["definition"], &wf); err != nil {
			return nil, invalidf("workflow %q (row %d) has an unreadable definition (%v)", str(row, "id"), i+1, err)
		}
		if wf.ID != str(row, "id") {
			return nil, invalidf("workflow row %q contains a definition for a different workflow id %q", str(row, "id"), wf.ID)
		}
	}
	for _, ref := range []struct{ table, col, parent string }{
		{"workflow_static_data", "workflow_id", "workflows"},
		{"credentials", "workflow_id", "workflows"},
		{"schedules", "workflow_id", "workflows"},
		{"service_env", "service_id", "services"},
	} {
		parent := workflowIDs
		if ref.parent == "services" {
			parent = serviceIDs
		}
		for _, row := range tableRows[ref.table] {
			if !parent[str(row, ref.col)] {
				return nil, invalidf("%s references unknown %s %q", ref.table, ref.col, str(row, ref.col))
			}
		}
	}
	return p, nil
}

// decodeBytea decodes PostgreSQL's JSON text form of bytea ("\x6162...").
func decodeBytea(raw json.RawMessage) ([]byte, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(s, `\x`) {
		return nil, errors.New("not hex bytea")
	}
	return hex.DecodeString(s[2:])
}

// ImportSummary reports what an import restored.
type ImportSummary struct {
	Counts map[string]int `json:"counts"`
}

// ImportBackup is STAGE + REPLACE + VERIFY + COMMIT in one transaction.
// ANY error rolls everything back: the existing database stays untouched.
func (s *Store) ImportBackup(ctx context.Context, p *ParsedBackup) (*ImportSummary, error) {
	if p == nil {
		return nil, invalidf("nothing to import")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) // no-op after a successful Commit

	// Never wait forever behind another writer; fail (and roll back) instead.
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '20s'`); err != nil {
		return nil, err
	}
	// One import at a time, and no concurrent writers to the tables being
	// replaced while it runs (readers are not blocked).
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('microflow-database-import'))`); err != nil {
		return nil, err
	}
	lockList := make([]string, 0, len(backupTables)+len(runtimeTables))
	for _, t := range backupTables {
		lockList = append(lockList, quoteIdent(t.name))
	}
	for _, t := range runtimeTables {
		lockList = append(lockList, quoteIdent(t))
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE `+strings.Join(lockList, ", ")+` IN EXCLUSIVE MODE`); err != nil {
		return nil, fmt.Errorf("could not lock the database for import (is another operation running?): %w", err)
	}

	// Refuse while anything is running (checked AFTER the locks, so no run
	// can start between this check and the replace).
	var active int
	if err := tx.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM executions WHERE finished_at IS NULL AND status IN ('queued','running','waiting'))
		     + (SELECT count(*) FROM run_all_jobs WHERE status IN ('queued','running'))`).Scan(&active); err != nil {
		return nil, err
	}
	if active > 0 {
		return nil, ErrBackupActiveRuns
	}

	// ---- STAGE: load the file into temp tables (dropped on commit/rollback).
	for _, t := range backupTables {
		tmp := quoteIdent("_bk_" + t.name)
		if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP`, tmp, quoteIdent(t.name))); err != nil {
			return nil, fmt.Errorf("stage %s: %w", t.name, err)
		}
		sel := make([]string, len(t.cols))
		for i, col := range t.cols {
			ref := "r." + quoteIdent(col.name)
			if col.def != "" {
				ref = "COALESCE(" + ref + ", " + col.def + ")"
			}
			sel[i] = ref
		}
		q := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM jsonb_populate_recordset(NULL::%s, $1::jsonb) AS r`,
			tmp, t.quotedCols(), strings.Join(sel, ", "), quoteIdent(t.name))
		if _, err := tx.Exec(ctx, q, string(p.rows[t.name])); err != nil {
			return nil, fmt.Errorf("stage %s: %w", t.name, err)
		}
	}

	// ---- REPLACE: clear everything persistent + the runtime tables that
	// point at it, then copy the staged rows in dependency order.
	for _, stmt := range []string{
		`DELETE FROM run_all_jobs`,
		`DELETE FROM services`, // cascades: workflows, executions, checkpoints, credentials, static data, schedules, service_env
		`DELETE FROM global_env`,
		`DELETE FROM google_account_credentials`,
		`DELETE FROM run_all_schedules`,
		`DELETE FROM schedules`,
	} {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return nil, fmt.Errorf("replace (%s): %w", stmt, err)
		}
	}
	for _, t := range backupTables {
		q := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s`, quoteIdent(t.name), t.quotedCols(), t.quotedCols(), quoteIdent("_bk_"+t.name))
		if _, err := tx.Exec(ctx, q); err != nil {
			return nil, fmt.Errorf("replace %s: %w", t.name, err)
		}
		if t.name == "services" {
			// Schema invariant: the 'default' Service always exists.
			if _, err := tx.Exec(ctx, `INSERT INTO services (id, name) VALUES ('default', 'Default') ON CONFLICT (id) DO NOTHING`); err != nil {
				return nil, err
			}
		}
	}

	// ---- VERIFY: what is now live must match what the file said.
	summary := &ImportSummary{Counts: map[string]int{}}
	names := make([]string, 0, len(backupTables))
	for _, t := range backupTables {
		names = append(names, t.name)
	}
	sort.Strings(names)
	for _, name := range names {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+quoteIdent(name)).Scan(&n); err != nil {
			return nil, err
		}
		expect := p.counts[name]
		if name == "services" {
			var hasDefault bool
			for _, row := range mustRows(p.rows[name]) {
				var id string
				_ = json.Unmarshal(row["id"], &id)
				if id == tenant.DefaultID {
					hasDefault = true
				}
			}
			if !hasDefault {
				expect++
			}
		}
		if n != expect {
			return nil, fmt.Errorf("verify %s: expected %d rows after import, found %d -- rolled back", name, expect, n)
		}
		summary.Counts[name] = n
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return summary, nil
}

func mustRows(arr []byte) []map[string]json.RawMessage {
	var rows []map[string]json.RawMessage
	_ = json.Unmarshal(arr, &rows)
	return rows
}
