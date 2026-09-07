-- MicroFlow schema. Plain, standard PostgreSQL — no Neon-specific
-- extensions or syntax, so this runs unmodified on Neon, Supabase,
-- Render Postgres, Railway, or a local/self-hosted instance reached via
-- a normal DATABASE_URL.

CREATE TABLE IF NOT EXISTS workflows (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    active      BOOLEAN NOT NULL DEFAULT false,
    definition  JSONB NOT NULL,      -- the parsed model.Workflow, serialized
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per workflow holds its $getWorkflowStaticData('global')
-- equivalent. Reads/writes go through SELECT ... FOR UPDATE (see
-- store.go WithLock) so concurrent executions of the same workflow
-- can't race on counters / dedup state / model-fallback queues (rule 13).
CREATE TABLE IF NOT EXISTS workflow_static_data (
    workflow_id TEXT PRIMARY KEY REFERENCES workflows(id) ON DELETE CASCADE,
    data        JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS executions (
    id           TEXT PRIMARY KEY,
    workflow_id  TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    mode         TEXT NOT NULL,
    status       TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ,
    error        TEXT,
    node_runs    JSONB NOT NULL DEFAULT '[]'::jsonb -- capped/pruned by the app layer, not unbounded (rule 18)
);
CREATE INDEX IF NOT EXISTS idx_executions_workflow ON executions(workflow_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_executions_finished_at ON executions(finished_at);

-- Credentials are stored ONLY as vault ciphertext (nonce+AES-GCM
-- sealed box); the server never writes plaintext secrets to this table
-- (rule 11/12).
CREATE TABLE IF NOT EXISTS credentials (
    workflow_id   TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    logical_name  TEXT NOT NULL,
    ciphertext    BYTEA NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, logical_name)
);

-- The single central Google account credential, shared automatically by
-- every Google node (googleSheets/youTube/gmail) across every workflow
-- (see internal/vault/central.go). Deliberately its own table with no
-- workflow_id/FK: this is one account-scoped secret, not a per-workflow
-- one, so it must not disappear if a workflow is ever deleted (unlike
-- `credentials` above, which cascades with its owning workflow). In
-- practice there is exactly one row (account='google') today.
CREATE TABLE IF NOT EXISTS google_account_credentials (
    account       TEXT PRIMARY KEY,
    ciphertext    BYTEA NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS schedules (
    id           TEXT PRIMARY KEY,
    workflow_id  TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    node_name    TEXT NOT NULL,
    cron_expr    TEXT,           -- either cron_expr or interval_seconds is set
    interval_seconds INT,
    next_run_at  TIMESTAMPTZ,
    enabled      BOOLEAN NOT NULL DEFAULT true
);
