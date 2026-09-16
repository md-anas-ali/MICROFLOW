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

-- One latest-only checkpoint per execution. The payload is the small,
-- engine-derived resume state; no checkpoint history is retained.
CREATE TABLE IF NOT EXISTS execution_checkpoints (
    execution_id   TEXT PRIMARY KEY REFERENCES executions(id) ON DELETE CASCADE,
    workflow_id    TEXT NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    workflow_hash  TEXT NOT NULL,
    workflow_updated_at TIMESTAMPTZ,
    mode           TEXT NOT NULL,
    status         TEXT NOT NULL,
    version        INT NOT NULL DEFAULT 1,
    payload        JSONB NOT NULL,
    payload_bytes  INT NOT NULL,
    lease_owner    TEXT,
    lease_until    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT execution_checkpoints_payload_size CHECK (payload_bytes BETWEEN 1 AND 4194304)
);
CREATE INDEX IF NOT EXISTS idx_execution_checkpoints_workflow ON execution_checkpoints(workflow_id);
CREATE INDEX IF NOT EXISTS idx_execution_checkpoints_recovery ON execution_checkpoints(status, updated_at);

-- Idempotent upgrade for installations that created the checkpoint table before
-- started_at was added. Existing rows receive their durable execution start time.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'execution_checkpoints'
          AND column_name = 'started_at'
    ) THEN
        ALTER TABLE execution_checkpoints ADD COLUMN started_at TIMESTAMPTZ;
        UPDATE execution_checkpoints c
        SET started_at = e.started_at
        FROM executions e
        WHERE e.id = c.execution_id;
        ALTER TABLE execution_checkpoints ALTER COLUMN started_at SET NOT NULL;
    END IF;
END $$;
