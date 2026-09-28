-- MicroFlow schema. Plain, standard PostgreSQL — no Neon-specific
-- extensions or syntax, so this runs unmodified on Neon, Supabase,
-- Render Postgres, Railway, or a local/self-hosted instance reached via
-- a normal DATABASE_URL.

-- Services (called "tenants" in Go code -- see internal/tenant) are
-- isolated logical workspaces inside one MicroFlow deployment. Every
-- workflow belongs to exactly one Service; Services never share
-- credentials, connected Google accounts, environment overrides, or
-- execution context (see internal/tenant's package doc).
CREATE TABLE IF NOT EXISTS services (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Every existing/new installation always has a 'default' Service so a
-- deployment predating Services (or one that never bothers creating a
-- second Service) keeps working exactly as before -- see the workflows
-- migration below, which backfills every pre-existing workflow onto it.
INSERT INTO services (id, name) VALUES ('default', 'Default')
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS workflows (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    service_id  TEXT NOT NULL DEFAULT 'default' REFERENCES services(id) ON DELETE CASCADE,
    active      BOOLEAN NOT NULL DEFAULT false,
    definition  JSONB NOT NULL,      -- the parsed model.Workflow, serialized
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotent upgrade for installations that created `workflows` before
-- service_id existed: add the column (defaulting new AND existing rows
-- to 'default', the same Service the INSERT above guarantees exists)
-- without ever touching/losing an existing workflow row (rule 14: no
-- destructive migration, existing workflows must not be lost).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'workflows'
          AND column_name = 'service_id'
    ) THEN
        ALTER TABLE workflows ADD COLUMN service_id TEXT NOT NULL DEFAULT 'default'
            REFERENCES services(id) ON DELETE CASCADE;
    END IF;
END $$;
-- Created only after the DO block above so an old table has the column.
CREATE INDEX IF NOT EXISTS idx_workflows_service ON workflows(service_id);

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
    -- Denormalized copy of the owning workflow's service_id at the time
    -- the execution was created, so per-Service execution history/
    -- Run-All bookkeeping never needs a join back to workflows (and
    -- still reads correctly even after a workflow row is gone).
    service_id   TEXT NOT NULL DEFAULT 'default',
    mode         TEXT NOT NULL,
    status       TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ,
    error        TEXT,
    node_runs    JSONB NOT NULL DEFAULT '[]'::jsonb -- capped/pruned by the app layer, not unbounded (rule 18)
);
CREATE INDEX IF NOT EXISTS idx_executions_workflow ON executions(workflow_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_executions_finished_at ON executions(finished_at);

-- Idempotent upgrade + backfill for installations that created
-- `executions` before service_id existed.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'executions'
          AND column_name = 'service_id'
    ) THEN
        ALTER TABLE executions ADD COLUMN service_id TEXT NOT NULL DEFAULT 'default';
        UPDATE executions e SET service_id = w.service_id
        FROM workflows w WHERE w.id = e.workflow_id;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_executions_service ON executions(service_id, started_at DESC);

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

-- Global Environment: deployment-wide configuration/secrets (rule 2),
-- e.g. DATABASE_URL-adjacent shared config, common API keys. Every
-- value is stored as vault ciphertext regardless of whether the person
-- flagged it "secret" -- simplest safe default, and it means this
-- table can never accidentally leak a plaintext value even if a future
-- caller forgets to check is_secret (rule 2: "নিরাপদভাবে store করবে").
-- is_secret only controls whether the dashboard ever offers to reveal
-- the value again after saving (rule 13: never return secrets
-- unnecessarily) -- both kinds are equally encrypted at rest.
CREATE TABLE IF NOT EXISTS global_env (
    key         TEXT PRIMARY KEY,
    ciphertext  BYTEA NOT NULL,
    is_secret   BOOLEAN NOT NULL DEFAULT true,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Service Environment: per-Service overrides of Global Environment
-- (precedence: Service Environment > Global Environment > process
-- environment -- rule 2). Cascades with its owning Service.
CREATE TABLE IF NOT EXISTS service_env (
    service_id  TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    key         TEXT NOT NULL,
    ciphertext  BYTEA NOT NULL,
    is_secret   BOOLEAN NOT NULL DEFAULT true,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (service_id, key)
);

-- Run All Services: one durable job at a time (see the partial unique
-- index below), so Run-All's progress survives a server restart (rule
-- 8/17) and a duplicate Run-All click can never start a second
-- concurrent sweep. `results` accumulates one entry per
-- service/workflow/execution as the sweep progresses, so a restart can
-- resume after the last recorded entry instead of re-running completed
-- work (rule 17: idempotency).
CREATE TABLE IF NOT EXISTS run_all_jobs (
    id             TEXT PRIMARY KEY,
    status         TEXT NOT NULL, -- queued | running | success | error | cancelled
    service_ids    JSONB NOT NULL,             -- ordered array of service ids for this sweep
    stop_on_failure BOOLEAN NOT NULL DEFAULT false,
    results        JSONB NOT NULL DEFAULT '[]'::jsonb,
    error          TEXT,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ
);
-- Constant-expression partial unique index: at most one row may ever be
-- queued/running at once, so starting Run-All while one is already in
-- flight is a database-level 409, not just an application-level check.
CREATE UNIQUE INDEX IF NOT EXISTS idx_run_all_jobs_single_active
    ON run_all_jobs ((1)) WHERE status IN ('queued', 'running');

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
