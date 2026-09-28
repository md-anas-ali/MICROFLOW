# MicroFlow Services (multi-workspace isolation)

One deployment, unlimited logical **Services**. Each Service is an isolated
workspace: its own workflows, Google/YouTube/Gmail/Sheets connections,
environment overrides and executions.

## Data model
- `services(id, name)`; a `default` Service is auto-created and every
  pre-existing workflow/execution is backfilled into it (non-destructive,
  idempotent migration in `internal/store/schema.sql`).
- `workflows.service_id` (immutable after creation), `executions.service_id`.
- `global_env`, `service_env(service_id, key)`: AES-GCM ciphertext only
  (same `MICROFLOW_MASTER_KEY`). Values are never returned by the API.
- Connected Google accounts: existing `google_account_credentials` table,
  keyed `svc:<serviceId>:<gmail|youtube|sheets>`. Only the Default Service
  falls back to the legacy single account; new Services never do.
- `run_all_jobs`: one durable row per sweep; a partial unique index allows
  only one queued/running sweep. Only the 20 newest finished rows are kept.

## Environment precedence
Service Environment > Global Environment > process environment
(`engine.RunContext.Env`). Used by Code node `$env`, `{{ $env.X }}` expressions,
`GOOGLE_SHEETS_URL`, and every `executeCommand` child process
(`engine.RunContext.ProcessEnv`: host/Render < Global < this Service's own
Environment, built per run; another Service's variables are never loaded).
Names stored in the dashboard are visible to that run's Code nodes; values
whose names look like secrets are added to the log/report redactor.
`GOOGLE_OAUTH_CLIENT_ID/SECRET/REDIRECT_URL` may be stored in Global
Environment; they are read at startup (restart after changing).

## API additions
- `GET/POST /api/services`, `GET/PATCH/DELETE /api/services/{id}`
  (delete needs `?confirmName=<exact name>`, refused while a run is active,
  Default cannot be deleted)
- `GET /api/services/{id}/workflows|executions`, `POST /api/services/{id}/run`
- `GET/POST /api/global-env`, `DELETE /api/global-env/{key}`
- `GET/POST /api/services/{id}/env`, `DELETE /api/services/{id}/env/{key}`
- `GET /api/services/{id}/google/connections|connect/{svc}`, `POST .../disconnect/{svc}`
- `POST /api/run-all` (`{"stopOnFailure":bool}`), `GET /api/run-all`, `POST /api/run-all/cancel`
- Existing routes unchanged. `POST /api/workflows/import` and
  `GET /api/workflows` accept `?serviceId=`. The UI sends
  `X-Microflow-Service` on every request; a workflow/execution belonging to
  another Service then answers 404. Requests without the header behave as
  before (backward compatible).

## Run All
Strictly sequential: Service 1 (each workflow to completion; per-execution
scratch dir + DB row already removed by the runner) -> Service 2 -> ...
Progress is persisted after every workflow; on restart `Resume` continues
after the last recorded step, so nothing is re-run. `stopOnFailure` decides
whether a failed workflow halts the sweep (default: continue).

## Tests
`go test -race ./...`; DB integration test:
`MICROFLOW_TEST_DATABASE_URL=postgres://... go test ./internal/store -run Tenancy`;
frontend: `node test/frontend/services_smoketest.js` (needs `npm i jsdom`).
