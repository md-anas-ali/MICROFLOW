# MicroFlow backend — STATUS (read this first)

## Environment reality (unchanged from the first draft)

Still true: this sandbox has **no Go toolchain and no network access**
(re-verified before this pass: `go: not found`, `curl`/`apt-get` → 403
`host_not_allowed`). Nothing in `backend/` has been compiled, run, or
tested here. Treat this as a thoroughly-written but **uncompiled**
second draft, not a verified one. What changed since the first draft is
scope, not verification status.

Since I can't run `go build`, I did a manual cross-file review instead
(grep for stale references after each rewrite, checked every changed
function signature against its callers by hand). That catches obvious
mismatches; it does not catch everything a real compiler would.

## What closed since the first draft

1. **OAuth token refresh** (`internal/vault/oauth.go`, new). Wraps the
   vault: on `Resolve`, if a stored credential has a `refreshToken` and
   its `expiresAt` is within 2 minutes of now (or unset), it calls
   Google's token endpoint directly (stdlib `net/http`, no
   `golang.org/x/oauth2` dependency) and persists the refreshed token
   before handing it to the node executor. Per-credential mutex prevents
   two concurrent node runs from double-refreshing. `cmd/server/main.go`
   now wires this in (`vault.NewOAuthResolver(baseVault)`) instead of
   passing the bare vault to node executors.

   **Still out of scope**: the *initial* OAuth consent flow (getting the
   first `refreshToken`/`clientId`/`clientSecret` into the vault at all)
   isn't built — that's normally a one-time browser-redirect flow an
   operator runs once per credential, same as setting up any n8n
   credential. You'll need to obtain those three values yourself and
   `vault.Put` them once, or write that setup flow if you want it
   self-service.

2. **Scheduler → engine wiring** (`cmd/server/main.go`). No longer
   logs-and-stops: at startup, every saved workflow is scanned for
   Schedule Trigger nodes, each becomes a `scheduler.Schedule` (reading
   n8n's actual `parameters.rule.interval[]` shape, or flatter
   `cronExpression`/`intervalSeconds` params), and the scheduler's fire
   callback now calls the same `runner.RunFromNode` the manual-execute
   API endpoint uses. A Schedule Trigger node MicroFlow can't parse into
   a cron/interval gets a startup warning naming the node, rather than
   silently never firing.

3. **Webhook → engine wiring** (`cmd/server/main.go`). Same pattern:
   every Webhook Trigger node across every saved workflow is registered
   with the webhook server at startup (path from the node's `path`
   parameter, or a generated `/webhook/<workflowId>/<nodeName>`
   fallback), and a fired webhook calls `runner.RunFromNode` with
   headers/query/body as the seed item.

4. **`internal/runner`** (new package). Both wiring fixes above needed
   the same "load workflow, build a RunContext, call engine.Run, persist
   the execution, clean up the scratch dir" logic that used to live only
   in `internal/api/server.go`'s manual-execute handler. That logic
   moved into `internal/runner.Runner.RunFromNode`, and `api.Server` now
   calls it too — manual/schedule/webhook triggers behave identically
   instead of the API path being special-cased.

5. **`$getWorkflowStaticData('global')` inside Code nodes**
   (`internal/nodes/code.go`). Rule 13 requires this and the first draft
   didn't implement it at all (only `SplitInBatches`'s internal loop
   state used static data). Now: if a Code node's source contains the
   string `getWorkflowStaticData` (cheap static check, not a full JS
   parse), the entire node execution — including every per-item run
   under "Run Once for Each Item" — happens inside one
   `StaticDataStore.WithLock` call, and the Go map handed to
   `$getWorkflowStaticData(scope)` is the same map goja wraps for JS
   property read/write, so mutations the script makes are visible on the
   Go side with no explicit export step. This is what the workflow's
   dedup-check and "Fallback 1/2 Guard" Code nodes need to actually work
   correctly under concurrent/overlapping executions — the first draft's
   STATUS.md incorrectly implied this was already handled since it's
   "just a Code node"; it wasn't, because the Code node had no
   static-data binding at all until this pass.

6. **`ListWorkflows`** added to `internal/store` (needed by both the
   scheduler and webhook startup registration passes) and to the
   `api.WorkflowStore` interface, plus a `GET /api/workflows` endpoint.

## What's implemented with real logic (full table, updated)

| Layer | File(s) | Status |
|---|---|---|
| n8n → MicroFlow parser/compat layer | `internal/parser/n8n.go` | Real, unchanged from draft 1. |
| Expression engine | `internal/expr/expr.go` | Real, unchanged. |
| Execution engine | `internal/engine/engine.go` | Real, unchanged. |
| Code node (JS + static data) | `internal/nodes/code.go` | Real, **extended this pass** with `$getWorkflowStaticData`. |
| IF / Wait / SplitOut / SplitInBatches | `internal/nodes/control.go` | Real, unchanged. |
| HTTP Request | `internal/nodes/http.go` | Real, unchanged. |
| Execute Command | `internal/nodes/command.go` | Real, unchanged. |
| Read/Write File | `internal/nodes/file.go` | Real, unchanged. |
| Google Sheets / YouTube / Gmail | `internal/nodes/google.go` | Real REST calls, unchanged; now actually gets refreshed tokens (see #1) instead of one that would just go stale. |
| Credential vault | `internal/vault/vault.go`, **`oauth.go` (new)** | Real AES-256-GCM + real refresh logic. Never tested against a real DB or real Google credentials. |
| Postgres store | `internal/store/postgres.go` | Real SQL, **+ListWorkflows**. Never run against a live database. |
| Scheduler | `internal/scheduler/scheduler.go` | Real, unchanged logic; **now actually wired to the engine** (#2). |
| Webhook server | `internal/webhook/webhook.go` | Real, unchanged; **now actually wired to the engine** (#3). |
| Runner (new) | `internal/runner/runner.go` | Real; the shared execution path all three trigger types use. |
| API server | `internal/api/server.go` | Real; **rewritten** to delegate to the runner, +`GET /api/workflows`. |

## What's still explicitly a gap — do not treat as done

1. **Initial OAuth consent flow** (getting the first token into the
   vault) — see #1 above. Not built.
2. **Frontend**: canvas now supports real drag/connect/add/delete/
   duplicate/rename (see `frontend/STATUS.md`), but there's still no
   execution-history list, no live/streaming execution progress, and no
   credential-management UI.
3. **RAM/CPU numbers are still unmeasured** — nothing changed here
   since draft 1; `MemGuard`'s 220MB default is still a starting point,
   not a measurement. Do the 1/10/50/100-run leak test yourself.
4. **No adversarial security test suite has been run.** The guards
   (parameterized SQL, argv-based exec, SSRF host filtering,
   path-traversal checks, webhook rate limit/size limit) are real code,
   not stubs, but nobody has tried to break them yet.
5. **Webhook path matching** uses `http.ServeMux`'s exact string
   patterns — fine for normal paths, but a trigger node renamed to
   something with unusual characters could produce an awkward URL; not
   a correctness bug, just worth a glance.

## What closed since draft 2 (this pass)

7. **Workflow export** (`internal/parser/export.go`, new + wired into
   `internal/api/server.go` as `GET /api/workflows/{id}/export`). Reverse
   of `Parse`: MicroFlow model → n8n's JSON shape, so a workflow you've
   edited in the MicroFlow UI can be exported back to a file n8n itself
   can open. Deterministic node/connection ordering (sorted by name) so
   repeated exports of an unchanged workflow are byte-identical. New
   round-trip test (`export_test.go`): parses your actual workflow,
   exports it, re-parses the export, and checks node/connection counts
   and every node's `originalType` string survive unchanged.
8. **Retry from a failed node** — this was already possible via
   `POST /api/workflows/{id}/execute?startNode=X` (added in draft 2's
   runner rewrite), it just had no dedicated UI. See frontend changes
   below; no backend change was needed here, just exposing what already
   existed.

## What's implemented with real logic (table, current)

Unchanged from draft 2's table, plus:

| Layer | File(s) | Status |
|---|---|---|
| n8n export (reverse of parser) | `internal/parser/export.go` | Real. Round-trip tested (structurally, via the new test) — not tested against a real n8n instance re-importing the file. |

## What's still explicitly a gap — do not treat as done

Same list as draft 2 (initial OAuth consent flow, RAM/CPU measurement,
adversarial security testing, webhook path-matching edge case), plus:

6. Export was never opened in a real n8n instance to confirm it
   actually re-imports cleanly there — only MicroFlow's own parser was
   used to validate the round trip, which obviously can't catch a
   MicroFlow-specific blind spot shared between `Parse` and `Export`.


## Before you rely on this for anything real

Same steps as draft 1/2:

```
go mod tidy
go build ./...
go test ./internal/parser/... ./internal/expr/... ./internal/engine/...
# then: real Postgres + your own section-25 checklist
```

Nothing about that sequence changed.
