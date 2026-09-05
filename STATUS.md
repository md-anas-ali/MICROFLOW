# MicroFlow — STATUS (read this first)

## Latest pass: real execution report showed a run cancelled after
## 30 minutes having done almost nothing — root cause was a runner
## bug, not the workflow

The user ran the previous build against their real workflow and sent
back the execution's debug-report JSON: `executionStatus: CANCELLED`,
`mainError: "context deadline exceeded"`, only 12 of 117 nodes
executed, `totalDuration: 30m3s`. All 12 nodes that did run succeeded
with no errors, which was the tell that this wasn't a workflow bug.

Diagnosis from the report's own timestamps: the execution record's
`startTime` was 09:49:10, but its first node didn't start until
10:17:19 — a 28-minute gap with zero recorded node activity, followed
by only ~2 minutes of real node execution before the 30-minute cutoff
fired mid-run.

**Root cause (`internal/runner/async.go` + `runner.go`):** both the
async execute path (`Manager.Start`/`runJob`) and the sync
scheduler/webhook path (`RunFromNode`) created the run's
`context.WithTimeout(..., r.Timeout)` (30 min, hardcoded) *before*
waiting to acquire a worker slot from the concurrency semaphore, then
used that same context for both the wait and the run. With this
deployment's low-RAM default of `MICROFLOW_MAX_CONCURRENT_EXECUTIONS=1`
(a single worker), an execution queued behind an already-running one
had its 30-minute budget ticking down the entire time it sat waiting
in line — so by the time it actually got a worker slot, almost nothing
was left, and it died a couple of minutes later with "context deadline
exceeded" even though the run itself was healthy and making normal
progress.

**Fixes:**

1. `Manager.Start`/`runJob` now use a plain cancellable
   `context.Context` (no deadline) for the semaphore wait; the
   `r.Timeout`-bounded run context is created only once a worker slot
   is actually acquired, derived from the same cancellable context so
   an explicit `Cancel()` still works at any point.
2. `RunFromNode` gets the identical fix for the scheduler/webhook path.
3. Added `Runner.WithTimeout(d time.Duration)` and wired
   `MICROFLOW_EXECUTION_TIMEOUT_MINUTES` (default raised **30 → 180**)
   in `cmd/server/main.go` — a 117-node pipeline with AI generation,
   TTS, and FFmpeg rendering can legitimately run well past 30 minutes
   even with no queueing involved, so the flat 30-minute default was
   already too aggressive on top of the queueing bug.
4. Regression test: `internal/runner/async_test.go`
   (`TestManagerQueueWaitDoesNotConsumeTimeout`) — occupies the sole
   worker slot directly, starts a queued execution behind it, holds
   the slot for 3x the configured timeout, then frees it and asserts
   the queued execution still completes successfully. Confirmed this
   test fails on the pre-fix code and passes on the fix (reverted the
   fix locally, watched it fail with the same symptom — status
   `cancelled` instead of `success` — then restored it).

**Verified this pass:** `go build ./...`, `go vet ./...`, `gofmt -l .`,
and `go test ./... -race` all clean; `test/frontend/smoketest.js`
34/34 passing; `cmd/e2echeck` against the user's own workflow JSON —
all 123 nodes parse, all connections resolve, full run completes with
no errors.

## Previous pass: re-imported "My workflow 14" from the live n8n canvas had
## drifted from the previously-fixed reference copy, plus a real
## MicroFlow engine bug that silently truncated every multi-item loop
## to one iteration

The user re-exported their workflow from n8n after further edits on the
live canvas (removing Gemini as a fallback model tier, among other
changes). That re-export had reverted several nodes to pre-fix state
and exposed one new engine-level bug and one new n8n-compatibility gap.
Found and fixed by actually running the workflow end-to-end via
`cmd/e2echeck`, not by inspection alone — five fix→retest cycles, each
one driven by a real failure the previous fix's retest surfaced:

1. **Workflow bug — every "live free-model discovery" controller was
   silently always empty.** All five controllers (Model / Fact /
   Semantic / Image Prompt / QC Model Controller) read
   `$node('Fetch OpenRouter Models...').json.json` — that extra `.json`
   doesn't exist in n8n's real (non-fullResponse) HTTP node output
   shape, so the inner value was always `undefined`, the live model
   list was always empty, and (for the four controllers with a Gemini
   fallback) every AI call silently ran on Gemini only, never actually
   using OpenRouter's live free list despite the code's own comments
   saying that's the point. Fixed all five to read `$node(...).json`
   directly.
2. **MicroFlow bug — Model Controller (no Gemini fallback, by the
   user's own design) crashed the whole run** when its model queue
   came up empty (`TypeError: Cannot read property 'model' of
   undefined`), instead of failing into its own existing retry loop.
   Added a safe empty-queue fallback so it degrades to a normal retry
   instead of an unhandled exception.
3. **Workflow bug — Get Image / Fallback Image (turbo) / Fallback Image
   2 / Image Retry Router had reverted to the pre-fix version**
   (missing `fullResponse` option, `$json` instead of
   `$node["Prep Scene"]` on retries) that a previous pass already
   diagnosed and fixed (see the "two real workflow bugs" entry below).
   Re-applied that fix.
4. **MicroFlow engine bug — `SplitInBatchesExecutor` ("Loop One By One"
   / n8n's classic Split-In-Batches loop) recomputed its item list from
   the CURRENT call's `input` on every invocation.** That's correct on
   the very first call, but every later call happens via the loop
   body's own output looping back into the same node — so `input` on
   those calls is only the single batch that just went around, not the
   original N items. With batchSize=1 (the default) this made the loop
   always report "finished" after exactly ONE iteration regardless of
   how many items it started with, and even then emitted an empty
   "done" output instead of the accumulated set — so a 5-scene video
   workflow silently only ever processed scene 1 and never reached
   final assembly/upload, with no error anywhere. This is a generic
   engine bug, not specific to this workflow — it would truncate every
   imported workflow's SplitInBatches loop the same way. Fixed to
   snapshot the real item list once into workflow static data on the
   first call and resume from that snapshot on every later call,
   verified to survive a real JSON marshal/unmarshal round-trip (not
   just the in-memory test double) since the Postgres-backed store
   does exactly that on every `WithLock`. Regression test:
   `internal/nodes/control_test.go` (`TestSplitInBatchesLoopsOverAllItems`).
5. **`require('fs')` used in two Code nodes** ("Build QC Vision
   Request", "Execution Log") to base64-encode local frame files /
   read a small timestamp file. MicroFlow's Code executor deliberately
   has no `require`/filesystem access (a real security boundary, not
   an oversight — see the security note atop `internal/nodes/code.go`).
   Rather than weaken that, added two narrow, allowlisted primitives:
   `$readFileBase64(path)` and `$readFileText(path)`, both scoped to
   the run's own scratch dir + the OS temp dir (same allowlist
   `ReadWriteFileExecutor` already uses) with a 5MB size cap, and
   updated both nodes to use them instead of `require('fs')`.
6. **MicroFlow bug — the YouTube video-upload executor returned
   `{"videoId": ...}`, but every downstream reference (and the real
   YouTube Data API v3 `videos.insert` response itself) uses `.id`.**
   This didn't throw — the expression just silently resolved to
   `undefined`/`<nil>`, which then got baked into the thumbnail-set
   URL, the pin-comment reminder link, and the execution log with no
   error anywhere. This is a generic MicroFlow bug that would break the
   same way for any real n8n-exported workflow with a YouTube upload
   step. Fixed to emit `id` (matching real n8n) alongside the old
   `videoId` key for backward compatibility.
7. **`cmd/e2echeck`'s own harness had a 90-second wall-clock cap**,
   which is fine for a smoke test but too short for a real end-to-end
   run of a workflow that deliberately paces itself with production
   rate-limit waits (~85s of real `Wait`/cooldown sleeps across a
   5-scene run) plus genuine FFmpeg render time. The run was being cut
   off mid-`Check Final Video` and misreported as a per-command FFmpeg
   timeout, when the actual cause was the harness's own outer deadline.
   Raised to 10 minutes (`MaxSteps=400` already bounds a genuine
   infinite-retry-loop bug independently of wall-clock time, so this
   doesn't weaken that protection).

**Verified this pass:**

```
go build ./...                          # clean
go vet ./...                            # clean
gofmt -l . (excl. third_party/)         # clean
go test ./... -race                     # all packages pass, incl. new
                                         #   TestSplitInBatchesLoopsOverAllItems
go run ./cmd/e2echeck test/testdata/sample_workflow.json
  # before fixes: crashed at node 14 ("Model Controller",
  #   TypeError: Cannot read property 'model' of undefined)
  # after fixes 1-3: 61/61 nodes success (one scene) -- matched the
  #   previously-verified checkpoint, but that turned out to be masking
  #   bug #4 below (only one scene ever ran, not five)
  # after fix #4 (SplitInBatches): 116/116 nodes success through QC
  #   frame extraction, then hit bug #5 (require('fs'))
  # after fix #5: 114/115 nodes, "Check Final Video" misreported as a
  #   5-minute FFmpeg timeout -- actually harness bug #7
  # after fix #7 (10-minute cap): 138/138 nodes success, full 5-scene
  #   run, real FFmpeg concat/color-grade/subtitle render (~18s),
  #   QC vision check, thumbnail, YouTube upload, comment, thumbnail-set
  #   -- but with videoId=<nil>/undefined baked into 3 places (bug #6)
  # after fix #6: 138/138 nodes success, videoId resolves correctly
  #   everywhere (thumbnail URL, comment body, execution log,
  #   pin-comment reminder link). Full run ~88s wall clock.
```

`test/testdata/sample_workflow.json` has been updated to this pass's
corrected workflow (all 6 fixes applied) so `cmd/e2echeck` exercises the
fixed path by default going forward.

**RAM / concurrency / scratch-dir requirements (re-verified, no changes
needed — already correct from a previous pass):** `MICROFLOW_SCRATCH_DIR`
defaults to `/tmp/microflow` (real disk, not an in-RAM path);
`ReadWriteFileExecutor` and the HTTP executor's `fullResponse` path both
spool binary data to disk rather than buffering it in the Go heap;
`MICROFLOW_MAX_CONCURRENT_EXECUTIONS` defaults to 1 (one full workflow
run at a time) and `Engine.MaxConcurrentHeavy` defaults to 2 (bounds
simultaneous FFmpeg/TTS/HTTP calls within a run); `MemGuard` throttles
new heavy work above a configurable heap ceiling (`MICROFLOW_HEAP_CEILING_MB`,
default 220MB, well under the 512MB container total).

**Note on `cmd/e2echeck` as "the lightweight non-AI compatibility
checker":** its own binary is ~13MB (`go build -ldflags="-s -w"`), not
1-3MB, because it statically links a real JS engine (goja) to actually
syntax-check and execute every Code node's script, not just pattern-match
node types. That's a deliberate trade-off — a true sub-3MB tool would
have to drop either the real JS syntax check or the real engine
execution, at which point it stops catching bugs like #1-#6 above and
becomes a much weaker linter. It's a dev-time/audit-time CLI, though —
it's never deployed to Render and never runs inside the 512MB service
process, so its binary size doesn't compete with the runtime RAM budget
at all.

---

## Previous fix: editor crashed on load (`executionId is not defined`)

`renderEditor(id, executionId)` took `executionId` as a parameter but
handed off rendering to a separate top-level function, `paintEditor()`,
which takes no arguments — yet `paintEditor`'s body referenced the bare
name `executionId` (to resume live-following an in-progress execution
when the canvas is opened via `?execution=<id>`). Under `"use strict"`
that's a `ReferenceError`, not a silent `undefined`, so **every** call
to `route()` that reached the editor (`#/workflows/<id>`, with or
without `?execution=`) threw immediately inside `route()`'s try/catch
and rendered the "Something went wrong — executionId is not defined"
empty-state instead of the canvas. The editor could never open at all.

This is exactly what `test/frontend/smoketest.js` caught (it wasn't
being run before this pass, or this would have been caught immediately
— `go build`/`vet`/`test` all stay green regardless of frontend JS
errors, they don't touch `app.js`).

Fixed by storing the execution id on `editorState` in `renderEditor`
(`editorState.executionId = executionId || ""`) instead of relying on
closure, and reading `editorState.executionId` inside `paintEditor`.

**Verified this pass:**
```
go build ./...                          # clean
go vet ./...                             # clean
gofmt -l . (excl. third_party/)          # clean
go test ./... -race                      # all packages pass
cd test/frontend && npm install && node smoketest.js
  # before fix: crashed after 3 assertions ("editor rendered a canvas"
  #   FAIL, "both nodes rendered" FAIL, then a hard crash trying to
  #   dispatchEvent on a node box that was never rendered)
  # after fix: 26/26 assertions pass
```

## This pass: fixed a real MicroFlow bug + two real workflow bugs, found by
## actually running "My workflow 14" end-to-end via cmd/e2echeck

Found while checking the workflow's script-writer "auto free model finder"
loop and its image pipeline against the real engine, not by inspection:

1. **MicroFlow bug — `internal/nodes/control.go` (`IfExecutor`/`fmtEq`)**:
   an IF node's numeric `$json` field (decodes from JSON as `float64`, e.g.
   `$json.statusCode`) compared against a string literal condition (e.g.
   `"200"`) could **never** be equal, regardless of the actual value.
   `toFloat` only accepted `float64`/`int`, so the numeric fast path bailed
   out to `toStr(a) == toStr(b)`, and the old `toStr` returned `""` for any
   non-string value. This is an extremely common n8n IF-node shape ("did
   this HTTP call return 200?"), so it silently broke that pattern for
   every imported workflow, not just this one. Fixed `toFloat` to parse
   numeric strings and `toStr` to format numbers/bools instead of
   returning `""`; regression test added:
   `internal/nodes/control_test.go`.
2. **Workflow bug — image generation could never succeed.** "Get Image"
   and both fallback image nodes never had n8n's "Full Response" option
   set, so `$json.statusCode` was never populated at all — combined with
   bug #1, "Image Generated?" was unsatisfiable even before today, but
   would have stayed broken after fixing #1 alone. Added
   `options.response.response.fullResponse: true` to all three nodes.
3. **Workflow bug — retries used stale/undefined data.** The pollinations
   URL on those same three nodes read `$json.imagePromptEncoded` /
   `$json.imageSeed`, and "Image Retry Router" read `$json.idx` — but by
   the time any of these run, `$json` is the *previous* HTTP node's
   response (n8n replaces `$json` with the response body), so those
   fields were `undefined` on every retry, producing broken
   `prompt/<nil>` URLs, and different scenes' retry counters collided on
   the shared key `wf.imageRetries['undefined']`. Switched all four
   references to `$node["Prep Scene"].json...`, matching how "Save Image"
   already (correctly) does it.
4. **Workflow bug — the script writer's "auto free model finder" never
   actually re-checked what's free.** `$getWorkflowStaticData('global')`
   persists *across* separate executions (e.g. each day's schedule
   trigger), but nothing reset `wf.liveModelQueue`/`modelAttempt`/
   `fullCycles` (or the Fact/Semantic/Image-Prompt/QC controllers'
   equivalents) at the start of a run — so a queue built on day 1 just
   kept getting reused on day 2, day 3, etc. until an entire lap failed
   inside a single run, defeating the "live" part of live free-model
   discovery. Added a reset block to "Mark Run Started" (already the
   first node of every run).
5. **Workflow bug — the same script-writer loop didn't advance properly
   through its model queue.** "Validate Attempt" reset `modelAttempt` to
   `0` on a merely shallow-valid API response (before "Parse JSON" had
   even checked whether the content was usable), so a later
   schema-validation failure only ever advanced the counter to `1`
   instead of past whichever model had just actually failed — the loop
   kept circling near the front of the queue instead of working through
   all the free models. Removed the premature reset so "Parse JSON"'s
   existing increment advances correctly.

**Verified this pass:**

```
go build ./...                          # clean
go vet ./...                            # clean
gofmt -l . (excl. third_party/)         # clean
go test ./...                           # all packages pass
go test ./... -race                     # clean
node test/frontend/smoketest.js         # 26/26 assertions pass
go run ./cmd/e2echeck test/testdata/sample_workflow.json
  # before fix #1: "Get Image"/fallbacks/Image Retry Router all broken,
  #   run finished with 5 node errors, image never saved for any scene
  # after all 5 fixes: 61/61 nodes success, run finishes in ~38s
  #   (was hitting the 90s wall-clock cap before), image saved on the
  #   very first attempt
```

`test/testdata/sample_workflow.json` has been updated to the corrected
workflow (all 5 fixes applied) so `cmd/e2echeck` now exercises the fixed
path by default.

---

## Previous pass: async execution + live SSE progress (spec sections M/N)

`POST /api/workflows/{id}/execute` used to block for the entire run
(one HTTP request open the whole time, no way to cancel, no live
progress). It now returns `202 {executionId, status:"queued"}`
immediately, and the run proceeds on a bounded worker pool in the
background.

**New/changed:**

- `internal/runner/async.go` (new): `Manager` — bounded concurrency
  (`MICROFLOW_MAX_CONCURRENT_EXECUTIONS`, default 1) and a bounded
  accepted-but-unfinished count (`MICROFLOW_MAX_QUEUED_EXECUTIONS`,
  default 5) that rejects further work with `ErrQueueFull` once full,
  rather than queuing without bound. `Runner.RunFromNode`'s internals
  were split into a shared `runOnce` so Manager and the existing
  synchronous scheduler/webhook paths use the identical
  scratch-dir/engine/persist/cleanup logic — no duplicated run logic.
- `internal/engine/engine.go`: added `RunContext.OnNodeRun`, fired
  synchronously right after every `appendNodeRun` — the hook live
  progress is built on. Also fixed a real bug found while wiring this
  up: a node whose `Execute` failed because the *run's own context*
  was cancelled/timed out was being reported as an ordinary
  `StatusError`, not `StatusCancelled` — fixed so cancellation reports
  the correct terminal state (and skips continueOnFail/onError, which
  are for business-logic failures, not "the whole run stopped").
- Bounded, non-blocking per-execution event broadcaster (drop-oldest
  for a slow subscriber, small replay buffer for reconnects) feeding
  `GET /api/executions/{id}/events` (SSE). Payloads are metadata only
  (`NodeRunResult`, never raw media).
- New endpoints: `GET /api/executions/{id}` (Manager's in-memory
  record, falling back to a new `Store.GetExecution` once evicted from
  memory after 10 minutes), `POST /api/executions/{id}/cancel`.
- `internal/api/server.go`: `Server.WithAsync(mgr, execLoader)` opts a
  server into the new behavior; a server built with plain `New()`
  (every existing test) keeps the old synchronous `/execute` response
  unchanged — `execute_test.go` passes untouched.
- `cmd/server/static/app.js`: `executeCurrentWorkflow` now POSTs, gets
  `executionId` back, and follows the run via `EventSource` instead of
  one long-lived fetch; the exec panel updates live per node instead of
  once at the end.

**Verified this pass** (this sandbox does have a working Go toolchain
after all — `apt-get install golang-go` plus `GOPROXY=direct
GOSUMDB=off` to fetch modules straight from `github.com`, bypassing the
blocked `proxy.golang.org` — contradicts what earlier passes of this
file said; that was accurate for those sessions, not this constraint):

```
go build ./...                          # clean
go vet ./...                            # clean
gofmt -l .                              # clean (excluding third_party/)
go test ./...                           # all packages pass
go test ./... -race                     # clean, repeated 5x on internal/runner+internal/api
```

New test coverage: `internal/runner/async_test.go` (7 tests — start
returns before completion, queued→running→success transition, queue-
full backpressure, cancel-while-queued, SSE subscribe delivers a
terminal event then closes, unknown-execution handling) and
`internal/api/async_execute_test.go` (6 tests — 202 response shape,
GET reflecting live/finished state, cancel, cancel-unknown-id, a real
SSE wire-format test that parses `data:` lines out of the raw response
body, sync-fallback-unaffected). Frontend: `test/frontend/smoketest.js`
extended with a fake `EventSource` and 6 new assertions covering the
execute button → POST → SSE → terminal-state → panel/button-reset
flow; 26/26 assertions pass.

**Not done in this pass** (still open from the original spec):
disk-backed execution-scoped scratch storage audit (section D — the
scratch dir itself exists per-execution already; the "never share
global media paths" sweep hasn't been done), the compatibility/
conformance validator and harness (sections U/V), actual 512MB
constrained-container memory testing (section R — no container/cgroup
tooling attempted in this sandbox), and cancellation propagation *into*
already-running `executeCommand` (FFmpeg/TTS) child processes wasn't
specifically re-verified this pass — `Cancel()` reliably stops the
engine loop and any node executor that itself watches `ctx.Done()`
(confirmed by test), but whether every node executor actually does
watch it predates this pass and wasn't re-audited here.

---

## Previous pass: central Google credential + real workflow editor

Two features requested on top of the previous draft, both now implemented,
built, vetted, tested, and (for the frontend) smoke-tested:

### 1. Central Google credential ("log in once")

Previously the only way to give a googleSheets/youTube/gmail node

credentials was per-node, per-workflow (`cmd/setcred` or the editor's
side-panel "Google Credentials" section). That still works unchanged,
but now there's also a single central account:

- `internal/vault/central.go` (new): `AccountVault` (encrypts with the
  *same* AEAD/master key as the existing per-node `Vault` — one
  `MICROFLOW_MASTER_KEY` covers both), `AccountResolver` (refreshes the
  central account's OAuth token exactly like per-node credentials do),
  and `CentralFallbackResolver`, which implements
  `engine.CredentialResolver` and is what actually gets wired into the
  engine: it tries a per-node override first, then falls back to the
  central account. **`internal/nodes/google.go` did not change at all**
  — the fallback happens entirely by swapping which resolver
  `cmd/server/main.go` hands to the runner.
- `internal/vault/oauth.go` was refactored (not behaviorally changed)
  to extract a shared `refreshEngine` so per-node and central resolvers
  share one implementation of the refresh/retry/mutex logic instead of
  two copies.
- New table `google_account_credentials` (`internal/store/schema.sql`),
  deliberately with no FK to `workflows` — it's account-scoped, not
  workflow-scoped, and must survive a workflow being deleted. New
  `internal/store/postgres.go` methods implement `vault.AccountStore`.
- New endpoints (`internal/api/server.go`): `GET/POST/DELETE
  /api/credentials/google` — status/save/clear, never returns secret
  bytes. Covered by new tests in `internal/api/credentials_test.go`
  (`TestCentralCredential*`), all passing.
- Frontend: a new "Google Credentials" page (`#/credentials`, linked
  from the sidebar) with masked Client Secret/Refresh Token fields, a
  configured/not-configured status badge, Save/Update and Clear. The
  old per-node section in a node's side panel is kept for backward
  compatibility, relabeled "(override)" with a note pointing at the
  central page — most workflows will never need it.

### 2. Real workflow editor

The canvas previously only supported click-to-select + a raw JSON
textarea for parameters. It now supports everything the person asked
for, all in `cmd/server/static/app.js` (still plain JS, no build step,
no new dependencies):

- **Drag to move**: mousedown-drag on a node box updates its position
  live (direct DOM style + a targeted redraw of just that node's
  connection lines, not a full canvas re-render, so it stays smooth);
  a full `drawCanvas()` runs once on mouseup to resync the canvas
  bounding box.
- **Add node**: a type dropdown + "+ Add node" button in the toolbar,
  covering every `model.NodeType` the Go engine can execute. New nodes
  get a generated id and empty `originalType` — `internal/parser/
  export.go`'s existing `reverseTypeMap` fallback already handles that
  correctly on export, so no parser change was needed.
- **Connect**: drag from a node's right-edge output dot to another
  node's left-edge input dot (visible connector handles on every node,
  temp dashed line follows the cursor while dragging). Duplicate
  connections are rejected with a toast.
- **Delete node**: a "Delete node" button in the side panel, or
  Delete/Backspace when a node is selected (guarded against firing
  while typing in a text field). Removes the node's own outgoing
  connections and any incoming connections pointing at it.
- **Delete connection**: click a connection line (a wide invisible hit
  path makes thin lines easy to click) → confirm → removed.
- **Position/workflow save + reload persistence**: unchanged mechanism
  — all of the above only mutates `editorState.workflow` in memory
  (same as the existing "Apply to workflow" pattern for params); the
  existing Save button POSTs the whole workflow to `.../save` exactly
  as before, and GET on reload returns exactly what was last saved.
  No backend changes were needed for this part at all.

**Verified this pass:**

```
go build ./...        # clean
go vet ./...           # clean
gofmt -l .              # clean (excluding vendored third_party/)
go test ./... -race    # all packages pass, race detector clean
```

Plus a real (not just syntax-checked) frontend smoke test:
`test/frontend/smoketest.js` loads the actual `index.html`/`app.js`
into jsdom, mocks the fetch calls to the real API endpoints, and drives
the DOM through: select → add node → delete node → drag node → delete
connection → connect via drag → save → reload in a fresh DOM instance
(proving persistence, not just in-memory state) → central credentials
page save. Run it yourself with:

```
cd test/frontend && npm install && node smoketest.js
```
All 18 assertions pass. This is not a substitute for opening a real
browser, but it does exercise the actual shipped JS against a DOM and
would catch wiring bugs (bad selectors, undefined ids, exceptions in
handlers) that a syntax check alone would miss.

**Still not built** (unchanged from before this pass — see "what's
still explicitly a gap" below): the initial OAuth consent flow (getting
the *first* Client ID/Secret/Refresh Token — the central credentials
page and per-node section both assume you already have these three
values from Google Cloud Console, same as before). `cmd/setcred` was
left untouched; it still only writes per-node credentials, not the
central one — use the frontend's Google Credentials page for that, or
call `POST /api/credentials/google` directly.

---

# Original backend — STATUS (below is from the previous pass)

## Environment reality (changed this pass — now actually verified)

Earlier drafts of this file said the sandbox had no Go toolchain and no
network access, so nothing here had ever been compiled. That was true at
the time, but it's no longer the whole picture: `apt-get install
golang-go` works in this sandbox, and while `proxy.golang.org` and
`gopkg.in` are blocked by the network allowlist, `github.com` and
`codeload.github.com` are not. That's enough to actually build this
module:

```
go build ./...        # clean
go vet ./...           # clean
go test ./... -race    # all packages pass, race detector clean
gofmt -l .              # clean (was not, before this pass — see below)
```

**One real bug was found and fixed doing this**: `go.mod` had four
`replace` directives pointing at `/home/claude/work/vendor/...` — an
absolute path from a *different* sandbox session that was never included
in this archive. That's not a style nit; it made `go mod tidy`/`go
build` fail outright with "no such file or directory" the moment anyone
who wasn't the original session tried to build this. Fixed by vendoring
the four affected modules (`gopkg.in/yaml.v2`, `yaml.v3`, `check.v1`,
`errgo.v2` — transitive test-only deps of `goja`, not something the
workflow engine itself uses) into `backend/third_party/` as real,
version-pinned upstream source fetched from their GitHub mirrors, and
pointing the replace directives at that relative, portable path instead.
This only matters in network-restricted environments like this one — on
a normal machine `go mod tidy` would resolve the same modules from the
real proxy without needing `third_party/` at all — but it's harmless
there and makes the build reproducible here too.

Also fixed: five source files (`model/workflow.go`, `nodes/control.go`,
`parser/export.go`, `parser/n8n.go`, `webhook/webhook.go`) had struct/const
blocks whose alignment had drifted out of `gofmt` compliance — cosmetic,
but `gofmt -l` now reports clean across the whole module (excluding the
vendored `third_party/` trees, which are left as upstream ships them).

No other bugs turned up — no failing tests, no vet warnings, no stray
TODOs/stubs in the actual engine code beyond the ones already listed
below as intentional gaps.

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
   execution-history list and no live/streaming execution progress.
   Credential management now exists in a minimal form: selecting a
   googleSheets/youTube/gmail node in the editor's side panel shows a
   "Google Credentials" section (Client ID/Client Secret/Refresh Token
   + Save Account) that calls the new `POST/GET
   /api/workflows/{id}/credentials` endpoints -- see
   `cmd/server/static/app.js`'s "google credentials" section and
   `internal/api/server.go`'s `handleSaveCredential`/
   `handleListCredentials`. There's still no bulk/cross-workflow
   account manager -- one node at a time, same scope as `cmd/setcred`.
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

The build/vet/test/gofmt sequence above has now actually been run and is
clean. What's still unverified is everything that needs real
infrastructure this sandbox doesn't have:

```
# On your machine, with real Postgres + real credentials:
go build ./...
go test ./...
# then: your own section-25 checklist against real Postgres, real
# YouTube/AI/TTS/Gmail/Sheets credentials, and the 1/10/50/100-run
# RAM leak test (MemGuard's 220MB default is still just a starting point).
```

Note: this archive (`microflow_backend_optimized_tar.gz`) is backend-only.
STATUS.md references `frontend/STATUS.md` and frontend UI changes that
aren't part of this tarball — that's expected if the frontend was
delivered separately, just flagging it in case it wasn't.

## Added this pass: `internal/edgetts` + `cmd/edgetts` (pure-Go TTS)

A workflow's "TTS (Edge->Silent)" executeCommand step shells out to a
logical `edge-tts` binary. That binary was never part of this repo --
main.go just expects an external, separately-installed `edge-tts` (the
Python package, which pulls in `aiohttp` + 7 more packages) at
`MICROFLOW_EDGE_TTS_PATH`.

`internal/edgetts` is a pure-Go, zero-new-dependency reimplementation
of the same unofficial Microsoft Edge "read aloud" protocol that Python
`edge-tts` wraps (hand-rolled RFC6455 WebSocket client in
`internal/edgetts/ws.go`, since adding gorilla/websocket or
golang.org/x/net/websocket would be exactly the kind of new dependency
this pass was asked to avoid). `cmd/edgetts` wraps it in a CLI that
accepts the same flags MicroFlow's workflow shell script actually
passes (`--rate`, `--voice`, `--file`, `--write-media`), so it's a
drop-in replacement: point `MICROFLOW_EDGE_TTS_PATH` at the built
`edgetts` binary (or just install it *as* `/usr/local/bin/edge-tts`,
the existing default, and change nothing else) and no python3/pip/
aiohttp stack is needed in the deployment image just for TTS. Compiles
to a single ~4MB stripped binary.

**UNVERIFIED against the real endpoint** -- `speech.platform.bing.com`
is outside this sandbox's network allowlist, so `edgetts.Synthesize`
has never actually been round-tripped against Microsoft's live service,
only unit-tested against fakes. What IS verified (`go test
./internal/edgetts/...`, all passing):

- `wsAcceptKey` against RFC 6455's own worked example (section 1.3) --
  not a Microsoft-specific vector, an authoritative one.
- Frame write/read round-trip over an in-memory `net.Pipe()` across the
  7-bit/16-bit/64-bit length encodings (0, 5, 125, 126, 500, 70000
  byte payloads), confirming client-side masking and length framing are
  self-consistent.
- `collectAudio`'s binary-frame header-length parsing and multi-frame
  audio concatenation, and that it stops exactly at `Path:turn.end`,
  against a fake in-process server sending the same message shapes the
  real protocol is documented (via edge-tts's own reverse engineering)
  to use.
- `collectAudio` treats "turn ended with zero audio bytes" as an error
  rather than silently returning an empty MP3.
- `secMSGEC` (the anti-abuse token Microsoft added in 2024) produces
  the right shape (64 uppercase hex chars) -- correctness of the
  Windows-epoch/5-minute-rounding math itself is NOT independently
  verified against a known-good value, since there's no public test
  vector for it the way there is for the WebSocket handshake.

None of that reaches the actual `wss://speech.platform.bing.com`
handshake, SSML acceptance, or whether Microsoft's anti-abuse checks
(the Sec-MS-GEC token, User-Agent/Origin sniffing) still match what's
implemented here -- that protocol is unofficial and Microsoft has
changed it before. **Test this against the real endpoint on a machine
with normal internet access before relying on it.** If it's wrong or
breaks later, the failure mode is safe: the workflow's own
"TTS (Edge->Silent)" step already treats a failing/empty edge-tts run
as expected and falls back to generating a silent clip with `ffmpeg`
(see its `EDGE_FAILED_SILENT` branch) -- so a protocol mismatch here
degrades the pipeline to silent voiceover, it doesn't break it.

## Live execution monitor / history (2026-09-02)

- Added `GET /api/executions` for a bounded execution list plus live queue/worker stats.
- Added global `GET /api/executions/events` SSE stream for near-instant monitor refreshes.
- Added an **Executions** UI page with live/running/queued status, queue occupancy, cancellation, per-node details, and links back to the workflow canvas.
- The workflow canvas can now reopen a specific execution with `?execution=<id>` and continue the per-node SSE stream while it is running.
- Added durable execution-history cleanup: terminal executions older than **12 hours** are deleted once at startup and automatically every 12 hours; queued/running executions are never deleted by the cleanup job.
- Added a database index on `executions.finished_at` for efficient retention cleanup.
- Added unit/API coverage for global execution events and live queue statistics.
## Live execution monitor follow-up fix

- Startup now marks queued/running/waiting executions left by a previous server process as cancelled, because the async Manager is intentionally in-memory and cannot resume them after restart.
- Cancel is idempotent for executions that finished between UI refresh and the cancel click; the API returns their terminal status instead of a misleading `execution not found`.
- The UI only offers `Open live canvas` when the execution's workflow ID still exists, preventing stale/deleted workflow IDs from opening a guaranteed `no rows in result set` page.
