# MicroFlow backend — STATUS (read this first)

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
