# MicroFlow — trimmed build for `workflow.json`

This is a stripped-down copy of MicroFlow kept to only what
`workflow.json` (the uploaded "My workflow 14") actually needs to run.

## What's inside

- `cmd/server` — the engine + HTTP API + embedded web UI.
- `cmd/edgetts` — a dependency-free Go replacement for the `edge-tts`
  CLI. Required: the workflow's "TTS (Edge->Silent)" node shells out to
  a bare `edge-tts` command, so this binary must be on `PATH` as
  `edge-tts` (see `Dockerfile`).
- `cmd/setcred` — one-shot CLI to provision Google OAuth credentials
  (client id/secret/refresh token) into the vault for every
  Gmail/Sheets/YouTube node in a saved workflow. This workflow uses
  all three, so you'll need this (or the "Connect with Google" button
  in the UI) before those nodes can run.
- `internal/*` — engine, node executors, parser, vault, scheduler,
  store, webhook, report. Only the node types this workflow uses are
  wired in `internal/nodes/registry.go`:
  code, if, wait, splitOut, splitInBatches, noOp, errorTrigger,
  scheduleTrigger, httpRequest, executeCommand, readWriteFile,
  googleSheets, youTube, gmail, stickyNote.
- `workflow.json` — the workflow to import.

## Removed from the original repo

- `cmd/e2echeck` — a standalone offline compatibility-checking tool,
  not needed to actually run a workflow.
- Every `*_test.go` file across the repo (including in `third_party/`)
  and the whole top-level `test/` folder (frontend smoketest +
  sample test workflow).
- `STATUS.md` — a development/testing verification log.

Dependencies in `go.mod`/`go.sum` and the vendored `third_party/`
packages were left untouched, since trimming them can't be verified
without a Go toolchain in this environment — pulling one out that
turns out to be needed transitively would silently break the build.

## Running it

```bash
export DATABASE_URL="postgres://user:pass@host/db?sslmode=require"
export MICROFLOW_MASTER_KEY="$(go run ./cmd/server genkey)"
go run ./cmd/server
```

Then import `workflow.json` via the UI (`POST /api/workflows/import`),
and provision Google credentials with `cmd/setcred` (see the comment
at the top of `cmd/setcred/main.go`) or the "Connect with Google"
button if `GOOGLE_OAUTH_CLIENT_ID`/`_SECRET`/`_REDIRECT_URL` are set.

For the container build (which also builds `cmd/edgetts` and installs
it as `edge-tts` on `PATH`, plus `ffmpeg`/`python3` which this
workflow's `executeCommand` nodes shell out to), see `Dockerfile`.

## Low-RAM tuning

This build is tuned as tightly as this workflow's own node graph
allows: `GOGC=15`, `GOMAXPROCS=1`, `GODEBUG=madvdontneed=1`, a 40MB Go
heap ceiling, one heavy step (FFmpeg/TTS/HTTP) and one full execution
in flight at a time, a single Postgres connection, and an Alpine
(musl) base image instead of Debian. All of it is env-overridable —
see the `ENV` block in `Dockerfile` and `cmd/server/main.go`'s
`configureGoRuntimeForLowRAM`. Full reasoning, the original measured
numbers this is built on, and which later knobs are turned past what
was actually measured (verify before trusting in production) are in
`LOWRAM.md`.
