# MicroFlow — OpenRouter fix + low-RAM pass

## 1. OpenRouter "User not found" fix

**Workflow JSON (`My_workflow_14_fixed.json`)** — applied identically to all
5 model-controller pairs (Model Controller, Fact, Image Prompt, QC, Semantic):

- Added a shared, run-scoped `wf.orBlacklist` (bounded to 100 entries).
- Each Validate node now classifies every failure as:
  - **permanent** (404, "user not found", "model/provider not found",
    "invalid model/provider", "no endpoints found", etc.) → the model is
    pushed onto `wf.orBlacklist` and dropped from the cached queue
    immediately; it is never selected again this run.
  - **transient** (429, 502/503/504, rate-limit, timeout, "overloaded") →
    never blacklisted; only skipped for the current lap, with a growing
    backoff (`5s × streak`, capped at 60s).
  - **other** (empty/truncated/non-JSON output) → unchanged prior
    behavior, skipped for the current lap only.
- The main "Model Controller" (the only one with no Gemini fallback) now
  throws a clear error and stops cleanly if every free OpenRouter model
  is blacklisted, instead of refetching the same dead list forever. The
  other four controllers always keep a Gemini entry, so they degrade
  gracefully without needing a hard stop.
- All 5 "Loop Wait" nodes now read `{{ $json.waitSeconds }}` instead of a
  hardcoded literal, so the computed backoff is actually used.
- Verified with a Node.js simulation harness: 404 → blacklist + skip
  forever; 429 → retried, never blacklisted; all-blacklisted → clean
  throw. See conversation for the harness output.

**MicroFlow engine (`internal/nodes/control.go`)** — the real reason the
wait delay was never synced: `WaitExecutor` only accepted a literal
`float64` "amount" and silently defaulted to 1s for any expression
string. Fixed to evaluate `{{ }}` expressions via `expr.EvalValue`
against the current item, falling back to 1s only if the expression is
missing/invalid. Literal-amount Wait nodes elsewhere in the workflow are
unaffected.

## 2. Low-RAM pass

**MicroFlow engine (`internal/nodes/http.go`, `registry.go`)** — added a
small, bounded (8 entries), short-TTL (20s, env-overridable via
`MICROFLOW_MODEL_LIST_CACHE_TTL_SECONDS`) GET-response cache, scoped
*only* to `openrouter.ai/api/v1/models` (an explicit allowlist, not a
general HTTP cache). All 5 "Fetch OpenRouter Models (...)" nodes hit
this exact URL, once per retry-loop iteration; previously each iteration
re-fetched and re-decoded the same ~100–300KB JSON body independently,
even though the JS-side queue built from it is already cached and only
rebuilt once per lap. This collapses that into one shared, short-lived,
read-only cache entry — never caches non-2xx responses, so a real
transient failure is still retried for real.

Everything else in the RAM budget (bounded `NodeRunCap` with a real copy
on trim so dropped entries actually become garbage, streamed binary
spooling to disk, shared `*http.Client`/`Transport`, `GOMEMLIMIT`/`GOGC`
tuning, `MaxConcurrentHeavy` serialization) was already in place and
needed no changes.

## Verification

```
gofmt -l .            → clean
go vet ./...          → clean
go test ./...         → ok (added control_test.go + http_test.go,
                          9 new tests, all passing, incl. -race)
go build ./...        → OK
go build -race ./...  → OK
go build ./cmd/server → OK (binary builds)
```

## Files in this delivery
- `My_workflow_14_fixed.json` — the patched n8n workflow, ready to import.
- `microflow-go-changes.diff` — unified diff of the 3 changed Go files
  (`internal/nodes/control.go`, `internal/nodes/http.go`,
  `internal/nodes/registry.go`) against the uploaded source.
- `control_test.go`, `http_test.go` — new test files (drop into
  `internal/nodes/` alongside the diffed files).
