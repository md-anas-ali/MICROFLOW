# e2echeck

Standalone dry-run verification harness, built while doing a real
production-level E2E check of `test/testdata/sample_workflow.json`
("My workflow 14"). Not wired into `go test`; run it manually:

```
go run ./cmd/e2echeck path/to/workflow.json
```

What it does, in order (stops early if an earlier stage fails):

1. **JS syntax check** — compiles every Code node's script through the
   exact same `goja.Compile` call `internal/nodes.CodeExecutor` uses
   (same IIFE wrapping), so a bad import doesn't silently mask a typo
   the real engine would also choke on.
2. **Graph integrity** — parses via the real `internal/parser.Parse`
   and checks every connection's source/target resolves to a real
   node, lists triggers, disabled nodes, and orphans.
3. **Real engine run** — builds `internal/nodes.DefaultRegistry` with:
   - a mocked `http.Client` (`mockhttp.go`) that returns schema-correct
     synthetic responses for the AI providers (Gemini/OpenRouter),
     LanguageTool, YouTube/Sheets/Gmail, Wikipedia, Google Trends, and
     Pollinations image endpoints this workflow calls — matched by
     reading each downstream Code node's actual parsing logic, not
     guessed;
   - the real `python3` binary (and whatever it can find, e.g.
     `ffmpeg`, on `PATH`) as the sole allowlisted `executeCommand`
     binary;
   - in-memory `StaticDataStore`/`CredentialResolver`/
     `GoogleAccountResolver` fakes (`fakes.go`).

   Then calls the real `engine.Run` and prints every node's
   status/attempt/duration/error in order, plus every outbound HTTP
   call intercepted.

This is NOT a substitute for a real deployment test against live
credentials, real ffmpeg/edge-tts, and real network egress — several
of those (Google OAuth flows in particular) are explicitly
unverifiable without a live account. It IS a strong, repeatable check
that MicroFlow's own Go code (routing, retries, JSON parsing, every
Code node's JS logic, file/path handling) behaves correctly against
this specific workflow, and it already caught one real bug (see
`internal/nodes/http.go`'s response-shape fix) that a live test would
have surfaced far more slowly and expensively.

To point `mockTransport` at a different workflow's AI providers, edit
the `strings.Contains(bodyStr, ...)` match keys in `mockhttp.go` to
match that workflow's own prompt text.
