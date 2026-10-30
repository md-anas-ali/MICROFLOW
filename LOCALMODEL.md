# Local model: Qwen3 1.7B (Settings → Model)

An **optional** small AI model that runs on the computer MicroFlow itself runs on.
It is never started on Render (install/start are refused when `RENDER` is set).

## Use it
Open ⚙️ → **Model**. Buttons: **Install / Uninstall**, **Start / Stop**, **Enable / Disable**.
Status shows model, runtime, process and whether AI requests currently use it.

* **Install** downloads (1) a pinned llama.cpp `llama-server` build (release `b11982`,
  SHA-256 checked, then run once with `--version` to prove it works) and (2) the official
  `Qwen/Qwen3-1.7B-GGUF` Q8_0 file (~1.8 GB, SHA-256 checked, GGUF header checked).
  Downloads resume if interrupted. Any hash/size mismatch deletes the file and aborts.
* **Start** launches `llama-server` bound to `127.0.0.1` on a random port with a random API key,
  low CPU priority, 1 slot, thinking mode off. It is stopped when MicroFlow exits and
  restarted at the next boot only if it was running. A crash is auto-restarted at most twice per hour.
* **Enable** lets AI requests use it. Enabled but not running = not used.

## How AI requests use it
In the HTTP Request node, a POST to an OpenAI-style `…/chat/completions` URL (OpenRouter and
compatible providers) or to Gemini `…:generateContent` is answered by Qwen3 first when it is
enabled, running and healthy. The answer is returned in that provider's response shape so the
workflow's own validation/retry nodes work unchanged. The normal provider request runs instead when:
the model is off/disabled/failing; the request has images/files, tools, streaming or a response
schema; the prompt does not fit the context; the answer was cut off by the token limit; or the
**same prompt is sent again in the same execution** (a workflow retry — so retries reach the free
providers rather than looping on the same local answer). After 3 consecutive local failures the
model is skipped for 2 minutes. Auto Free Finder, free-only guard, discovery, retry and validation
are workflow/provider logic and are untouched.

## Settings (environment variables, all optional)
`MICROFLOW_LOCAL_MODEL_DIR` (install location; default per-user cache dir),
`MICROFLOW_QWEN_CTX` (context tokens, default 4096), `MICROFLOW_QWEN_THREADS` (default half the
cores, max 4), `MICROFLOW_QWEN_TIMEOUT_SECONDS` (per request, default 180),
`MICROFLOW_QWEN_START_TIMEOUT_SECONDS` (default 180), `MICROFLOW_QWEN_ATTEMPTS_PER_PROMPT` (default 1).

## Platforms
Windows x64/arm64, Linux x64/arm64, macOS arm64/x64 (CPU builds). Needs ~2 GB disk for
downloads and roughly 2–3 GB free RAM while running.
