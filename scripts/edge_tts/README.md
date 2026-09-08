# edge-tts wrapper (`edge_tts_min.py`)

## What this is and isn't

This is a minimal CLI wrapper around the real, online `edge-tts==4.0.11`
PyPI package (https://github.com/rany2/edge-tts). It is **not** a
reimplementation of Microsoft's protocol -- all the actual websocket
work happens inside `edge_tts.Communicate`, imported from the real
package. This file only adds: argument parsing that matches the CLI
shape the workflow already invokes, a single-flight concurrency lock,
an input-length ceiling, and guaranteed cleanup of partial output.

## Why this replaced the old pure-Go implementation

The repo previously shipped `internal/edgetts` + `cmd/edgetts`: a
hand-rolled, dependency-free Go reimplementation of Microsoft's
undocumented Edge "read aloud" websocket protocol, installed on `PATH`
as `edge-tts` so no Python/pip stack was needed at all. Its own doc
comment was explicit that it had **never been run against the live
Microsoft endpoint** -- it was written and `go build`/`go vet` clean
in a sandbox with no network route to `speech.platform.bing.com`, so
its handshake headers, anti-abuse token (`Sec-MS-GEC`), and framing
were never verified against reality, only against the reverse-engineered
edge-tts source it was ported from. That's a real reliability risk for
a protocol Microsoft doesn't document and can change without notice.

This pass reverses that decision on explicit instruction: use the real
`edge-tts==4.0.11` package instead, accept the larger (but bounded and
now measured) Python/aiohttp footprint as the cost of using the actual
upstream implementation that the rest of the ecosystem also depends on.

## Architecture: short-lived subprocess, not a persistent worker

The workflow's own "TTS (Edge->Silent)" node already invokes `edge-tts`
as a one-shot subprocess from inside a `python3 -c <script>`
`executeCommand` node -- once per scene, a handful of times per
workflow run, with the workflow otherwise idle between runs. Given that
call pattern, a persistent Python worker would mean paying the
`edge_tts`/`aiohttp` import cost (~25MB above bare Python -- see
measurements below) **all the time**, for a feature used a few seconds
per run. A short-lived subprocess pays that cost only for the
~0.2-2 seconds a request actually takes, and its memory is fully and
unconditionally reclaimed by the OS the instant the process exits --
not "returns close to baseline", but gone, because there is no process
left to hold it.

This was a real comparison, not an assumption: "idle RSS of a resident
process that has already imported edge_tts" is exactly the "python3 +
import edge_tts" row in the table below (~35MB), which is what a
persistent worker would hold resident between every request. For this
workflow's call pattern (bursty, infrequent, one call per scene) that
is strictly worse than paying the same ~35MB only during the request
itself. If a future deployment moves to a high-frequency, many-calls-
per-minute TTS API, that trade-off should be re-examined --
process-startup latency (see "what wasn't measured" below) is the
thing that would tip it back the other way, and this pass did not
attempt to measure that with the real endpoint blocked.

Concurrency is enforced at exactly 1 regardless of architecture: the
wrapper takes a blocking `flock` on a fixed path
(`MICROFLOW_TTS_LOCK_PATH`, default `/tmp/microflow-edge-tts.lock`)
before touching the network, so two overlapping calls -- from the same
run, from two concurrent runs, or from multiple Go server workers if
that ever changes -- can never result in two Python+aiohttp processes
running at once. A caller that can't get the lock within
`MICROFLOW_TTS_LOCK_WAIT_SECONDS` (default 25s) fails cleanly (same as
any other edge-tts failure -- the workflow's silent-audio fallback
handles it) rather than queuing unboundedly.

## Audio memory strategy

`edge_tts.Communicate.stream()` is an async generator that yields
small dictionaries such as `{"type": "audio", "data": bytes}` as frames arrive over the websocket. This wrapper writes `audio_bytes` straight to a `.part` file
as each chunk arrives and never accumulates chunks in a list/bytearray;
peak audio-related memory is one chunk (Microsoft's frames are small,
small transport chunks), not the whole
clip. On full success the `.part` file is atomically `os.replace()`d
onto the real output path; on any exception (including a timeout) the
`.part` file is deleted before the exception propagates, so a caller
never sees a truncated/corrupt file at the path it asked for -- only
either a complete file or no file. SubMaker/word-boundary subtitle
tracking (which the real CLI does unconditionally) is skipped entirely,
since this workflow's TTS step has no use for subtitles from this call.

## Concurrency, limits, cleanup -- where they live

| Requirement | Where enforced |
|---|---|
| Concurrency = 1 | `flock` on `MICROFLOW_TTS_LOCK_PATH`, blocking with a bounded wait |
| Max input length | `MICROFLOW_TTS_MAX_CHARS` (default 20000 chars), checked before any import/network work |
| No stale output on failure | `.part` write + atomic rename; `_cleanup_output()` on every exception path and at the top of `main()` |
| Synthesis timeout | `--timeout` (from the same CLI flag the old Go binary used), enforced via `asyncio.wait_for` |
| No cache | None exists -- every run starts and ends with no retained state; nothing is written outside the requested output path and the (empty, lock-only) lock file |

## Measurements

**Method**: same as `LOWRAM.md`'s own convention -- sampling
`/proc/[pid]/status`'s `VmHWM` (the kernel's authoritative per-process
peak resident set size) every 10ms for the life of the subprocess
(`bench_rss.py`, not included in the deployed image, used only for this
measurement pass).

**Honest limitation, stated up front**: this sandbox's network egress
is allow-listed and does **not** include Microsoft's TTS host
(`speech.platform.bing.com`); a real request gets `403
host_not_allowed` from the sandbox's own egress proxy before ever
reaching Microsoft. This is the same limitation the previous pass hit
(see `LOWRAM.md`'s "edge-tts" section) and it was not possible to get a
clean full successful-synthesis run in this sandbox. What follows is
what actually was measured, and what's an honest gap.

Test machine: this sandbox, Ubuntu 24.04 x86_64, Python 3.12 (glibc,
via a throwaway venv) -- **not** the exact target image (Alpine 3.19 /
Python 3.11 / musl); numbers on the real Alpine image will differ
somewhat (musl's allocator has a smaller per-process arena overhead
than glibc's per `LOWRAM.md`'s own note for python3/ffmpeg, so if
anything the real numbers are more likely to be a little lower than
these than higher, but that is reasoning, not a second measurement).

| Measurement | Peak `VmHWM` |
|---|---|
| Bare `python3 -c "pass"` (process floor) | **9.2 MB** |
| `python3 -c "import asyncio"` | **19.4 MB** |
| `python3 -c "import edge_tts"` (pulls in aiohttp + its deps) | **34.8-35.0 MB** |
| This wrapper, empty-input rejection (fails before importing edge_tts) | **20.5-21.3 MB** |
| This wrapper, short text (~12 words), wrapper import + connection-attempt path (blocked before audio) | **36.3-36.4 MB** |
| This wrapper, long text (3000 words), same blocked-connection path | **36.2-37.1 MB** |
| This wrapper, 5 sequential short-text attempts | **36.2-36.3 MB each, no growth trend** |
| This wrapper, 2 overlapping attempts (flock contention) | second caller blocks for the lock, then either proceeds after hand-off or fails with a clear lock-timeout message -- never runs concurrently with the first (confirmed by wall-clock: the waiting call's total elapsed time tracks the holder's remaining hold time, not zero) |
| This wrapper, synthesis timeout (mocked slow/hung stream, since the real endpoint can't be reached to time out mid-stream) | raises `asyncio.TimeoutError`, `.part` file removed, exit 1 -- confirmed via a unit-level mock of `Communicate.stream` that sleeps past the timeout |
| This wrapper, full happy path (mocked `Communicate.stream` streaming realistic chunk sizes) | output file written correctly, `.part` cleaned up, exit 0 -- confirms the file-lifecycle logic independent of network reachability |
| Post-run RSS (any outcome) | **0 -- the process has exited; there is nothing left to measure.** This is the concrete advantage of the short-lived-subprocess architecture: "does it return to baseline" isn't a question that needs an approximate answer here. |

**What was not, and could not be, measured here:**

- Peak RSS during actual receipt of real audio-frame data from
  Microsoft's service (blocked by sandbox network policy, as above).
  Given chunks are written to disk as they arrive and never
  accumulated, there's no structural reason to expect this to exceed
  the ~36-37MB connection-established number by more than a small,
  bounded amount (each yielded dictionary + its `bytes` object, then
  immediately written and eligible for GC) -- but that is reasoning
  from the code, not a measurement, and is reported as such.
- Cold-start latency/RSS specifically on the target Alpine/musl/
  Python 3.11 image (no Docker in this sandbox -- see the Dockerfile's
  own header comment for the same caveat carried over from the
  previous pass).
- Whether a persistent-worker architecture's actual request latency
  (not just its idle RSS, which was measured indirectly via the
  "import edge_tts" row) would have justified the extra resident
  memory for some other, higher-frequency call pattern than this
  workflow's.

## Remaining bottleneck

`aiohttp` (and, more fundamentally, `asyncio`) is the entire cost above
bare Python (~9MB -> ~35MB, roughly +10MB for asyncio and +15MB for
aiohttp's import graph). This is not something this wrapper can trim
further while satisfying the non-negotiable requirement to use
`edge-tts==4.0.11` -- `aiohttp` is edge-tts's one direct dependency,
required for the websocket connection to Microsoft, and `asyncio` is
required to drive it at all. There is no import inside this wrapper
that isn't one of: Python's own stdlib (argparse, os, sys, fcntl, time,
errno, asyncio) or `edge_tts` itself.
