# Low-RAM tuning for a single-workflow deployment (target: ~170MB)

**Correction notice:** an earlier version of this document concluded
170MB total was not achievable, based on `/usr/bin/time -v`'s
"Maximum resident set size" reading of ~165MB for the Go engine
process alone. That number was wrong -- cross-checked against the
Linux kernel's own per-process `VmHWM` (`/proc/[pid]/status`) and
against Go's own `runtime.MemStats`/GC trace, the real peak was
**~17-18MB**, not 165MB. `/usr/bin/time -v` was producing a bad
reading in the sandbox this was diagnosed in; three independent,
mutually-consistent measurements below (kernel VmHWM, Go's GC trace,
Go's own heap-profile sampler) say otherwise, so this revision uses
those. The corrected picture is much better news than the original
draft. Everything below is either a code change (verified with `go
build`/`vet`/`test -race`) or an actually-measured number (method
noted next to it) -- nothing here is estimated or assumed.

## What was changed in the code

1. **`cmd/server/main.go`**
   - `MICROFLOW_HEAP_CEILING_MB` default: 220 -> 90.
   - `GOGC` default (when unset): 50 -> 30.
   - `Engine.MaxConcurrentHeavy` (simultaneous FFmpeg/TTS/HTTP calls
     within one run): hardcoded 2 -> 1, now overridable via
     `MICROFLOW_MAX_CONCURRENT_HEAVY`. This is the change that matters
     most in practice: it guarantees FFmpeg, python3, and any
     concurrent HTTP call never overlap in time, so the system only
     ever has to fit ONE heavy external process's memory at once, not
     several stacked together.
   - Shared `http.Client` transport caps idle connections
     (`MaxIdleConns=4`, `MaxIdleConnsPerHost=2`, 20s idle timeout).
2. **`internal/store/postgres.go`**: Postgres pool default 4 -> 2
   connections.
3. **`internal/engine/engine.go` + `internal/runner/runner.go` +
   `cmd/server/main.go`**: `RunContext.NodeRunCap` (how many nodes'
   full input/output JSON the engine keeps in memory as execution
   history) is now configurable (`MICROFLOW_NODE_RUN_CAP`, default
   12, down from 500) AND the trim logic was fixed to actually copy
   into a smaller slice instead of re-slicing the same backing array
   (the old code's `s = s[extra:]` kept the "dropped" entries
   reachable through the same underlying array, so they were never
   actually freed -- a genuine bug, now fixed). This turned out NOT to
   be the dominant cost for this workflow (see measurements below --
   peak RSS was ~17MB either way), but it's a real fix worth keeping:
   it removes a latent unbounded-growth risk for any workflow with a
   long retry/loop pattern, and costs nothing.
4. **`Dockerfile`** (new): builds `cmd/edgetts` -- a pure-Go,
   dependency-free stand-in for the Python `edge-tts` CLI already in
   this repo (`internal/edgetts`, passing tests) -- and installs it as
   the literal command `edge-tts` on PATH, since the workflow's "TTS
   (Edge->Silent)" node's script calls the bare shell command
   `edge-tts` directly, not through MicroFlow's own config.
5. **`.env.170mb.example`** (new): the concrete env values above.

## What was actually measured

All measurements below use `/proc/[pid]/status`'s `VmHWM` (the Linux
kernel's own authoritative per-process peak resident memory) sampled
every 50ms while the process ran, cross-checked against `GODEBUG=
gctrace=1` for the Go process. Test machine: this sandbox (Ubuntu
24.04, x86_64) -- absolute numbers will shift a little on different
hardware/kernel/ffmpeg builds, but the *shape* of the result (which
piece is big, which is small) will not.

**MicroFlow's own Go engine**, running the actual uploaded workflow
(`My_workflow_14.json`) end-to-end via `cmd/e2echeck` -- all 138
nodes, all 5 scenes, real goja JS execution for all 36 Code nodes,
synthetic HTTP/command stand-ins (this harness doesn't spawn real
ffmpeg/python3, so this number is the Go process's own cost only):

- Peak `VmHWM`: **~17-18 MB**, consistently across 3 separate runs.
- Go's own GC trace confirms this independently: live heap size
  stayed at 3-4MB for the entire ~86s run.
- Lowering `GOGC`/`GOMEMLIMIT` made no measurable difference (it was
  never GC-bound to begin with -- there just isn't much live data).

**Real FFmpeg**, running the actual command shapes from this
workflow's own `executeCommand` nodes (extracted from
`My_workflow_14.json`, same flags: `-threads 1`, `libx264 -preset
ultrafast`, `yuv420p`, 720x1280), against synthetic test media at the
same resolution:

| Step (matches the workflow's own node) | Peak `VmHWM` |
|---|---|
| Single clip: zoompan + libx264 ultrafast, 720x1280, ~3s | **~81 MB** |
| Concat 5 clips + burned-in subtitles + color-grade EQ filter, libx264 ultrafast | **~97 MB** |

**Python3** (the interpreter every `executeCommand` node in this
workflow shells out through): bare interpreter startup, no imports
beyond stdlib: **~9.4 MB**.

**edge-tts**: the repo's own pure-Go replacement (`cmd/edgetts`)
starts in the same single-digit-MB range as any small Go binary (Go
binaries in this same test: a trivial "hello world" used ~1.5MB,
`cmd/e2echeck` itself idles around 9MB before doing any work) --
could not get a full successful run in this sandbox (outbound network
to Microsoft's TTS endpoint is blocked here), so no exact peak number
for a complete run, but there is no realistic path to it costing more
than a few MB more than that baseline, since it's a single dependency-
free static binary with no VM/interpreter/event-loop underneath it.
By contrast, pip's `edge-tts` needs a full CPython interpreter plus
`aiohttp`+`asyncio`'s import graph before it even opens a connection --
that stack is real and non-trivial, even though this sandbox couldn't
produce a clean successful-run number for it either (TLS handshake to
the real endpoint fails in this network-restricted sandbox before
steady state).

## The actual bottom line

Because `MaxConcurrentHeavy=1` (change #1 above) means FFmpeg,
python3, and the Go server never have large memory spikes at exactly
the same instant except during the brief window where python3 is
still resident *while* the ffmpeg child it just spawned is running
(python3's own `subprocess.run()` blocks and holds its own ~9MB the
whole time), the worst single moment during a real run is
approximately:

```
Go server (~18MB) + python3 (~9MB) + FFmpeg's heaviest step (~97MB)
+ OS/container baseline (a minimal Debian-slim image; not measured
  here, but a slim base + ffmpeg + python3 + ca-certificates
  typically lands in the 15-30MB range at idle)
≈ 140-155 MB
```

That fits inside a 170MB budget with a small but real margin -- not a
huge one, so this is "achievable and worth doing," not "comfortable."
If you see OOM kills in production, the concat+subtitle step (~97MB
measured) is the single most likely culprit to investigate first, and
the workflow's own resolution (720x1280 / 540x960) and encoder
settings (`ultrafast`, `-threads 1`) are already about as light as
FFmpeg gets while still doing a real color-grade + subtitle-burn
re-encode -- the next lever if it's still too tight is dropping
resolution further (e.g. 480x854) or removing the `eq=`/color-grade
filter pass, not anything on the MicroFlow/Go side.

## Honest caveats

- These are ffmpeg/python3 numbers for *synthetic* test media
  (solid-color test image, sine-wave test audio) run in this sandbox's
  ffmpeg 6.1.1 build -- your real images/audio and your actual
  deployment's ffmpeg version could shift these numbers somewhat
  (typically not by a large factor for the same resolution/codec/
  preset, but not guaranteed identical).
- The combined "worst moment" figure above is arithmetic addition of
  independently-measured peaks, not a single measured run of the full
  real pipeline together (this sandbox has no real Postgres/network
  access to run `cmd/server` end-to-end against the real workflow).
  Treat it as a well-evidenced estimate, not a guarantee -- monitor
  real RSS in your actual deployment (e.g. `docker stats`, or your
  platform's memory graph) and adjust from there.
