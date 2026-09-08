# Low-RAM deployment image for MicroFlow, tuned for a single always-on
# workflow (see LOWRAM.md for the measured numbers and honest limits of
# this tuning).
#
# TTS ENGINE: real, online Microsoft Edge TTS via the `"edge-tts==7.2.8"`
# PyPI package (see scripts/edge_tts/edge_tts_min.py and
# scripts/edge_tts/README.md). This deliberately replaces an earlier
# pure-Go reimplementation of the same undocumented Microsoft protocol
# that used to live at internal/edgetts/cmd/edgetts -- that code was
# never exercised against the live service (this repo's own sandbox has
# never had network access to it either; see LOWRAM.md's history) and
# reverse-engineered protocol details like the Sec-MS-GEC anti-abuse
# token are exactly the kind of thing Microsoft can silently change.
# "edge-tts==7.2.8" is the actual upstream project every "Edge TTS"
# integration in the wild is built on, so it degrades the same way
# everyone else's does, not in some bespoke way only this repo hits.
#
# The workflow's own "TTS (Edge->Silent)" node calls the bare shell
# command `edge-tts`, not a MicroFlow-configured path (see
# internal/nodes/command.go's doc comment) -- so, same as before, this
# only works if whatever is on PATH as `edge-tts` behaves like the CLI
# the workflow expects (`--rate --voice --file --write-media`). Here
# that's a thin wrapper script (~150 lines, see the README next to it)
# invoking the real edge_tts.Communicate class directly, not edge-tts's
# own bundled CLI -- see that README for exactly why (single-flight
# concurrency lock, no SubMaker/subtitle bookkeeping, hard input-length
# ceiling, guaranteed no partial output file on failure).
#
# python3 itself is NOT a new cost introduced by this: all 13
# executeCommand nodes in this workflow already invoke `python3 -c
# <script>` as their orchestration layer (which then shells out to
# ffmpeg/ffprobe), so a python3 interpreter was already required in
# this image before edge-tts entered the picture at all. What IS new
# is the edge_tts + aiohttp package install (see stage 2 below) --
# that overhead is real and is reported honestly in
# scripts/edge_tts/README.md's measurements, not hidden.
#
# Alpine (musl libc) is used for all three stages so the prebuilt
# `musllinux_1_2` wheels for aiohttp (and its transitive deps:
# multidict, yarl, frozenlist, propcache) apply directly -- confirmed
# available on PyPI for cp311/musllinux_1_2_x86_64 as of this writing,
# so stage 2 needs no C compiler at all, just pip + network. If a
# future aiohttp/edge-tts release ever lacks a musllinux wheel for the
# Python version this pins, that stage will fail loudly at `docker
# build` time (not silently fall back to a slow compile), which is the
# safer failure mode here.
#
# CAVEAT carried over from the previous pass (being honest per
# LOWRAM.md's own rule about not claiming unmeasured numbers): Alpine's
# `ffmpeg` package has historically shipped with a broad codec set
# including libx264, but verify `ffmpeg -encoders | grep 264` in the
# built image before relying on it. If it's missing libx264, switch the
# base back to `debian:bookworm-slim` with `apt-get install -y
# --no-install-recommends ffmpeg python3 ca-certificates`, and for
# stage 2 use `python:3.11-slim-bookworm` with `pip install
# --no-cache-dir --target=... "edge-tts==7.2.8"` (manylinux wheels for
# aiohttp cover glibc too, so this swap doesn't reintroduce a compiler
# requirement).

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
RUN go build -ldflags="-s -w" -o /out/microflow-server ./cmd/server

# Fetches "edge-tts==7.2.8" and its one direct dependency (aiohttp) as
# prebuilt wheels ONLY (--only-binary=:all: below turns a missing
# wheel into a hard build failure instead of silently compiling, per
# the caveat above). aiohttp's own transitive deps (multidict, yarl,
# frozenlist, propcache, aiosignal, aiohappyeyeballs, idna, attrs,
# typing_extensions) are pulled in automatically by pip's resolver --
# nothing here is hand-picked or trimmed beyond what edge-tts actually
# imports. --target installs into a plain directory (not a venv), so
# the final stage only needs to COPY that directory and set PYTHONPATH
# -- no pip/setuptools/wheel need to exist in the runtime image at all.
FROM python:3.11-alpine3.19 AS pytts
RUN pip install --no-cache-dir --no-compile --only-binary=:all: \
      --target=/pytts-deps "edge-tts==7.2.8" "aiohttp==3.14.3" \
 && find /pytts-deps -name "*.dist-info" -type d -exec rm -rf {} + \
 && find /pytts-deps -name "__pycache__" -type d -exec rm -rf {} + \
 && find /pytts-deps -name "*.egg-info" -type d -exec rm -rf {} + \
 # edge_playback pulls in nothing extra by itself but isn't reachable
 # from this deployment's CLI-only usage -- drop it, it's dead weight.
 && rm -rf /pytts-deps/edge_playback

FROM alpine:3.19

# Verify the vendored Edge TTS runtime at image build time.
RUN PYTHONPATH=/opt/microflow/pytts-deps /usr/bin/python3 -c 'import edge_tts; print("edge-tts", getattr(edge_tts, "__version__", "unknown"))'
COPY scripts/edge_tts/edge_tts_min.py /usr/local/bin/edge-tts
RUN chmod +x /usr/local/bin/edge-tts
COPY scripts/edge_tts/tts-selftest.sh /usr/local/bin/tts-selftest.sh
RUN chmod +x /usr/local/bin/tts-selftest.sh
RUN apk add --no-cache ffmpeg python3 ca-certificates bash
COPY --from=build /out/microflow-server /usr/local/bin/microflow-server
COPY --from=pytts /pytts-deps /opt/microflow/pytts-deps
COPY internal/store/schema.sql /app/internal/store/schema.sql
WORKDIR /app

# See LOWRAM.md for what these defaults were actually measured against
# and where the real ceiling for this workflow currently sits, and
# scripts/edge_tts/README.md for the TTS-specific numbers.
# TTS wrapper controls: single-flight is always 1; the wait setting
# only bounds how long a second caller waits before falling back.
ENV MICROFLOW_HEAP_CEILING_MB=40 \
    GOGC=15 \
    GOMAXPROCS=1 \
    GODEBUG=madvdontneed=1 \
    MICROFLOW_MAX_CONCURRENT_HEAVY=1 \
    MICROFLOW_MAX_CONCURRENT_EXECUTIONS=1 \
    MICROFLOW_MAX_QUEUED_EXECUTIONS=2 \
    MICROFLOW_DB_MAX_CONNS=1 \
    MICROFLOW_SCRATCH_DIR=/tmp/microflow \
    MICROFLOW_FFMPEG_PATH=/usr/bin/ffmpeg \
    MICROFLOW_PYTHON_PATH=/usr/bin/python3 \
    MICROFLOW_EDGE_TTS_PATH=/usr/local/bin/edge-tts \
    PYTHONPATH=/opt/microflow/pytts-deps \
    PYTHONDONTWRITEBYTECODE=1 \
    MICROFLOW_TTS_MAX_CHARS=20000 \
    MICROFLOW_TTS_LOCK_PATH=/tmp/microflow-edge-tts.lock \
    MICROFLOW_TTS_LOCK_WAIT_SECONDS=25 \
    MICROFLOW_TTS_TIMEOUT_SECONDS=45 \
    MICROFLOW_TTS_MAX_ATTEMPTS=2

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/microflow-server"]
