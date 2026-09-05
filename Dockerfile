# Low-RAM deployment image for MicroFlow, tuned for a single always-on
# workflow (see LOWRAM.md for the measured numbers and honest limits of
# this tuning, including which numbers below are re-used from that
# measurement pass vs. new and NOT independently re-measured in this
# sandbox -- there is no Docker/network access here to build and
# profile this exact image, only to read/reason about it).
#
# Things this image does that a naive `go build` + `pip install
# edge-tts` + `debian` setup would not:
#
#   1. Builds cmd/edgetts (a pure-Go, dependency-free replacement for
#      the subset of the Python `edge-tts` CLI this workflow's "TTS
#      (Edge->Silent)" node actually calls) and installs it ON PATH AS
#      `edge-tts` itself -- the node's script calls the bare command
#      `edge-tts`, not a MicroFlow-configured path, so this only works
#      if the binary on PATH named `edge-tts` IS the Go one. This
#      removes an entire nested Python+asyncio+aiohttp+websockets
#      process (and the whole python3-pip/venv layer needed to install
#      it) from every TTS call.
#   2. Strips both Go binaries (-s -w -trimpath).
#   3. Uses Alpine (musl libc) instead of Debian for both the build and
#      runtime stage, not just a "slim" Debian variant. This is a
#      genuine step further than the previous pass: musl's allocator
#      has a much smaller per-process memory-arena overhead than
#      glibc's, which matters for two long-lived-per-run processes in
#      this workflow (python3, ffmpeg) that are NOT part of the Go heap
#      and so aren't touched by any of the Go-side tuning below at all.
#      CAVEAT (being honest per LOWRAM.md's own rule about not claiming
#      unmeasured numbers): this was not build-tested in this pass --
#      Alpine's `ffmpeg` package has historically shipped with a broad
#      codec set including libx264, but verify `ffmpeg -encoders | grep
#      264` in the built image before relying on it, since Alpine
#      package contents do shift between versions. If it's missing
#      libx264, switch the base back to `debian:bookworm-slim` with
#      `apt-get install -y --no-install-recommends ffmpeg python3
#      ca-certificates` (the previous, verified-working combination)
#      and drop the `apk` lines for `apt-get` ones.
#
# python3 itself is NOT removable: all 13 executeCommand nodes in this
# workflow invoke `python3 -c <script>` as their orchestration layer
# (which then shells out to ffmpeg/ffprobe) -- that's how the workflow
# is built, not something MicroFlow chooses, so a minimal python3 is
# still required in the runtime image.

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
ENV CGO_ENABLED=0 GOFLAGS=-trimpath
RUN go build -ldflags="-s -w" -o /out/microflow-server ./cmd/server \
 && go build -ldflags="-s -w" -o /out/edge-tts ./cmd/edgetts

FROM alpine:3.19
RUN apk add --no-cache ffmpeg python3 ca-certificates
COPY --from=build /out/microflow-server /usr/local/bin/microflow-server
# Installed as the literal name `edge-tts` -- see the header comment
# above for why the exact name on PATH matters here.
COPY --from=build /out/edge-tts /usr/local/bin/edge-tts
COPY internal/store/schema.sql /app/internal/store/schema.sql
WORKDIR /app

# See LOWRAM.md for what these defaults were actually measured against
# and where the real ceiling for this workflow currently sits, and for
# which of the values below are carried over from that measurement
# pass vs. tightened further without a fresh measurement to back them.
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
    MICROFLOW_EDGE_TTS_PATH=/usr/local/bin/edge-tts

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/microflow-server"]
