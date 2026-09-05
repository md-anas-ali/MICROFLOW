# Low-RAM deployment image for MicroFlow, tuned for a single always-on
# workflow (see LOWRAM.md for the measured numbers and honest limits of
# this tuning). Two things this image does that a naive `go build` +
# `pip install edge-tts` setup would not:
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
#   2. Strips both Go binaries (-s -w) and uses a slim runtime base
#      instead of a full Debian/Ubuntu image.
#
# python3 itself is NOT removable: all 13 executeCommand nodes in this
# workflow invoke `python3 -c <script>` as their orchestration layer
# (which then shells out to ffmpeg/ffprobe) -- that's how the workflow
# is built, not something MicroFlow chooses, so a minimal python3 is
# still required in the runtime image.

FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY . .
ENV CGO_ENABLED=0
RUN go build -ldflags="-s -w" -o /out/microflow-server ./cmd/server \
 && go build -ldflags="-s -w" -o /out/edge-tts ./cmd/edgetts

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg python3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/microflow-server /usr/local/bin/microflow-server
# Installed as the literal name `edge-tts` -- see the header comment
# above for why the exact name on PATH matters here.
COPY --from=build /out/edge-tts /usr/local/bin/edge-tts
COPY internal/store/schema.sql /app/internal/store/schema.sql
WORKDIR /app

# See LOWRAM.md for what these defaults were actually measured against
# and where the real ceiling for this workflow currently sits.
ENV MICROFLOW_HEAP_CEILING_MB=90 \
    GOGC=30 \
    MICROFLOW_MAX_CONCURRENT_HEAVY=1 \
    MICROFLOW_MAX_CONCURRENT_EXECUTIONS=1 \
    MICROFLOW_DB_MAX_CONNS=2 \
    MICROFLOW_SCRATCH_DIR=/tmp/microflow \
    MICROFLOW_FFMPEG_PATH=/usr/bin/ffmpeg \
    MICROFLOW_PYTHON_PATH=/usr/bin/python3 \
    MICROFLOW_EDGE_TTS_PATH=/usr/local/bin/edge-tts

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/microflow-server"]
