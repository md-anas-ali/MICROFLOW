#!/bin/sh
set -eu

# MICROFLOW Microsoft Edge TTS launcher.
# Contract remains compatible with:
# edge-tts --rate=+18% --voice en-US-AndrewNeural --file INPUT --write-media OUTPUT

LOCK_PATH="${MICROFLOW_TTS_LOCK_PATH:-/tmp/microflow-edge-tts.lock}"
LOCK_WAIT="${MICROFLOW_TTS_LOCK_WAIT_SECONDS:-25}"
TIMEOUT_SECONDS="${MICROFLOW_TTS_TIMEOUT_SECONDS:-45}"
MAX_ATTEMPTS="${MICROFLOW_TTS_MAX_ATTEMPTS:-2}"

mkdir -p "$(dirname "$LOCK_PATH")"

cleanup() {
  rmdir "$LOCK_PATH" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

i=0
while ! mkdir "$LOCK_PATH" 2>/dev/null; do
  i=$((i + 1))
  if [ "$i" -ge "$LOCK_WAIT" ]; then
    echo "ERROR: Edge TTS lock timeout after ${LOCK_WAIT}s" >&2
    exit 75
  fi
  sleep 1
done

attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
  tmp_out=""
  # Detect --write-media output path without requiring a full parser.
  prev=""
  for arg in "$@"; do
    if [ "$prev" = "--write-media" ]; then
      tmp_out="$arg"
      break
    fi
    prev="$arg"
  done

  if [ -n "$tmp_out" ]; then
    rm -f "$tmp_out"
  fi

  if command -v timeout >/dev/null 2>&1; then
    if timeout "${TIMEOUT_SECONDS}s" /usr/bin/python3 -m edge_tts "$@"; then
      rc=0
    else
      rc=$?
    fi
  else
    /usr/bin/python3 -m edge_tts "$@" &
    pid=$!
    elapsed=0
    while kill -0 "$pid" 2>/dev/null; do
      if [ "$elapsed" -ge "$TIMEOUT_SECONDS" ]; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        rc=124
        break
      fi
      sleep 1
      elapsed=$((elapsed + 1))
    done
    if [ "${rc:-999}" = "999" ]; then
      wait "$pid"
      rc=$?
    fi
  fi

  if [ "$rc" -eq 0 ] && [ -n "$tmp_out" ] && [ -s "$tmp_out" ]; then
    exit 0
  fi

  echo "Edge TTS attempt ${attempt}/${MAX_ATTEMPTS} failed (exit=${rc:-1})" >&2
  attempt=$((attempt + 1))
  sleep 1
done

echo "ERROR: Edge TTS failed after ${MAX_ATTEMPTS} attempts" >&2
exit 1
