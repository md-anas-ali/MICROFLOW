#!/bin/sh
set -eu

echo "=== MICROFLOW Edge TTS self-test ==="
python3 --version
python3 -c 'import edge_tts; print("edge-tts:", getattr(edge_tts, "__version__", "unknown"))'
echo "edge-tts path: $(command -v edge-tts || true)"
echo "wrapper path: $(command -v edge-tts)"

tmp="${TMPDIR:-/tmp}/microflow-tts-selftest-$$"
mkdir -p "$tmp"
trap 'rm -rf "$tmp"' EXIT

printf '%s\n' "This is a MicroFlow Edge TTS self test." > "$tmp/input.txt"

edge-tts \
  --rate=+18% \
  --voice=en-US-AndrewNeural \
  --file "$tmp/input.txt" \
  --write-media "$tmp/output.mp3"

test -s "$tmp/output.mp3"

ffprobe -v error -show_entries format=format_name,duration \
  -of default=noprint_wrappers=1 "$tmp/output.mp3"

echo "PASS: Edge TTS generated a valid non-empty MP3."
