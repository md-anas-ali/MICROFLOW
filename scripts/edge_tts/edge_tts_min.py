#!/bin/sh
# MICROFLOW Edge TTS launcher.
# Keep the workflow contract exactly the same as n8n:
# edge-tts --rate=+18% --voice en-US-AndrewNeural --file INPUT --write-media OUTPUT
#
# This intentionally delegates to the real edge-tts==4.0.11 CLI rather than
# reimplementing its protocol. The workflow itself owns the silent-audio
# fallback when Microsoft Edge TTS fails.
exec /usr/bin/python3 -m edge_tts "$@"
