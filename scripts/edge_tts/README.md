# MICROFLOW Edge TTS

MICROFLOW follows the workflow's existing TTS setup exactly:

```text
edge-tts --rate=+18% --voice en-US-AndrewNeural --file /tmp/tts_text_<N>.txt --write-media /tmp/scene_<N>.mp3
```

The workflow writes the scene voice text to a temporary UTF-8 file, calls the
real `edge-tts==4.0.11` CLI, validates that an audio file was produced, and
falls back to generated silent MP3 audio if Microsoft Edge TTS fails.

The Docker image installs `edge-tts==4.0.11`; `/usr/local/bin/edge-tts` is a
small launcher for the package's real CLI (`python3 -m edge_tts`). No custom
Microsoft TTS protocol implementation is used.

The exact TTS node is kept compatible with the supplied n8n workflow, including
the voice, +18% rate, text-file input, output path, and silent fallback.
