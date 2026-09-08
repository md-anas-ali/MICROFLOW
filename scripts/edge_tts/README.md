# Microsoft Edge TTS

MicroFlow uses **Microsoft Edge TTS only** through the Python `edge-tts` package.

## Runtime

- Python: 3.11
- `edge-tts`: **7.2.8**
- `aiohttp`: 3.14.3
- Default voice: `en-US-AndrewNeural`
- Default rate: `+18%`
- Concurrency: 1
- No GPU
- No paid API key

## Command contract

```sh
edge-tts --rate=+18% --voice=en-US-AndrewNeural --file INPUT --write-media OUTPUT
```

The launcher keeps this contract while adding a single-flight lock, bounded timeout, retry, and output validation.

## Important

Edge TTS is an online Microsoft service. The container must have outbound internet access.

`edge-tts` 4.0.11 is intentionally not used. The project is pinned to 7.2.8 because older releases can fail against Microsoft's current Edge TTS service.

## Self-test

Inside the built container:

```sh
/usr/local/bin/tts-selftest.sh
```

A successful test ends with:

```text
PASS: Edge TTS generated a valid non-empty MP3.
```
