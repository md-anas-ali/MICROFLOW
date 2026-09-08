#!/usr/bin/env python3
"""Minimal-RAM drop-in `edge-tts` CLI replacement, backed by the real
`edge-tts==4.0.11` PyPI package (online Microsoft Edge "read aloud"
service -- NOT a local/offline model).

This exists instead of running edge-tts's own bundled __main__/util.py
CLI for four concrete reasons, all in service of the memory/lifecycle
rules this was built under (see MICROFLOW's LOWRAM.md /
scripts/edge_tts/README.md for the measured numbers):

  1. Enforces MICROFLOW's own single-flight concurrency limit (a
     flock on a fixed path) so two overlapping TTS calls can never run
     as two simultaneous Python+aiohttp processes, regardless of how
     many workers/executions the Go server has in flight -- the stock
     CLI has no such guard.
  2. Skips SubMaker/word-boundary subtitle bookkeeping entirely (the
     workflow this feeds never uses subtitles from this step -- see
     internal/nodes' "TTS (Edge->Silent)" node) -- one less object
     graph retained for the life of the request.
  3. Enforces a hard input-length ceiling before ever opening a socket,
     so a malformed/huge input can't turn into a huge outbound request
     or a long-held connection.
  4. Guarantees no partial/corrupt output file is ever left behind on
     failure -- the calling shell script's own validmp3() check
     already treats a missing file as "fall back to silence", so this
     just makes that path reliable instead of relying on every caller
     remembering to `rm -f` first.

Everything else is intentionally as close to the upstream CLI's own
`_tts()` as possible (see edge_tts/util.py in the installed package)
because that function already does the one thing that matters most for
RAM here: it streams each audio chunk straight to the output file as
it arrives over the websocket (`media_file.write(chunk)`), and never
concatenates the full clip into one in-memory buffer.

CLI contract (unchanged from the pure-Go binary this replaces, and
from upstream edge-tts itself for the flags used here):

    edge-tts --rate=+18% --voice en-US-AndrewNeural \
             --file in.txt --write-media out.mp3

Exit code is non-zero on any failure. Nothing is printed on success.
"""
from __future__ import annotations

import argparse
import asyncio
import errno
import fcntl
import os
import sys
import time

# Imported lazily (after arg parsing / input validation) so a bad
# invocation (missing args, empty file, oversized input) never pays
# for edge_tts's own import graph (aiohttp + its transitive deps) at
# all -- see "import only what's required" in the design notes above.

MAX_CHARS = int(os.environ.get("MICROFLOW_TTS_MAX_CHARS", "20000"))
LOCK_PATH = os.environ.get("MICROFLOW_TTS_LOCK_PATH", "/tmp/microflow-edge-tts.lock")
# How long a caller will wait to acquire the single-flight lock before
# giving up (NOT the synthesis timeout itself -- see --timeout below).
# Bounded so a stuck/hung prior run can't wedge every future caller
# forever; the workflow's own fallback-to-silence path handles a
# failure here the same as any other edge-tts failure.
LOCK_WAIT_SECONDS = float(os.environ.get("MICROFLOW_TTS_LOCK_WAIT_SECONDS", "25"))


def parse_args(argv):
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--rate", default="+0%")
    p.add_argument("--voice", default="en-US-AndrewNeural")
    p.add_argument("--file", required=True, help="path to a UTF-8 text file to speak")
    p.add_argument("--write-media", required=True, dest="write_media")
    p.add_argument("--timeout", type=int, default=20, help="seconds to wait for synthesis")
    return p.parse_args(argv)


class LockTimeout(Exception):
    """Raised when another edge-tts run doesn't finish in time to hand
    off the single-flight lock. Deliberately NOT a subclass of
    (asyncio.)TimeoutError: Python 3.11+ made asyncio.TimeoutError an
    alias of the builtin TimeoutError, so reusing that name here would
    make main()'s except clauses unable to tell "the synthesis call
    itself timed out" apart from "never got a turn to run at all" --
    two different failures worth different log messages even though
    both end in the same exit code and cleanup.
    """


class _SingleFlight:
    """Blocking, cross-process mutex via flock on a fixed path.

    This is the whole concurrency story: no worker pool, no queue
    object, no thread -- just one open file descriptor and one kernel
    call, released automatically (even on crash/SIGKILL) when the
    process exits. Costs zero heap once acquired.
    """

    def __init__(self, path: str, wait_seconds: float):
        self.path = path
        self.wait_seconds = wait_seconds
        self._fd = None

    def __enter__(self):
        self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o644)
        deadline = time.monotonic() + self.wait_seconds
        while True:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except OSError as e:
                if e.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                if time.monotonic() >= deadline:
                    os.close(self._fd)
                    self._fd = None
                    raise LockTimeout(
                        f"timed out after {self.wait_seconds}s waiting for another "
                        "edge-tts run to finish (concurrency limit is 1)"
                    )
                time.sleep(0.1)

    def __exit__(self, *exc):
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


def _cleanup_output(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


async def _synthesize(text: str, voice: str, rate: str, out_path: str, timeout: int) -> None:
    # Deferred import: only paid for once we know we actually need to
    # talk to the service (see module docstring).
    import edge_tts  # edge-tts==4.0.11

    # edge-tts 4.0.11 expects the text and voice at construction time.
    # Its streaming API yields small dict chunks; write audio chunks
    # immediately instead of accumulating them.
    communicate = edge_tts.Communicate(text, voice, rate=rate)
    wrote_any = False
    tmp_path = out_path + ".part"
    try:
        # Bounded, single-writer, append-only file handle -- audio
        # chunks are written as they arrive and never held in a Python
        # list/bytearray, so peak RAM here is one chunk (a few KB),
        # not the whole clip. Writing to a .part path and renaming
        # only on full success means a request that dies partway
        # through never leaves a half-written file at the real output
        # path for the caller's validmp3() check to trip over.
        with open(tmp_path, "wb") as media_file:
            async def _run_once():
                nonlocal wrote_any
                async for chunk in communicate.stream():
                    # edge-tts 4.0.11 yields dict chunks. Ignore metadata
                    # and write each audio payload immediately.
                    if chunk.get("type") == "audio":
                        audio_bytes = chunk.get("data")
                        if audio_bytes:
                            media_file.write(audio_bytes)
                            wrote_any = True

            await asyncio.wait_for(_run_once(), timeout=timeout)
    except BaseException:
        _cleanup_output(tmp_path)
        raise

    if not wrote_any:
        _cleanup_output(tmp_path)
        raise RuntimeError("edge-tts returned no audio data")

    os.replace(tmp_path, out_path)


def main(argv=None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])

    _cleanup_output(args.write_media)  # never leave a stale file from a prior run

    try:
        with open(args.file, "r", encoding="utf-8") as f:
            text = f.read().strip()
    except OSError as e:
        print(f"edge-tts: reading --file: {e}", file=sys.stderr)
        return 1

    if not text:
        print("edge-tts: --file is empty", file=sys.stderr)
        return 1

    if len(text) > MAX_CHARS:
        print(
            f"edge-tts: input is {len(text)} chars, exceeds MICROFLOW_TTS_MAX_CHARS={MAX_CHARS}",
            file=sys.stderr,
        )
        return 1

    try:
        with _SingleFlight(LOCK_PATH, LOCK_WAIT_SECONDS):
            asyncio.run(_synthesize(text, args.voice, args.rate, args.write_media, args.timeout))
    except LockTimeout as e:
        print(f"edge-tts: {e}", file=sys.stderr)
        return 1
    except asyncio.TimeoutError:
        # NB: asyncio.TimeoutError IS the builtin TimeoutError on
        # Python 3.11+, which is exactly why LockTimeout above is its
        # own class rather than reusing TimeoutError -- this clause
        # only ever means "the synthesis call itself timed out".
        print(f"edge-tts: timed out after {args.timeout}s", file=sys.stderr)
        return 1
    except Exception as e:  # network failure, invalid voice, service error, etc.
        print(f"edge-tts: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
