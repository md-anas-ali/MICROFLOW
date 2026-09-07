// Command edgetts is a drop-in, pure-Go replacement for the subset of
// the Python `edge-tts` CLI that MicroFlow's workflows actually invoke:
//
//	edge-tts --rate=+18% --voice en-US-AndrewNeural --file in.txt --write-media out.mp3
//
// Point MICROFLOW_EDGE_TTS_PATH at this binary instead of a
// pip-installed edge-tts, and no python3/pip/aiohttp stack is needed in
// the deployment image at all -- see internal/edgetts's package doc for
// what this does and does not verify.
//
// Exit code is non-zero on any failure (network, protocol, empty
// input); MicroFlow's executeCommand node and the workflow's own shell
// script already treat a non-zero/empty-output edge-tts run as
// "generate silence instead" (see the "TTS (Edge->Silent)" step), so
// callers don't need special handling beyond what they already have.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"microflow/internal/edgetts"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "edgetts: "+err.Error())
		os.Exit(1)
	}
}

func run() error {
	var (
		rate       = flag.String("rate", "+0%", "Speech rate delta, e.g. +18%% or -10%%")
		voice      = flag.String("voice", "en-US-AndrewNeural", "Voice name")
		inFile     = flag.String("file", "", "Path to a text file containing the text to speak (required)")
		outFile    = flag.String("write-media", "", "Path to write the resulting MP3 to (required)")
		timeoutSec = flag.Int("timeout", 20, "Seconds to wait for synthesis before failing")
	)
	flag.Parse()

	if *inFile == "" || *outFile == "" {
		flag.Usage()
		return fmt.Errorf("--file and --write-media are both required")
	}

	textBytes, err := os.ReadFile(*inFile)
	if err != nil {
		return fmt.Errorf("reading --file: %w", err)
	}
	text := strings.TrimSpace(string(textBytes))
	if text == "" {
		return fmt.Errorf("--file is empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
	defer cancel()

	audio, err := edgetts.Synthesize(ctx, text, edgetts.Options{Voice: *voice, Rate: *rate})
	if err != nil {
		return err
	}

	if err := os.WriteFile(*outFile, audio, 0o644); err != nil {
		return fmt.Errorf("writing --write-media: %w", err)
	}
	return nil
}
