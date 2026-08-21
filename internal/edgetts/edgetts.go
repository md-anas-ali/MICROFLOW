// Package edgetts is a pure-Go, dependency-free implementation of the
// same unofficial Microsoft Edge "read aloud" neural TTS protocol the
// popular Python `edge-tts` package wraps. It exists so MicroFlow's
// deployment doesn't need python3 + pip + edge-tts + aiohttp (9
// packages) installed just to render voiceover -- one static Go binary
// covers it instead.
//
// UNVERIFIED: this talks to a real, undocumented Microsoft endpoint
// (speech.platform.bing.com) reverse-engineered from the open-source
// edge-tts project. It was written and compiled in a sandbox whose
// network allowlist does NOT include that host, so it has never been
// exercised against the live service -- only go build/go vet clean. Two
// things reduce the risk of shipping it anyway:
//
//  1. Microsoft can and occasionally does change this protocol (they
//     added the Sec-MS-GEC anti-abuse token in 2024, implemented below).
//     If they change it again, calls here will start failing.
//  2. The workflow's own "TTS (Edge->Silent)" executeCommand step
//     already treats edge-tts failure as expected and falls back to a
//     generated silent clip (see its EDGE_FAILED_SILENT branch) -- so a
//     protocol break here degrades gracefully instead of breaking the
//     pipeline.
//
// Test this against the real endpoint on a machine with normal internet
// access before relying on it (rule of thumb this repo already follows
// for the Google API calls in internal/nodes/google.go).
package edgetts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	wsHost = "speech.platform.bing.com"
	wsPath = "/consumer/speech/synthesize/readaloud/edge/v1"

	// trustedClientToken is a long-lived constant baked into the Edge
	// browser itself and used by every edge-tts client; it is not a
	// per-user secret (see edge-tts's own source/README for the same
	// value).
	trustedClientToken = "6A5AA1D4EAFF4E9FB37E23D68491D6F4"

	chromeVersion = "130.0.2849.68"
	userAgent     = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" + chromeVersion + " Safari/537.36 Edg/" + chromeVersion
	wsOrigin      = "chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold"

	// winEpochOffsetSeconds converts a Unix timestamp to the Windows
	// (1601-01-01) epoch used by secMSGEC below.
	winEpochOffsetSeconds = 11644473600
)

// secMSGEC reproduces Microsoft's anti-abuse token: SHA-256 of the
// current time (rounded down to a 5-minute window, expressed as Windows
// 100ns ticks) concatenated with trustedClientToken, uppercase hex.
func secMSGEC() string {
	ticks := time.Now().Unix() + winEpochOffsetSeconds
	ticks -= ticks % 300 // round down to nearest 5 minutes
	winTicks := ticks * 10_000_000
	sum := sha256.Sum256([]byte(strconv.FormatInt(winTicks, 10) + trustedClientToken))
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}

func randomHexID(n int) (string, error) {
	b := make([]byte, n/2)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return strings.ToUpper(hex.EncodeToString(b)), nil
}

// Options configures a Synthesize call. Voice and Rate map directly to
// the `--voice`/`--rate` flags the workflow's shell steps already pass
// to edge-tts (e.g. Voice: "en-US-AndrewNeural", Rate: "+18%").
type Options struct {
	Voice string
	Rate  string // e.g. "+18%", "-10%", or "" for default
}

// Synthesize renders text to MP3 bytes (audio-24khz-48kbitrate-mono-mp3,
// same format edge-tts requests) via Microsoft's Edge read-aloud
// service. See the package doc for the verification caveat.
func Synthesize(ctx context.Context, text string, opts Options) ([]byte, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("edgetts: empty text")
	}
	voice := opts.Voice
	if voice == "" {
		voice = "en-US-AndrewNeural"
	}
	rate := opts.Rate
	if rate == "" {
		rate = "+0%"
	}

	connID, err := randomHexID(32)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(
		"?TrustedClientToken=%s&Sec-MS-GEC=%s&Sec-MS-GEC-Version=1-%s&ConnectionId=%s",
		trustedClientToken, secMSGEC(), chromeVersion, connID,
	)

	headers := http.Header{}
	headers.Set("Origin", wsOrigin)
	headers.Set("User-Agent", userAgent)
	headers.Set("Pragma", "no-cache")
	headers.Set("Cache-Control", "no-cache")

	conn, err := wsDial(ctx, wsHost, wsPath+query, headers)
	if err != nil {
		return nil, fmt.Errorf("edgetts: connect: %w", err)
	}
	defer conn.Close()

	if err := sendConfig(conn); err != nil {
		return nil, fmt.Errorf("edgetts: send config: %w", err)
	}
	if err := sendSSML(conn, text, voice, rate); err != nil {
		return nil, fmt.Errorf("edgetts: send ssml: %w", err)
	}

	return collectAudio(ctx, conn)
}

func timestamp() string {
	// Matches the loose RFC1123-ish format edge-tts sends; the server
	// doesn't appear to validate it strictly, only require the header.
	return time.Now().UTC().Format("Mon Jan 02 2006 15:04:05 GMT+0000 (Coordinated Universal Time)")
}

func sendConfig(conn *wsConn) error {
	msg := "X-Timestamp:" + timestamp() + "\r\n" +
		"Content-Type:application/json; charset=utf-8\r\n" +
		"Path:speech.config\r\n\r\n" +
		`{"context":{"synthesis":{"audio":{"metadataoptions":{` +
		`"sentenceBoundaryEnabled":"false","wordBoundaryEnabled":"false"},` +
		`"outputFormat":"audio-24khz-48kbitrate-mono-mp3"}}}}`
	return conn.writeText(msg)
}

func sendSSML(conn *wsConn, text, voice, rate string) error {
	reqID, err := randomHexID(32)
	if err != nil {
		return err
	}
	ssml := fmt.Sprintf(
		"<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' xml:lang='en-US'>"+
			"<voice name='%s'><prosody pitch='+0Hz' rate='%s' volume='+0%%'>%s</prosody></voice></speak>",
		voice, rate, escapeSSML(text),
	)
	msg := "X-RequestId:" + reqID + "\r\n" +
		"Content-Type:application/ssml+xml\r\n" +
		"X-Timestamp:" + timestamp() + "\r\n" +
		"Path:ssml\r\n\r\n" + ssml
	return conn.writeText(msg)
}

func escapeSSML(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return r.Replace(s)
}

// collectAudio reads server messages until "Path:turn.end", concatenating
// the audio payload out of every binary "Path:audio" frame. Binary
// frames are: 2-byte big-endian header length, then that many bytes of
// \r\n-terminated headers (same "Key:Value\r\n...\r\n\r\n" shape as the
// text control messages), then raw audio bytes for the rest of the
// frame.
func collectAudio(ctx context.Context, conn *wsConn) ([]byte, error) {
	var audio []byte
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(30 * time.Second)
	}
	for {
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("edgetts: timed out waiting for turn.end")
		}
		msg, err := conn.readMessage()
		if err != nil {
			return nil, fmt.Errorf("edgetts: read: %w", err)
		}
		switch msg.opcode {
		case opText:
			s := string(msg.payload)
			if strings.Contains(s, "Path:turn.end") {
				if len(audio) == 0 {
					return nil, fmt.Errorf("edgetts: turn ended with no audio data")
				}
				return audio, nil
			}
			// Path:turn.start / Path:response / audio.metadata -- no
			// audio bytes to extract, just progress markers.
		case opBinary:
			if len(msg.payload) < 2 {
				continue
			}
			headerLen := int(msg.payload[0])<<8 | int(msg.payload[1])
			if 2+headerLen > len(msg.payload) {
				continue // malformed/truncated frame, skip rather than panic
			}
			header := string(msg.payload[2 : 2+headerLen])
			if strings.Contains(header, "Path:audio") {
				audio = append(audio, msg.payload[2+headerLen:]...)
			}
		case opClose:
			return nil, fmt.Errorf("edgetts: server closed connection before turn.end")
		}
	}
}
