package nodes

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"microflow/internal/model"
)

// spoolBinary writes data to the execution's scratch directory and
// returns a reference, never keeping the bytes in the returned Item's
// JSON map. Callers should discard `data` after this call so the
// garbage collector can reclaim it promptly (rule 7/18: bounded memory
// for large image/audio/video payloads).
func spoolBinary(scratchDir, nodeName string, data []byte, mimeType string) (model.BinaryRef, error) {
	if scratchDir == "" {
		scratchDir = os.TempDir()
	}
	if err := os.MkdirAll(scratchDir, 0o700); err != nil {
		return model.BinaryRef{}, err
	}
	suffix := randSuffix()
	fname := filepath.Join(scratchDir, sanitizeFilename(nodeName)+"-"+suffix+extFor(mimeType))
	if err := os.WriteFile(fname, data, 0o600); err != nil {
		return model.BinaryRef{}, err
	}
	return model.BinaryRef{FileName: fname, MimeType: mimeType, SizeBytes: int64(len(data))}, nil
}

// spoolBinaryStream is spoolBinary's streaming counterpart: it copies r
// directly into the spooled file (constant, small buffer via io.Copy)
// instead of requiring the caller to already have the whole payload as
// a []byte. Use this whenever the source is itself a stream (an HTTP
// response body, in particular) so a large image/audio/video response
// is never briefly held whole in RAM on its way to disk. maxBytes caps
// how much will be written; exceeding it is an error and the partial
// file is removed.
func spoolBinaryStream(scratchDir, nodeName string, r io.Reader, mimeType string, maxBytes int64) (model.BinaryRef, error) {
	if scratchDir == "" {
		scratchDir = os.TempDir()
	}
	if err := os.MkdirAll(scratchDir, 0o700); err != nil {
		return model.BinaryRef{}, err
	}
	suffix := randSuffix()
	fname := filepath.Join(scratchDir, sanitizeFilename(nodeName)+"-"+suffix+extFor(mimeType))

	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return model.BinaryRef{}, err
	}
	defer f.Close()

	// Copy with a one-byte-over cap so we can detect "exceeded the limit"
	// without ever holding more than maxBytes+1 in flight (io.Copy itself
	// streams via a small internal buffer, not a full-size one).
	limited := io.LimitReader(r, maxBytes+1)
	n, copyErr := io.Copy(f, limited)
	if copyErr != nil {
		os.Remove(fname)
		return model.BinaryRef{}, copyErr
	}
	if n > maxBytes {
		os.Remove(fname)
		return model.BinaryRef{}, fmt.Errorf("spoolBinaryStream: payload exceeded %d byte cap", maxBytes)
	}
	return model.BinaryRef{FileName: fname, MimeType: mimeType, SizeBytes: n}, nil
}

func randSuffix() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func sanitizeFilename(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		return "file"
	}
	return string(out)
}

func extFor(mimeType string) string {
	switch mimeType {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "audio/mpeg":
		return ".mp3"
	case "audio/wav":
		return ".wav"
	case "video/mp4":
		return ".mp4"
	default:
		return ".bin"
	}
}

func jsonUnmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}
