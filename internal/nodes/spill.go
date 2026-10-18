package nodes

// spill.go: optional "use Storage if it is there, otherwise stay on the
// existing RAM path" helpers for the few places that still hold a whole
// temporary payload in memory.
//
// Design rules (see CHANGES.md):
//   - Small payloads never touch the disk (no extra I/O, no slowdown).
//   - Only a large body (> spillThreshold) is staged in the execution's
//     scratch dir, and only when storageUsable() says the scratch dir is
//     real, writable, has room and is NOT RAM-backed (tmpfs/ramfs).
//   - Every failure (no dir, read-only FS, disk full, I/O error) silently
//     falls back to the old in-memory read. Nothing here can fail a run
//     that would have succeeded before.
//   - The staged file is always removed before the function returns.

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

const (
	defaultSpillThreshold = 1 << 20 // 1 MiB: below this, RAM is cheaper than disk
	defaultMinFreeBytes   = 64 << 20
)

// errBodyTooLarge is returned by readBodyCapped when the body exceeds the
// caller's cap (same condition the old code checked after io.ReadAll).
var errBodyTooLarge = errors.New("response body exceeded cap")

func envInt64(name string, def int64) int64 {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

// spillEnabled: MICROFLOW_STORAGE_SPILL=0 forces the old RAM-only path.
func spillEnabled() bool { return os.Getenv("MICROFLOW_STORAGE_SPILL") != "0" }

func spillThreshold() int64 {
	return envInt64("MICROFLOW_SPILL_THRESHOLD_KB", defaultSpillThreshold>>10) << 10
}

func minFreeBytes() int64 {
	return envInt64("MICROFLOW_STORAGE_MIN_FREE_MB", defaultMinFreeBytes>>20) << 20
}

// storageUsable reports whether dir can be used to stage `need` bytes
// without risking RAM (tmpfs) or a full disk. Any doubt => false.
func storageUsable(dir string, need int64) bool {
	if dir == "" || !spillEnabled() {
		return false
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return false
	}
	free, ramBacked, known := storageStat(dir)
	if ramBacked {
		return false // staging in tmpfs would just move the bytes from heap to RAM
	}
	if known && free < uint64(need+minFreeBytes()) {
		return false
	}
	return true
}

// readBodyCapped reads r fully (at most max bytes) and returns the bytes.
// Behaviour matches `io.ReadAll(io.LimitReader(r, max+1))` + a length check,
// but when the body turns out to be large and Storage is usable it is
// streamed to a temp file first and then read back into ONE exactly-sized
// buffer, avoiding ReadAll's repeated grow-and-copy (which transiently
// holds up to ~2x the body). Returns errBodyTooLarge when len > max.
func readBodyCapped(r io.Reader, max int64, scratchDir string) ([]byte, error) {
	thr := spillThreshold()
	if thr <= 0 || thr >= max {
		return readAllCapped(r, max)
	}
	head, err := io.ReadAll(io.LimitReader(r, thr))
	if err != nil {
		return nil, err
	}
	if int64(len(head)) < thr { // hit EOF inside the threshold: small body, RAM only
		return head, nil
	}
	// Large body. Stage on Storage if we can; otherwise continue in RAM.
	if !storageUsable(scratchDir, max) {
		return readRestInRAM(head, r, max)
	}
	f, err := os.CreateTemp(scratchDir, "body-*.tmp")
	if err != nil { // cannot stage: old RAM behaviour
		return readRestInRAM(head, r, max)
	}
	name := f.Name()
	defer os.Remove(name)
	total := int64(len(head))
	_, werr := f.Write(head)
	head = nil // release the prefix before the large allocation below
	if werr == nil {
		var n int64
		n, werr = io.Copy(f, io.LimitReader(r, max+1-total))
		total += n
	}
	if werr != nil {
		f.Close()
		return nil, werr
	}
	if total > max {
		f.Close()
		return nil, errBodyTooLarge
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	buf := make([]byte, total)
	_, err = io.ReadFull(f, buf)
	f.Close()
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// readRestInRAM is the original behaviour (one io.ReadAll, no extra copy):
// the already-read prefix is replayed in front of the remaining stream.
func readRestInRAM(head []byte, r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.MultiReader(bytes.NewReader(head), io.LimitReader(r, max+1-int64(len(head)))))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, errBodyTooLarge
	}
	return b, nil
}

func readAllCapped(r io.Reader, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, errBodyTooLarge
	}
	return b, nil
}

// fileToBase64 returns the base64 of the file at path, encoding in small
// chunks straight into a pre-sized builder. The old os.ReadFile +
// EncodeToString + string() path held the raw bytes, the encoded []byte AND
// the string copy at once (~2.8x the file size); this holds only the result.
func fileToBase64(path string, size int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var sb strings.Builder
	sb.Grow(base64.StdEncoding.EncodedLen(int(size)))
	enc := base64.NewEncoder(base64.StdEncoding, &sb)
	if _, err := io.Copy(enc, io.LimitReader(f, size)); err != nil {
		return "", err
	}
	if err := enc.Close(); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// fileToString reads a (size-capped) text file into a string with a single
// allocation instead of os.ReadFile's []byte plus string() copy.
func fileToString(path string, size int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var sb strings.Builder
	sb.Grow(int(size))
	if _, err := io.Copy(&sb, io.LimitReader(f, size)); err != nil {
		return "", err
	}
	return sb.String(), nil
}
