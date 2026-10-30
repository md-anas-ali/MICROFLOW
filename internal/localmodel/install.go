package localmodel

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	maxExtractBytes   = 1 << 30 // 1 GiB total unpacked runtime
	maxExtractEntries = 4000
)

// allowedDownloadHost limits where a download (and every redirect hop) may go.
func allowedDownloadHost(h string) bool {
	h = strings.ToLower(h)
	for _, suf := range []string{"github.com", "githubusercontent.com", "huggingface.co", "hf.co"} {
		if h == suf || strings.HasSuffix(h, "."+suf) {
			return true
		}
	}
	return false
}

type progressFn func(done, total int64)

// download fetches urls[i] (first that works) into dest+".part", resuming a
// previous partial file when the server supports Range, verifies SHA-256 (and
// optionally the Hugging Face linked ETag), then atomically renames to dest.
// On a hash/size failure the partial file is deleted so a retry starts clean.
func (m *Manager) download(ctx context.Context, urls []string, dest, wantSHA string, minB, maxB int64, linkedETag bool, prog progressFn) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	part := dest + ".part"
	var lastErr error
	for _, u := range urls {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := m.downloadOne(ctx, u, part, wantSHA, minB, maxB, linkedETag, prog)
		if err == nil {
			if err := os.Rename(part, dest); err != nil {
				return err
			}
			return nil
		}
		lastErr = err
		var fatal *fatalDownloadError
		if errors.As(err, &fatal) || ctx.Err() != nil {
			break // integrity problems and cancellation are never retried on another URL
		}
	}
	return lastErr
}

type fatalDownloadError struct{ msg string }

func (e *fatalDownloadError) Error() string { return e.msg }

func (m *Manager) downloadOne(ctx context.Context, rawURL, part, wantSHA string, minB, maxB int64, linkedETag bool, prog progressFn) error {
	pu, err := url.Parse(rawURL)
	if err != nil || (pu.Scheme != "https" && !m.sp.AllowAnyHost) {
		return &fatalDownloadError{"download URL must be https"}
	}
	if !m.sp.AllowAnyHost && !allowedDownloadHost(pu.Hostname()) {
		return &fatalDownloadError{"download host not allowed: " + pu.Hostname()}
	}
	var linked string
	client := &http.Client{
		Timeout: 0, // large file: bounded by ctx + stall detection below
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > 8 {
				return errors.New("too many redirects")
			}
			if !m.sp.AllowAnyHost && (req.URL.Scheme != "https" || !allowedDownloadHost(req.URL.Hostname())) {
				return fmt.Errorf("redirect to disallowed host %q", req.URL.Hostname())
			}
			if req.Response != nil && linked == "" {
				linked = strings.Trim(req.Response.Header.Get("X-Linked-Etag"), "\"")
			}
			return nil
		},
	}
	var have int64
	if st, err := os.Stat(part); err == nil {
		have = st.Size()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "MicroFlow-LocalModel/1.0")
	if have > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", have))
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if linkedETag && linked != "" && !strings.EqualFold(linked, wantSHA) {
		os.Remove(part)
		return &fatalDownloadError{"Hugging Face reports a different file hash than the pinned one; refusing to install"}
	}
	flags := os.O_CREATE | os.O_WRONLY
	switch resp.StatusCode {
	case http.StatusOK:
		have = 0 // server ignored Range: start over
		flags |= os.O_TRUNC
	case http.StatusPartialContent:
		flags |= os.O_APPEND
	case http.StatusRequestedRangeNotSatisfiable:
		os.Remove(part) // stale partial: retry from scratch next time
		return errors.New("partial download was invalid and was removed; retry")
	default:
		return fmt.Errorf("download failed: HTTP %d from %s", resp.StatusCode, pu.Hostname())
	}
	total := have + resp.ContentLength
	if resp.ContentLength < 0 {
		total = 0
	}
	if maxB > 0 && total > maxB {
		return &fatalDownloadError{fmt.Sprintf("download is unexpectedly large (%d bytes)", total)}
	}
	if err := m.checkDisk(filepath.Dir(part), total-have); err != nil {
		return &fatalDownloadError{err.Error()}
	}
	f, err := os.OpenFile(part, flags, 0o644)
	if err != nil {
		return err
	}
	done := have
	buf := make([]byte, 256<<10)
	lastProg := time.Now()
	stall := time.AfterFunc(90*time.Second, func() { resp.Body.Close() })
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			stall.Reset(90 * time.Second)
			if _, werr := f.Write(buf[:n]); werr != nil {
				stall.Stop()
				f.Close()
				return werr
			}
			done += int64(n)
			if maxB > 0 && done > maxB {
				stall.Stop()
				f.Close()
				os.Remove(part)
				return &fatalDownloadError{"download exceeded the maximum allowed size"}
			}
			if prog != nil && time.Since(lastProg) > 250*time.Millisecond {
				prog(done, total)
				lastProg = time.Now()
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			stall.Stop()
			f.Close()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("download interrupted (partial file kept for resume): %w", rerr)
		}
	}
	stall.Stop()
	if err := f.Close(); err != nil {
		return err
	}
	if prog != nil {
		prog(done, total)
	}
	if done < minB {
		os.Remove(part)
		return &fatalDownloadError{fmt.Sprintf("downloaded file is too small (%d bytes)", done)}
	}
	sum, err := fileSHA256(part)
	if err != nil {
		return err
	}
	if !strings.EqualFold(sum, wantSHA) {
		os.Remove(part)
		return &fatalDownloadError{"SHA-256 mismatch: downloaded file was discarded"}
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// safeJoin resolves name under root and rejects anything that escapes it.
func safeJoin(root, name string) (string, error) {
	name = strings.ReplaceAll(name, "\\", "/")
	if name == "" || strings.HasPrefix(name, "/") || filepath.IsAbs(name) {
		return "", fmt.Errorf("unsafe path in archive: %q", name)
	}
	for _, p := range strings.Split(name, "/") {
		if p == ".." {
			return "", fmt.Errorf("unsafe path in archive: %q", name)
		}
	}
	return filepath.Join(root, filepath.FromSlash(name)), nil
}

type extractBudget struct {
	bytes   int64
	entries int
}

func (b *extractBudget) entry() error {
	b.entries++
	if b.entries > maxExtractEntries {
		return errors.New("archive has too many entries")
	}
	return nil
}

func copyLimited(dst io.Writer, src io.Reader, b *extractBudget) error {
	n, err := io.Copy(dst, io.LimitReader(src, maxExtractBytes-b.bytes+1))
	b.bytes += n
	if b.bytes > maxExtractBytes {
		return errors.New("archive unpacks to more than the allowed size")
	}
	return err
}

func extractArchive(archive, dest string) error {
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	if strings.HasSuffix(archive, ".zip") {
		return extractZip(archive, dest)
	}
	return extractTarGz(archive, dest)
}

func extractZip(archive, dest string) error {
	zr, err := zip.OpenReader(archive)
	if err != nil {
		return err
	}
	defer zr.Close()
	var b extractBudget
	for _, zf := range zr.File {
		if err := b.entry(); err != nil {
			return err
		}
		target, err := safeJoin(dest, zf.Name)
		if err != nil {
			return err
		}
		if zf.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if !zf.FileInfo().Mode().IsRegular() {
			continue // no symlinks/devices from zip archives
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		rc, err := zf.Open()
		if err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
		if err != nil {
			rc.Close()
			return err
		}
		err = copyLimited(out, rc, &b)
		rc.Close()
		if cerr := out.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func extractTarGz(archive, dest string) error {
	f, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var b extractBudget
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := b.entry(); err != nil {
			return err
		}
		target, err := safeJoin(dest, hdr.Name)
		if err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
			if err != nil {
				return err
			}
			err = copyLimited(out, tr, &b)
			if cerr := out.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return err
			}
		case tar.TypeSymlink:
			// Shared-library version links (libX.so -> libX.so.0). Only
			// relative targets that stay inside dest are allowed.
			if filepath.IsAbs(hdr.Linkname) {
				return fmt.Errorf("unsafe symlink in archive: %q", hdr.Name)
			}
			resolved := filepath.Join(filepath.Dir(target), hdr.Linkname)
			rel, err := filepath.Rel(dest, resolved)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return fmt.Errorf("unsafe symlink in archive: %q", hdr.Name)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			os.Remove(target)
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return err
			}
		default:
			// hard links, devices, fifos, pax headers: ignored
		}
	}
}

func serverBinaryName() string {
	if runtime.GOOS == "windows" {
		return "llama-server.exe"
	}
	return "llama-server"
}

// findServerBinary locates llama-server inside an extracted runtime tree.
func findServerBinary(root string) (string, error) {
	var found string
	filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || found != "" {
			return nil
		}
		if !d.IsDir() && d.Name() == serverBinaryName() {
			found = p
		}
		return nil
	})
	if found == "" {
		return "", errors.New("llama-server binary not found in the downloaded runtime")
	}
	return found, nil
}

// verifyRuntime runs `llama-server --version` to prove the unpacked runtime
// actually executes on this machine (missing libraries, wrong CPU, ...).
func verifyRuntime(bin string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "--version")
	cmd.Dir = filepath.Dir(bin)
	out, err := cmd.CombinedOutput()
	s := strings.TrimSpace(string(out))
	if err != nil && !strings.Contains(strings.ToLower(s), "version") {
		return "", fmt.Errorf("runtime check failed: %v: %s", err, tail(s, 300))
	}
	if !strings.Contains(strings.ToLower(s), "version") {
		return "", fmt.Errorf("runtime check failed: unexpected output: %s", tail(s, 300))
	}
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(strings.ToLower(line), "version") {
			return strings.TrimSpace(line), nil
		}
	}
	return s, nil
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// checkGGUF verifies the GGUF magic bytes.
func checkGGUF(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil || string(magic[:]) != "GGUF" {
		return errors.New("model file is not a valid GGUF")
	}
	return nil
}
