// Package compress implements a lossless, streaming, transport-only HTTP
// compression layer for MicroFlow.
//
// It never touches internal/engine, internal/nodes, internal/model, the
// workflow JSON format, node executors, SSE, or execution semantics in any
// way. It only wraps three existing seams, all at the HTTP boundary:
//
//   - ResponseMiddleware wraps the server's top-level http.Handler to
//     gzip-compress outgoing responses.
//   - RequestMiddleware wraps the same handler to transparently
//     gzip-decompress incoming request bodies.
//   - WrapTransport wraps an outbound *http.Client's Transport (e.g.
//     deps.HTTPClient, used by the httpRequest node) to verify MicroFlow's
//     own integrity trailer when the far end is another MicroFlow instance.
//
// Compression/decompression is negotiated with the standard HTTP
// Content-Encoding mechanism (Accept-Encoding / Content-Encoding: gzip), so
// any compliant HTTP client or server -- MicroFlow or not -- interoperates
// correctly with zero awareness that this package exists. Every route
// handler, node executor, and the workflow runner keep reading and writing
// exactly the same bytes as before compression existed.
package compress

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// Enabled lets the whole layer be switched off with one environment
// variable, with no redeploy of different code -- the ultimate operational
// fallback if compression is ever suspected of causing an issue in
// production. Existing HTTP behavior (uncompressed, exactly as before this
// package existed) resumes immediately.
func Enabled() bool {
	return os.Getenv("MICROFLOW_DISABLE_COMPRESSION") == ""
}

// ChecksumTrailer is an HTTP trailer -- not a header, since the hash of the
// full body isn't known until the body has been entirely streamed --
// carrying the SHA-256 of the original, uncompressed bytes. This is
// belt-and-suspenders on top of gzip's own built-in CRC32/ISIZE footer
// (RFC 1952), which already makes truncation or corruption detectable: a
// receiver that checks this trailer gets an extra, independent integrity
// signal; a receiver that ignores it (any non-MicroFlow HTTP client, or an
// older MicroFlow build) is completely unaffected, since HTTP trailers are
// optional and invisible to code that doesn't look for them.
const ChecksumTrailer = "X-Microflow-Content-Sha256"

// minCompressSize: below this, gzip's own frame overhead (~20-30 bytes)
// plus the checksum trailer can make a response *larger*, not smaller, so
// compression is skipped for tiny bodies.
const minCompressSize = 256

// skipCompressPrefixes: content types that are already compressed (or are
// arbitrary binary blobs where compression is unlikely to help). These are
// sent through unchanged rather than uselessly re-compressed -- covers the
// "don't recompress MP4/MP3/JPG/PNG" requirement generically, by type
// rather than by a hardcoded extension list.
var skipCompressPrefixes = []string{
	"image/", "video/", "audio/",
	"application/zip", "application/gzip", "application/x-gzip",
	"application/x-7z-compressed", "application/x-rar-compressed",
	"application/pdf", "font/", "application/wasm",
}

func looksAlreadyCompressed(contentType string) bool {
	ct := strings.ToLower(contentType)
	for _, p := range skipCompressPrefixes {
		if strings.HasPrefix(ct, p) {
			return true
		}
	}
	return false
}

func clientAcceptsGzip(r *http.Request) bool {
	for _, enc := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
		if strings.TrimSpace(strings.SplitN(strings.TrimSpace(enc), ";", 2)[0]) == "gzip" {
			return true
		}
	}
	return false
}

// isUpgradeOrStream excludes WebSocket upgrades and Server-Sent Events from
// compression entirely. SSE in particular depends on the handler
// controlling flush timing event-by-event; a generic compressor sitting in
// front of it is unnecessary risk for a stream that is normally small,
// already-flushed, and latency-sensitive -- requirement #10 (don't touch
// SSE/execution semantics) is honored by simply not intercepting it.
func isUpgradeOrStream(r *http.Request) bool {
	if strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		return true
	}
	return strings.Contains(r.Header.Get("Accept"), "text/event-stream")
}

// ---------------------------------------------------------------------
// Server side: outgoing responses
// ---------------------------------------------------------------------

// ResponseMiddleware gzip-compresses HTTP responses, streaming, whenever the
// client advertises gzip support. Wrap the existing top-level handler with
// it at the one place the server is constructed, e.g.:
//
//	handler := compress.RequestMiddleware(compress.ResponseMiddleware(mux))
//	srv := &http.Server{Handler: handler, ...}
//
// Nothing else changes: every handler underneath keeps writing normal,
// uncompressed bytes to its http.ResponseWriter exactly as before.
func ResponseMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !Enabled() || !clientAcceptsGzip(r) || isUpgradeOrStream(r) {
			next.ServeHTTP(w, r)
			return
		}
		cw := &compressingWriter{underlying: w, header: w.Header()}
		next.ServeHTTP(cw, r)
		cw.finish()
		if cw.err != nil {
			log.Printf("compress: response stream error for %s (client may see a truncated body): %v", r.URL.Path, cw.err)
		}
	})
}

// compressingWriter decides once, on the first Write (or on Close if the
// handler never writes a body), whether to compress -- based only on the
// Content-Type/Content-Length the handler itself already set, never on
// buffering the payload -- then streams every subsequent byte straight
// through a gzip.Writer with no additional buffering. A multi-gigabyte
// response never sits in memory at once: only gzip's own small internal
// window does.
type compressingWriter struct {
	underlying http.ResponseWriter
	header     http.Header
	status     int
	headerSent bool
	decided    bool
	compress   bool
	gz         *gzip.Writer
	sum        hash.Hash
	err        error
}

func (cw *compressingWriter) Header() http.Header { return cw.header }

// Unwrap lets http.ResponseController (net/http's mechanism, since Go 1.20,
// for reaching Hijack/SetReadDeadline/etc. through a chain of wrapping
// ResponseWriters) find the real underlying ResponseWriter. Deliberately
// NOT embedding http.ResponseWriter directly here: an embedded interface
// left unset would satisfy interfaces like http.Hijacker at compile time
// but panic with a nil pointer at call time -- Unwrap is the safe,
// idiomatic way to expose the same capability.
func (cw *compressingWriter) Unwrap() http.ResponseWriter { return cw.underlying }

func (cw *compressingWriter) WriteHeader(status int) {
	if cw.headerSent {
		return
	}
	cw.status = status
	// The real WriteHeader call is deferred until decide() (triggered by
	// the first Write) or finish(), because Content-Encoding/Trailer must
	// be set on the header map *before* the status line is sent.
}

func (cw *compressingWriter) Write(p []byte) (int, error) {
	if cw.err != nil {
		return 0, cw.err
	}
	if !cw.decided {
		cw.decide(p)
	}
	if !cw.compress {
		return cw.underlying.Write(p)
	}
	if cw.sum != nil {
		cw.sum.Write(p) // hash the ORIGINAL bytes, before gzip -- for the integrity trailer
	}
	n, err := cw.gz.Write(p)
	if err != nil {
		cw.err = fmt.Errorf("gzip write: %w", err)
		return n, cw.err
	}
	return n, nil
}

func (cw *compressingWriter) Flush() {
	if cw.compress && cw.gz != nil {
		_ = cw.gz.Flush()
	}
	if f, ok := cw.underlying.(http.Flusher); ok {
		f.Flush()
	}
}

func (cw *compressingWriter) decide(first []byte) {
	cw.decided = true
	ct := cw.header.Get("Content-Type")
	if ct == "" && len(first) > 0 {
		n := len(first)
		if n > 512 {
			n = 512
		}
		ct = http.DetectContentType(first[:n])
	}
	switch {
	case looksAlreadyCompressed(ct):
		cw.compress = false
	default:
		if clStr := cw.header.Get("Content-Length"); clStr != "" {
			if cl, err := strconv.Atoi(clStr); err == nil && cl < minCompressSize {
				cw.compress = false
				break
			}
		}
		cw.compress = true
	}
	if cw.compress {
		cw.header.Del("Content-Length") // final compressed length is unknown up front; chunked encoding takes over
		cw.header.Set("Content-Encoding", "gzip")
		existingVary := cw.header.Get("Vary")
		if existingVary == "" {
			cw.header.Set("Vary", "Accept-Encoding")
		} else if !strings.Contains(existingVary, "Accept-Encoding") {
			cw.header.Set("Vary", existingVary+", Accept-Encoding")
		}
		cw.header.Set("Trailer", ChecksumTrailer)
		cw.sum = sha256.New()
		cw.gz = gzip.NewWriter(cw.underlying)
	}
	cw.sendHeader()
}

func (cw *compressingWriter) sendHeader() {
	if cw.headerSent {
		return
	}
	cw.headerSent = true
	status := cw.status
	if status == 0 {
		status = http.StatusOK
	}
	cw.underlying.WriteHeader(status)
}

func (cw *compressingWriter) finish() {
	if !cw.decided {
		// Handler set a status/headers but wrote no body (e.g. 204, or an
		// empty response) -- nothing to compress, just send headers.
		cw.sendHeader()
		return
	}
	if !cw.compress || cw.gz == nil {
		return
	}
	if err := cw.gz.Close(); err != nil {
		cw.err = fmt.Errorf("gzip close: %w", err)
		return
	}
	if cw.sum != nil {
		cw.underlying.Header().Set(ChecksumTrailer, hex.EncodeToString(cw.sum.Sum(nil)))
	}
}

// ---------------------------------------------------------------------
// Server side: incoming requests
// ---------------------------------------------------------------------

// RequestMiddleware transparently gzip-decompresses incoming request bodies
// carrying Content-Encoding: gzip, streaming, before the request ever
// reaches a handler. If the sender attached the integrity trailer, it is
// verified once the body is fully read; a mismatch surfaces as a read
// error to whatever is consuming the body (e.g. json.Decode), rather than
// silently completing on truncated or corrupted data.
//
// Every existing handler is unaffected: it keeps calling r.Body.Read /
// json.NewDecoder(r.Body).Decode exactly as before and never sees
// compressed bytes.
func RequestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !Enabled() || !strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			// Malformed/corrupt compressed body: fail loudly and safely
			// rather than handing a handler bytes it would misparse as
			// valid uncompressed input.
			http.Error(w, "invalid gzip request body", http.StatusBadRequest)
			return
		}
		r.Body = &verifyingReader{gz: gz, raw: r.Body, sum: sha256.New(), req: r}
		r.Header.Del("Content-Encoding")
		r.ContentLength = -1
		next.ServeHTTP(w, r)
	})
}

// verifyingReader decompresses on the fly and, once the gzip stream ends,
// compares the running SHA-256 of everything it handed out against the
// sender's ChecksumTrailer (if present).
type verifyingReader struct {
	gz   *gzip.Reader
	raw  io.ReadCloser
	sum  hash.Hash
	req  *http.Request
	done bool
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	n, err := v.gz.Read(p)
	if n > 0 {
		v.sum.Write(p[:n])
	}
	if err == io.EOF && !v.done {
		v.done = true
		if expected := v.req.Trailer.Get(ChecksumTrailer); expected != "" {
			got := hex.EncodeToString(v.sum.Sum(nil))
			if !strings.EqualFold(got, expected) {
				return n, fmt.Errorf("compress: request body checksum mismatch (integrity check failed)")
			}
		}
	}
	return n, err
}

func (v *verifyingReader) Close() error {
	_ = v.gz.Close()
	return v.raw.Close()
}

// ---------------------------------------------------------------------
// Client side: outbound requests made BY MicroFlow (e.g. the httpRequest
// node's deps.HTTPClient)
// ---------------------------------------------------------------------

// WrapTransport wraps an existing http.RoundTripper (pass nil for
// http.DefaultTransport) so that, in addition to Go's standard library's
// already-automatic transparent gzip response decompression (this package
// does not duplicate or replace that -- it is on by default for any
// *http.Client whose Transport does not itself set a manual
// Accept-Encoding header), MicroFlow's own ChecksumTrailer is verified
// against the decompressed bytes whenever the far end attaches one (i.e.
// whenever the far end is another MicroFlow instance running
// ResponseMiddleware).
//
// A response from any ordinary third-party API -- no gzip, no MicroFlow
// trailer -- passes through completely unaffected: nothing here sends a
// custom header a third party wouldn't understand, and no outbound request
// body is ever altered.
//
// The one exception is outbound request-body compression, and it is
// strictly opt-in per destination host via MICROFLOW_COMPRESS_UPLOAD_HOSTS
// (see maybeCompressRequestBody's doc comment below) -- this is what
// reduces the "Service-Initiated" / "Service-Initiated (Private Link)"
// side of outbound bandwidth for calls between an operator's own MicroFlow
// instances. With that variable unset (the default), this function's
// behavior toward request bodies is unchanged from before: nothing is
// ever compressed on the way out.
func WrapTransport(rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	return &verifyingTransport{next: rt}
}

type verifyingTransport struct{ next http.RoundTripper }

func (t *verifyingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = maybeCompressRequestBody(req)
	resp, err := t.next.RoundTrip(req)
	if err != nil || resp == nil || resp.Body == nil || !Enabled() {
		return resp, err
	}
	resp.Body = &verifyingReadCloser{body: resp.Body, sum: sha256.New(), resp: resp}
	return resp, nil
}

// maybeCompressRequestBody is the sending half of the same integrity
// mechanism verifyingReader (RequestMiddleware, server side) already
// implements for reading: it gzip-compresses an outbound request body,
// streaming it through a bounded buffer via io.Pipe (never buffering the
// whole payload), and attaches the same ChecksumTrailer -- computed over
// the ORIGINAL bytes, exactly like ResponseMiddleware does for responses
// -- so that a receiving MicroFlow instance's *existing, unmodified*
// RequestMiddleware decompresses and verifies it with zero further
// changes on that end.
//
// This only ever runs for a destination host explicitly listed in
// MICROFLOW_COMPRESS_UPLOAD_HOSTS (comma-separated hostnames, each
// optionally with :port, matched case-insensitively against req.URL.Host
// -- a bare hostname also matches that host on any port). That allowlist
// is intentionally the operator's own other MicroFlow instances (reached
// over the public internet or over a Render Private Link -- the code path
// here is identical either way, since Private Link is just a different
// route to the same host:port and this package never touches routing).
// An arbitrary third-party API (Google, OpenRouter, a pasted webhook URL)
// is never a safe target: it has no obligation to accept a gzip-encoded
// request body, so compressing toward it could turn a working call into a
// broken or misinterpreted one. Leaving the variable unset -- the default
// -- makes this function return req unchanged in every case, so every
// existing outbound call keeps behaving exactly as it did before this
// function existed.
func maybeCompressRequestBody(req *http.Request) *http.Request {
	if !Enabled() || req.Body == nil || req.Body == http.NoBody {
		return req
	}
	if req.Header.Get("Content-Encoding") != "" {
		return req // caller already encoded the body its own way
	}
	if looksAlreadyCompressed(req.Header.Get("Content-Type")) {
		return req // e.g. a video/audio upload -- gzip would not help and could grow it
	}
	if cl := req.ContentLength; cl > 0 && cl < minCompressSize {
		return req // not worth gzip's own frame overhead
	}
	if !uploadHostAllowed(req.URL.Host) {
		return req
	}

	req2 := req.Clone(req.Context())
	// GetBody, if the caller set one, would replay the ORIGINAL
	// (uncompressed) body -- wrong once Content-Encoding says gzip. Clear
	// it so a retry/redirect at the Client layer (which reads GetBody
	// from its own copy of the pre-Transport request, never from req2)
	// is unaffected, and so nothing downstream is tempted to replay our
	// already-consumed pipe reader.
	req2.GetBody = nil
	req2.ContentLength = -1
	req2.Header.Del("Content-Length")
	req2.Header.Set("Content-Encoding", "gzip")
	// Trailer keys must be declared before the body is sent; the value is
	// filled in by the goroutine below just before the pipe reaches EOF.
	req2.Trailer = http.Header{ChecksumTrailer: nil}

	origBody := req.Body
	pr, pw := io.Pipe()
	req2.Body = pr
	go func() {
		sum := sha256.New()
		gz := gzip.NewWriter(pw)
		buf := make([]byte, 32*1024)
		for {
			n, rerr := origBody.Read(buf)
			if n > 0 {
				sum.Write(buf[:n])
				if _, werr := gz.Write(buf[:n]); werr != nil {
					_ = origBody.Close()
					_ = pw.CloseWithError(fmt.Errorf("compress: request gzip write: %w", werr))
					return
				}
			}
			if rerr != nil {
				if rerr != io.EOF {
					_ = origBody.Close()
					_ = pw.CloseWithError(rerr)
					return
				}
				break
			}
		}
		_ = origBody.Close()
		if err := gz.Close(); err != nil {
			_ = pw.CloseWithError(fmt.Errorf("compress: request gzip close: %w", err))
			return
		}
		req2.Trailer.Set(ChecksumTrailer, hex.EncodeToString(sum.Sum(nil)))
		_ = pw.Close()
	}()
	return req2
}

// uploadHostAllowed reports whether host (req.URL.Host, so possibly
// "example.com:8080") is one of the operator's own MicroFlow peers, per
// MICROFLOW_COMPRESS_UPLOAD_HOSTS. Read from the environment on every
// call (like Enabled()) rather than cached, so the allowlist can be
// changed without a redeploy.
func uploadHostAllowed(host string) bool {
	list := os.Getenv("MICROFLOW_COMPRESS_UPLOAD_HOSTS")
	if list == "" {
		return false
	}
	h := strings.ToLower(host)
	hostOnly := h
	if i := strings.LastIndex(h, ":"); i >= 0 {
		hostOnly = h[:i]
	}
	for _, entry := range strings.Split(list, ",") {
		e := strings.ToLower(strings.TrimSpace(entry))
		if e != "" && (e == h || e == hostOnly) {
			return true
		}
	}
	return false
}

type verifyingReadCloser struct {
	body io.ReadCloser
	sum  hash.Hash
	resp *http.Response
	done bool
}

func (v *verifyingReadCloser) Read(p []byte) (int, error) {
	n, err := v.body.Read(p)
	if n > 0 {
		v.sum.Write(p[:n])
	}
	if err == io.EOF && !v.done {
		v.done = true
		if expected := v.resp.Trailer.Get(ChecksumTrailer); expected != "" {
			got := hex.EncodeToString(v.sum.Sum(nil))
			if !strings.EqualFold(got, expected) {
				return n, fmt.Errorf("compress: response checksum mismatch (integrity check failed)")
			}
		}
	}
	return n, err
}

func (v *verifyingReadCloser) Close() error { return v.body.Close() }
