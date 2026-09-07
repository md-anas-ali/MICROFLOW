package edgetts

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// This test file exercises the wire-protocol logic (framing, header
// parsing, multi-frame audio reassembly, SSML building) end-to-end
// in-process, without any real network I/O. That matters because the
// live Microsoft endpoint (speech.platform.bing.com) is outside the
// network allowlist of the sandboxes/CI this package builds in -- see
// edgetts.go's and ws.go's UNVERIFIED package doc notes -- so this is
// the closest thing to a real integration test available here.
//
// Framing roles matter: wsConn.writeFrame (ws.go) always masks its
// output (correct client->server behavior), and wsConn.readMessage
// always *rejects* a masked incoming frame (correct, because a
// conformant server never masks server->client frames -- see
// readMessage's own comment). That means this package's own writeFrame
// can only be used to play the "client" role and its own readMessage
// can only be used to play the "client reading from a server" role.
// To simulate the *server* side in tests (both sending unmasked
// frames and reading the masked frames our client sends), this file
// implements small, independent raw framing helpers below rather than
// reusing wsConn's client-only methods for the server role.

// rawWriteServerFrame writes one RFC6455 frame with NO mask bit set,
// exactly as a conformant WebSocket server does when sending to a
// client. This is what feeds wsConn.readMessage (the code under test)
// in the tests below.
func rawWriteServerFrame(conn net.Conn, opcode byte, payload []byte) error {
	var header []byte
	header = append(header, 0x80|opcode) // FIN + opcode, no more fragments
	n := len(payload)
	switch {
	case n <= 125:
		header = append(header, byte(n))
	case n <= 65535:
		header = append(header, 126, byte(n>>8), byte(n))
	default:
		ext := make([]byte, 8)
		nn := n
		for i := 7; i >= 0; i-- {
			ext[i] = byte(nn)
			nn >>= 8
		}
		header = append(header, 127)
		header = append(header, ext...)
	}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

// rawReadClientFrame reads one masked RFC6455 frame exactly as a
// conformant WebSocket server does when receiving from a client
// (unmasking the payload before returning it). This is used to verify
// what wsConn.writeFrame (the code under test, used by sendConfig/
// sendSSML) actually puts on the wire.
func rawReadClientFrame(br *bufio.Reader) (opcode byte, payload []byte, err error) {
	first, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	second, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	opcode = first & 0x0f
	if second&0x80 == 0 {
		return 0, nil, errors.New("test harness: expected a masked client frame")
	}
	length := int64(second & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			return 0, nil, err
		}
		length = int64(ext[0])<<8 | int64(ext[1])
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			return 0, nil, err
		}
		length = 0
		for _, b := range ext {
			length = length<<8 | int64(b)
		}
	}
	var maskKey [4]byte
	if _, err := io.ReadFull(br, maskKey[:]); err != nil {
		return 0, nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(br, buf); err != nil {
		return 0, nil, err
	}
	for i := range buf {
		buf[i] ^= maskKey[i%4]
	}
	return opcode, buf, nil
}

// newPipeConns returns a wsConn playing the client role (backed by
// one end of a net.Pipe, using the real production framing code) and
// the raw net.Conn/*bufio.Reader for the other end, which tests drive
// directly via the raw* helpers above to play the server role.
func newPipeConns() (client *wsConn, serverConn net.Conn, serverBR *bufio.Reader) {
	a, b := net.Pipe()
	client = &wsConn{conn: a, br: bufio.NewReader(a)}
	return client, b, bufio.NewReader(b)
}

// serverSendAudioFrames writes a "Path:audio" binary frame per chunk
// of data (mirroring the real service splitting audio across several
// frames), then a "Path:turn.end" text frame -- exactly what
// collectAudio (edgetts.go) expects to receive from a real server.
func serverSendAudioFrames(t *testing.T, serverConn net.Conn, audio []byte, chunkSize int) {
	t.Helper()
	header := "Path:audio\r\nContent-Type:audio/mpeg\r\n\r\n"
	if chunkSize <= 0 {
		chunkSize = len(audio)
		if chunkSize == 0 {
			chunkSize = 1
		}
	}
	for i := 0; i < len(audio); i += chunkSize {
		end := i + chunkSize
		if end > len(audio) {
			end = len(audio)
		}
		payload := append([]byte{}, byte(len(header)>>8), byte(len(header)))
		payload = append(payload, []byte(header)...)
		payload = append(payload, audio[i:end]...)
		if err := rawWriteServerFrame(serverConn, opBinary, payload); err != nil {
			t.Errorf("server write audio frame: %v", err)
			return
		}
	}
	if err := rawWriteServerFrame(serverConn, opText, []byte("Path:turn.end\r\n\r\n{}")); err != nil {
		t.Errorf("server write turn.end: %v", err)
	}
}

func TestCollectAudio_SingleFrame(t *testing.T) {
	client, serverConn, _ := newPipeConns()
	want := []byte("fake-mp3-bytes-not-silent-0123456789")

	done := make(chan struct{})
	go func() {
		defer close(done)
		serverSendAudioFrames(t, serverConn, want, 0)
	}()

	got, err := collectAudio(context.Background(), client)
	<-done
	if err != nil {
		t.Fatalf("collectAudio error: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("audio mismatch: got %q want %q", got, want)
	}
}

func TestCollectAudio_MultiFrameReassembly(t *testing.T) {
	client, serverConn, _ := newPipeConns()
	// Large enough that a naive implementation which only reads the
	// first binary frame would fail this test.
	want := bytes.Repeat([]byte{0x4a, 0x11, 0x9c, 0x02}, 3000) // 12000 bytes

	done := make(chan struct{})
	go func() {
		defer close(done)
		serverSendAudioFrames(t, serverConn, want, 777) // ugly chunk size on purpose
	}()

	got, err := collectAudio(context.Background(), client)
	<-done
	if err != nil {
		t.Fatalf("collectAudio error: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("reassembled audio mismatch: got %d bytes want %d bytes", len(got), len(want))
	}
}

func TestCollectAudio_IgnoresNonAudioBinaryFrames(t *testing.T) {
	client, serverConn, _ := newPipeConns()
	want := []byte("only-this-should-survive")

	done := make(chan struct{})
	go func() {
		defer close(done)
		// A binary frame whose header path is NOT "Path:audio" should
		// be skipped entirely, not concatenated into the result.
		junkHeader := "Path:metadata\r\n\r\n"
		junkPayload := append([]byte{}, byte(len(junkHeader)>>8), byte(len(junkHeader)))
		junkPayload = append(junkPayload, []byte(junkHeader)...)
		junkPayload = append(junkPayload, []byte("should-not-appear-in-output")...)
		if err := rawWriteServerFrame(serverConn, opBinary, junkPayload); err != nil {
			t.Errorf("server write junk frame: %v", err)
			return
		}
		serverSendAudioFrames(t, serverConn, want, 0)
	}()

	got, err := collectAudio(context.Background(), client)
	<-done
	if err != nil {
		t.Fatalf("collectAudio error: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want only the audio-path payload %q (non-audio frame leaked through)", got, want)
	}
}

func TestCollectAudio_TurnEndWithNoAudioIsError(t *testing.T) {
	client, serverConn, _ := newPipeConns()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := rawWriteServerFrame(serverConn, opText, []byte("Path:turn.start\r\n\r\n{}")); err != nil {
			t.Errorf("server write turn.start: %v", err)
			return
		}
		if err := rawWriteServerFrame(serverConn, opText, []byte("Path:turn.end\r\n\r\n{}")); err != nil {
			t.Errorf("server write turn.end: %v", err)
		}
	}()

	_, err := collectAudio(context.Background(), client)
	<-done
	if err == nil {
		t.Fatal("expected an error for turn.end with no audio, got nil")
	}
	if !strings.Contains(err.Error(), "no audio data") {
		t.Fatalf("expected a 'no audio data' error, got: %v", err)
	}
}

func TestCollectAudio_ServerClosesEarlyIsError(t *testing.T) {
	client, serverConn, _ := newPipeConns()

	done := make(chan struct{})
	go func() {
		defer close(done)
		serverConn.Close() // abrupt close, no turn.end -- simulates a dropped connection
	}()

	_, err := collectAudio(context.Background(), client)
	<-done
	if err == nil {
		t.Fatal("expected an error when the server closes before turn.end, got nil")
	}
}

func TestCollectAudio_RespectsContextDeadline(t *testing.T) {
	client, serverConn, _ := newPipeConns()
	defer serverConn.Close()
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Server never sends anything -- collectAudio must not hang forever.
	start := time.Now()
	_, err := collectAudio(ctx, client)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("collectAudio took too long to time out: %v", elapsed)
	}
}

// TestClientWriteFrame_WireFormat verifies wsConn.writeFrame (the
// production client-side sender used by sendConfig/sendSSML) produces
// correctly masked, correctly length-prefixed frames across all three
// RFC6455 length encodings, by having a raw "server" reader unmask and
// reassemble what actually went out on the wire.
func TestClientWriteFrame_WireFormat(t *testing.T) {
	// NOTE: a 0-byte payload is deliberately not covered here: net.Pipe
	// (the in-memory transport this test uses to avoid real network
	// I/O) requires every Write to be matched by a Read that consumes
	// it, but io.ReadFull on a 0-length buffer returns immediately
	// without ever calling Read -- so a 0-byte frame would hang this
	// *test harness* specifically (not production code; sendConfig/
	// sendSSML never send empty payloads, and writeFrame's header-only
	// length encoding for n==0 is already covered by inspection: it's
	// the same `case n <= 125` branch small payloads use).
	sizes := map[string]int{
		"small":            125,
		"medium_2byte_len": 65535,
		"large_8byte_len":  200000,
	}
	for name, n := range sizes {
		n := n
		t.Run(name, func(t *testing.T) {
			client, _, serverBR := newPipeConns()
			payload := bytes.Repeat([]byte{0xAB, 0xCD}, n/2+1)[:n]

			errCh := make(chan error, 1)
			go func() { errCh <- client.writeFrame(opBinary, payload) }()

			opcode, got, err := rawReadClientFrame(serverBR)
			if werr := <-errCh; werr != nil {
				t.Fatalf("writeFrame error: %v", werr)
			}
			if err != nil {
				t.Fatalf("rawReadClientFrame error: %v", err)
			}
			if opcode != opBinary {
				t.Fatalf("opcode = %x, want %x", opcode, opBinary)
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload mismatch for size %d: got %d bytes, want %d bytes", n, len(got), len(payload))
			}
		})
	}
}

// TestSynthesize_ProtocolEndToEnd exercises sendConfig, sendSSML, and
// collectAudio together (the exact sequence Synthesize() in edgetts.go
// runs after a successful wsDial) against a fake server implemented
// with the raw helpers above, proving the full non-network call path
// builds a spec-correct request and reassembles the response into
// real, non-empty audio bytes.
func TestSynthesize_ProtocolEndToEnd(t *testing.T) {
	client, serverConn, serverBR := newPipeConns()
	want := bytes.Repeat([]byte{0x11, 0x22, 0x33}, 500)

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		_, configPayload, err := rawReadClientFrame(serverBR)
		if err != nil {
			t.Errorf("server: read config: %v", err)
			return
		}
		if !strings.Contains(string(configPayload), "Path:speech.config") {
			t.Errorf("server: expected speech.config first, got: %s", configPayload)
		}
		_, ssmlPayload, err := rawReadClientFrame(serverBR)
		if err != nil {
			t.Errorf("server: read ssml: %v", err)
			return
		}
		s := string(ssmlPayload)
		if !strings.Contains(s, "Path:ssml") {
			t.Errorf("server: expected ssml, got: %s", s)
		}
		if !strings.Contains(s, "en-US-AndrewNeural") {
			t.Errorf("server: voice not present in ssml: %s", s)
		}
		if !strings.Contains(s, "+18%") {
			t.Errorf("server: rate not present in ssml: %s", s)
		}
		if !strings.Contains(s, "hello world") {
			t.Errorf("server: text not present in ssml: %s", s)
		}
		if err := rawWriteServerFrame(serverConn, opText, []byte("Path:turn.start\r\n\r\n{}")); err != nil {
			t.Errorf("server: write turn.start: %v", err)
			return
		}
		serverSendAudioFrames(t, serverConn, want, 900)
	}()

	if err := sendConfig(client); err != nil {
		t.Fatalf("sendConfig: %v", err)
	}
	if err := sendSSML(client, "hello world, this is a MicroFlow audio pipeline test.", "en-US-AndrewNeural", "+18%"); err != nil {
		t.Fatalf("sendSSML: %v", err)
	}
	got, err := collectAudio(context.Background(), client)
	<-serverDone
	if err != nil {
		t.Fatalf("collectAudio: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("end-to-end audio mismatch: got %d bytes, want %d bytes", len(got), len(want))
	}
}

func TestSynthesize_RejectsEmptyText(t *testing.T) {
	_, err := Synthesize(context.Background(), "   ", Options{})
	if err == nil {
		t.Fatal("expected an error for empty/whitespace-only text, got nil")
	}
}

func TestEscapeSSML(t *testing.T) {
	in := `Tom & Jerry said "hi" <there> it's fine`
	want := `Tom &amp; Jerry said &quot;hi&quot; &lt;there&gt; it&apos;s fine`
	if got := escapeSSML(in); got != want {
		t.Fatalf("escapeSSML(%q) = %q, want %q", in, got, want)
	}
}

func TestSecMSGEC_FormatAndStability(t *testing.T) {
	a := secMSGEC()
	b := secMSGEC()
	if a != b {
		t.Fatalf("secMSGEC should be stable within the same 5-minute window: %q != %q", a, b)
	}
	if len(a) != 64 {
		t.Fatalf("expected a 64-char uppercase hex SHA-256 digest, got %d chars: %q", len(a), a)
	}
	if strings.ToUpper(a) != a {
		t.Fatalf("expected uppercase hex, got %q", a)
	}
}

func TestRandomHexID_LengthAndUniqueness(t *testing.T) {
	a, err := randomHexID(32)
	if err != nil {
		t.Fatalf("randomHexID: %v", err)
	}
	b, err := randomHexID(32)
	if err != nil {
		t.Fatalf("randomHexID: %v", err)
	}
	if len(a) != 32 || len(b) != 32 {
		t.Fatalf("expected 32-char IDs, got %d and %d", len(a), len(b))
	}
	if a == b {
		t.Fatalf("two random IDs collided: %q", a)
	}
}
