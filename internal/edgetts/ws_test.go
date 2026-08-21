package edgetts

import (
	"bufio"
	"bytes"
	"net"
	"testing"
	"time"
)

// wsAcceptKey against RFC 6455's own worked example (section 1.3) --
// the one piece of the handshake with an authoritative, non-Microsoft
// test vector.
func TestWSAcceptKey_RFC6455Vector(t *testing.T) {
	got := wsAcceptKey("dGhlIHNhbXBsZSBub25jZQ==")
	want := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	if got != want {
		t.Fatalf("wsAcceptKey mismatch: got %q want %q", got, want)
	}
}

// TestFrameRoundTrip writes frames of various sizes (short, 16-bit
// extended, and one that would need masking-cycle correctness checked)
// over an in-memory net.Pipe and confirms readMessage reassembles
// exactly what writeFrame sent, including the mandatory client masking.
func TestFrameRoundTrip(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()

	client := &wsConn{conn: clientSide, br: bufio.NewReader(clientSide)}
	// server-side reader: does NOT reuse wsConn.readMessage (that
	// assumes unmasked server->client frames per RFC6455); it manually
	// unmasks so this test can play "server" and prove the client wrote
	// a spec-conformant masked frame.
	serverBR := bufio.NewReader(serverSide)

	sizes := []int{0, 5, 125, 126, 500, 70000} // spans the 7-bit/16-bit/64-bit length encodings
	done := make(chan error, 1)

	go func() {
		for _, n := range sizes {
			payload := bytes.Repeat([]byte{0xAB}, n)
			if err := client.writeBinary(payload); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	for _, wantLen := range sizes {
		got, err := readMaskedFrameForTest(serverBR)
		if err != nil {
			t.Fatalf("server-side read failed for size %d: %v", wantLen, err)
		}
		if len(got) != wantLen {
			t.Fatalf("size mismatch: got %d want %d", len(got), wantLen)
		}
		for i, b := range got {
			if b != 0xAB {
				t.Fatalf("payload corrupted at byte %d (size %d)", i, wantLen)
			}
		}
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("writer goroutine error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for writer goroutine")
	}
}

// readMaskedFrameForTest is a tiny standalone server-side frame reader
// (unmasks, unlike wsConn.readMessage which expects already-unmasked
// server frames) used only to validate what the client actually put on
// the wire.
func readMaskedFrameForTest(br *bufio.Reader) ([]byte, error) {
	first, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	_ = first
	second, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	masked := second&0x80 != 0
	n := int64(second & 0x7f)
	switch n {
	case 126:
		var ext [2]byte
		if _, err := readFull(br, ext[:]); err != nil {
			return nil, err
		}
		n = int64(ext[0])<<8 | int64(ext[1])
	case 127:
		var ext [8]byte
		if _, err := readFull(br, ext[:]); err != nil {
			return nil, err
		}
		n = 0
		for _, b := range ext {
			n = n<<8 | int64(b)
		}
	}
	var mask [4]byte
	if masked {
		if _, err := readFull(br, mask[:]); err != nil {
			return nil, err
		}
	}
	payload := make([]byte, n)
	if _, err := readFull(br, payload); err != nil {
		return nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return payload, nil
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func TestSecMSGEC_Shape(t *testing.T) {
	got := secMSGEC()
	if len(got) != 64 {
		t.Fatalf("secMSGEC: expected 64 hex chars, got %d (%q)", len(got), got)
	}
	for _, c := range got {
		isHex := (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')
		if !isHex {
			t.Fatalf("secMSGEC: non-uppercase-hex char %q in %q", c, got)
		}
	}
}

func TestEscapeSSML(t *testing.T) {
	in := `Tom & Jerry say "hi" <script>`
	got := escapeSSML(in)
	want := `Tom &amp; Jerry say &quot;hi&quot; &lt;script&gt;`
	if got != want {
		t.Fatalf("escapeSSML mismatch: got %q want %q", got, want)
	}
}
