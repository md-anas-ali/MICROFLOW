package edgetts

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"
)

// writeUnmaskedFrameForTest writes one unfragmented, UNmasked frame
// directly to conn, standing in for the real server side of the
// connection (RFC 6455 5.1 forbids the server from masking frames --
// only wsConn.writeFrame, the client path, masks). readMessage only
// knows how to read unmasked frames, matching what a real server sends.
func writeUnmaskedFrameForTest(conn net.Conn, opcode byte, payload []byte) error {
	var header []byte
	header = append(header, 0x80|opcode)

	n := len(payload)
	switch {
	case n <= 125:
		header = append(header, byte(n))
	case n <= 65535:
		header = append(header, 126, byte(n>>8), byte(n))
	default:
		ext := make([]byte, 8)
		for i := 7; i >= 0; i-- {
			ext[i] = byte(n)
			n >>= 8
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

// TestCollectAudio_ParsesBinaryFramesAndStopsAtTurnEnd drives
// collectAudio against a fake "server" (the other end of an in-memory
// pipe) that sends exactly the message shapes Microsoft's real endpoint
// is documented (by the edge-tts project's reverse engineering) to send:
// a turn.start text frame, one or more binary "Path:audio\r\n\r\n<mp3
// bytes>" frames (each prefixed by a 2-byte header length), and a
// closing "Path:turn.end" text frame. This is the one piece of
// edgetts.go that's fully exercisable without the real Microsoft
// endpoint, and it's also the piece most likely to have an off-by-one
// bug (header-length parsing), so it's worth pinning down.
func TestCollectAudio_ParsesBinaryFramesAndStopsAtTurnEnd(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()

	client := &wsConn{conn: clientSide, br: bufio.NewReader(clientSide)}

	wantAudio := append([]byte("FAKE-MP3-CHUNK-1-"), []byte("FAKE-MP3-CHUNK-2-")...)

	serverErr := make(chan error, 1)
	go func() {
		if err := writeUnmaskedFrameForTest(serverSide, opText, []byte("X-Timestamp:now\r\nPath:turn.start\r\n\r\n")); err != nil {
			serverErr <- err
			return
		}
		// Two separate binary audio frames, split arbitrarily, to prove
		// collectAudio concatenates across frames rather than assuming
		// one frame holds everything.
		for _, chunk := range [][]byte{
			[]byte("FAKE-MP3-CHUNK-1-"),
			[]byte("FAKE-MP3-CHUNK-2-"),
		} {
			header := "Path:audio\r\n\r\n"
			frame := make([]byte, 0, 2+len(header)+len(chunk))
			frame = append(frame, byte(len(header)>>8), byte(len(header)))
			frame = append(frame, []byte(header)...)
			frame = append(frame, chunk...)
			if err := writeUnmaskedFrameForTest(serverSide, opBinary, frame); err != nil {
				serverErr <- err
				return
			}
		}
		// An audio.metadata text frame in between, which real servers
		// send and which must be ignored (not mistaken for turn.end or
		// audio).
		if err := writeUnmaskedFrameForTest(serverSide, opText, []byte("Path:audio.metadata\r\n\r\n{}")); err != nil {
			serverErr <- err
			return
		}
		if err := writeUnmaskedFrameForTest(serverSide, opText, []byte("X-Timestamp:now\r\nPath:turn.end\r\n\r\n")); err != nil {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gotAudio, err := collectAudio(ctx, client)
	if err != nil {
		t.Fatalf("collectAudio: %v", err)
	}
	if string(gotAudio) != string(wantAudio) {
		t.Fatalf("audio mismatch:\ngot:  %q\nwant: %q", gotAudio, wantAudio)
	}

	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatalf("server goroutine error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server goroutine")
	}
}

// TestCollectAudio_NoAudioIsAnError guards against silently returning
// an empty MP3 (which downstream ffmpeg steps would treat as a valid
// but broken file) if the server ends the turn without ever sending
// audio.
func TestCollectAudio_NoAudioIsAnError(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer clientSide.Close()
	defer serverSide.Close()

	client := &wsConn{conn: clientSide, br: bufio.NewReader(clientSide)}

	go func() {
		_ = writeUnmaskedFrameForTest(serverSide, opText, []byte("Path:turn.start\r\n\r\n"))
		_ = writeUnmaskedFrameForTest(serverSide, opText, []byte("Path:turn.end\r\n\r\n"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := collectAudio(ctx, client)
	if err == nil {
		t.Fatal("expected an error when turn.end arrives with no audio, got nil")
	}
}
