package edgetts

// A minimal RFC 6455 WebSocket client -- just enough to talk to a single
// endpoint that sends text/binary frames and expects masked client
// frames, with no permessage-deflate or other extensions. Written by
// hand (instead of pulling gorilla/websocket or golang.org/x/net/websocket)
// so this package adds zero new go.mod dependencies, matching the
// project's existing pattern (see vault/oauth.go's comment on avoiding
// golang.org/x/oauth2 for the same reason).
//
// UNVERIFIED: never exercised against a live server in the sandbox this
// was written in (speech.platform.bing.com is outside the sandbox's
// network allowlist) -- see edgetts.go's package doc for the same
// caveat and why it's safe to ship anyway.

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type wsConn struct {
	conn net.Conn
	br   *bufio.Reader
}

// wsDial performs the TCP+TLS connect and the HTTP Upgrade handshake.
// extraHeaders lets the caller add Origin/User-Agent/etc, since the
// target server here (like many browser-facing WS endpoints) checks
// those before accepting the upgrade.
func wsDial(ctx context.Context, host, path string, extraHeaders http.Header) (*wsConn, error) {
	dialer := &net.Dialer{Timeout: 15 * time.Second}
	rawConn, err := dialer.DialContext(ctx, "tcp", host+":443")
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	tlsConn := tls.Client(rawConn, &tls.Config{ServerName: host})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		rawConn.Close()
		return nil, fmt.Errorf("tls handshake: %w", err)
	}

	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		tlsConn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	var req strings.Builder
	fmt.Fprintf(&req, "GET %s HTTP/1.1\r\n", path)
	fmt.Fprintf(&req, "Host: %s\r\n", host)
	req.WriteString("Upgrade: websocket\r\n")
	req.WriteString("Connection: Upgrade\r\n")
	fmt.Fprintf(&req, "Sec-WebSocket-Key: %s\r\n", key)
	req.WriteString("Sec-WebSocket-Version: 13\r\n")
	for k, vs := range extraHeaders {
		for _, v := range vs {
			fmt.Fprintf(&req, "%s: %s\r\n", k, v)
		}
	}
	req.WriteString("\r\n")

	if _, err := io.WriteString(tlsConn, req.String()); err != nil {
		tlsConn.Close()
		return nil, fmt.Errorf("send handshake: %w", err)
	}

	br := bufio.NewReader(tlsConn)
	resp, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil {
		tlsConn.Close()
		return nil, fmt.Errorf("read handshake response: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		tlsConn.Close()
		return nil, fmt.Errorf("handshake: server returned %s (expected 101)", resp.Status)
	}
	wantAccept := wsAcceptKey(key)
	if resp.Header.Get("Sec-WebSocket-Accept") != wantAccept {
		tlsConn.Close()
		return nil, errors.New("handshake: Sec-WebSocket-Accept mismatch")
	}

	return &wsConn{conn: tlsConn, br: br}, nil
}

func wsAcceptKey(clientKey string) string {
	h := sha1.New()
	io.WriteString(h, clientKey+wsGUID)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

const (
	opText   = 0x1
	opBinary = 0x2
	opClose  = 0x8
)

// writeFrame sends one unfragmented, masked (client->server per RFC6455
// 5.1) frame.
func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	var header []byte
	header = append(header, 0x80|opcode) // FIN + opcode

	n := len(payload)
	switch {
	case n <= 125:
		header = append(header, 0x80|byte(n)) // MASK bit set
	case n <= 65535:
		header = append(header, 0x80|126, byte(n>>8), byte(n))
	default:
		ext := make([]byte, 8)
		for i := 7; i >= 0; i-- {
			ext[i] = byte(n)
			n >>= 8
		}
		header = append(header, 0x80|127)
		header = append(header, ext...)
	}

	mask := make([]byte, 4)
	if _, err := rand.Read(mask); err != nil {
		return err
	}
	header = append(header, mask...)

	masked := make([]byte, len(payload))
	for i, b := range payload {
		masked[i] = b ^ mask[i%4]
	}

	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(masked)
	return err
}

func (c *wsConn) writeText(s string) error   { return c.writeFrame(opText, []byte(s)) }
func (c *wsConn) writeBinary(b []byte) error { return c.writeFrame(opBinary, b) }

// wsMessage is one fully-reassembled (continuation frames merged)
// message from the server.
type wsMessage struct {
	opcode  byte
	payload []byte
}

// readMessage reads frames until a FIN frame completes a message.
// Server->client frames are never masked (RFC6455 5.1), so no unmasking
// is needed here.
func (c *wsConn) readMessage() (wsMessage, error) {
	var (
		msgOpcode byte
		buf       []byte
	)
	for {
		first, err := c.br.ReadByte()
		if err != nil {
			return wsMessage{}, err
		}
		second, err := c.br.ReadByte()
		if err != nil {
			return wsMessage{}, err
		}
		fin := first&0x80 != 0
		opcode := first & 0x0f
		payloadLen := int64(second & 0x7f)

		switch payloadLen {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return wsMessage{}, err
			}
			payloadLen = int64(ext[0])<<8 | int64(ext[1])
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return wsMessage{}, err
			}
			payloadLen = 0
			for _, b := range ext {
				payloadLen = payloadLen<<8 | int64(b)
			}
		}
		// second&0x80 (server MASK bit) should be 0; if a non-conformant
		// server sets it we'd need the mask key here too, but that's not
		// expected from this endpoint so it's treated as an error.
		if second&0x80 != 0 {
			return wsMessage{}, errors.New("unexpected masked frame from server")
		}

		chunk := make([]byte, payloadLen)
		if _, err := io.ReadFull(c.br, chunk); err != nil {
			return wsMessage{}, err
		}

		if opcode == 0x0 { // continuation
			buf = append(buf, chunk...)
		} else {
			msgOpcode = opcode
			buf = append(buf[:0:0], chunk...) // start fresh
		}

		if opcode == opClose {
			return wsMessage{opcode: opClose, payload: buf}, nil
		}
		if fin {
			return wsMessage{opcode: msgOpcode, payload: buf}, nil
		}
	}
}

func (c *wsConn) Close() error {
	_ = c.writeFrame(opClose, nil)
	return c.conn.Close()
}
