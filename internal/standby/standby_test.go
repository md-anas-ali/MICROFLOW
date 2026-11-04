package standby

import (
	"net"
	"net/http"
	"testing"
)

func TestStandbyHealthAndRebind(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	s := serve(ln)

	for path, want := range map[string]int{
		"/": 200, "/healthz": 200, "/health": 200,
		"/webhook/abc": 503, "/api/workflows": 503,
	} {
		resp, err := http.Get("http://" + addr + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s = %d, want %d", path, resp.StatusCode, want)
		}
		if want == 503 && resp.Header.Get("Retry-After") == "" {
			t.Errorf("GET %s: missing Retry-After", path)
		}
	}
	resp, err := http.Post("http://"+addr+"/healthz", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Errorf("POST /healthz = %d, want 503", resp.StatusCode)
	}

	s.Close()
	// The port must be free immediately: the real server binds it next.
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("port not released after Close: %v", err)
	}
	ln2.Close()
	s.Close() // idempotent
	var nilS *Server
	nilS.Close() // nil-safe
}

func TestStartUnbindableAddrReturnsNil(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if s := Start(ln.Addr().String()); s != nil {
		s.Close()
		t.Fatal("expected nil when the address is already in use")
	}
}
