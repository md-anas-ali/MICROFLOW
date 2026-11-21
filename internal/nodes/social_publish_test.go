package nodes

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"microflow/internal/engine"
	"microflow/internal/model"
)

func socialRC(t *testing.T) *engine.RunContext {
	return &engine.RunContext{Execution: &model.Execution{ID: "x"}, ScratchDir: t.TempDir(), HeavyWorkGate: make(chan struct{}, 1)}
}

func socialInput(t *testing.T, size int) model.NodeOutput {
	f := filepath.Join(t.TempDir(), "v.mp4")
	if err := os.WriteFile(f, make([]byte, size), 0o600); err != nil {
		t.Fatal(err)
	}
	return model.NodeOutput{{{JSON: map[string]any{}, Binary: map[string]model.BinaryRef{"data": {FileName: f, MimeType: "video/mp4", SizeBytes: int64(size)}}}}}
}

func runSocial(t *testing.T, params map[string]any, in model.NodeOutput) (map[string]any, error) {
	node := &model.Node{Name: "Social", Type: model.TypeSocialPublish, Parameters: params}
	out, err := (&SocialPublishExecutor{}).Execute(context.Background(), socialRC(t), node, in)
	if err != nil {
		return nil, err
	}
	return out[0][0].JSON, nil
}

func TestFacebookReelsFlow(t *testing.T) {
	var mu sync.Mutex
	var phases []string
	var uploaded int64
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/upload") {
			if r.Header.Get("file_size") != "1234" || !strings.HasPrefix(r.Header.Get("Authorization"), "OAuth ") {
				t.Errorf("bad upload headers: %v", r.Header)
			}
			n, _ := io.Copy(io.Discard, r.Body)
			uploaded = n
			w.Write([]byte(`{"success":true}`))
			return
		}
		r.ParseForm()
		phases = append(phases, r.PostForm.Get("upload_phase"))
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("token not in header")
		}
		if r.PostForm.Get("upload_phase") == "start" {
			w.Write([]byte(`{"video_id":"V1","upload_url":"` + srv.URL + `/upload"}`))
			return
		}
		if r.PostForm.Get("video_state") != "PUBLISHED" {
			t.Errorf("state %q", r.PostForm.Get("video_state"))
		}
		w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()
	old := graphBase
	graphBase = srv.URL
	defer func() { graphBase = old }()

	res, err := runSocial(t, map[string]any{"platform": "facebookReels", "pageId": "P", "accessToken": "tok", "description": "hi"}, socialInput(t, 1234))
	if err != nil {
		t.Fatal(err)
	}
	if res["id"] != "V1" || uploaded != 1234 || strings.Join(phases, ",") != "start,finish" {
		t.Fatalf("res=%v uploaded=%d phases=%v", res, uploaded, phases)
	}
}

func TestInstagramReelsFlow(t *testing.T) {
	polls := 0
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/IG/media":
			r.ParseForm()
			if r.PostForm.Get("media_type") != "REELS" || r.PostForm.Get("upload_type") != "resumable" {
				t.Errorf("bad form %v", r.PostForm)
			}
			w.Write([]byte(`{"id":"C1","uri":"` + srv.URL + `/rupload"}`))
		case r.URL.Path == "/rupload":
			io.Copy(io.Discard, r.Body)
			w.Write([]byte(`{"success":true}`))
		case r.URL.Path == "/C1":
			polls++
			code := "IN_PROGRESS"
			if polls >= 2 {
				code = "FINISHED"
			}
			w.Write([]byte(`{"status_code":"` + code + `"}`))
		case r.URL.Path == "/IG/media_publish":
			w.Write([]byte(`{"id":"M9"}`))
		case r.URL.Path == "/M9":
			w.Write([]byte(`{"permalink":"https://instagram.com/reel/abc"}`))
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	old := graphBase
	graphBase = srv.URL
	defer func() { graphBase = old }()

	res, err := runSocial(t, map[string]any{"platform": "instagramReels", "igUserId": "IG", "accessToken": "tok", "caption": "c"}, socialInput(t, 50))
	if err != nil {
		t.Fatal(err)
	}
	if res["id"] != "M9" || res["url"] != "https://instagram.com/reel/abc" || polls < 2 {
		t.Fatalf("res=%v polls=%d", res, polls)
	}
}

func TestTikTokChunkedUpload(t *testing.T) {
	var ranges []string
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/post/publish/video/init/":
			var b map[string]any
			json.NewDecoder(r.Body).Decode(&b)
			si := b["source_info"].(map[string]any)
			if si["total_chunk_count"].(float64) != 2 { // 25 MB / 10 MB chunk -> 2, last absorbs remainder
				t.Errorf("chunks %v", si)
			}
			w.Write([]byte(`{"data":{"publish_id":"PID","upload_url":"` + srv.URL + `/put"}}`))
		case "/put":
			ranges = append(ranges, r.Header.Get("Content-Range"))
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(201)
		case "/v2/post/publish/status/fetch/":
			w.Write([]byte(`{"data":{"status":"PUBLISH_COMPLETE"}}`))
		}
	}))
	defer srv.Close()
	old := tiktokBase
	tiktokBase = srv.URL
	defer func() { tiktokBase = old }()

	size := 25 << 20
	res, err := runSocial(t, map[string]any{"platform": "tiktok", "accessToken": "tok", "title": "t"}, socialInput(t, size))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"bytes 0-10485759/26214400", "bytes 10485760-26214399/26214400"}
	if res["status"] != "PUBLISH_COMPLETE" || strings.Join(ranges, "|") != strings.Join(want, "|") {
		t.Fatalf("res=%v ranges=%v", res, ranges)
	}
}

func TestDailymotionFlow(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file/upload":
			w.Write([]byte(`{"upload_url":"` + srv.URL + `/up"}`))
		case "/up":
			if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
				t.Errorf("not multipart")
			}
			io.Copy(io.Discard, r.Body)
			w.Write([]byte(`{"url":"https://files/x.mp4"}`))
		case "/me/videos":
			r.ParseForm()
			if r.PostForm.Get("url") != "https://files/x.mp4" || r.PostForm.Get("is_created_for_kids") != "false" {
				t.Errorf("bad form %v", r.PostForm)
			}
			w.Write([]byte(`{"id":"x123"}`))
		}
	}))
	defer srv.Close()
	old := dailymotionBase
	dailymotionBase = srv.URL
	defer func() { dailymotionBase = old }()

	res, err := runSocial(t, map[string]any{"platform": "dailymotion", "accessToken": "tok", "title": "T"}, socialInput(t, 300))
	if err != nil {
		t.Fatal(err)
	}
	if res["id"] != "x123" {
		t.Fatalf("res=%v", res)
	}
}

func TestManualPacksNeverClaimUpload(t *testing.T) {
	long := strings.Repeat("a", 200)
	res, err := runSocial(t, map[string]any{"platform": "snapchatSpotlight", "title": "T", "caption": long}, socialInput(t, 10))
	if err != nil {
		t.Fatal(err)
	}
	if res["manualUploadRequired"] != true || res["warnings"] == nil {
		t.Fatalf("res=%v", res)
	}
	res, err = runSocial(t, map[string]any{"platform": "rumble", "title": "T"}, socialInput(t, 10))
	if err != nil || res["manualUploadRequired"] != true {
		t.Fatalf("res=%v err=%v", res, err)
	}
}

func TestMissingTokenIsPermanent(t *testing.T) {
	_, err := runSocial(t, map[string]any{"platform": "facebookReels", "pageId": "P"}, socialInput(t, 10))
	if err == nil || !engine.IsPermanent(err) {
		t.Fatalf("want permanent error, got %v", err)
	}
}

func TestSkipIfUnconfigured(t *testing.T) {
	res, err := runSocial(t, map[string]any{"platform": "tiktok", "accessToken": "", "skipIfUnconfigured": "true"}, socialInput(t, 10))
	if err != nil || res["skipped"] != true {
		t.Fatalf("res=%v err=%v", res, err)
	}
}
