package nodes

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"microflow/internal/engine"
	"microflow/internal/model"
	"microflow/internal/parser"
)

// Runs the social branch of examples/My_workflow_with_social.json through the real
// engine (real expressions, real Code node, real file read) against fake APIs.
func TestUserWorkflowSocialBranchE2E(t *testing.T) {
	raw, err := os.ReadFile("../../examples/My_workflow_with_social.json")
	if err != nil {
		t.Skip("example missing")
	}
	pr, err := parser.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	wf := pr.Workflow
	keep := map[string]bool{"Social Captions": true, "Read Final Video (Social)": true, "Post to Facebook Reels": true, "Post to Instagram Reels": true,
		"Post to TikTok": true, "Post to Dailymotion": true, "Rumble Manual Pack": true, "Snapchat Spotlight Manual Pack": true}
	for name := range wf.Nodes {
		if !keep[name] {
			delete(wf.Nodes, name)
		}
	}
	for src := range wf.Connections {
		if !keep[src] {
			delete(wf.Connections, src)
		}
	}
	// stand-ins for the upstream part of the workflow
	wf.Nodes["Trigger"] = &model.Node{ID: "t", Name: "Trigger", Type: model.TypeManualTrigger, Parameters: map[string]any{}}
	wf.Nodes["Build YT Meta"] = &model.Node{ID: "b", Name: "Build YT Meta", Type: model.TypeCode, Parameters: map[string]any{
		"jsCode": "return {json:{title:'Test Title',description:'Hello world.\\n\\nNarration is AI-generated.',tags:['shorts','science','physics']}};"}}
	wf.Connections["Trigger"] = []model.Connection{{SourceName: "Trigger", TargetName: "Build YT Meta"}}
	wf.Connections["Build YT Meta"] = []model.Connection{{SourceName: "Build YT Meta", TargetName: "Social Captions"}}

	var fbDesc atomic.Value
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/upload") {
			io.Copy(io.Discard, r.Body)
			w.Write([]byte(`{"success":true}`))
			return
		}
		r.ParseForm()
		if r.PostForm.Get("upload_phase") == "start" {
			w.Write([]byte(`{"video_id":"V1","upload_url":"` + srv.URL + `/upload"}`))
			return
		}
		fbDesc.Store(r.PostForm.Get("description"))
		w.Write([]byte(`{"success":true}`))
	}))
	defer srv.Close()
	old := graphBase
	graphBase = srv.URL
	defer func() { graphBase = old }()

	if err := os.WriteFile("/tmp/final.mp4", make([]byte, 2048), 0o600); err != nil {
		t.Fatal(err)
	}
	rc := &engine.RunContext{Workflow: wf, Execution: &model.Execution{ID: "e"}, ScratchDir: t.TempDir(), HeavyWorkGate: make(chan struct{}, 1),
		GlobalEnv: map[string]string{"FB_PAGE_ID": "PAGE", "FB_PAGE_TOKEN": "tok"}} // only Facebook configured
	eng := engine.New(DefaultRegistry(Deps{ScratchRoot: rc.ScratchDir}))
	ex, err := eng.Run(context.Background(), rc, "Trigger", model.NodeOutput{{{JSON: map[string]any{}}}})
	if err != nil {
		t.Fatalf("run: %v (%s)", err, ex.Error)
	}
	res := map[string]model.Item{}
	for _, nr := range ex.NodeRuns {
		if len(nr.Output) > 0 && len(nr.Output[0]) > 0 {
			res[nr.NodeName] = nr.Output[0][0]
		}
		if nr.Error != "" {
			t.Errorf("node %s error: %s", nr.NodeName, nr.Error)
		}
	}
	if res["Post to Facebook Reels"].JSON["id"] != "V1" {
		t.Fatalf("facebook not posted: %v", res["Post to Facebook Reels"].JSON)
	}
	for _, n := range []string{"Post to Instagram Reels", "Post to TikTok", "Post to Dailymotion"} {
		if res[n].JSON["skipped"] != true {
			t.Errorf("%s should be skipped: %v", n, res[n].JSON)
		}
	}
	if res["Rumble Manual Pack"].JSON["manualUploadRequired"] != true {
		t.Errorf("rumble pack: %v", res["Rumble Manual Pack"].JSON)
	}
	d, _ := fbDesc.Load().(string)
	if !strings.Contains(d, "Test Title") || !strings.Contains(d, "#science") || strings.Contains(d, "#shorts") || !strings.Contains(d, "AI-generated") {
		t.Errorf("facebook description wrong: %q", d)
	}
}
