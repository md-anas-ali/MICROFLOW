package nodes

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"microflow/internal/model"
)

type credStub struct{}

func (credStub) Resolve(ctx context.Context, wf, name string) (map[string]string, error) {
	return map[string]string{"accessToken": "t"}, nil
}

// fake in-memory spreadsheet speaking just the Sheets v4 calls used by the executor.
type fakeSheet struct{ rows [][]any }

func (f *fakeSheet) RoundTrip(r *http.Request) (*http.Response, error) {
	path := r.URL.EscapedPath()
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
	}
	respond := func(v any) (*http.Response, error) {
		b, _ := json.Marshal(v)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(b)), Header: http.Header{}, Request: r}, nil
	}
	idx := strings.Index(path, "/values/")
	if strings.HasSuffix(path, "/values:batchUpdate") {
		var req struct {
			Data []struct {
				Range  string  `json:"range"`
				Values [][]any `json:"values"`
			} `json:"data"`
		}
		json.Unmarshal(body, &req)
		for _, d := range req.Data {
			ref := d.Range[strings.Index(d.Range, "!")+1:]
			col := 0
			i := 0
			for i < len(ref) && ref[i] >= 'A' && ref[i] <= 'Z' {
				col = col*26 + int(ref[i]-'A') + 1
				i++
			}
			col--
			row, _ := strconv.Atoi(ref[i:])
			for len(f.rows) < row {
				f.rows = append(f.rows, nil)
			}
			for len(f.rows[row-1]) <= col {
				f.rows[row-1] = append(f.rows[row-1], "")
			}
			f.rows[row-1][col] = d.Values[0][0]
		}
		return respond(map[string]any{})
	}
	ref, _ := url.PathUnescape(path[idx+len("/values/"):])
	ref = strings.TrimSuffix(ref, ":append")
	part := ref[strings.Index(ref, "!")+1:]
	switch {
	case r.Method == "GET" && part == "1:1":
		if len(f.rows) == 0 {
			return respond(map[string]any{})
		}
		return respond(map[string]any{"values": [][]any{f.rows[0]}})
	case r.Method == "GET":
		return respond(map[string]any{"values": f.rows})
	case r.Method == "PUT": // header write A1:..1
		var req struct{ Values [][]any }
		json.Unmarshal(body, &req)
		if len(f.rows) == 0 {
			f.rows = [][]any{nil}
		}
		f.rows[0] = req.Values[0]
		return respond(map[string]any{})
	case r.Method == "POST": // append
		var req struct{ Values [][]any }
		json.Unmarshal(body, &req)
		f.rows = append(f.rows, req.Values...)
		return respond(map[string]any{})
	}
	return &http.Response{StatusCode: 400, Body: io.NopCloser(strings.NewReader("bad")), Header: http.Header{}}, nil
}

func TestGoogleSheets_HeaderAwareAppendUpdateRead(t *testing.T) {
	t.Setenv("GOOGLE_SHEETS_URL", "https://docs.google.com/spreadsheets/d/abcdefghijklmnopqrstuvwxyz0123456789/edit")
	fake := &fakeSheet{rows: [][]any{{"poolKey", "cycle", "status", "runId"}}}
	old := http.DefaultClient.Transport
	http.DefaultClient.Transport = fake
	defer func() { http.DefaultClient.Transport = old }()

	e := &GoogleSheetsExecutor{Creds: credStub{}, Service: "sheets"}
	rc := newTestRunContext()
	rc.Workflow = &model.Workflow{ID: "w"}
	app := &model.Node{Name: "A", Parameters: map[string]any{"operation": "append", "range": "UsedTopics!A:Z"}}
	in := model.NodeOutput{{{JSON: map[string]any{"status": "reserved", "runId": "r1", "poolKey": "k1", "cycle": float64(1), "title": "T1"}}}}
	if _, err := e.Execute(context.Background(), rc, app, in); err != nil {
		t.Fatal(err)
	}
	in2 := model.NodeOutput{{{JSON: map[string]any{"status": "reserved", "runId": "r2", "poolKey": "k2", "cycle": float64(1)}}}}
	if _, err := e.Execute(context.Background(), rc, app, in2); err != nil {
		t.Fatal(err)
	}
	upd := &model.Node{Name: "U", Parameters: map[string]any{"operation": "update", "range": "UsedTopics!A:Z", "matchingColumn": "runId"}}
	if _, err := e.Execute(context.Background(), rc, upd, model.NodeOutput{{{JSON: map[string]any{"runId": "r1", "status": "uploaded", "videoId": "V1"}}}}); err != nil {
		t.Fatal(err)
	}
	// unknown runId => upsert append
	if _, err := e.Execute(context.Background(), rc, upd, model.NodeOutput{{{JSON: map[string]any{"runId": "r9", "status": "uploaded"}}}}); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(fake.rows)
	t.Logf("rows=%s", b)
	hdr := fake.rows[0]
	if len(hdr) != 6 {
		t.Fatalf("header=%v", hdr)
	}
	get := func(row int, name string) any {
		for i, h := range hdr {
			if h == name && i < len(fake.rows[row]) {
				return fake.rows[row][i]
			}
		}
		return nil
	}
	if get(1, "poolKey") != "k1" || get(1, "status") != "uploaded" || get(1, "videoId") != "V1" || get(1, "title") != "T1" {
		t.Fatalf("row1 wrong: %v", fake.rows[1])
	}
	if get(2, "status") != "reserved" || get(2, "poolKey") != "k2" {
		t.Fatalf("row2 wrong: %v", fake.rows[2])
	}
	if get(3, "runId") != "r9" || get(3, "status") != "uploaded" {
		t.Fatalf("row3 wrong: %v", fake.rows[3])
	}
	// read back keyed by header, header row intact
	rd := &model.Node{Name: "R", Parameters: map[string]any{"range": "UsedTopics!A:Z"}}
	out, err := e.Execute(context.Background(), rc, rd, model.NodeOutput{{{JSON: map[string]any{}}}})
	if err != nil || len(out[0]) != 3 || out[0][0].JSON["videoId"] != "V1" {
		t.Fatalf("read: %v %v", out, err)
	}
	// empty sheet (header only) still yields one item
	fake.rows = fake.rows[:1]
	out, err = e.Execute(context.Background(), rc, rd, model.NodeOutput{{{JSON: map[string]any{}}}})
	if err != nil || len(out[0]) != 1 {
		t.Fatalf("empty read: %v %v", out, err)
	}
}
