package nodes

import (
	"reflect"
	"testing"

	"microflow/internal/expr"
	"microflow/internal/model"
)

func uploadNode(opts map[string]any) *model.Node {
	return &model.Node{Name: "YouTube Upload (Scheduled)", Parameters: map[string]any{"options": opts}}
}

func TestUploadOptionsTagsLanguageLocation(t *testing.T) {
	ec := expr.Context{
		NodeOutputs: map[string]map[string]any{"Build YT Meta": {
			"tags": []any{"shorts", "science", "Shorts", "space facts", "a<b>"},
			"lat":  float64(35.6762), "lng": float64(139.6503), "place": "Japan",
		}},
	}
	snip := map[string]any{}
	rec := applyYouTubeUploadOptions(uploadNode(map[string]any{
		"tags":                `{{ $node["Build YT Meta"].json.tags }}`,
		"defaultLanguage":     "en",
		"latitude":            `{{ $node["Build YT Meta"].json.lat }}`,
		"longitude":           `{{ $node["Build YT Meta"].json.lng }}`,
		"locationDescription": `{{ $node["Build YT Meta"].json.place }}`,
	}), ec, snip)

	if want := []string{"shorts", "science", "space facts", "ab"}; !reflect.DeepEqual(snip["tags"], want) {
		t.Fatalf("tags = %#v, want %#v", snip["tags"], want)
	}
	if snip["defaultLanguage"] != "en" || snip["defaultAudioLanguage"] != "en" {
		t.Fatalf("language not set: %#v", snip)
	}
	loc := rec["location"].(map[string]any)
	if loc["latitude"] != 35.6762 || loc["longitude"] != 139.6503 || rec["locationDescription"] != "Japan" {
		t.Fatalf("bad recordingDetails: %#v", rec)
	}
	if got := videoInsertParts(map[string]any{"recordingDetails": rec}); got != "snippet,status,recordingDetails" {
		t.Fatalf("parts = %q", got)
	}
}

func TestUploadOptionsBadLocationIsSkippedNotFatal(t *testing.T) {
	snip := map[string]any{}
	for _, o := range []map[string]any{
		{"latitude": "abc", "longitude": "10"},
		{"latitude": 95.0, "longitude": 10.0},
		{"latitude": "", "longitude": ""},
		{},
	} {
		if rec := applyYouTubeUploadOptions(uploadNode(o), expr.Context{}, snip); rec != nil {
			t.Fatalf("expected nil for %#v, got %#v", o, rec)
		}
	}
	if got := videoInsertParts(map[string]any{}); got != "snippet,status" {
		t.Fatalf("parts = %q", got)
	}
}

func TestCleanTagsCommaStringAndLimit(t *testing.T) {
	if got := cleanYouTubeTags("a, b ,#c,,a"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Fatalf("got %#v", got)
	}
	long := make([]any, 0, 100)
	for i := 0; i < 100; i++ {
		long = append(long, "tagnumber"+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	total := 0
	for _, s := range cleanYouTubeTags(long) {
		total += len(s) + 1
	}
	if total > 500 {
		t.Fatalf("tags exceed 500: %d", total)
	}
}
