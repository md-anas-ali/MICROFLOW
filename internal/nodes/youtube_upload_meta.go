package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"microflow/internal/expr"
	"microflow/internal/model"
)

// applyYouTubeUploadOptions copies the optional upload metadata from the
// node's "options" object into the videos.insert request body:
//
//	options.tags                 -> snippet.tags (array, or comma-separated string)
//	options.defaultLanguage      -> snippet.defaultLanguage
//	options.defaultAudioLanguage -> snippet.defaultAudioLanguage (falls back to defaultLanguage;
//	                                YouTube Auto Dubbing needs the ORIGINAL audio language)
//	options.latitude/longitude   -> recordingDetails.location (the Studio "Location" field)
//	options.locationDescription  -> recordingDetails.locationDescription
//
// Every value may be a {{ expression }}. Nothing here can fail an upload: a bad or
// missing value is simply skipped, so a metadata problem never costs a finished video.
// The returned map is the "recordingDetails" part, or nil when no valid location was given.
func applyYouTubeUploadOptions(node *model.Node, ec expr.Context, snip map[string]any) map[string]any {
	opts, _ := node.Parameters["options"].(map[string]any)
	if opts == nil {
		return nil
	}
	eval := func(key string) any {
		raw, ok := opts[key]
		if !ok || raw == nil {
			return nil
		}
		if s, isStr := raw.(string); isStr {
			v, err := expr.EvalValue(s, ec)
			if err != nil {
				return nil
			}
			return v
		}
		return raw
	}

	if tags := cleanYouTubeTags(eval("tags")); len(tags) > 0 {
		snip["tags"] = tags
	}
	lang := strings.TrimSpace(fmt.Sprint(orEmpty(eval("defaultLanguage"))))
	if lang != "" {
		snip["defaultLanguage"] = lang
	}
	audio := strings.TrimSpace(fmt.Sprint(orEmpty(eval("defaultAudioLanguage"))))
	if audio == "" {
		audio = lang
	}
	if audio != "" {
		snip["defaultAudioLanguage"] = audio
	}

	lat, okLat := uploadFloat(eval("latitude"))
	lng, okLng := uploadFloat(eval("longitude"))
	if !okLat || !okLng || lat < -90 || lat > 90 || lng < -180 || lng > 180 {
		return nil
	}
	rec := map[string]any{"location": map[string]any{"latitude": lat, "longitude": lng}}
	if d := strings.TrimSpace(fmt.Sprint(orEmpty(eval("locationDescription")))); d != "" {
		if len(d) > 100 {
			d = d[:100]
		}
		rec["locationDescription"] = d
	}
	return rec
}

func orEmpty(v any) any {
	if v == nil {
		return ""
	}
	return v
}

func uploadFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(s, 64)
		return f, err == nil
	}
	return 0, false
}

// cleanYouTubeTags accepts []any / []string / a JSON-array string / a comma-separated
// string and returns de-duplicated tags that respect YouTube's limits (no < or >,
// <= 500 characters in total counting the quotes YouTube adds around tags with spaces).
func cleanYouTubeTags(v any) []string {
	var raw []string
	switch t := v.(type) {
	case nil:
		return nil
	case []string:
		raw = t
	case []any:
		for _, x := range t {
			raw = append(raw, fmt.Sprint(x))
		}
	case string:
		s := strings.TrimSpace(t)
		if strings.HasPrefix(s, "[") {
			var arr []string
			if json.Unmarshal([]byte(s), &arr) == nil {
				raw = arr
				break
			}
		}
		raw = strings.Split(s, ",")
	default:
		return nil
	}
	seen := map[string]bool{}
	var out []string
	total := 0
	for _, tag := range raw {
		tag = strings.TrimSpace(strings.NewReplacer("<", "", ">", "", "#", "").Replace(tag))
		if tag == "" || seen[strings.ToLower(tag)] {
			continue
		}
		cost := len([]rune(tag)) + 1 // separator
		if strings.Contains(tag, " ") {
			cost += 2 // YouTube counts the surrounding quotes
		}
		if total+cost > 500 {
			break
		}
		seen[strings.ToLower(tag)] = true
		out = append(out, tag)
		total += cost
	}
	return out
}

// ensureVideoLocation makes sure the Studio "Location" really is on the video. It reads the
// video back; when the location is missing or different it writes it with videos.update
// (part=recordingDetails) and reads it back once more. It never returns an error to the caller:
// the second return value is a short human-readable result for the run log.
func ensureVideoLocation(ctx context.Context, token, videoID string, rec map[string]any, operationID string) (bool, string) {
	loc, _ := rec["location"].(map[string]any)
	wantLat, _ := loc["latitude"].(float64)
	wantLng, _ := loc["longitude"].(float64)
	getURL := "https://www.googleapis.com/youtube/v3/videos?part=recordingDetails&id=" + url.QueryEscape(videoID)

	read := func() (bool, error) {
		var out struct {
			Items []struct {
				RecordingDetails struct {
					Location struct {
						Latitude  float64 `json:"latitude"`
						Longitude float64 `json:"longitude"`
					} `json:"location"`
				} `json:"recordingDetails"`
			} `json:"items"`
		}
		if err := googleAPICall(ctx, "GET", getURL, token, nil, &out, operationID); err != nil {
			return false, err
		}
		if len(out.Items) == 0 {
			return false, nil
		}
		l := out.Items[0].RecordingDetails.Location
		return math.Abs(l.Latitude-wantLat) < 0.01 && math.Abs(l.Longitude-wantLng) < 0.01, nil
	}

	if ok, err := read(); err == nil && ok {
		return true, "location set on upload"
	}
	body, _ := json.Marshal(map[string]any{"id": videoID, "recordingDetails": rec})
	if err := googleAPICall(ctx, "PUT", "https://www.googleapis.com/youtube/v3/videos?part=recordingDetails", token, body, nil, operationID); err != nil {
		return false, "location update failed: " + err.Error()
	}
	if ok, err := read(); err != nil {
		return false, "location written but could not be verified: " + err.Error()
	} else if !ok {
		return false, "location written but YouTube still reports none"
	}
	return true, "location set with videos.update"
}
