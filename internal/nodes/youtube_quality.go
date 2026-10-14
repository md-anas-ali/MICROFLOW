package nodes

// "quality" operation of the analytics node: the Script Quality Gate.
//
// It runs locally on one finished script (no network, no Google credential, no
// API quota), scores the hook / title / thumbnail text / pacing / metadata with
// the deterministic engine in internal/yta, swaps in a better alternative the
// writer already produced when it is clearly better and safe, cleans the
// SEO metadata, and reports every decision. It can never fail the pipeline: on
// any problem the item passes through untouched with ytQuality.status
// "skipped".

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"microflow/internal/engine"
	"microflow/internal/expr"
	"microflow/internal/model"
	"microflow/internal/yta"
)

func ytaQualityEnvBool(rc *engine.RunContext, key string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(rc.Env(key))) {
	case "false", "0", "off", "no":
		return false
	case "true", "1", "on", "yes":
		return true
	}
	return def
}

func anyStrings(v any) []string {
	var out []string
	switch x := v.(type) {
	case []string:
		out = append(out, x...)
	case []any:
		for _, e := range x {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
	case string:
		for _, p := range strings.Split(x, "||") {
			if p = strings.TrimSpace(p); p != "" && p != "<nil>" {
				out = append(out, p)
			}
		}
	}
	return out
}

// ytaQualityOwns reports whether an experiment (or an adopted setting) owns
// the hook / the title for this execution, in which case the gate must not
// rewrite it or the experiment would be contaminated.
func ytaQualityOwns(ctx context.Context, rc *engine.RunContext, execID string) (keepHook, keepTitle bool) {
	if rc.StaticData == nil {
		return false, false
	}
	var g yta.Guidance
	var ok bool
	if err := ytaRead(ctx, rc, func(st *yta.State) { g, ok = st.Pinned(execID) }); err != nil || !ok {
		return false, false
	}
	owns := func(setting string) bool {
		if g.Setting == setting && (g.Arm == "treatment" || g.Arm == "baseline") {
			return true
		}
		for _, kv := range g.Adopted {
			if strings.HasPrefix(kv, setting+"=") {
				return true
			}
		}
		return false
	}
	return owns(yta.SettingHookStyle), owns(yta.SettingTitleStyle)
}

func ytaQuality(ctx context.Context, rc *engine.RunContext, node *model.Node, execID string, items []model.Item) (model.NodeOutput, error) {
	enabled := ytaQualityEnvBool(rc, "MICROFLOW_QUALITY_ENABLED", true)
	apply := ytaQualityEnvBool(rc, "MICROFLOW_QUALITY_APPLY", true)
	gain := 8
	if v, err := strconv.Atoi(strings.TrimSpace(rc.Env("MICROFLOW_QUALITY_MIN_GAIN"))); err == nil && v >= 3 && v <= 30 {
		gain = v
	}
	keepHook, keepTitle := ytaQualityOwns(ctx, rc, execID)

	out := make([]model.Item, 0, len(items))
	for _, it := range items {
		j := map[string]any{}
		for k, v := range it.JSON {
			j[k] = v
		}
		skip := func(why string) {
			j["ytQuality"] = map[string]any{"status": "skipped", "reason": why}
			out = append(out, model.Item{JSON: j, Binary: it.Binary})
		}
		if !enabled {
			skip("disabled by MICROFLOW_QUALITY_ENABLED")
			continue
		}
		in := yta.ScriptIn{}
		in.Title, _ = it.JSON["title"].(string)
		in.Topic, _ = it.JSON["topic"].(string)
		in.Hook, _ = it.JSON["hook"].(string)
		in.Description, _ = it.JSON["description"].(string)
		in.Tags = anyStrings(it.JSON["tags"])
		sceneObjs, _ := it.JSON["scenes"].([]any)
		for _, so := range sceneObjs {
			if m, ok := so.(map[string]any); ok {
				v, _ := m["voice"].(string)
				in.Voices = append(in.Voices, v)
			}
		}
		if in.Title == "" || len(in.Voices) == 0 {
			skip("item has no title or scenes")
			continue
		}
		ec := rc.ExprContext(it.JSON)
		eval := func(name string) any {
			v, err := expr.EvalValue(node.ParamString(name, ""), ec)
			if err != nil {
				return nil
			}
			return v
		}
		in.HookOptions = anyStrings(eval("hookOptions"))
		in.TitleOptions = anyStrings(eval("titleOptions"))
		if s := fmt.Sprint(eval("thumbText")); s != "" && s != "<nil>" {
			in.ThumbText = s
		}
		res := yta.ImproveScript(in, yta.ImproveOpts{KeepHook: keepHook, KeepTitle: keepTitle, MinGain: gain, Apply: apply})

		j["title"] = res.Title
		j["description"] = res.Description
		tags := make([]any, 0, len(res.Tags))
		for _, t := range res.Tags {
			tags = append(tags, t)
		}
		j["tags"] = tags
		if res.Hook != "" {
			j["hook"] = res.Hook
		}
		if apply && res.Report.HookReplaced {
			scenes := make([]any, 0, len(sceneObjs))
			for i, so := range sceneObjs {
				if m, ok := so.(map[string]any); ok && i == 0 {
					nm := map[string]any{}
					for k, v := range m {
						nm[k] = v
					}
					nm["voice"] = res.Voices[0]
					scenes = append(scenes, nm)
				} else {
					scenes = append(scenes, so)
				}
			}
			j["scenes"] = scenes
		}
		j["thumbText"] = res.ThumbText
		words := make([]string, 0, len(res.Report.Script.SceneWords))
		for _, w := range res.Report.Script.SceneWords {
			words = append(words, strconv.Itoa(w))
		}
		// flat fields for the register step (which reads them through expressions)
		j["qHookScore"] = res.Report.Hook.Verdict
		j["qTitleScore"] = res.Report.Title.Score
		j["qSceneWords"] = strings.Join(words, ",")
		rep := res.Report
		rep.Status = "ok"
		// plain maps (JSON field names), so Code nodes and expressions see the same keys
		var repMap map[string]any
		if b, err := json.Marshal(rep); err == nil && json.Unmarshal(b, &repMap) == nil {
			j["ytQuality"] = repMap
		} else {
			j["ytQuality"] = map[string]any{"status": "skipped", "reason": "report could not be encoded"}
		}
		out = append(out, model.Item{JSON: j, Binary: it.Binary})
	}
	return model.NodeOutput{out}, nil
}
