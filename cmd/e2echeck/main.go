// Command e2echeck is a standalone dry-run verification harness for a
// specific imported n8n workflow. It does NOT call real external APIs
// or spawn real ffmpeg/edge-tts -- it parses the workflow with
// MicroFlow's real n8n compatibility layer, validates the graph, syntax
// checks every embedded script, and then drives MicroFlow's real
// engine/registry against synthetic HTTP + command stand-ins so the
// engine's routing/retry/error logic and every Code node's actual JS
// logic run for real.
package main

import (
	"fmt"
	"os"

	"microflow/internal/model"
	"microflow/internal/parser"
)

func main() {
	badJS := checkJS()

	if len(os.Args) < 2 {
		fmt.Println("usage: e2echeck <workflow.json>")
		os.Exit(1)
	}

	if badJS > 0 {
		fmt.Println("Stopping before graph run: fix JS syntax errors above first.")
		os.Exit(1)
	}

	res := loadWorkflow(os.Args[1])
	triggers, structOK := checkGraph(res)
	if !structOK {
		fmt.Println("Stopping before engine run: fix structural issues above first.")
		os.Exit(1)
	}
	fmt.Printf("Triggers found: %v\n", triggers)

	start := pickStartTrigger(res, triggers)
	fmt.Printf("\nStarting engine run from trigger: %q\n\n", start)
	runEngine(res, start)
}

// pickStartTrigger prefers a manual/schedule trigger over the error
// trigger, since the error trigger is the workflow's own failure path,
// not its normal entry point.
func pickStartTrigger(res *parser.ParseResult, triggers []string) string {
	for _, t := range triggers {
		if n := res.Workflow.Nodes[t]; n != nil && n.Type == model.TypeScheduleTrigger {
			return t
		}
	}
	for _, t := range triggers {
		if n := res.Workflow.Nodes[t]; n != nil && n.Type == model.TypeManualTrigger {
			return t
		}
	}
	if len(triggers) > 0 {
		return triggers[0]
	}
	fmt.Println("no trigger node found")
	os.Exit(1)
	return ""
}
