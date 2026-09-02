package main

import (
	"fmt"
	"os"
	"sort"

	"microflow/internal/model"
	"microflow/internal/parser"
)

func loadWorkflow(path string) *parser.ParseResult {
	raw, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("read workflow:", err)
		os.Exit(1)
	}
	res, err := parser.Parse(raw)
	if err != nil {
		fmt.Println("parser.Parse:", err)
		os.Exit(1)
	}
	return res
}

// checkGraph reports compatibility + structural integrity using
// MicroFlow's actual parsed model.Workflow (not a re-implementation).
func checkGraph(res *parser.ParseResult) (triggers []string, ok bool) {
	wf := res.Workflow
	fmt.Println("=== Compatibility checklist (internal/parser.Parse, real n8n import path) ===")
	fmt.Printf("Total nodes parsed: %d\n", len(wf.Nodes))
	unsupported := res.Unsupported()
	if len(unsupported) == 0 {
		fmt.Println("All node types map to a MicroFlow executor. 0 unsupported.")
	} else {
		fmt.Printf("%d node(s) map to TypeUnknown (no executor):\n", len(unsupported))
		for _, u := range unsupported {
			fmt.Printf("  - %s (%s)\n", u.NodeName, u.OriginalType)
		}
	}

	fmt.Println("\n=== Connection integrity (every source/target must exist as a node) ===")
	badConn := 0
	var sourceNames []string
	for s := range wf.Connections {
		sourceNames = append(sourceNames, s)
	}
	sort.Strings(sourceNames)
	for _, s := range sourceNames {
		if _, ok := wf.Nodes[s]; !ok {
			fmt.Printf("  BAD: connection source %q is not a parsed node\n", s)
			badConn++
		}
		for _, c := range wf.Connections[s] {
			if _, ok := wf.Nodes[c.TargetName]; !ok {
				fmt.Printf("  BAD: %s -> %q, target node does not exist\n", s, c.TargetName)
				badConn++
			}
		}
	}
	if badConn == 0 {
		fmt.Println("All connection endpoints resolve to real nodes. 0 dangling edges.")
	}

	fmt.Println("\n=== Trigger nodes ===")
	for _, n := range wf.Nodes {
		switch n.Type {
		case model.TypeManualTrigger, model.TypeScheduleTrigger, model.TypeWebhookTrigger, model.TypeErrorTrigger:
			triggers = append(triggers, n.Name)
			fmt.Printf("  %s (%s)%s\n", n.Name, n.Type, disabledSuffix(n))
		}
	}
	sort.Strings(triggers)

	fmt.Println("\n=== Disabled nodes ===")
	disabledCount := 0
	for _, n := range wf.Nodes {
		if n.Disabled {
			fmt.Printf("  %s (%s)\n", n.Name, n.Type)
			disabledCount++
		}
	}
	if disabledCount == 0 {
		fmt.Println("  none")
	}

	fmt.Println("\n=== Nodes with no incoming AND no outgoing connection (orphans, excluding triggers/sticky notes) ===")
	orphans := 0
	for name, n := range wf.Nodes {
		if n.Type == model.TypeStickyNote {
			continue
		}
		isTrigger := false
		for _, t := range triggers {
			if t == name {
				isTrigger = true
			}
		}
		if isTrigger {
			continue
		}
		if len(wf.Incoming(name)) == 0 && len(wf.Connections[name]) == 0 {
			fmt.Printf("  %s (%s)\n", name, n.Type)
			orphans++
		}
	}
	if orphans == 0 {
		fmt.Println("  none")
	}

	fmt.Println()
	return triggers, badConn == 0 && len(unsupported) == 0
}

func disabledSuffix(n *model.Node) string {
	if n.Disabled {
		return " [DISABLED]"
	}
	return ""
}
