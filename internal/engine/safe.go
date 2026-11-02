package engine

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"

	"microflow/internal/model"
)

// safeExecute runs one node and converts a panic inside the executor into an
// ordinary (permanent) node error. Without it, a nil-deref or similar bug in
// any single node would crash the whole server process and every other
// running/queued execution with it.
func safeExecute(ctx context.Context, exec NodeExecutor, rc *RunContext, node *model.Node, in model.NodeOutput) (out model.NodeOutput, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("engine: node %q panicked: %v\n%s", node.Name, r, debug.Stack())
			out = nil
			err = Permanent(fmt.Errorf("node %q crashed (panic): %v", node.Name, r))
		}
	}()
	return exec.Execute(ctx, rc, node, in)
}
