package nodes

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"microflow/internal/engine"
	"microflow/internal/expr"
	"microflow/internal/model"
)

// ReadWriteFileExecutor covers Save Image / Read Final Video / Read
// Thumbnail. Files are read/written directly against the scratch dir on
// disk; nothing is buffered fully in RAM for large media beyond a single
// read/write syscall's worth (rule 7).
type ReadWriteFileExecutor struct {
	ScratchRoot string
}

func (e *ReadWriteFileExecutor) Execute(_ context.Context, rc *engine.RunContext, node *model.Node, input model.NodeOutput) (model.NodeOutput, error) {
	operation, _ := node.Parameters["operation"].(string) // "read" | "write"
	var out []model.Item

	// allowedRoots: the run's own isolated scratch dir is always allowed.
	// The OS temp dir (e.g. /tmp) is ALSO allowed because many imported
	// n8n workflows (this one included -- see Save Image/Get Image and
	// every executeCommand python/ffmpeg step) hardcode fixed paths like
	// /tmp/scene_1.jpg shared across steps. MicroFlow cannot sandbox
	// those paths at all when they're embedded in opaque executeCommand
	// script text (cmd.Dir is set to the scratch dir, but a script that
	// hardcodes an absolute /tmp/... path ignores that anyway) -- so a
	// readWriteFile node rejecting the exact same path a sibling
	// executeCommand step in the same run already wrote to unguarded
	// blocks a workflow without adding real isolation (rule: don't add
	// a check that only stops the honest path while the same file is
	// already reachable another way in the same run).
	//
	// This does NOT relax protection against path-traversal escapes
	// (../..) relative to whichever root matches -- only widens which
	// roots are acceptable. It also does not fully solve concurrent-
	// execution collisions on shared /tmp paths for workflows authored
	// this way (STATUS.md's "never share global media paths" sweep);
	// MICROFLOW_MAX_CONCURRENT_EXECUTIONS defaults to 1, which is the
	// operative mitigation today.
	allowedRoots := []string{rc.ScratchDir, os.TempDir()}

	for _, it := range flatten(input) {
		exprCtx := rc.ExprContext(it.JSON)
		pathTmpl := node.ParamString("fileName", node.ParamString("path", ""))
		path, err := expr.Eval(pathTmpl, exprCtx)
		if err != nil {
			return nil, err
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(rc.ScratchDir, path)
		}
		if err := guardPathTraversal(allowedRoots, path); err != nil {
			return nil, fmt.Errorf("readWriteFile %q: %w", node.Name, err)
		}

		switch operation {
		case "write":
			ref, ok := it.Binary["data"]
			if !ok {
				return nil, fmt.Errorf("readWriteFile %q: no binary data to write", node.Name)
			}
			if err := copyFile(ref.FileName, path); err != nil {
				return nil, err
			}
			out = append(out, model.Item{JSON: map[string]any{"path": path}})
		default: // "read"
			info, err := os.Stat(path)
			if err != nil {
				return nil, fmt.Errorf("readWriteFile %q: %w", node.Name, err)
			}
			ref := model.BinaryRef{FileName: path, SizeBytes: info.Size(), OriginalName: filepath.Base(path)}
			out = append(out, model.Item{JSON: map[string]any{"path": path}, Binary: map[string]model.BinaryRef{"data": ref}})
		}
	}
	return model.NodeOutput{out}, nil
}

// guardPathTraversal reports an error unless target resolves inside at
// least one of roots. Empty roots (e.g. ScratchDir being "" in a
// misconfigured/test context) are skipped, matching the previous
// single-root behavior of "empty root = no restriction".
func guardPathTraversal(roots []string, target string) error {
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	anyRealRoot := false
	for _, root := range roots {
		if root == "" {
			continue
		}
		anyRealRoot = true
		absRoot, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(absRoot, absTarget)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return nil
		}
	}
	if !anyRealRoot {
		return nil
	}
	return fmt.Errorf("path %q escapes allowed directories", target)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	outF, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer outF.Close()
	buf := make([]byte, 256*1024) // bounded copy buffer, not whole-file-in-RAM
	for {
		n, rerr := in.Read(buf)
		if n > 0 {
			if _, werr := outF.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if rerr != nil {
			break
		}
	}
	return nil
}
