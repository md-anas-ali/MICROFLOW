package nodes

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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
		if err := guardPathTraversal(rc.ScratchDir, path); err != nil {
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

func guardPathTraversal(root, target string) error {
	if root == "" {
		return nil
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(absRoot, absTarget)
	if err != nil || rel == ".." || len(rel) >= 2 && rel[:2] == ".." {
		return fmt.Errorf("path %q escapes scratch directory", target)
	}
	return nil
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
