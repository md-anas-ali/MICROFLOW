package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/dop251/goja"
)

func checkJS() int {
	files, _ := filepath.Glob("cmd/e2echeck/code_nodes/*.js")
	sort.Strings(files)
	bad := 0
	fmt.Println("=== JS syntax check (goja compiler, same engine MicroFlow's CodeExecutor uses) ===")
	for _, f := range files {
		src, err := os.ReadFile(f)
		if err != nil {
			fmt.Println("READ ERROR", f, err)
			bad++
			continue
		}
		wrapped := "(function(){\n" + string(src) + "\n})()" // must match internal/nodes/code.go's wrapCode exactly
		if _, err := goja.Compile(f, wrapped, false); err != nil {
			fmt.Printf("SYNTAX ERROR in %s: %v\n", f, err)
			bad++
			continue
		}
	}
	fmt.Printf("%d/%d Code node JS files: syntax errors\n\n", bad, len(files))
	return bad
}
