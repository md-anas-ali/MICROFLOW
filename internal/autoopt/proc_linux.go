//go:build linux

package autoopt

import (
	"os"
	"strconv"
	"strings"
)

// groupHasLiveMember reports whether any NON-zombie process is in process group
// pgid. kill(-pgid, 0) also succeeds for zombies that nobody has reaped yet,
// which would make a correctly terminated group look "still alive". This scan
// only runs on the rare path where a group was found to exist at all.
func groupHasLiveMember(pgid int) bool {
	ents, err := os.ReadDir("/proc")
	if err != nil {
		return true // cannot tell: be conservative
	}
	for _, e := range ents {
		name := e.Name()
		if name == "" || name[0] < '0' || name[0] > '9' {
			continue
		}
		b, err := os.ReadFile("/proc/" + name + "/stat")
		if err != nil {
			continue
		}
		s := string(b)
		i := strings.LastIndexByte(s, ')') // comm may contain spaces/parentheses
		if i < 0 || i+2 >= len(s) {
			continue
		}
		f := strings.Fields(s[i+2:]) // state ppid pgrp ...
		if len(f) < 3 || f[0] == "Z" || f[0] == "X" {
			continue
		}
		if g, err := strconv.Atoi(f[2]); err == nil && g == pgid {
			return true
		}
	}
	return false
}
