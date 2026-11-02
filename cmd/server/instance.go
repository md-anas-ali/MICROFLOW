package main

import (
	"os"
	"strings"
)

// singleInstanceEnabled reports whether the one-server-per-database guard is
// on (default). Set MICROFLOW_SINGLE_INSTANCE=0 (or false/off/no) to disable
// it, e.g. when DATABASE_URL goes through a transaction-mode pooler where
// session advisory locks do not work.
func singleInstanceEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("MICROFLOW_SINGLE_INSTANCE"))) {
	case "0", "false", "off", "no":
		return false
	}
	return true
}
