//go:build unix && !linux

package localmodel

import "syscall"

func setPdeathsig(a *syscall.SysProcAttr) {}
