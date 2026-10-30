//go:build linux

package localmodel

import "syscall"

// If MicroFlow dies abruptly the kernel kills the model process too.
func setPdeathsig(a *syscall.SysProcAttr) { a.Pdeathsig = syscall.SIGTERM }
