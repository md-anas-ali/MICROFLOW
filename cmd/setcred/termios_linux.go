//go:build linux

package main

import (
	"syscall"
	"unsafe"
)

// termios mirrors the fields struct termios uses on Linux (asm-generic
// layout, matches glibc/musl on amd64/arm64). Only the pieces we touch
// (Lflag) plus the fixed layout are needed to round-trip TCGETS/TCSETS.
type termios struct {
	Iflag  uint32
	Oflag  uint32
	Cflag  uint32
	Lflag  uint32
	Line   uint8
	Cc     [32]uint8
	Ispeed uint32
	Ospeed uint32
}

const (
	tcgets = 0x5401
	tcsets = 0x5402
	echo   = 0x00000008
)

// disableEcho turns off terminal echo on fd (best-effort). It returns a
// restore func and true on success; on any failure (not a TTY, ioctl
// unsupported, etc.) it returns (nil, false) and the caller falls back
// to a normal visible read.
func disableEcho(fd uintptr) (restore func(), ok bool) {
	var oldState termios
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, tcgets, uintptr(unsafe.Pointer(&oldState))); errno != 0 {
		return nil, false
	}
	newState := oldState
	newState.Lflag &^= echo
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, tcsets, uintptr(unsafe.Pointer(&newState))); errno != 0 {
		return nil, false
	}
	return func() {
		_, _, _ = syscall.Syscall(syscall.SYS_IOCTL, fd, tcsets, uintptr(unsafe.Pointer(&oldState)))
	}, true
}
