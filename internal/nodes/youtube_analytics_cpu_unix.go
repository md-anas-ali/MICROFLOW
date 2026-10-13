//go:build unix

package nodes

import "syscall"

// ytaCPUms returns this process's cumulative user+system CPU time in ms.
func ytaCPUms() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return -1
	}
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec)*1000 + float64(t.Usec)/1000 }
	return tv(ru.Utime) + tv(ru.Stime)
}
