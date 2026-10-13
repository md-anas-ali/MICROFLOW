//go:build !unix

package nodes

// ytaCPUms: CPU time is not reported on this platform (-1 = unavailable).
func ytaCPUms() float64 { return -1 }
