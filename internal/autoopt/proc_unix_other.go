//go:build unix && !linux

package autoopt

// No /proc here: fall back to the plain kill(-pgid, 0) answer.
func groupHasLiveMember(pgid int) bool { return true }
