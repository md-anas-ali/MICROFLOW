//go:build linux

package nodes

import "syscall"

const (
	tmpfsMagic = 0x01021994
	ramfsMagic = 0x858458f6
)

// storageStat returns free bytes for dir and whether it is RAM-backed.
// known=false means the OS could not tell us (caller then trusts the dir).
func storageStat(dir string) (free uint64, ramBacked bool, known bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, false, false
	}
	t := int64(st.Type)
	return st.Bavail * uint64(st.Bsize), t == tmpfsMagic || t == ramfsMagic, true
}
