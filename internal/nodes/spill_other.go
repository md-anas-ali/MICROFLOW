//go:build !linux

package nodes

// storageStat: no portable free-space/tmpfs probe here, so report "unknown"
// and let the caller rely on the write itself (any error falls back to RAM).
func storageStat(dir string) (free uint64, ramBacked bool, known bool) {
	return 0, false, false
}
