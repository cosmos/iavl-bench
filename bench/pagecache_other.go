//go:build !linux

package bench

// EvictFromPageCache is a no-op on non-Linux platforms.
// The fadvise syscall with FADV_DONTNEED only works reliably on Linux.
func EvictFromPageCache(dir string) error {
	// No-op on non-Linux platforms
	return nil
}
