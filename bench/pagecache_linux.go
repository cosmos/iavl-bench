//go:build linux

package bench

import (
	"os"
)

// EvictFromPageCache drops all clean caches system-wide by writing to
// /proc/sys/vm/drop_caches. This is the nuclear option but it works
// reliably even for mmap'd files. Requires root.
//
// Value 3 = drop pagecache, dentries, and inodes
func EvictFromPageCache(dir string) error {
	return os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0644)
}
