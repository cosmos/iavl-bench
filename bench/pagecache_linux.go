//go:build linux

package bench

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// EvictFromPageCache walks the directory and advises the kernel to evict
// all file data from the page cache using FADV_DONTNEED. This forces
// subsequent reads to actually hit disk rather than serving from cache.
//
// This is useful for benchmarking disk read performance - without this,
// mmap'd files stay in the page cache even after ForceToDisk() is called.
func EvictFromPageCache(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("opening %s: %w", path, err)
		}
		defer f.Close()

		size := info.Size()
		if size == 0 {
			return nil
		}

		// FADV_DONTNEED tells the kernel we don't need this data anymore,
		// causing it to evict the pages from the page cache
		err = unix.Fadvise(int(f.Fd()), 0, size, unix.FADV_DONTNEED)
		if err != nil {
			return fmt.Errorf("fadvise on %s: %w", path, err)
		}

		return nil
	})
}
