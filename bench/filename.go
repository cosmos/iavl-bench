package bench

import (
	"fmt"
	"path/filepath"
)

func dataFilename(dataDir string, version int64) string {
	return filepath.Join(dataDir, fmt.Sprintf("%09d.delimpb", version))
}
