package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func dataFilename(dataDir string, version int64) string {
	return filepath.Join(dataDir, fmt.Sprintf("%09d.delimpb", version))
}

func infoFilename(dataDir string) string {
	return filepath.Join(dataDir, "changeset_info.json")
}

type testdataInfo struct {
	Versions   int64    `json:"versions"`
	StoreNames []string `json:"store_names"`
}

func writeInfoFile(dataDir string, info testdataInfo) error {
	filename := infoFilename(dataDir)
	bz, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("error marshaling info file: %w", err)
	}
	return os.WriteFile(filename, bz, 0o644)
}

func readInfoFile(dataDir string) (testdataInfo, error) {
	filename := infoFilename(dataDir)
	bz, err := os.ReadFile(filename)
	if err != nil {
		return testdataInfo{}, fmt.Errorf("error reading info file: %w", err)
	}
	var info testdataInfo
	err = json.Unmarshal(bz, &info)
	if err != nil {
		return testdataInfo{}, fmt.Errorf("error unmarshaling info file: %w", err)
	}
	return info, nil
}
