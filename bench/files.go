package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func changesetDataFilename(dataDir string, version int64) string {
	return filepath.Join(dataDir, fmt.Sprintf("%09d.delimpb", version))
}

func changesetInfoFilename(dataDir string) string {
	return filepath.Join(dataDir, "changeset_info.json")
}

type changesetInfo struct {
	Versions    int64         `json:"versions"`
	StoreNames  []string      `json:"store_names"`
	StoreParams []StoreParams `json:"store_params"`
}

func writeChangesetInfo(dataDir string, info changesetInfo) error {
	filename := changesetInfoFilename(dataDir)
	bz, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("error marshaling info file: %w", err)
	}
	return os.WriteFile(filename, bz, 0o644)
}

func readChangesetInfo(dataDir string) (changesetInfo, error) {
	filename := changesetInfoFilename(dataDir)
	bz, err := os.ReadFile(filename)
	if err != nil {
		return changesetInfo{}, fmt.Errorf("error reading info file: %w", err)
	}
	var info changesetInfo
	err = json.Unmarshal(bz, &info)
	if err != nil {
		return changesetInfo{}, fmt.Errorf("error unmarshaling info file: %w", err)
	}
	return info, nil
}
