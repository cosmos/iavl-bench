// Package multitreeutil provides utility functions for managing version information in a multitree database directory.
package util

import (
	"encoding/json"
	"fmt"
	"os"
)

type info struct {
	Version int64 `json:"version"`
}

// LoadVersion loads the current version from the info.json file in the given dbDir.
func LoadVersion(dbDir string) (int64, error) {
	bz, err := os.ReadFile(fmt.Sprintf("%s/info.json", dbDir))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var i info
	if err := json.Unmarshal(bz, &i); err != nil {
		return 0, err
	}
	return i.Version, nil

}

// SaveVersion saves the given version to the info.json file in the given dbDir.
func SaveVersion(dbDir string, version int64) error {
	i := info{Version: version}
	bz, err := json.MarshalIndent(i, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(fmt.Sprintf("%s/info.json", dbDir), bz, 0o644)
}
