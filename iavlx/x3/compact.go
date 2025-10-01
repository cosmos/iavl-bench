package x3

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
)

type RetainCriteria func(version, orphanVersion uint32) bool

type compacter struct {
	logger   *slog.Logger
	criteria RetainCriteria
	store    *TreeStore
	reader   *ChangesetReader
	writer   *ChangesetWriter
}

func Compact(logger *slog.Logger, reader *ChangesetReader, criteria RetainCriteria, store *TreeStore) (*ChangesetReader, error) {
	dir := reader.dir
	dirName := filepath.Base(dir)
	split := strings.Split(dir, ".")
	revision := uint32(0)
	if len(split) == 2 {
		dirName = split[0]
		_, err := fmt.Sscanf(split[1], "%d", &revision)
		if err != nil {
			return nil, fmt.Errorf("failed to parse revision from changeset dir: %w", err)
		}
	}
	revision++
	dirName = fmt.Sprintf("%s.%d", dirName, revision)
	newDir := filepath.Join(filepath.Dir(dir), dirName)
	logger.Info("compacting changeset", "from", reader.dir, "to", newDir)
	writer, err := NewChangesetWriter(dir, 0, store)
	if err != nil {
		return nil, fmt.Errorf("failed to create new changeset writer: %w", err)
	}
	c := &compacter{
		logger:   logger,
		criteria: criteria,
		store:    store,
		reader:   reader,
		writer:   writer,
	}
	err = c.compactLeaves()
	if err != nil {
		return nil, fmt.Errorf("failed to compact leaves: %w", err)
	}
	return nil, fmt.Errorf("not implemented")
}

func (c *compacter) compactLeaves() error {
	return nil
}
