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
	reader   *Changeset
	writer   *ChangesetWriter
}

func Compact(logger *slog.Logger, reader *Changeset, criteria RetainCriteria, store *TreeStore) (*Changeset, error) {
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

//func (cs *Changeset) compactBranches(retainCriteria RetainCriteria, newBranches *StructReader[BranchLayout]) error {
//	if !cs.sealed {
//		return fmt.Errorf("changeset is not sealed")
//	}
//
//	n := cs.branchesData.OnDiskCount()
//	skipped := 0
//	for i := uint32(0); i < n; i++ {
//		branch := cs.branchesData.Item(i)
//		if retainCriteria(uint32(branch.id.Version()), branch.orphanVersion) {
//			// TODO update relative pointers
//			// TODO save key data to KV store if needed
//			err := newBranches.Append(&branch)
//			if err != nil {
//				return fmt.Errorf("failed to compact branch node %s: %w", branch.id, err)
//			}
//		} else {
//			skipped++
//			// TODO remove key from KV store if possible
//		}
//	}
//
//	return nil
//}
//
//func (cs *Changeset) compactLeaves(retainCriteria RetainCriteria, newBranches *StructReader[LeafLayout]) error {
//	if !cs.sealed {
//		return fmt.Errorf("changeset is not sealed")
//	}
//
//	n := cs.leavesData.OnDiskCount()
//	for i := uint32(0); i < n; i++ {
//		leaf := cs.leavesData.Item(i)
//		if retainCriteria(uint32(leaf.id.Version()), leaf.orphanVersion) {
//			// TODO save key data to KV store if needed
//			err := newBranches.Append(&leaf)
//			if err != nil {
//				return fmt.Errorf("failed to compact leaf node %s: %w", leaf.id, err)
//			}
//		} else {
//			// TODO remove key from KV store if possible
//		}
//	}
//
//	return nil
//}
//
//func (cs *Changeset) MarkOrphans(version uint32, nodeIds []NodeID) error {
//	// TODO add locking
//	for _, nodeId := range nodeIds {
//		if nodeId.IsLeaf() {
//			leaf, err := cs.ResolveLeaf(nodeId, 0)
//			if err != nil {
//				return err
//			}
//			if leaf.orphanVersion == 0 {
//				leaf.orphanVersion = version
//			}
//		} else {
//			branch, err := cs.ResolveBranch(nodeId, 0)
//			if err != nil {
//				return err
//			}
//			if branch.orphanVersion == 0 {
//				branch.orphanVersion = version
//			}
//		}
//	}
//
//	// TODO flush changes to disk
//
//	return nil
//}
