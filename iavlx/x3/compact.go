package x3

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
)

type CompactOptions struct {
	RetainCriteria RetainCriteria
	CompactWAL     bool
}

type RetainCriteria func(version, orphanVersion uint32) bool

type compacter struct {
	logger *slog.Logger

	criteria   RetainCriteria
	compactWAL bool

	store  *TreeStore
	reader *Changeset
	writer *ChangesetWriter

	leafOffsetRemappings map[uint32]uint32
	keyCache             map[string]uint32
}

func Compact(logger *slog.Logger, reader *Changeset, opts CompactOptions, store *TreeStore) (*Changeset, error) {
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

	if !opts.CompactWAL {
		// if not compacting WAL, we can share the same KVLog
		// TODO update writer to point to this file
	}

	c := &compacter{
		logger:               logger,
		criteria:             opts.RetainCriteria,
		compactWAL:           opts.CompactWAL,
		store:                store,
		reader:               reader,
		writer:               writer,
		keyCache:             make(map[string]uint32),
		leafOffsetRemappings: make(map[uint32]uint32),
	}

	_, err = c.compact()
	if err != nil {
		return nil, fmt.Errorf("failed to compact leaves: %w", err)
	}
	newReader, err := writer.Seal()
	if err != nil {
		return nil, fmt.Errorf("failed to seal new changeset: %w", err)
	}

	return newReader, nil
}

func (c *compacter) compact() (*Changeset, error) {
	versionsData := c.reader.versionsData
	numVersions := versionsData.Count()
	leavesData := c.reader.leavesData
	branchesData := c.reader.branchesData
	leafOrphanCount := uint32(0)
	branchOrphanCount := uint32(0)
	leafOrphanVersionTotal := uint64(0)
	branchOrphanVersionTotal := uint64(0)
	for i := 0; i < numVersions; i++ {
		verInfo := *versionsData.UnsafeItem(uint32(i)) // copy
		newLeafStartIdx := uint32(0)
		newLeafEndIdx := uint32(0)
		leafStartOffset := verInfo.Leaves.StartOffset
		leafCount := verInfo.Leaves.Count
		newLeafStartOffset := uint32(c.writer.leavesData.Count())
		newLeafCount := uint32(0)
		for j := uint32(0); j < leafCount; j++ {
			leaf := *leavesData.UnsafeItem(leafStartOffset + j) // copy
			id := leaf.Id
			retain := c.criteria(uint32(id.Version()), leaf.OrphanVersion)
			if !retain {
				continue
			}

			if leaf.OrphanVersion != 0 {
				leafOrphanCount++
				leafOrphanVersionTotal += uint64(leaf.OrphanVersion)
			}

			if newLeafStartIdx == 0 {
				newLeafStartIdx = id.Index()
			}
			newLeafEndIdx = id.Index()
			newLeafCount++

			if c.compactWAL {
				k, v, err := c.reader.ReadKV(id, leaf.KeyOffset)
				if err != nil {
					return nil, fmt.Errorf("failed to read KV for leaf %s: %w", id, err)
				}

				offset, err := c.writer.kvlog.WriteKV(k, v)
				if err != nil {
					return nil, fmt.Errorf("failed to write KV for leaf %s: %w", id, err)
				}

				leaf.KeyOffset = offset
				c.keyCache[string(k)] = offset
			}

			err := c.writer.leavesData.Append(&leaf)
			if err != nil {
				return nil, fmt.Errorf("failed to append leaf %s: %w", id, err)
			}

			c.leafOffsetRemappings[uint32(i)] = uint32(c.writer.leavesData.Count()) // 1-based
		}

		newBranchStartIdx := uint32(0)
		newBranchEndIdx := uint32(0)
		branchStartOffset := verInfo.Branches.StartOffset
		branchCount := verInfo.Branches.Count
		newBranchStartOffset := uint32(c.writer.branchesData.Count())
		newBranchCount := uint32(0)
		skippedBranches := 0
		for j := uint32(0); j < branchCount; j++ {
			branch := *branchesData.UnsafeItem(branchStartOffset + j) // copy
			id := branch.Id
			retain := c.criteria(uint32(id.Version()), branch.OrphanVersion)
			if !retain {
				skippedBranches++
				continue
			}

			if branch.OrphanVersion != 0 {
				branchOrphanCount++
				branchOrphanVersionTotal += uint64(branch.OrphanVersion)
			}

			if newBranchStartIdx == 0 {
				newBranchStartIdx = id.Index()
			}
			newBranchEndIdx = id.Index()
			newBranchCount++

			var err error
			branch.Left, err = c.updateNodeRef(branch.Left, skippedBranches)
			if err != nil {
				return nil, fmt.Errorf("failed to update left ref for branch %s: %w", id, err)
			}
			branch.Right, err = c.updateNodeRef(branch.Right, skippedBranches)
			if err != nil {
				return nil, fmt.Errorf("failed to update right ref for branch %s: %w", id, err)
			}

			if c.compactWAL {
				k, err := c.reader.ReadK(id, branch.KeyOffset)
				if err != nil {
					return nil, fmt.Errorf("failed to read key for branch %s: %w", id, err)
				}
				offset, ok := c.keyCache[string(k)]
				if !ok {
					offset, err = c.writer.kvlog.WriteK(k)
				}
				if err != nil {
					return nil, fmt.Errorf("failed to write key for branch %s: %w", id, err)
				}
				branch.KeyOffset = offset
			}

			err = c.writer.branchesData.Append(&branch)
			if err != nil {
				return nil, fmt.Errorf("failed to append branch %s: %w", id, err)
			}
		}

		verInfo = VersionInfo{
			Leaves: NodeSetInfo{
				StartIndex:  newLeafStartIdx,
				EndIndex:    newLeafEndIdx,
				StartOffset: newLeafStartOffset,
				Count:       newLeafCount,
			},
			Branches: NodeSetInfo{
				StartIndex:  newBranchStartIdx,
				EndIndex:    newBranchEndIdx,
				StartOffset: newBranchStartOffset,
				Count:       newBranchCount,
			},
			RootID: verInfo.RootID,
		}

		err := c.writer.versionsData.Append(&verInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to append version info for versiond: %w", err)
		}
	}

	info := ChangesetInfo{
		StartVersion:             c.reader.info.StartVersion,
		EndVersion:               c.reader.info.EndVersion,
		LeafOrphans:              leafOrphanCount,
		BranchOrphans:            branchOrphanCount,
		LeafOrphanVersionTotal:   leafOrphanVersionTotal,
		BranchOrphanVersionTotal: branchOrphanVersionTotal,
	}

	return c.writer.seal(info)
}

//	func (c *compacter) compactLeaves() error {
//		leavesData := c.reader.leavesData
//		n := leavesData.Count()
//		for i := 1; i <= n; i++ {
//			leaf := *leavesData.UnsafeItem(uint32(i) - 1) // convert to 0-based & copy
//			retain := c.criteria(uint32(leaf.Id.Version()), leaf.OrphanVersion)
//			if !retain {
//				continue
//			}
//
//			if c.compactWAL {
//				k, v, err := c.reader.ReadKV(leaf.Id, leaf.KeyOffset)
//				if err != nil {
//					return fmt.Errorf("failed to read KV for leaf %s: %w", leaf.Id, err)
//				}
//
//				offset, err := c.writer.kvlog.WriteKV(k, v)
//				if err != nil {
//					return fmt.Errorf("failed to write KV for leaf %s: %w", leaf.Id, err)
//				}
//
//				leaf.KeyOffset = offset
//				c.keyCache[string(k)] = offset
//			}
//
//			err := c.writer.leavesData.Append(&leaf)
//			if err != nil {
//				return fmt.Errorf("failed to append leaf %s: %w", leaf.Id, err)
//			}
//
//			c.leafOffsetRemappings[uint32(i)] = uint32(c.writer.leavesData.Count()) // 1-based
//		}
//		return nil
//	}
func (c *compacter) updateNodeRef(ref NodeRef, skipped int) (NodeRef, error) {
	if ref.IsNodeID() {
		return ref, nil
	}
	relPtr := ref.AsRelativePointer()
	if relPtr.IsLeaf() {
		oldOffset := relPtr.Offset()
		newOffset, ok := c.leafOffsetRemappings[uint32(oldOffset)]
		if !ok {
			return 0, fmt.Errorf("failed to find remapping for leaf offset %d", oldOffset)
		}
		return NodeRef(NewNodeRelativePointer(true, int64(newOffset))), nil
	} else {
		// branch nodes we reduce by the number of skipped nodes
		oldOffset := relPtr.Offset()
		newOffset := oldOffset - int64(skipped)
		if newOffset < 1 {
			return 0, fmt.Errorf("invalid new branch offset: %d", newOffset)
		}
		return NodeRef(NewNodeRelativePointer(false, newOffset)), nil
	}
}

//
//func (c *compacter) compactBranches() error {
//	branchesData := c.reader.branchesData
//	n := branchesData.Count()
//	skipped := 0
//	for i := 1; i <= n; i++ {
//		branch := *branchesData.UnsafeItem(uint32(i) - 1) // convert to 0-based & copy
//		retain := c.criteria(uint32(branch.Id.Version()), branch.OrphanVersion)
//		if !retain {
//			skipped++
//			continue
//		}
//		var err error
//		branch.Left, err = c.updateNodeRef(branch.Left, skipped)
//		if err != nil {
//			return fmt.Errorf("failed to update left ref for branch %s: %w", branch.Id, err)
//		}
//		branch.Right, err = c.updateNodeRef(branch.Right, skipped)
//		if err != nil {
//			return fmt.Errorf("failed to update right ref for branch %s: %w", branch.Id, err)
//		}
//
//		if c.compactWAL {
//			k, err := c.reader.ReadK(branch.Id, branch.KeyOffset)
//			if err != nil {
//				return fmt.Errorf("failed to read key for branch %s: %w", branch.Id, err)
//			}
//			offset, ok := c.keyCache[string(k)]
//			if !ok {
//				offset, err = c.writer.kvlog.WriteK(k)
//				if err != nil {
//					return fmt.Errorf("failed to write key for branch %s: %w", branch.Id, err)
//				}
//			}
//			branch.KeyOffset = offset
//		}
//
//		err = c.writer.branchesData.Append(&branch)
//		if err != nil {
//			return fmt.Errorf("failed to append branch %s: %w", branch.Id, err)
//		}
//	}
//	return nil
//}

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
