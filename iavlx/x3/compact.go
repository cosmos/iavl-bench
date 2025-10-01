package x3

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type CompactOptions struct {
	RetainCriteria RetainCriteria
	CompactWAL     bool
}

type RetainCriteria func(createVersion, orphanVersion uint32) bool

type Compactor struct {
	logger *slog.Logger

	criteria   RetainCriteria
	compactWAL bool

	reader    *Changeset
	treeStore *TreeStore
	dir       string
	kvlogPath string

	leavesWriter   *StructWriter[LeafLayout]
	branchesWriter *StructWriter[BranchLayout]
	versionsWriter *StructWriter[VersionInfo]
	kvlogWriter    *KVLogWriter

	leafOffsetRemappings map[uint32]uint32
	keyCache             map[string]uint32

	pendingOrphans    []pendingOrphan
	pendingOrphansLoc sync.Mutex
}

type pendingOrphan struct {
	version uint32
	nodeId  NodeID
}

func NewCompacter(logger *slog.Logger, reader *Changeset, opts CompactOptions, store *TreeStore) (*Compactor, error) {
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

	err := os.MkdirAll(newDir, 0o755)
	if err != nil {
		return nil, fmt.Errorf("failed to create new changeset dir: %w", err)
	}

	kvlogPath := reader.kvlogPath
	var kvlogWriter *KVLogWriter
	if opts.CompactWAL {
		kvlogPath = filepath.Join(newDir, "kv.log")
		kvlogWriter, err = NewKVDataWriter(kvlogPath)
		if err != nil {
			return nil, fmt.Errorf("failed to create KV log writer: %w", err)
		}
	}

	leavesWriter, err := NewStructWriter[LeafLayout](filepath.Join(newDir, "leaves.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create leaves writer: %w", err)
	}

	branchesWriter, err := NewStructWriter[BranchLayout](filepath.Join(newDir, "branches.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create branches writer: %w", err)
	}

	versionsWriter, err := NewStructWriter[VersionInfo](filepath.Join(newDir, "versions.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create versions writer: %w", err)
	}

	c := &Compactor{
		logger:               logger,
		criteria:             opts.RetainCriteria,
		compactWAL:           opts.CompactWAL,
		reader:               reader,
		treeStore:            store,
		dir:                  newDir,
		kvlogPath:            kvlogPath,
		leavesWriter:         leavesWriter,
		branchesWriter:       branchesWriter,
		versionsWriter:       versionsWriter,
		kvlogWriter:          kvlogWriter,
		keyCache:             make(map[string]uint32),
		leafOffsetRemappings: make(map[uint32]uint32),
	}

	return c, nil
}

func (c *Compactor) Compact() (*Changeset, error) {
	versionsData := c.reader.versionsData
	numVersions := versionsData.Count()
	leavesData := c.reader.leavesData
	branchesData := c.reader.branchesData
	leafOrphanCount := uint32(0)
	branchOrphanCount := uint32(0)
	leafOrphanVersionTotal := uint64(0)
	branchOrphanVersionTotal := uint64(0)
	c.logger.Info("starting compaction", "versions", numVersions)
	for i := 0; i < numVersions; i++ {
		c.logger.Info("compacting version", "version", c.reader.info.StartVersion+uint32(i))
		verInfo := *versionsData.UnsafeItem(uint32(i)) // copy
		newLeafStartIdx := uint32(0)
		newLeafEndIdx := uint32(0)
		leafStartOffset := verInfo.Leaves.StartOffset
		leafCount := verInfo.Leaves.Count
		newLeafStartOffset := uint32(c.leavesWriter.Count())
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

				offset, err := c.kvlogWriter.WriteKV(k, v)
				if err != nil {
					return nil, fmt.Errorf("failed to write KV for leaf %s: %w", id, err)
				}

				leaf.KeyOffset = offset
				c.keyCache[string(k)] = offset
			}

			err := c.leavesWriter.Append(&leaf)
			if err != nil {
				return nil, fmt.Errorf("failed to append leaf %s: %w", id, err)
			}

			oldLeafFileIdx := leafStartOffset + j + 1 // 1-based file index
			c.leafOffsetRemappings[oldLeafFileIdx] = uint32(c.leavesWriter.Count())
		}

		newBranchStartIdx := uint32(0)
		newBranchEndIdx := uint32(0)
		branchStartOffset := verInfo.Branches.StartOffset
		branchCount := verInfo.Branches.Count
		newBranchStartOffset := uint32(c.branchesWriter.Count())
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
				c.logger.Error("failed to update left ref",
					"branchId", id,
					"branchOrphanVersion", branch.OrphanVersion,
					"leftRef", branch.Left)
				return nil, fmt.Errorf("failed to update left ref for branch %s: %w", id, err)
			}
			branch.Right, err = c.updateNodeRef(branch.Right, skippedBranches)
			if err != nil {
				c.logger.Error("failed to update right ref",
					"branchId", id,
					"branchOrphanVersion", branch.OrphanVersion,
					"rightRef", branch.Right)
				return nil, fmt.Errorf("failed to update right ref for branch %s: %w", id, err)
			}

			if c.compactWAL {
				k, err := c.reader.ReadK(id, branch.KeyOffset)
				if err != nil {
					return nil, fmt.Errorf("failed to read key for branch %s: %w", id, err)
				}
				offset, ok := c.keyCache[string(k)]
				if !ok {
					offset, err = c.kvlogWriter.WriteK(k)
				}
				if err != nil {
					return nil, fmt.Errorf("failed to write key for branch %s: %w", id, err)
				}
				branch.KeyOffset = offset
			}

			err = c.branchesWriter.Append(&branch)
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

		err := c.versionsWriter.Append(&verInfo)
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

	return c.seal(info)
}

func (c *Compactor) updateNodeRef(ref NodeRef, skipped int) (NodeRef, error) {
	if ref.IsNodeID() {
		return ref, nil
	}
	relPtr := ref.AsRelativePointer()
	if relPtr.IsLeaf() {
		oldOffset := relPtr.Offset()
		newOffset, ok := c.leafOffsetRemappings[uint32(oldOffset)]
		if !ok {
			// Debug: look up the orphaned leaf
			oldLeaf := c.reader.leavesData.UnsafeItem(uint32(oldOffset) - 1)
			c.logger.Error("leaf remapping failed - orphaned leaf still referenced",
				"leafOffset", oldOffset,
				"leafId", oldLeaf.Id,
				"leafOrphanVersion", oldLeaf.OrphanVersion,
				"remappings", c.leafOffsetRemappings)
			return 0, fmt.Errorf("failed to find remapping for leaf offset %d", oldOffset)
		}
		return NodeRef(NewNodeRelativePointer(true, int64(newOffset))), nil
	} else {
		// branch nodes we reduce by the number of skipped nodes
		oldOffset := relPtr.Offset()
		newOffset := oldOffset - int64(skipped)
		return NodeRef(NewNodeRelativePointer(false, newOffset)), nil
	}
}

func (c *Compactor) MarkOrphan(orphanVersion uint32, orphanNodeId NodeID) {
	c.pendingOrphansLoc.Lock()
	defer c.pendingOrphansLoc.Unlock()

	c.pendingOrphans = append(c.pendingOrphans, pendingOrphan{version: orphanVersion, nodeId: orphanNodeId})
}

func (c *Compactor) seal(info ChangesetInfo) (*Changeset, error) {
	infoWriter, err := NewStructWriter[ChangesetInfo](filepath.Join(c.dir, "info.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create changeset info writer: %w", err)
	}
	if err := infoWriter.Append(&info); err != nil {
		return nil, fmt.Errorf("failed to write changeset info: %w", err)
	}
	if infoWriter.Count() != 1 {
		return nil, fmt.Errorf("expected info writer count to be 1, got %d", infoWriter.Count())
	}
	infoFile, err := infoWriter.Dispose()
	if err != nil {
		return nil, fmt.Errorf("failed to dispose info writer: %w", err)
	}

	leavesFile, err := c.leavesWriter.Dispose()
	if err != nil {
		return nil, fmt.Errorf("failed to dispose leaves writer: %w", err)
	}

	branchesFile, err := c.branchesWriter.Dispose()
	if err != nil {
		return nil, fmt.Errorf("failed to dispose branches writer: %w", err)
	}

	versionsFile, err := c.versionsWriter.Dispose()
	if err != nil {
		return nil, fmt.Errorf("failed to dispose versions writer: %w", err)
	}

	var kvDataFile *os.File
	if c.kvlogWriter != nil {
		kvDataFile, err = c.kvlogWriter.Dispose()
		if err != nil {
			return nil, fmt.Errorf("failed to dispose KV log writer: %w", err)
		}
	} else {
		// Reusing existing KV log - open it
		kvDataFile, err = os.OpenFile(c.kvlogPath, os.O_RDWR, 0o644)
		if err != nil {
			return nil, fmt.Errorf("failed to open existing KV log: %w", err)
		}
	}

	reader := NewChangeset(c.dir, c.kvlogPath, c.treeStore)
	err = reader.Init(infoFile, kvDataFile, leavesFile, branchesFile, versionsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize changeset reader: %w", err)
	}

	return reader, nil
}

func (c *Compactor) ApplyPendingOrphans(newChangeset *Changeset) error {
	c.pendingOrphansLoc.Lock()
	defer c.pendingOrphansLoc.Unlock()

	for _, po := range c.pendingOrphans {
		err := newChangeset.MarkOrphan(po.version, po.nodeId)
		if err != nil {
			return fmt.Errorf("failed to mark orphan %s for version %d: %w", po.nodeId, po.version, err)
		}
	}
	c.pendingOrphans = nil

	return nil
}
