package x3

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

	processedChangesets []*Changeset
	treeStore           *TreeStore
	dir                 string
	kvlogPath           string

	leavesWriter   *StructWriter[LeafLayout]
	branchesWriter *StructWriter[BranchLayout]
	versionsWriter *StructWriter[VersionInfo]
	kvlogWriter    *KVLogWriter

	leafOffsetRemappings map[uint32]uint32
	keyCache             map[string]uint32

	// Running totals across all processed changesets
	leafOrphanCount          uint32
	branchOrphanCount        uint32
	leafOrphanVersionTotal   uint64
	branchOrphanVersionTotal uint64
}

func NewCompacter(logger *slog.Logger, reader *Changeset, opts CompactOptions, store *TreeStore) (*Compactor, error) {
	dir := reader.dir
	dirName := filepath.Base(dir)
	split := strings.Split(dirName, ".") // Split base name only, not full path
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

	// Ensure absolute path
	absNewDir, err := filepath.Abs(newDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for %s: %w", newDir, err)
	}
	newDir = absNewDir

	err = os.MkdirAll(newDir, 0o755)
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

	// Process first changeset immediately
	err = c.processChangeset(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to process initial changeset: %w", err)
	}

	return c, nil
}

func (c *Compactor) processChangeset(reader *Changeset) error {
	// Compute KV offset delta for non-CompactWAL mode
	kvOffsetDelta := uint32(0)
	if c.kvlogWriter != nil && !c.compactWAL {
		kvOffsetDelta = uint32(c.kvlogWriter.Size())
	}

	versionsData := reader.versionsData
	numVersions := versionsData.Count()
	leavesData := reader.leavesData
	branchesData := reader.branchesData

	c.logger.Debug("processing changeset for compaction", "versions", numVersions)
	for i := 0; i < numVersions; i++ {
		c.logger.Debug("compacting version", "version", reader.info.StartVersion+uint32(i))
		verInfo := *versionsData.UnsafeItem(uint32(i)) // copy
		newLeafStartIdx := uint32(0)
		newLeafEndIdx := uint32(0)
		leafStartOffset := verInfo.Leaves.StartOffset
		leafCount := verInfo.Leaves.Count
		newLeafStartOffset := uint32(c.leavesWriter.Count())
		newLeafCount := uint32(0)
		// Iterate leaves
		// For each leaf, check if it should be retained
		for j := uint32(0); j < leafCount; j++ {
			leaf := *leavesData.UnsafeItem(leafStartOffset + j) // copy
			id := leaf.Id
			retain := leaf.OrphanVersion == 0 || c.criteria(uint32(id.Version()), leaf.OrphanVersion)
			if !retain {
				continue
			}

			if leaf.OrphanVersion != 0 {
				c.leafOrphanCount++
				c.leafOrphanVersionTotal += uint64(leaf.OrphanVersion)
			}

			if newLeafStartIdx == 0 {
				newLeafStartIdx = id.Index()
			}
			newLeafEndIdx = id.Index()
			newLeafCount++

			if c.compactWAL {
				k, v, err := reader.ReadKV(id, leaf.KeyOffset)
				if err != nil {
					return fmt.Errorf("failed to read KV for leaf %s: %w", id, err)
				}

				offset, err := c.kvlogWriter.WriteKV(k, v)
				if err != nil {
					return fmt.Errorf("failed to write KV for leaf %s: %w", id, err)
				}

				leaf.KeyOffset = offset
				c.keyCache[string(k)] = offset
			} else {
				// When not compacting WAL, add offset delta
				leaf.KeyOffset += kvOffsetDelta
			}

			err := c.leavesWriter.Append(&leaf)
			if err != nil {
				return fmt.Errorf("failed to append leaf %s: %w", id, err)
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
			retain := branch.OrphanVersion == 0 || c.criteria(uint32(id.Version()), branch.OrphanVersion)
			if !retain {
				skippedBranches++
				continue
			}

			if branch.OrphanVersion != 0 {
				c.branchOrphanCount++
				c.branchOrphanVersionTotal += uint64(branch.OrphanVersion)
			}

			if newBranchStartIdx == 0 {
				newBranchStartIdx = id.Index()
			}
			newBranchEndIdx = id.Index()
			newBranchCount++

			var err error
			left := branch.Left
			branch.Left, err = c.updateNodeRef(reader, left, skippedBranches)
			if err != nil {
				c.logger.Error("failed to update left ref",
					"branchId", id,
					"branchOrphanVersion", branch.OrphanVersion,
					"leftRef", left)
				return fmt.Errorf("failed to update left ref for branch %s: %w", id, err)
			}
			right := branch.Right
			branch.Right, err = c.updateNodeRef(reader, right, skippedBranches)
			if err != nil {
				c.logger.Error("failed to update right ref",
					"branchId", id,
					"branchOrphanVersion", branch.OrphanVersion,
					"rightRef", right)
				return fmt.Errorf("failed to update right ref for branch %s: %w", id, err)
			}

			if c.compactWAL {
				k, err := reader.ReadK(id, branch.KeyOffset)
				if err != nil {
					return fmt.Errorf("failed to read key for branch %s: %w", id, err)
				}
				offset, ok := c.keyCache[string(k)]
				if !ok {
					offset, err = c.kvlogWriter.WriteK(k)
				}
				if err != nil {
					return fmt.Errorf("failed to write key for branch %s: %w", id, err)
				}
				branch.KeyOffset = offset
			} else {
				// When not compacting WAL, add offset delta
				branch.KeyOffset += kvOffsetDelta
			}

			err = c.branchesWriter.Append(&branch)
			if err != nil {
				return fmt.Errorf("failed to append branch %s: %w", id, err)
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
			return fmt.Errorf("failed to append version info for version %d: %w", reader.info.StartVersion+uint32(i), err)
		}
	}

	// Track this changeset as processed
	c.processedChangesets = append(c.processedChangesets, reader)

	return nil
}

func (c *Compactor) AddChangeset(cs *Changeset) error {
	// TODO: Support joining changesets when CompactWAL=false
	// This requires copying the entire KV log and tracking cumulative offsets
	if !c.compactWAL {
		return fmt.Errorf("joining changesets is only supported when CompactWAL=true")
	}
	return c.processChangeset(cs)
}

func (c *Compactor) Seal() (*Changeset, error) {
	if len(c.processedChangesets) == 0 {
		return nil, fmt.Errorf("no changesets processed")
	}

	info := ChangesetInfo{
		StartVersion:             c.processedChangesets[0].info.StartVersion,
		EndVersion:               c.processedChangesets[len(c.processedChangesets)-1].info.EndVersion,
		LeafOrphans:              c.leafOrphanCount,
		BranchOrphans:            c.branchOrphanCount,
		LeafOrphanVersionTotal:   c.leafOrphanVersionTotal,
		BranchOrphanVersionTotal: c.branchOrphanVersionTotal,
	}

	return c.sealWithInfo(info)
}

func (c *Compactor) updateNodeRef(reader *Changeset, ref NodeRef, skipped int) (NodeRef, error) {
	if ref.IsNodeID() {
		return ref, nil
	}
	relPtr := ref.AsRelativePointer()
	if relPtr.IsLeaf() {
		oldOffset := relPtr.Offset()
		newOffset, ok := c.leafOffsetRemappings[uint32(oldOffset)]
		if !ok {
			// Debug: look up the orphaned leaf
			oldLeaf := reader.leavesData.UnsafeItem(uint32(oldOffset) - 1)
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

func (c *Compactor) sealWithInfo(info ChangesetInfo) (*Changeset, error) {
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

	// Validate initialization
	if reader.info == nil {
		return nil, fmt.Errorf("BUG: compacted changeset init resulted in nil info")
	}
	if reader.infoReader == nil || reader.infoReader.Count() == 0 {
		return nil, fmt.Errorf("BUG: compacted info reader not properly initialized, count=%d", reader.infoReader.Count())
	}
	return reader, nil
}

func (c *Compactor) TotalBytes() uint64 {
	total := uint64(c.leavesWriter.Size() + c.branchesWriter.Size() + c.versionsWriter.Size())
	if c.kvlogWriter != nil {
		total += uint64(c.kvlogWriter.Size())
	}
	return total
}
