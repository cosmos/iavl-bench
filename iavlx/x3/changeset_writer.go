package x3

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type ChangesetWriter struct {
	startVersion  uint32
	endVersion    uint32
	stagedVersion uint32

	dir          string
	kvlog        *KVLogWriter
	branchesData *StructWriter[BranchLayout]
	leavesData   *StructWriter[LeafLayout]
	versionsData *StructWriter[VersionInfo]

	reader *Changeset

	keyCache map[string]uint32
}

func NewChangesetWriter(dir string, startVersion uint32, treeStore *TreeStore) (*ChangesetWriter, error) {
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, fmt.Errorf("failed to create changeset dir: %w", err)
	}

	kvData, err := NewKVDataWriter(filepath.Join(dir, "kv.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create KV data store: %w", err)
	}

	leavesData, err := NewStructWriter[LeafLayout](filepath.Join(dir, "leaves.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create leaves data file: %w", err)
	}

	branchesData, err := NewStructWriter[BranchLayout](filepath.Join(dir, "branches.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create branches data file: %w", err)
	}

	versionsData, err := NewStructWriter[VersionInfo](filepath.Join(dir, "versions.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create versions data file: %w", err)
	}

	cs := &ChangesetWriter{
		dir:           dir,
		startVersion:  0,
		endVersion:    0,
		stagedVersion: startVersion,
		kvlog:         kvData,
		branchesData:  branchesData,
		leavesData:    leavesData,
		versionsData:  versionsData,
		reader:        NewChangeset(dir, treeStore),
		keyCache:      make(map[string]uint32),
	}
	return cs, nil
}

func (cs *ChangesetWriter) WriteWALUpdates(updates []KVUpdate) error {
	return cs.kvlog.WriteUpdates(updates)
}

func (cs *ChangesetWriter) WriteWALCommit(version uint32) error {
	return cs.kvlog.WriteCommit(version)
}

func (cs *ChangesetWriter) SaveRoot(root *NodePointer, version uint32, totalLeaves, totalBranches uint32) error {
	if version != cs.stagedVersion {
		return fmt.Errorf("version mismatch: expected %d, got %d", cs.stagedVersion, version)
	}

	var versionInfo VersionInfo
	versionInfo.Branches.StartOffset = uint32(cs.branchesData.Count())
	versionInfo.Leaves.StartOffset = uint32(cs.leavesData.Count())
	if totalBranches > 0 {
		versionInfo.Branches.StartIndex = 1
		versionInfo.Branches.Count = totalBranches
		versionInfo.Branches.EndIndex = totalBranches
	}
	if totalLeaves > 0 {
		versionInfo.Leaves.StartIndex = 1
		versionInfo.Leaves.Count = totalLeaves
		versionInfo.Leaves.EndIndex = totalLeaves
	}

	if root != nil {
		err := cs.writeNode(root)
		if err != nil {
			return err
		}

		versionInfo.RootID = root.id
	}

	// commit version info
	err := cs.versionsData.Append(&versionInfo)
	if err != nil {
		return fmt.Errorf("failed to write version info: %w", err)
	}

	// Set start version on first successful save
	if cs.startVersion == 0 {
		cs.startVersion = version
	}

	// Always update end version
	cs.endVersion = version

	cs.stagedVersion++

	return nil
}

func (cs *ChangesetWriter) Flush() error {
	// Flush all data to disk
	err := cs.leavesData.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush leaf data: %w", err)
	}
	err = cs.branchesData.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush branch data: %w", err)
	}
	err = cs.kvlog.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush KV data: %w", err)
	}
	err = cs.versionsData.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush version data: %w", err)
	}
	return nil
}

func (cs *ChangesetWriter) writeNode(np *NodePointer) error {
	memNode := np.mem.Load()
	if memNode == nil {
		return nil // already persisted
	}
	if memNode.version != cs.stagedVersion {
		return nil // not part of this version
	}
	if memNode.IsLeaf() {
		return cs.writeLeaf(np, memNode)
	} else {
		return cs.writeBranch(np, memNode)
	}
}

func (cs *ChangesetWriter) writeBranch(np *NodePointer, node *MemNode) error {
	// recursively write children in post-order traversal
	err := cs.writeNode(node.left)
	if err != nil {
		return err
	}
	err = cs.writeNode(node.right)
	if err != nil {
		return err
	}

	// TODO cache key offset in memory to avoid duplicate writes
	keyOffset, ok := cs.keyCache[string(node.key)]
	if !ok {
		var err error
		keyOffset, err = cs.kvlog.WriteK(node.key)
		if err != nil {
			return fmt.Errorf("failed to write key data: %w", err)
		}
	}

	// now write parent
	parentIdx := int64(cs.branchesData.Count() + 1) // +1 to account for the node being written
	leftRef := cs.createNodeRef(parentIdx, node.left)
	rightRef := cs.createNodeRef(parentIdx, node.right)

	layout := BranchLayout{
		Id:            np.id,
		Left:          leftRef,
		Right:         rightRef,
		KeyOffset:     keyOffset,
		KeyLoc:        0, // TODO
		Height:        node.height,
		Size:          uint32(node.size), // TODO check overflow
		OrphanVersion: 0,
	}
	copy(layout.Hash[:], node.hash) // TODO check length

	err = cs.branchesData.Append(&layout) // TODO check error
	if err != nil {
		return fmt.Errorf("failed to write branch node: %w", err)
	}

	np.fileIdx = uint32(cs.branchesData.Count())
	np.store = cs.reader

	return nil
}

func (cs *ChangesetWriter) writeLeaf(np *NodePointer, node *MemNode) error {
	keyOffset := node.keyOffset
	if keyOffset == 0 {
		var err error
		keyOffset, err = cs.kvlog.WriteKV(node.key, node.value)
		if err != nil {
			return fmt.Errorf("failed to write key-value data: %w", err)
		}
	}

	layout := LeafLayout{
		Id:            np.id,
		KeyOffset:     keyOffset,
		OrphanVersion: 0,
	}
	copy(layout.Hash[:], node.hash) // TODO check length

	err := cs.leavesData.Append(&layout)
	if err != nil {
		return fmt.Errorf("failed to write leaf node: %w", err)
	}

	np.fileIdx = uint32(cs.leavesData.Count())
	np.store = cs.reader

	cs.keyCache[string(node.key)] = keyOffset

	return nil
}

func (cs *ChangesetWriter) createNodeRef(parentIdx int64, np *NodePointer) NodeRef {
	if np.store == cs.reader {
		if np.id.IsLeaf() {
			return NodeRef(NewNodeRelativePointer(true, int64(np.fileIdx)))
		} else {
			// for branch nodes the relative offset is the difference between the parent ID index and the branch ID index
			relOffset := int64(np.fileIdx) - parentIdx
			return NodeRef(NewNodeRelativePointer(false, relOffset))
		}
	} else {
		return NodeRef(np.id)
	}
}

func (cs *ChangesetWriter) TotalBytes() uint64 {
	return uint64(cs.leavesData.Size() +
		cs.branchesData.Size() +
		cs.versionsData.Size() +
		cs.kvlog.Size())
}

func (cs *ChangesetWriter) Seal() (*Changeset, error) {
	info := ChangesetInfo{
		StartVersion: cs.startVersion,
		EndVersion:   cs.endVersion,
	}
	return cs.seal(info)
}

func (cs *ChangesetWriter) seal(info ChangesetInfo) (*Changeset, error) {
	infoWriter, err := NewStructWriter[ChangesetInfo](filepath.Join(cs.dir, "info.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create changeset info file: %w", err)
	}
	if err := infoWriter.Append(&info); err != nil {
		return nil, fmt.Errorf("failed to write changeset info: %w", err)
	}
	if infoWriter.Count() != 1 {
		return nil, fmt.Errorf("expected info writer count to be 1, got %d", infoWriter.Count())
	}
	infoFile, err := infoWriter.Dispose()
	if err != nil {
		return nil, fmt.Errorf("failed to close changeset info file: %w", err)
	}

	var errs []error
	leavesFile, err := cs.leavesData.Dispose()
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to close leaves data: %w", err))
	}
	branchesFile, err := cs.branchesData.Dispose()
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to close branches data: %w", err))
	}
	versionsFile, err := cs.versionsData.Dispose()
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to close versions data: %w", err))
	}
	kvDataFile, err := cs.kvlog.Dispose()
	if err != nil {
		errs = append(errs, fmt.Errorf("failed to close KV data: %w", err))
	}

	err = errors.Join(errs...)
	if err != nil {
		return nil, err
	}

	err = cs.reader.Init(
		infoFile,
		kvDataFile,
		leavesFile,
		branchesFile,
		versionsFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open changeset reader: %w", err)
	}

	return cs.reader, nil
}
