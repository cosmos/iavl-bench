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
	kvdata       *KVDataWriter
	branchesData *StructWriter[BranchLayout]
	leavesData   *StructWriter[LeafLayout]
	versionsData *StructWriter[VersionInfo]

	reader *ChangesetReader
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
		kvdata:        kvData,
		branchesData:  branchesData,
		leavesData:    leavesData,
		versionsData:  versionsData,
		reader:        NewChangesetReader(dir, treeStore),
	}
	return cs, nil
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

	// Flush all data to disk
	err = cs.leavesData.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush leaf data: %w", err)
	}
	err = cs.branchesData.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush branch data: %w", err)
	}
	err = cs.kvdata.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush KV data: %w", err)
	}
	err = cs.versionsData.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush version data: %w", err)
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
	keyOffset, err := cs.kvdata.WriteK(node.key)
	if err != nil {
		return fmt.Errorf("failed to write key data: %w", err)
	}

	// now write parent
	parentIdx := int64(cs.branchesData.Count() + 1) // +1 to account for the node being written
	leftRef := cs.createNodeRef(parentIdx, node.left)
	rightRef := cs.createNodeRef(parentIdx, node.right)

	// Validate NodeRefs before storing
	if leftRef.IsRelativePointer() && leftRef.IsLeaf() {
		testOffset := leftRef.AsRelativePointer().Offset()
		if testOffset < 1 || testOffset > 100000 {
			panic(fmt.Sprintf("BUG: created leftRef with bad offset %d, raw=0x%016X", testOffset, uint64(leftRef)))
		}
	}
	if rightRef.IsRelativePointer() && rightRef.IsLeaf() {
		testOffset := rightRef.AsRelativePointer().Offset()
		if testOffset < 1 || testOffset > 100000 {
			panic(fmt.Sprintf("BUG: created rightRef with bad offset %d, raw=0x%016X", testOffset, uint64(rightRef)))
		}
	}

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
	keyOffset, err := cs.kvdata.WriteKV(node.key, node.value)
	if err != nil {
		return fmt.Errorf("failed to write key-value data: %w", err)
	}

	layout := LeafLayout{
		Id:            np.id,
		KeyOffset:     keyOffset,
		OrphanVersion: 0,
	}
	copy(layout.Hash[:], node.hash) // TODO check length

	err = cs.leavesData.Append(&layout)
	if err != nil {
		return fmt.Errorf("failed to write leaf node: %w", err)
	}

	np.fileIdx = uint32(cs.leavesData.Count())
	np.store = cs.reader

	return nil
}

func (cs *ChangesetWriter) createNodeRef(parentIdx int64, np *NodePointer) NodeRef {
	if np.store == cs.reader {
		if np.id.IsLeaf() {
			offset := int64(np.fileIdx)
			if offset < 1 || offset > 1000000 {
				panic(fmt.Sprintf("BUG: creating leaf NodeRef with suspicious offset %d (fileIdx=%d, leavesCount=%d)",
					offset, np.fileIdx, cs.leavesData.Count()))
			}
			return NodeRef(NewNodeRelativePointer(true, offset))
		} else {
			// for branch nodes the relative offset is the difference between the parent ID index and the branch ID index
			relOffset := int64(np.fileIdx) - parentIdx
			if relOffset < -1000000 || relOffset > 1000000 {
				panic(fmt.Sprintf("BUG: creating branch NodeRef with suspicious relOffset %d (fileIdx=%d, parentIdx=%d, branchesCount=%d)",
					relOffset, np.fileIdx, parentIdx, cs.branchesData.Count()))
			}
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
		cs.kvdata.Size())
}

func (cs *ChangesetWriter) Seal() (*ChangesetReader, error) {
	info := ChangesetInfo{
		StartVersion: cs.startVersion,
		EndVersion:   cs.endVersion,
	}
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
	if err := infoWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close changeset info file: %w", err)
	}

	var errs []error
	if err := cs.leavesData.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close leaves data: %w", err))
	}
	if err := cs.branchesData.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close branches data: %w", err))
	}
	if err := cs.versionsData.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close versions data: %w", err))
	}
	if err := cs.kvdata.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close KV data: %w", err))
	}

	err = errors.Join(errs...)
	if err != nil {
		return nil, err
	}

	err = cs.reader.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open changeset reader: %w", err)
	}

	return cs.reader, nil
}
