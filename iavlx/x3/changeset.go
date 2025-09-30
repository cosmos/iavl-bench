package x3

import (
	"fmt"
	"os"
	"path/filepath"
)

type Changeset struct {
	sealed    bool
	compacted *Changeset

	startVersion  uint32
	endVersion    uint32
	stagedVersion uint32

	KVDataReader
	branchesData *NodeReader[BranchLayout]
	leavesData   *NodeReader[LeafLayout]
	versionsData *StructReader[VersionInfo]
}

func (cs *Changeset) Resolve(nodeId NodeID, fileIdx uint32) (Node, error) {
	if nodeId.IsLeaf() {
		layout, err := cs.ResolveLeaf(nodeId, fileIdx)
		if err != nil {
			return nil, err
		}
		return &LeafPersisted{layout: layout, store: cs}, nil
	} else {
		layout, err := cs.ResolveBranch(nodeId, fileIdx)
		if err != nil {
			return nil, err
		}
		return &BranchPersisted{layout: layout, store: cs, selfIdx: fileIdx}, nil
	}
}

func NewChangeset(dir string, startVersion uint32) (*Changeset, error) {
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, fmt.Errorf("failed to create changeset dir: %w", err)
	}

	kvDataStore, err := NewKVDataReader(filepath.Join(dir, "kv.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create KV data store: %w", err)
	}

	leavesData, err := NewNodeReader[LeafLayout](filepath.Join(dir, "leaves.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create leaves data file: %w", err)
	}

	branchesData, err := NewNodeReader[BranchLayout](filepath.Join(dir, "branches.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create branches data file: %w", err)
	}

	versionsData, err := NewStructReader[VersionInfo](filepath.Join(dir, "versions.dat"))
	if err != nil {
		return nil, fmt.Errorf("failed to create versions data file: %w", err)
	}

	cs := &Changeset{
		startVersion:  0,
		endVersion:    0,
		stagedVersion: startVersion,
		KVDataReader:  *kvDataStore,
		branchesData:  branchesData,
		leavesData:    leavesData,
		versionsData:  versionsData,
	}
	return cs, nil
}

func (cs *Changeset) ResolveLeaf(nodeId NodeID, fileIdx uint32) (LeafLayout, error) {
	if compacted := cs.compacted; compacted != nil {
		return compacted.ResolveLeaf(nodeId, fileIdx)
	}
	if fileIdx == 0 {
		return LeafLayout{}, fmt.Errorf("NodeID resolution not implemented for leaves")
	} else {
		fileIdx-- // convert to 0-based index
		return cs.leavesData.Item(fileIdx), nil
	}
}

func (cs *Changeset) ResolveBranch(nodeId NodeID, fileIdx uint32) (BranchLayout, error) {
	if compacted := cs.compacted; compacted != nil {
		return compacted.ResolveBranch(nodeId, fileIdx)
	}
	if fileIdx == 0 {
		return BranchLayout{}, fmt.Errorf("NodeID resolution not implemented for branches")
	} else {
		fileIdx-- // convert to 0-based index
		return cs.branchesData.Item(fileIdx), nil
	}
}

func (cs *Changeset) ResolveNodeRef(nodeRef NodeRef, selfIdx uint32) *NodePointer {
	if nodeRef.IsNodeID() {
		return &NodePointer{
			id: nodeRef.AsNodeID(),
			// TODO should we find the actual store for this version to speed up lookups, or make that lazy?
			store: cs,
		}
	}
	offset := nodeRef.AsRelativePointer().Offset()
	if nodeRef.IsLeaf() {
		layout := cs.leavesData.Item(uint32(offset - 1)) // convert to 0-based index
		return &NodePointer{
			id:      layout.id,
			store:   cs,
			fileIdx: uint32(offset),
		}
	} else {
		idx := int64(selfIdx) + offset
		layout := cs.branchesData.Item(uint32(idx - 1)) // convert to 0-based index
		return &NodePointer{
			id:      layout.id,
			store:   cs,
			fileIdx: uint32(idx),
		}
	}
}

func (cs *Changeset) SaveRoot(root *NodePointer, version uint32, totalLeaves, totalBranches uint32) error {
	if cs.sealed {
		return fmt.Errorf("changeset is sealed")
	}

	if version != cs.stagedVersion {
		return fmt.Errorf("version mismatch: expected %d, got %d", cs.stagedVersion, version)
	}

	var versionInfo VersionInfo
	versionInfo.Branches.StartOffset = cs.branchesData.TotalCount()
	versionInfo.Leaves.StartOffset = cs.leavesData.TotalCount()
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

		err = cs.leavesData.SaveAndRemap()
		if err != nil {
			return fmt.Errorf("failed to save leaf data: %w", err)
		}
		err = cs.branchesData.SaveAndRemap()
		if err != nil {
			return fmt.Errorf("failed to save branch data: %w", err)
		}
		err = cs.KVDataReader.SaveAndRemap()
		if err != nil {
			return fmt.Errorf("failed to save KV data: %w", err)
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

func (cs *Changeset) writeNode(np *NodePointer) error {
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

func (cs *Changeset) writeBranch(np *NodePointer, node *MemNode) error {
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
	keyOffset, err := cs.KVDataReader.WriteK(node.key)
	if err != nil {
		return fmt.Errorf("failed to write key data: %w", err)
	}

	// now write parent
	parentIdx := int64(cs.branchesData.TotalCount() + 1) // +1 to account for the node being written
	leftRef := cs.createNodeRef(parentIdx, node.left)
	rightRef := cs.createNodeRef(parentIdx, node.right)
	layout := BranchLayout{
		id:            np.id,
		left:          leftRef,
		right:         rightRef,
		keyOffset:     keyOffset,
		keyLoc:        0, // TODO
		height:        node.height,
		size:          uint32(node.size), // TODO check overflow
		orphanVersion: 0,
	}
	copy(layout.hash[:], node.hash) // TODO check length

	err = cs.branchesData.Append(&layout) // TODO check error
	if err != nil {
		return fmt.Errorf("failed to write branch node: %w", err)
	}

	np.fileIdx = cs.branchesData.TotalCount()
	np.store = cs

	return nil
}

func (cs *Changeset) writeLeaf(np *NodePointer, node *MemNode) error {
	keyOffset, err := cs.KVDataReader.WriteKV(node.key, node.value)
	if err != nil {
		return fmt.Errorf("failed to write key-value data: %w", err)
	}

	layout := LeafLayout{
		id:            np.id,
		keyOffset:     keyOffset,
		orphanVersion: 0,
	}
	copy(layout.hash[:], node.hash) // TODO check length

	err = cs.leavesData.Append(&layout)
	if err != nil {
		return fmt.Errorf("failed to write leaf node: %w", err)
	}

	np.fileIdx = cs.leavesData.TotalCount()
	np.store = cs

	return nil
}

func (cs *Changeset) createNodeRef(parentIdx int64, np *NodePointer) NodeRef {
	if np.store == cs {
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

func (cs *Changeset) compactBranches(retainCriteria RetainCriteria, newBranches *StructReader[BranchLayout]) error {
	if !cs.sealed {
		return fmt.Errorf("changeset is not sealed")
	}

	n := cs.branchesData.OnDiskCount()
	skipped := 0
	for i := uint32(0); i < n; i++ {
		branch := cs.branchesData.Item(i)
		if retainCriteria(uint32(branch.id.Version()), branch.orphanVersion) {
			// TODO update relative pointers
			// TODO save key data to KV store if needed
			err := newBranches.Append(&branch)
			if err != nil {
				return fmt.Errorf("failed to compact branch node %s: %w", branch.id, err)
			}
		} else {
			skipped++
			// TODO remove key from KV store if possible
		}
	}

	return nil
}

func (cs *Changeset) compactLeaves(retainCriteria RetainCriteria, newBranches *StructReader[LeafLayout]) error {
	if !cs.sealed {
		return fmt.Errorf("changeset is not sealed")
	}

	n := cs.leavesData.OnDiskCount()
	for i := uint32(0); i < n; i++ {
		leaf := cs.leavesData.Item(i)
		if retainCriteria(uint32(leaf.id.Version()), leaf.orphanVersion) {
			// TODO save key data to KV store if needed
			err := newBranches.Append(&leaf)
			if err != nil {
				return fmt.Errorf("failed to compact leaf node %s: %w", leaf.id, err)
			}
		} else {
			// TODO remove key from KV store if possible
		}
	}

	return nil
}

func (cs *Changeset) MarkOrphans(version uint32, nodeIds []NodeID) error {
	// TODO add locking
	for _, nodeId := range nodeIds {
		if nodeId.IsLeaf() {
			leaf, err := cs.ResolveLeaf(nodeId, 0)
			if err != nil {
				return err
			}
			if leaf.orphanVersion == 0 {
				leaf.orphanVersion = version
			}
		} else {
			branch, err := cs.ResolveBranch(nodeId, 0)
			if err != nil {
				return err
			}
			if branch.orphanVersion == 0 {
				branch.orphanVersion = version
			}
		}
	}

	// TODO flush changes to disk

	return nil
}

func (cs *Changeset) TotalBytes() uint64 {
	return uint64(cs.leavesData.file.Offset() +
		cs.branchesData.file.Offset() +
		cs.versionsData.file.Offset() +
		cs.KVDataReader.file.Offset())
}

var _ NodeStore = (*Changeset)(nil)
