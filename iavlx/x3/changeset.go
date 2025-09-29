package x3

import (
	"fmt"
)

type Changeset struct {
	sealed    bool
	compacted *Changeset

	startVersion  uint32
	endVersion    uint32
	stagedVersion uint32

	KVDataStore
	branchesData *NodeFile[BranchLayout]
	leavesData   *NodeFile[LeafLayout]
	versionsData *StructFile[VersionInfo]
}

func (cs *Changeset) ResolveLeaf(nodeId NodeID, fileIdx uint32) (*LeafLayout, error) {
	if compacted := cs.compacted; compacted != nil {
		return compacted.ResolveLeaf(nodeId, fileIdx)
	}
	//TODO implement me
	panic("implement me")
}

func (cs *Changeset) ResolveBranch(nodeId NodeID, fileIdx uint32) (*BranchLayout, error) {
	if compacted := cs.compacted; compacted != nil {
		return compacted.ResolveBranch(nodeId, fileIdx)
	}
	//TODO implement me
	panic("implement me")
}

func (cs *Changeset) ResolveNodeID(nodeRef NodeRef, selfIdx uint32) (NodeID, error) {
	//TODO implement me
	panic("implement me")
}

func (cs *Changeset) SaveRoot(root *NodePointer, version uint32) error {
	if cs.sealed {
		return fmt.Errorf("changeset is sealed")
	}

	if version != cs.stagedVersion {
		return fmt.Errorf("version mismatch: expected %d, got %d", cs.stagedVersion, version)
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
	}
	// TODO save version to commit log
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
	nodeId := np.id
	// recursively write children in post-order traversal
	err := cs.writeNode(node.left)
	if err != nil {
		return err
	}
	err = cs.writeNode(node.right)
	if err != nil {
		return err
	}

	// now write parent
	leftRef := cs.createNodeRef(node.left)
	rightRef := cs.createNodeRef(node.right)
	layout := BranchLayout{
		id:    nodeId,
		left:  leftRef,
		right: rightRef,
		// TODO key
		keyOffset:     0,
		keyLoc:        0,
		height:        node.height,
		size:          uint32(node.size), // TODO check overflow
		orphanVersion: 0,
		// TODO hash
	}

	err = cs.branchesData.Append(&layout) // TODO check error
	if err != nil {
		return fmt.Errorf("failed to write branch node: %w", err)
	}

	// convert to []byte unsafely
	np.fileIdx = cs.branchesData.TotalCount()
	np.store = cs

	return nil
}

func (cs *Changeset) writeLeaf(np *NodePointer, node *MemNode) error {
	// write key

	layout := LeafLayout{
		id:            np.id,
		keyOffset:     0, // TODO
		orphanVersion: 0,
		hash:          [32]byte{}, // TODO
	}
	err := cs.leavesData.Append(&layout) // TODO check error
	if err != nil {
		return fmt.Errorf("failed to write leaf node: %w", err)
	}

	np.fileIdx = cs.leavesData.TotalCount()
	np.store = cs

	return nil
}

func (cs *Changeset) createNodeRef(np *NodePointer) NodeRef {
	if np.store == cs {
		if np.id.IsLeaf() {
			return NodeRef(np.id)
		} else {
			// for branch nodes the relative offset is the difference between the parent ID index and the branch ID index
			relOffset := int64(np.fileIdx) - int64(cs.branchesData.TotalCount()+1)
			return NodeRef(NewNodeRelativePointer(false, relOffset))
		}
	} else {
		return NodeRef(np.id)
	}
}

func (cs *Changeset) compactBranches(retainCriteria RetainCriteria, newBranches *StructFile[BranchLayout]) error {
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
			err := newBranches.Append(branch)
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

func (cs *Changeset) compactLeaves(retainCriteria RetainCriteria, newBranches *StructFile[LeafLayout]) error {
	if !cs.sealed {
		return fmt.Errorf("changeset is not sealed")
	}

	n := cs.leavesData.OnDiskCount()
	for i := uint32(0); i < n; i++ {
		leaf := cs.leavesData.Item(i)
		if retainCriteria(uint32(leaf.id.Version()), leaf.orphanVersion) {
			// TODO save key data to KV store if needed
			err := newBranches.Append(leaf)
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
		cs.KVDataStore.file.Offset())
}
