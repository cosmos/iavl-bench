package x3

import (
	"fmt"
)

type Changeset struct {
	sealed    bool
	compacted *Changeset

	startVersion  uint64
	endVersion    uint64
	stagedVersion uint64

	branchFileIdx uint32
	leafFileIdx   uint32

	KVDataStore
	branchesFile *MmapFile
	branchesData Branches
	leavesFile   *MmapFile
	leavesData   Leaves
	versionsFile *MmapFile
	versionsData Versions
}

func (cs *Changeset) ResolveLeaf(nodeId NodeID, fileIdx uint64) (LeafLayout, error) {
	if compacted := cs.compacted; compacted != nil {
		return compacted.ResolveLeaf(nodeId, fileIdx)
	}
	//TODO implement me
	panic("implement me")
}

func (cs *Changeset) ResolveBranch(nodeId NodeID, fileIdx uint64) (BranchData, error) {
	if compacted := cs.compacted; compacted != nil {
		return compacted.ResolveBranch(nodeId, fileIdx)
	}
	//TODO implement me
	panic("implement me")
}

func (cs *Changeset) SaveRoot(root *NodePointer, version uint64) error {
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

		err = cs.leavesFile.SaveAndRemap()
		if err != nil {
			return fmt.Errorf("failed to save leaf data: %w", err)
		}
		err = cs.branchesFile.SaveAndRemap()
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

	// convert to []byte unsafely
	cs.branchFileIdx++
	np.fileIdx = cs.branchFileIdx
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

	cs.leafFileIdx++
	np.fileIdx = cs.leafFileIdx
	np.store = cs

	return nil
}

func (cs *Changeset) createNodeRef(np *NodePointer) NodeRef {
	if np.store == cs {
		if np.id.IsLeaf() {
			return NodeRef(np.id)
		} else {
			// for branch nodes the relative offset is the difference between the parent ID index and the branch ID index
			relOffset := int64(np.fileIdx) - int64(cs.branchFileIdx+1)
			return NodeRef(NewNodeRelativePointer(false, relOffset))
		}
	} else {
		return NodeRef(np.id)
	}
}
