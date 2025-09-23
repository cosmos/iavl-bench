package internal

import (
	"path/filepath"
	"sync/atomic"
)

type RollingDiff struct {
	*WAL
	stagedVersion       uint64
	savedVersion        atomic.Uint64
	leafFileIdx         int64 // the offset within the leaf file in number of nodes
	branchFileIdx       int64 // the offset within the branch file in number of nodes
	leafVersionStartIdx int64 // the offset within the leaf file in number of nodes for the start of this version

	leafData   *MmapFile
	branchData *MmapFile
}

func NewRollingDiff(wal *WAL, dir string, startVersion uint64) (*RollingDiff, error) {
	leafFile := filepath.Join(dir, "leaves.dat")
	leafData, err := NewMmapFile(leafFile)
	if err != nil {
		return nil, err
	}

	branchFile := filepath.Join(dir, "branches.dat")
	branchData, err := NewMmapFile(branchFile)
	if err != nil {
		return nil, err
	}

	rd := &RollingDiff{
		WAL:                 wal,
		stagedVersion:       startVersion + 1,
		leafFileIdx:         int64(leafData.Offset() / SizeLeaf),
		branchFileIdx:       int64(branchData.Offset() / SizeBranch),
		leafVersionStartIdx: int64(leafData.Offset() / SizeLeaf),
		leafData:            leafData,
		branchData:          branchData,
	}

	return rd, nil
}

//func (rd *RollingDiff) WriteVersion(version uint64, rootPtr *NodePointer) error {
//	if rootPtr == nil {
//		// TODO advance the version even if root is nil
//		return nil
//	}
//
//	//root, err := rootPtr.Resolve()
//	//if err != nil {
//	//	return err
//	//}
//	//
//	//panic("TODO")
//}

//	func (rd *RollingDiff) writeNode(node *MemNode) error {
//		if node.IsLeaf() {
//			return rd.writeLeaf(node)
//		} else {
//			return rd.writeBranch(node)
//		}
//	}
func (rd *RollingDiff) writeRoot(root *NodePointer, lastBranchIdx uint32) error {
	if root == nil {
		// TODO advance the version even if root is nil
		return nil
	}

	err := rd.writeNode(root, lastBranchIdx)
	if err != nil {
		return err
	}

	// TODO write root node index and other data to commit file
	rd.savedVersion.Store(rd.stagedVersion)
	rd.stagedVersion++
	rd.leafVersionStartIdx = rd.leafFileIdx
	return nil
}

func (rd *RollingDiff) writeNode(np *NodePointer, span uint32) error {
	memNode := np.mem.Load()
	if memNode == nil {
		return nil // already persisted
	}
	if memNode.version != rd.stagedVersion {
		return nil // not part of this version
	}
	if memNode.IsLeaf() {
		return rd.writeLeaf(np.id, memNode)
	} else {
		// TODO subtree size (can be figured out by the ID of the sibling if any)
		return rd.writeBranch(np.id, memNode, span)
	}
}

func (rd *RollingDiff) writeBranch(nodeId NodeID, node *MemNode, subtreeSpan uint32) error {
	leftRef := rd.createNodeRef(nodeId, node.left)
	rightRef := rd.createNodeRef(nodeId, node.right)
	var buf [SizeBranch]byte
	keyRef := node._keyRef.toKeyRef()
	err := encodeBranchNode(node, buf, nodeId, leftRef, rightRef, keyRef, subtreeSpan)
	if err != nil {
		return err
	}
	_, err = rd.branchData.Write(buf[:])
	if err != nil {
		return err
	}
	rd.branchFileIdx++
	// recursively write children
	leftSpan := node.right.id.Index() - nodeId.Index() - 1
	err = rd.writeNode(node.left, leftSpan)
	if err != nil {
		return err
	}
	rightSpan := subtreeSpan - leftSpan - 1
	err = rd.writeNode(node.right, rightSpan)
	if err != nil {
		return err
	}
	return nil
}

func (rd *RollingDiff) writeLeaf(nodeId NodeID, node *MemNode) error {
	var buf [SizeLeaf]byte
	err := encodeLeafNode(node, buf, nodeId)
	if err != nil {
		return err
	}
	_, err = rd.leafData.Write(buf[:])
	if err != nil {
		return err
	}

	rd.leafFileIdx++
	return nil
}

func (rd *RollingDiff) createNodeRef(parentId NodeID, np *NodePointer) NodeRef {
	if np.store == rd {
		if np.id.IsLeaf() {
			// for leaf nodes the relative offset is the leaf ID index plus the starting index for this version
			return NodeRef(NewNodeRelativePointer(true, int64(np.id.Index())+rd.leafVersionStartIdx))
		} else {
			// for branch nodes the relative offset is the difference between the parent ID index and the branch ID index
			return NodeRef(NewNodeRelativePointer(false, int64(np.id.Index()-parentId.Index())))
		}
	} else {
		return NodeRef(np.id)
	}
}

func (rd *RollingDiff) ResolveLeaf(nodeId NodeID, fileIdx int64) (LeafLayout, error) {
	offset := fileIdx * SizeLeaf
	bz, err := rd.leafData.Slice(int(offset), SizeLeaf)
	if err != nil {
		return LeafLayout{}, err
	}
	return LeafLayout{data: (*[SizeLeaf]byte)(bz)}, nil
}

func (rd *RollingDiff) ResolveBranch(nodeId NodeID, fileIdx int64) (BranchData, error) {
	offset := fileIdx * SizeLeaf
	bz, err := rd.leafData.Slice(int(offset), SizeLeaf)
	if err != nil {
		return BranchData{}, err
	}
	branchLayout := BranchLayout{data: (*[SizeBranch]byte)(bz)}
	// TODO resolve left and right ID if they are relative pointers
	return BranchData{
		selfOffset: fileIdx,
		layout:     branchLayout,
	}, nil
}

var _ NodeStore = &RollingDiff{}
