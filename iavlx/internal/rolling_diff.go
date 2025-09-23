package internal

import (
	"fmt"
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
func (rd *RollingDiff) writeRoot(version uint64, root *NodePointer, lastBranchIdx uint32) error {
	if version != rd.stagedVersion {
		return fmt.Errorf("version mismatch: expected %d, got %d", rd.stagedVersion, version)
	}
	if root != nil {
		err := rd.writeNode(root, lastBranchIdx)
		if err != nil {
			return err
		}

		err = rd.leafData.SaveAndRemap()
		if err != nil {
			return fmt.Errorf("failed to save leaf data: %w", err)
		}
		err = rd.branchData.SaveAndRemap()
		if err != nil {
			return fmt.Errorf("failed to save branch data: %w", err)
		}
	}
	// TODO save version to commit log
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
		return rd.writeLeaf(np, memNode)
	} else {
		// TODO subtree size (can be figured out by the ID of the sibling if any)
		return rd.writeBranch(np, memNode, span)
	}
}

func (rd *RollingDiff) writeBranch(np *NodePointer, node *MemNode, subtreeSpan uint32) error {
	nodeId := np.id
	// recursively write children in post-order traversal
	leftSpan := node.right.id.Index() - nodeId.Index() - 1
	err := rd.writeNode(node.left, leftSpan)
	if err != nil {
		return err
	}
	rightSpan := subtreeSpan - leftSpan - 1
	err = rd.writeNode(node.right, rightSpan)
	if err != nil {
		return err
	}

	// now write parent
	leftRef := rd.createNodeRef(nodeId, node.left)
	rightRef := rd.createNodeRef(nodeId, node.right)
	var buf [SizeBranch]byte
	keyRef := node._keyRef.toKeyRef()
	err = encodeBranchNode(node, &buf, nodeId, leftRef, rightRef, keyRef, subtreeSpan)
	if err != nil {
		return err
	}
	_, err = rd.branchData.Write(buf[:])
	if err != nil {
		return err
	}

	rd.branchFileIdx++
	np.fileIdx = rd.branchFileIdx
	np.store = rd

	return nil
}

func (rd *RollingDiff) writeLeaf(np *NodePointer, node *MemNode) error {
	nodeId := np.id
	var buf [SizeLeaf]byte
	err := encodeLeafNode(node, &buf, nodeId)
	if err != nil {
		return err
	}
	_, err = rd.leafData.Write(buf[:])
	if err != nil {
		return err
	}

	rd.leafFileIdx++
	np.fileIdx = rd.leafFileIdx
	np.store = rd
	return nil
}

func (rd *RollingDiff) createNodeRef(parentId NodeID, np *NodePointer) NodeRef {
	if np.store == rd {
		if np.id.IsLeaf() {
			// for leaf nodes the relative offset is the leaf ID index plus the starting index for this version
			return NodeRef(NewNodeRelativePointer(true, np.fileIdx))
		} else {
			// for branch nodes the relative offset is the difference between the parent ID index and the branch ID index
			return NodeRef(NewNodeRelativePointer(false, np.fileIdx-(rd.branchFileIdx+1)))
		}
	} else {
		return NodeRef(np.id)
	}
}

func (rd *RollingDiff) ResolveLeaf(nodeId NodeID, fileIdx int64) (LeafLayout, error) {
	if fileIdx <= 0 {
		return LeafLayout{}, fmt.Errorf("node ID resolution not supported yet")
	}

	fileIdx--
	offset := fileIdx * SizeLeaf
	bz, err := rd.leafData.SliceExact(int(offset), SizeLeaf)
	if err != nil {
		return LeafLayout{}, err
	}
	return LeafLayout{data: (*[SizeLeaf]byte)(bz)}, nil
}

func (rd *RollingDiff) resolveBranchLayout(fileIdx int64) (BranchLayout, error) {
	fileIdx--
	offset := fileIdx * SizeBranch
	bz, err := rd.branchData.SliceExact(int(offset), SizeBranch)
	if err != nil {
		return BranchLayout{}, err
	}
	return BranchLayout{data: (*[SizeBranch]byte)(bz)}, nil
}

func (rd *RollingDiff) resolveNodeId(curBranchIdx int64, relPtr NodeRelativePointer) (NodeID, error) {
	if relPtr.IsLeaf() {
		leafLayout, err := rd.ResolveLeaf(0, relPtr.Offset())
		if err != nil {
			return 0, err
		}
		return leafLayout.NodeID(), err
	} else {
		offset := curBranchIdx + relPtr.Offset()
		branchLayout, err := rd.resolveBranchLayout(offset)
		if err != nil {
			return 0, err
		}
		return branchLayout.NodeID(), nil
	}
}

func (rd *RollingDiff) ResolveBranch(nodeId NodeID, fileIdx int64) (BranchData, error) {
	if fileIdx == 0 {
		return BranchData{}, fmt.Errorf("node ID resolution not supported yet")
	}

	branchLayout, err := rd.resolveBranchLayout(fileIdx)
	if err != nil {
		return BranchData{}, err
	}
	var leftId, rightId NodeID
	if left := branchLayout.Left(); left.IsRelativePointer() {
		leftId, err = rd.resolveNodeId(fileIdx, left.AsRelativePointer())
		if err != nil {
			return BranchData{}, err
		}
	}
	if right := branchLayout.Right(); right.IsRelativePointer() {
		rightId, err = rd.resolveNodeId(fileIdx, right.AsRelativePointer())
		if err != nil {
			return BranchData{}, err
		}
	}
	return BranchData{
		selfOffset: fileIdx,
		layout:     branchLayout,
		leftId:     leftId,
		rightId:    rightId,
	}, nil
}

var _ NodeStore = &RollingDiff{}
