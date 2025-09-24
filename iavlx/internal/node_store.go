package internal

import "fmt"

type NodeStore interface {
	KVData
	ResolveLeaf(nodeId NodeID, fileIdx uint64) (LeafLayout, error)
	ResolveBranch(nodeId NodeID, fileIdx uint64) (BranchData, error)
}

type BasicNodeStore struct {
	KVData
	leafData    LeavesFile
	branchData  BranchesFile
	leafIndex   NodeIndex
	branchIndex NodeIndex
}

type NodeIndex interface {
	Locate(id NodeID) (uint64, error)
}

func (b *BasicNodeStore) ResolveLeaf(nodeId NodeID, fileIdx uint64) (LeafLayout, error) {
	if fileIdx <= 0 {
		return LeafLayout{}, fmt.Errorf("node ID resolution not supported yet")
	}

	fileIdx--
	return b.leafData.Leaf(fileIdx)
}

func (b *BasicNodeStore) resolveBranchLayout(fileIdx uint64) (BranchLayout, error) {
	fileIdx--
	return b.branchData.Branch(fileIdx)
}

func (b *BasicNodeStore) resolveNodeId(curBranchIdx uint64, relPtr NodeRelativePointer) (NodeID, error) {
	if relPtr.IsLeaf() {
		leafLayout, err := b.ResolveLeaf(0, uint64(relPtr.Offset()))
		if err != nil {
			return 0, err
		}
		return leafLayout.NodeID(), err
	} else {
		// convert from relative to absolute index
		offset := int64(curBranchIdx) + relPtr.Offset()
		branchLayout, err := b.resolveBranchLayout(uint64(offset))
		if err != nil {
			return 0, err
		}
		return branchLayout.NodeID(), nil
	}
}

func (b *BasicNodeStore) ResolveBranch(nodeId NodeID, fileIdx uint64) (BranchData, error) {
	if fileIdx == 0 {
		return BranchData{}, fmt.Errorf("node ID resolution not supported yet")
	}

	branchLayout, err := b.resolveBranchLayout(fileIdx)
	if err != nil {
		return BranchData{}, err
	}
	var leftId, rightId NodeID
	if left := branchLayout.Left(); left.IsRelativePointer() {
		leftId, err = b.resolveNodeId(fileIdx, left.AsRelativePointer())
		if err != nil {
			return BranchData{}, err
		}
	}
	if right := branchLayout.Right(); right.IsRelativePointer() {
		rightId, err = b.resolveNodeId(fileIdx, right.AsRelativePointer())
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

var _ NodeStore = &BasicNodeStore{}
