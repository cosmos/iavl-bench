package internal

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync/atomic"
)

type RollingDiff struct {
	*BasicNodeStore
	wal                 *WAL
	stagedVersion       uint64
	savedVersion        atomic.Uint64
	leafFileIdx         uint64 // the offset within the leaf file in number of nodes
	branchFileIdx       uint64 // the offset within the branch file in number of nodes
	leafVersionStartIdx uint64 // the offset within the leaf file in number of nodes for the start of this version
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

	nodeStore := &BasicNodeStore{
		KVData:     wal,
		leafData:   LeavesFile{leafData},
		branchData: BranchesFile{branchData},
	}
	rd := &RollingDiff{
		wal:                 wal,
		BasicNodeStore:      nodeStore,
		stagedVersion:       startVersion + 1,
		leafFileIdx:         nodeStore.leafData.Count(),
		branchFileIdx:       nodeStore.branchData.Count(),
		leafVersionStartIdx: nodeStore.leafData.Count(),
	}

	// Initialize savedVersion to the starting version
	rd.savedVersion.Store(startVersion)

	return rd, nil
}

// WriteRoot writes the root node and commits the version
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
	if memKeyRef, ok := node._keyRef.(*MemNode); ok {
		if bytes.Compare(memKeyRef.key, node.key) != 0 {
			panic(fmt.Sprintf("key ref mismatch: node key %x, key ref %x", node.key, memKeyRef.key))
		}
	}
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
			return NodeRef(NewNodeRelativePointer(true, int64(np.fileIdx)))
		} else {
			// for branch nodes the relative offset is the difference between the parent ID index and the branch ID index
			relOffset := int64(np.fileIdx) - int64(rd.branchFileIdx+1)
			return NodeRef(NewNodeRelativePointer(false, relOffset))
		}
	} else {
		return NodeRef(np.id)
	}
}

var _ NodeStore = &RollingDiff{}
