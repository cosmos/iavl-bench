package internal

import (
	"bufio"
	"fmt"
	"os"
)

type RollingDiff struct {
	*WAL
	stagedVersion    uint64
	leafVersionIdx   uint32 // the index within the version of the leaf nodes
	branchVersionIdx uint32 // the index within the version of the branch nodes
	leafFileIdx      int64  // the offset within the leaf file in number of nodes
	branchFileIdx    int64  // the offset within the branch file in number of nodes

	leafFile     *os.File
	leafWriter   *bufio.Writer
	branchFile   *os.File
	branchWriter *bufio.Writer
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
func (rd *RollingDiff) writeRoot(root *NodePointer) error {
	if root == nil {
		// TODO advance the version even if root is nil
		return nil
	}
	// post-order traversal
	stack1 := []*NodePointer{root}
	var stack2 []*NodePointer

	for len(stack1) > 0 {
		nodePtr := stack1[len(stack1)-1]
		stack1 = stack1[:len(stack1)-1]
		stack2 = append(stack2, nodePtr)
		node := nodePtr.mem.Load()
		if node.left.mem.Load() != nil {
			stack1 = append(stack1, node.left)
		}
		if node.right.mem.Load() != nil {
			stack1 = append(stack1, node.right)
		}
	}

	for i := len(stack2) - 1; i >= 0; i-- {
		nodePtr := stack2[i]
		node := nodePtr.mem.Load()
		if node == nil {
			return fmt.Errorf("node is nil, expected an in-memory node")
		}

		if node.IsLeaf() {
			nodeId, fileIdx, err := rd.writeLeaf(node)
			if err != nil {
				return err
			}
			nodePtr.id = nodeId
			nodePtr.fileIdx = fileIdx
			// TODO reference key offset in the WAL here (or we can move writing that to *NodePointer too?)
		} else {
			id, fileIdx, err := rd.writeBranch(node)
			if err != nil {
				return err
			}
			nodePtr.id = id
			nodePtr.fileIdx = fileIdx
			// TODO reference key ref here too
		}

	}
	panic("not implemented")
}

func (rd *RollingDiff) writeLeaf(node *MemNode) (id NodeID, fileOffset int64, err error) {
	id = NewNodeID(true, rd.stagedVersion, rd.leafVersionIdx)
	rd.leafVersionIdx++
	var buf [SizeLeaf]byte
	err = encodeLeafNode(node, buf, id)
	if err != nil {
		return 0, 0, err
	}
	_, err = rd.leafWriter.Write(buf[:])
	if err != nil {
		return 0, 0, err
	}

	offset := rd.leafFileIdx
	rd.leafFileIdx++
	return 0, offset, nil
}

func (rd *RollingDiff) Get(ref NodeRef) (Node, error) {
	//TODO implement me
	panic("implement me")
}

func (rd *RollingDiff) writeBranch(node *MemNode) (id NodeID, fileOffset int64, err error) {
	id = NewNodeID(true, rd.stagedVersion, rd.branchVersionIdx)
	rd.branchVersionIdx++
	fileIdx := rd.branchFileIdx
	leftRef := rd.createNodeRef(fileIdx, node.left)
	rightRef := rd.createNodeRef(fileIdx, node.right)
	var buf [SizeBranch]byte
	keyRef := node._keyRef.toKeyRef()
	err = encodeBranchNode(node, buf, id, leftRef, rightRef, keyRef)
	if err != nil {
		return 0, 0, err
	}
	_, err = rd.branchWriter.Write(buf[:])
	if err != nil {
		return 0, 0, err
	}
	rd.branchFileIdx++
	return id, fileIdx, nil
}

func (rd *RollingDiff) createNodeRef(curFileIdx int64, np *NodePointer) NodeRef {
	if np.store == rd {
		isLeaf := np.id.IsLeaf()
		return NodeRef(NewNodeRelativePointer(isLeaf, curFileIdx-np.fileIdx))
	} else {
		return NodeRef(np.id)
	}
}

var _ NodeStore = &RollingDiff{}
