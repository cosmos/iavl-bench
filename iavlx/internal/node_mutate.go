package internal

import "bytes"

type MutationContext struct {
	Version uint64
	Store   NodeStore
}

// setRecursive do set operation.
// it always do modification and return new `MemNode`, even if the value is the same.
// also returns if it's an update or insertion, if update, the tree height and balance is not changed.
func setRecursive(nodePtr *NodePointer, key, value []byte, leafNode *NodePointer, ctx MutationContext) (*NodePointer, bool, error) {
	if nodePtr == nil {
		return leafNode, true, nil
	}

	node, err := nodePtr.Resolve()
	if err != nil {
		return nil, false, err
	}

	nodeKey, err := node.Key()
	if err != nil {
		return nil, false, err
	}
	if node.IsLeaf() {
		cmp := bytes.Compare(key, nodeKey)
		if cmp == 0 {
			return leafNode, true, nil
		}
		n := &MemNode{
			height:  1,
			size:    2,
			version: ctx.Version,
			key:     nodeKey,
		}
		switch cmp {
		case -1:
			n.left = leafNode
			n.right = nodePtr
		case 1:
			n.left = nodePtr
			n.right = leafNode
		default:
			panic("unreachable")
		}
		return NewNodePointer(n), false, nil
	} else {
		var (
			newChildPtr *NodePointer
			newNode     *MemNode
			updated     bool
			err         error
		)
		if bytes.Compare(key, nodeKey) == -1 {
			newChildPtr, updated, err = setRecursive(node.Left(), key, value, leafNode, ctx)
			if err != nil {
				return nil, false, err
			}
			newNode, err = node.MutateBranch(ctx)
			if err != nil {
				return nil, false, err
			}
			newNode.left = newChildPtr
		} else {
			newChildPtr, updated, err = setRecursive(node.Right(), key, value, leafNode, ctx)
			if err != nil {
				return nil, false, err
			}
			newNode, err = node.MutateBranch(ctx)
			if err != nil {
				return nil, false, err
			}
			newNode.right = newChildPtr
		}

		if !updated {
			newNode.updateHeightSize()
			newNode, err = newNode.reBalance(ctx)
			if err != nil {
				return nil, false, err
			}
		}

		return NewNodePointer(newNode), updated, nil
	}
}
