package internal

import "bytes"

type MutationContext struct {
	Version uint64
}

// setRecursive do set operation.
// it always do modification and return new `MemNode`, even if the value is the same.
// also returns if it's an update or insertion, if update, the tree height and balance is not changed.
// TODO maybe we can just use *MemNode for leafNode and use its key, value directly?
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
			_keyRef: leafNode._keyRef,
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
			err = newNode.updateHeightSize()
			if err != nil {
				return nil, false, err
			}

			newNode, err = newNode.reBalance(ctx)
			if err != nil {
				return nil, false, err
			}
		}

		return NewNodePointer(newNode), updated, nil
	}
}

// removeRecursive returns:
// - (nil, origNode, nil) -> nothing changed in subtree
// - (value, nil, newKey) -> leaf node is removed
// - (value, new node, newKey) -> subtree changed
func removeRecursive(nodePtr *NodePointer, key []byte, ctx MutationContext) (value []byte, newNodePtr *NodePointer, newKey []byte, err error) {
	if nodePtr == nil {
		return nil, nil, nil, nil
	}

	node, err := nodePtr.Resolve()
	if err != nil {
		return nil, nil, nil, err
	}

	nodeKey, err := node.Key()
	if err != nil {
		return nil, nil, nil, err
	}

	if node.IsLeaf() {
		if bytes.Equal(nodeKey, key) {
			value, err := node.Value()
			return value, nil, nil, err
		}
		return nil, nodePtr, nil, nil
	}

	if bytes.Compare(key, nodeKey) == -1 {
		value, newLeft, newKey, err := removeRecursive(node.Left(), key, ctx)
		if err != nil {
			return nil, nil, nil, err
		}

		if value == nil {
			return nil, nodePtr, nil, nil
		}

		if newLeft == nil {
			return value, node.Right(), nodeKey, nil
		}

		newNode, err := node.MutateBranch(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		newNode.left = newLeft
		err = newNode.updateHeightSize()
		if err != nil {
			return nil, nil, nil, err
		}
		newNode, err = newNode.reBalance(ctx)
		if err != nil {
			return nil, nil, nil, err
		}

		return value, NewNodePointer(newNode), newKey, nil
	}

	value, newRight, newKey, err := removeRecursive(node.Right(), key, ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	if value == nil {
		return nil, nodePtr, nil, nil
	}

	if newRight == nil {
		return value, node.Left(), nil, nil
	}

	newNode, err := node.MutateBranch(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	newNode.right = newRight
	if newKey != nil {
		newNode.key = newKey
	}

	err = newNode.updateHeightSize()
	if err != nil {
		return nil, nil, nil, err
	}

	newNode, err = newNode.reBalance(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return value, NewNodePointer(newNode), nil, nil
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func (node *MemNode) updateHeightSize() error {
	leftNode, err := node.left.Resolve()
	if err != nil {
		return err
	}

	rightNode, err := node.right.Resolve()
	if err != nil {
		return err
	}

	node.height = maxUint8(leftNode.Height(), rightNode.Height()) + 1
	node.size = leftNode.Size() + rightNode.Size()
	return nil
}

func maxUint8(a, b uint8) uint8 {
	if a > b {
		return a
	}
	return b
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func (node *MemNode) reBalance(ctx MutationContext) (*MemNode, error) {
	balance, err := calcBalance(node)
	if err != nil {
		return nil, err
	}
	switch {
	case balance > 1:
		left, err := node.left.Resolve()
		if err != nil {
			return nil, err
		}

		leftBalance, err := calcBalance(left)
		if err != nil {
			return nil, err
		}

		if leftBalance >= 0 {
			// left left
			return node.rotateRight(ctx)
		}

		// left right
		newLeft, err := left.MutateBranch(ctx)
		if err != nil {
			return nil, err
		}
		newLeft, err = newLeft.rotateLeft(ctx)
		if err != nil {
			return nil, err
		}
		node.left = NewNodePointer(newLeft)
		return node.rotateRight(ctx)
	case balance < -1:
		right, err := node.right.Resolve()
		if err != nil {
			return nil, err
		}

		rightBalance, err := calcBalance(right)
		if err != nil {
			return nil, err
		}

		if rightBalance <= 0 {
			// right right
			return node.rotateLeft(ctx)
		}

		// right left
		newRight, err := right.MutateBranch(ctx)
		if err != nil {
			return nil, err
		}
		newRight, err = newRight.rotateRight(ctx)
		node.right = NewNodePointer(newRight)
		return node.rotateLeft(ctx)
	default:
		// nothing changed
		return node, err
	}
}

func calcBalance(node Node) (int, error) {
	leftNode, err := node.Left().Resolve()
	if err != nil {
		return 0, err
	}

	rightNode, err := node.Right().Resolve()
	if err != nil {
		return 0, err
	}

	return int(leftNode.Height()) - int(rightNode.Height()), nil
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func (node *MemNode) rotateRight(ctx MutationContext) (*MemNode, error) {
	left, err := node.left.Resolve()
	if err != nil {
		return nil, err
	}
	newSelf, err := left.MutateBranch(ctx)
	if err != nil {
		return nil, err
	}
	node.left = left.Right()
	newSelf.right = NewNodePointer(node)

	err = node.updateHeightSize()
	if err != nil {
		return nil, err
	}
	err = newSelf.updateHeightSize()
	if err != nil {
		return nil, err
	}

	return newSelf, nil
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func (node *MemNode) rotateLeft(ctx MutationContext) (*MemNode, error) {
	right, err := node.right.Resolve()
	if err != nil {
		return nil, err
	}

	newSelf, err := right.MutateBranch(ctx)
	if err != nil {
		return nil, err
	}

	node.right = right.Left()
	newSelf.left = NewNodePointer(node)

	err = node.updateHeightSize()
	if err != nil {
		return nil, err
	}

	err = newSelf.updateHeightSize()
	if err != nil {
		return nil, err
	}

	return newSelf, nil
}
