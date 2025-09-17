package iavlx

import "bytes"

func (node *Node) copy() *Node {
	newNode := NewNode()
	*newNode = *node
	return newNode
}

// IMPORTANT: wrapNewNode MUST only be called on newly created nodes that are not in the tree yet.
// This ensures that there is only one pointer to each node, which makes it
// possible to use a node pool for memory management when we evict nodes.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func wrapNewNode(node *Node) *NodePointer {
	if node == nil {
		return nil
	}
	ptr := &NodePointer{}
	ptr.ptr.Store(node)
	return ptr
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func balanceNewNode(store NodeFactory, newNode *Node) (*Node, error) {
	balance, err := newNode.calcBalance(store)
	if err != nil {
		return nil, err
	}
	switch {
	case balance > 1:
		left, err := newNode.left.Get(store)
		if err != nil {
			return nil, err
		}

		leftBalance, err := left.calcBalance(store)
		if err != nil {
			return nil, err
		}

		if leftBalance >= 0 {
			// left left
			return rotateNewRight(store, newNode)
		}

		// left right
		newLeft, err := rotateNewLeft(store, store.MutateBranchNode(left))
		if err != nil {
			return nil, err
		}
		newNode.left = wrapNewNode(newLeft)
		return rotateNewRight(store, newNode)
	case balance < -1:
		right, err := newNode.right.Get(store)
		if err != nil {
			return nil, err
		}

		rightBalance, err := right.calcBalance(store)
		if err != nil {
			return nil, err
		}

		if rightBalance <= 0 {
			// right right
			return rotateNewLeft(store, newNode)
		}

		// right left
		newRight, err := rotateNewRight(store, store.MutateBranchNode(right))
		if err != nil {
			return nil, err
		}
		newNode.right = wrapNewNode(newRight)
		return rotateNewLeft(store, newNode)
	default:
		// nothing changed
		return newNode, err
	}
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func rotateNewRight(store NodeFactory, newNode *Node) (*Node, error) {
	left, err := newNode.left.Get(store)
	if err != nil {
		return nil, err
	}
	newSelf := store.MutateBranchNode(left)
	newNode.left = left.right
	newSelf.right = wrapNewNode(newNode)

	err = updateNewNodeHeightSize(store, newNode)
	if err != nil {
		return nil, err
	}
	err = updateNewNodeHeightSize(store, newSelf)
	if err != nil {
		return nil, err
	}

	return newSelf, nil
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func rotateNewLeft(store NodeFactory, newNode *Node) (*Node, error) {
	right, err := newNode.right.Get(store)
	if err != nil {
		return nil, err
	}
	newSelf := store.MutateBranchNode(right)
	newNode.right = right.left
	newSelf.left = wrapNewNode(newNode)

	err = updateNewNodeHeightSize(store, newNode)
	if err != nil {
		return nil, err
	}

	err = updateNewNodeHeightSize(store, newSelf)
	if err != nil {
		return nil, err
	}

	return newSelf, nil
}

// IMPORTANT: nodes that call this method must be new or copies first
func updateNewNodeHeightSize(store NodeFactory, newNode *Node) error {
	leftNode, err := newNode.left.Get(store)
	if err != nil {
		return err
	}

	rightNode, err := newNode.right.Get(store)
	if err != nil {
		return err
	}

	newNode.subtreeHeight = maxInt8(leftNode.subtreeHeight, rightNode.subtreeHeight) + 1
	newNode.size = leftNode.size + rightNode.size
	return nil
}

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}

// setRecursive do set operation.
// returns if it's an update or insertion, if update, the tree height and balance is not changed.
func setRecursive(store NodeFactory, nodePtr *NodePointer, key, value []byte) (*NodePointer, bool, error) {
	if nodePtr == nil {
		return wrapNewNode(store.NewLeafNode(key, value)), true, nil
	}

	node, err := nodePtr.Get(store)
	if err != nil {
		return nil, false, err
	}

	if node.isLeaf() {
		cmp := bytes.Compare(key, node.key)
		if cmp == 0 {
			// just updating value
			return wrapNewNode(store.MutateLeafNode(node, value)), true, nil
		}

		// need to create a new internal node
		newNode := store.NewBranchNode()
		newNode.subtreeHeight = 1
		newNode.size = 2
		switch cmp {
		case -1:
			newNode.key = node.key
			newNode.left = wrapNewNode(store.NewLeafNode(key, value))
			newNode.right = nodePtr
		case 1:
			newNode.key = key
			newNode.left = nodePtr
			newNode.right = wrapNewNode(store.NewLeafNode(key, value))
		default:
			panic("unreachable")
		}
		return wrapNewNode(newNode), false, nil
	} else {
		var (
			newChild *NodePointer
			updated  bool
		)
		newNode := store.MutateBranchNode(node)
		if bytes.Compare(key, node.key) == -1 {
			newChild, updated, err = setRecursive(store, node.left, key, value)
			if err != nil {
				return nil, false, err
			}
			newNode.left = newChild
		} else {
			newChild, updated, err = setRecursive(store, node.right, key, value)
			if err != nil {
				return nil, false, err
			}
			newNode.right = newChild
		}

		if !updated {
			err := updateNewNodeHeightSize(store, newNode)
			if err != nil {
				return nil, false, err
			}
			newNode, err = balanceNewNode(store, newNode)
			if err != nil {
				return nil, false, err
			}
		}

		return wrapNewNode(newNode), updated, nil
	}
}

// removeRecursive returns:
// - (nil, origNode, nil, nil) -> nothing changed in subtree
// - (value, nil, newKey, nil) -> leaf node is removed
// - (value, new node, newKey, nil) -> subtree changed
func removeRecursive(store NodeFactory, nodePtr *NodePointer, key []byte) (value []byte, newNodePtr *NodePointer, newKey []byte, err error) {
	if nodePtr == nil {
		return nil, nil, nil, nil
	}

	node, err := nodePtr.Get(store)
	if err != nil {
		return nil, nil, nil, err
	}

	if node.isLeaf() {
		if bytes.Equal(node.key, key) {
			store.DropNode(node)
			return node.value, nil, nil, nil
		}
		return nil, nodePtr, nil, nil
	}

	if bytes.Compare(key, node.key) == -1 {
		value, newLeft, newKey, err := removeRecursive(store, node.left, key)
		if err != nil {
			return nil, nil, nil, err
		}
		if value == nil {
			return nil, nodePtr, nil, nil
		}
		if newLeft == nil {
			right := node.right
			store.DropNode(node)
			return value, right, node.key, nil
		}

		newNode := store.MutateBranchNode(node)
		newNode.left = newLeft
		err = updateNewNodeHeightSize(store, newNode)
		if err != nil {
			return nil, nil, nil, err
		}
		newNode, err = balanceNewNode(store, newNode)
		if err != nil {
			return nil, nil, nil, err
		}

		return value, wrapNewNode(newNode), newKey, nil
	}

	value, newRight, newKey, err := removeRecursive(store, node.right, key)
	if err != nil {
		return nil, nil, nil, err
	}

	if value == nil {
		return nil, nodePtr, nil, nil
	}
	left := node.left
	if newRight == nil {
		store.DropNode(node)
		return value, left, nil, nil
	}

	newNode := store.MutateBranchNode(node)
	newNode.right = newRight
	if newKey != nil {
		newNode.key = newKey
	}
	err = updateNewNodeHeightSize(store, newNode)
	if err != nil {
		return nil, nil, nil, err
	}

	newNode, err = balanceNewNode(store, newNode)
	if err != nil {
		return nil, nil, nil, err
	}

	return value, wrapNewNode(newNode), nil, nil
}
