package iavlx

import (
	"bytes"
	"sync/atomic"
)

// NodeKey uniquely represents nodes.
// For now the first 32 bits represent the version and the last 32 bits represent
// order in which the node was created in that version with the high bit distinguishing
// between leaf and internal nodes.
type NodeKey uint64

type NodeReader interface {
	Load(*NodePointer) (*Node, error)
}

type NodeWriter interface {
	NodeReader
	NewLeafNode(key, value []byte) *Node
	NewBranchNode() *Node
	CopyLeafNode(node *Node, newValue []byte) *Node
	CopyNode(*Node) *Node
	DeleteNode(node *Node)
}

type nodeStatic struct {
	key           []byte
	value         []byte
	hash          []byte
	nodeKey       NodeKey
	size          int64
	subtreeHeight int8
}

type Node struct {
	nodeStatic
	leftNode  NodePointer
	rightNode NodePointer
}

type NodePointer struct {
	ptr atomic.Pointer[Node]
	key NodeKey
}

func (node *Node) get(store NodeReader, key []byte) (index int64, value []byte, err error) {
	if node.isLeaf() {
		switch bytes.Compare(node.key, key) {
		case -1:
			return 1, nil, nil
		case 1:
			return 0, nil, nil
		default:
			return 0, node.value, nil
		}
	}

	if bytes.Compare(key, node.key) < 0 {
		leftNode, err := node.getLeftNode(store)
		if err != nil {
			return 0, nil, err
		}

		return leftNode.get(store, key)
	}

	rightNode, err := node.getRightNode(store)
	if err != nil {
		return 0, nil, err
	}

	index, value, err = rightNode.get(store, key)
	if err != nil {
		return 0, nil, err
	}

	index += node.size - rightNode.size
	return index, value, nil
}

func (node *Node) isLeaf() bool {
	return node.subtreeHeight == 0
}

func (node *Node) getLeftNode(store NodeReader) (*Node, error) {
	leftNodeKey := node.leftNode.Load()
	if leftNodeKey != nil {
		return leftNodeKey, nil
	}

	return store.GetLeft(node)
}

func (node *Node) getRightNode(store NodeReader) (*Node, error) {
	rightNodeKey := node.rightNode.Load()
	if rightNodeKey != nil {
		return rightNodeKey, nil
	}

	return store.GetRight(node)
}

func (node *Node) calcBalance(store NodeReader) (int, error) {
	leftNode, err := node.getLeftNode(store)
	if err != nil {
		return 0, err
	}

	rightNode, err := node.getRightNode(store)
	if err != nil {
		return 0, err
	}

	return int(leftNode.subtreeHeight) - int(rightNode.subtreeHeight), nil
}

// IMPORTANT: nodes that call this method must be new or copies first
func (node *Node) balance(store NodeWriter) (*Node, error) {
	balance, err := node.calcBalance(store)
	if err != nil {
		return nil, err
	}
	switch {
	case balance > 1:
		left, err := node.getLeftNode(store)
		if err != nil {
			return nil, err
		}

		leftBalance, err := left.calcBalance(store)
		if err != nil {
			return nil, err
		}

		if leftBalance >= 0 {
			// left left
			return node.rotateRight(store)
		}

		// left right
		left, err = store.CopyNode(left).rotateLeft(store)
		if err != nil {
			return nil, err
		}
		node.leftNode.Store(left)
		return node.rotateRight(store)
	case balance < -1:
		right, err := node.getRightNode(store)
		if err != nil {
			return nil, err
		}

		rightBalance, err := right.calcBalance(store)
		if err != nil {
			return nil, err
		}

		if rightBalance <= 0 {
			// right right
			return node.rotateLeft(store)
		}

		// right left
		right, err = store.CopyNode(right).rotateRight(store)
		if err != nil {
			return nil, err
		}
		node.rightNode.Store(right)
		return node.rotateLeft(store)
	default:
		// nothing changed
		return node, err
	}
}

// IMPORTANT: nodes that call this method must be new or copies first
func (node *Node) rotateRight(store NodeWriter) (*Node, error) {
	left, err := node.getLeftNode(store)
	if err != nil {
		return nil, err
	}
	newSelf := store.CopyNode(left)
	leftRight, err := left.getRightNode(store)
	if err != nil {
		return nil, err
	}
	node.leftNode.Store(leftRight)
	newSelf.rightNode.Store(node)

	err = node.updateHeightSize(store)
	if err != nil {
		return nil, err
	}
	err = newSelf.updateHeightSize(store)
	if err != nil {
		return nil, err
	}

	return newSelf, nil
}

// IMPORTANT: nodes that call this method must be new or copies first
func (node *Node) rotateLeft(store NodeWriter) (*Node, error) {
	right, err := node.getRightNode(store)
	if err != nil {
		return nil, err
	}
	newSelf := store.CopyNode(right)
	rightLeft, err := right.getLeftNode(store)
	if err != nil {
		return nil, err
	}
	node.rightNode.Store(rightLeft)
	newSelf.leftNode.Store(node)

	err = node.updateHeightSize(store)
	if err != nil {
		return nil, err
	}

	err = newSelf.updateHeightSize(store)
	if err != nil {
		return nil, err
	}

	return newSelf, nil
}

// IMPORTANT: nodes that call this method must be new or copies first
func (node *Node) updateHeightSize(store NodeWriter) error {
	leftNode, err := node.getLeftNode(store)
	if err != nil {
		return err
	}

	rightNode, err := node.getRightNode(store)
	if err != nil {
		return err
	}

	node.subtreeHeight = maxInt8(leftNode.subtreeHeight, rightNode.subtreeHeight) + 1
	node.size = leftNode.size + rightNode.size
	return nil
}

func (node *Node) copy() *Node {
	newNode := &Node{}
	newNode.nodeStatic = node.nodeStatic
	newNode.leftNode.Store(node.leftNode.Load())
	newNode.rightNode.Store(node.rightNode.Load())
	return newNode
}

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}

// setRecursive do set operation.
// returns if it's an update or insertion, if update, the tree height and balance is not changed.
func setRecursive(store NodeWriter, node *Node, key, value []byte) (*Node, bool, error) {
	if node == nil {
		// creating a new leaf node
		node = store.NewBranchNode()
		node.key = key
		node.value = value
		return node, true, nil
	}

	if node.isLeaf() {
		cmp := bytes.Compare(key, node.key)
		if cmp == 0 {
			// just updating value
			return store.CopyLeafNode(node, value), true, nil
		}

		// need to create a new internal node
		newNode := store.NewBranchNode()
		newNode.key = node.key
		newNode.value = node.value
		newNode.subtreeHeight = 1
		newNode.size = 2
		switch cmp {
		case -1:
			newNode.leftNode.Store(store.NewLeafNode(key, value))
			newNode.rightNode.Store(node)
		case 1:
			newNode.leftNode.Store(node)
			newNode.rightNode.Store(store.NewLeafNode(key, value))
		default:
			panic("unreachable")
		}
		return newNode, false, nil
	} else {
		var (
			newChild *Node
			updated  bool
		)
		newNode := store.CopyNode(node)
		if bytes.Compare(key, node.key) == -1 {
			left, err := node.getLeftNode(store)
			if err != nil {
				return nil, false, err
			}
			newChild, updated, err = setRecursive(store, left, key, value)
			if err != nil {
				return nil, false, err
			}
			newNode.leftNode.Store(newChild)
		} else {
			right, err := node.getRightNode(store)
			if err != nil {
				return nil, false, err
			}
			newChild, updated, err = setRecursive(store, right, key, value)
			if err != nil {
				return nil, false, err
			}
			newNode.rightNode.Store(newChild)
		}

		if !updated {
			err := newNode.updateHeightSize(store)
			if err != nil {
				return nil, false, err
			}
			newNode, err = newNode.balance(store)
			if err != nil {
				return nil, false, err
			}
		}

		return newNode, updated, nil
	}
}

func newLeafNode(key, value []byte) *Node {
	node := &Node{}
	node.key = key
	node.value = value
	node.size = 1
	return node
}

// removeRecursive returns:
// - (nil, origNode, nil, nil) -> nothing changed in subtree
// - (value, nil, newKey, nil) -> leaf node is removed
// - (value, new node, newKey, nil) -> subtree changed
func removeRecursive(store NodeWriter, node *Node, key []byte) (value []byte, newNode *Node, newKey []byte, err error) {
	if node == nil {
		return nil, nil, nil, nil
	}

	if node.isLeaf() {
		if bytes.Equal(node.key, key) {
			return node.value, nil, nil, nil
		}
		return nil, node, nil, nil
	}

	if bytes.Compare(key, node.key) == -1 {
		left, err := node.getLeftNode(store)
		if err != nil {
			return nil, nil, nil, err
		}
		value, newLeft, newKey, err := removeRecursive(store, left, key)
		if err != nil {
			return nil, nil, nil, err
		}
		if value == nil {
			return nil, node, nil, nil
		}
		if newLeft == nil {
			right, err := node.getRightNode(store)
			if err != nil {
				return nil, nil, nil, err
			}
			return value, right, node.key, nil
		}

		newNode := store.CopyNode(node)
		newNode.leftNode.Store(newLeft)
		err = newNode.updateHeightSize(store)
		if err != nil {
			return nil, nil, nil, err
		}
		newNode, err = newNode.balance(store)
		if err != nil {
			return nil, nil, nil, err
		}

		return value, newNode, newKey, nil
	}

	right, err := node.getRightNode(store)
	if err != nil {
		return nil, nil, nil, err
	}
	value, newRight, newKey, err := removeRecursive(store, right, key)
	if err != nil {
		return nil, nil, nil, err
	}

	if value == nil {
		return nil, node, nil, nil
	}
	left, err := node.getLeftNode(store)
	if err != nil {
		return nil, nil, nil, err
	}
	if newRight == nil {
		return value, left, nil, nil
	}

	newNode = store.CopyNode(node)
	newNode.rightNode.Store(newRight)
	if newKey != nil {
		newNode.key = newKey
	}
	err = newNode.updateHeightSize(store)
	if err != nil {
		return nil, nil, nil, err
	}

	newNode, err = newNode.balance(store)
	if err != nil {
		return nil, nil, nil, err
	}

	return value, newNode, nil, nil
}
