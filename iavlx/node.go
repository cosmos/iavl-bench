package iavlx

import (
	"bytes"
	"sync/atomic"
)

func NewLeafNodeKey(version uint32, seq uint32) NodeKey {
	//return NodeKey(uint64(version))<<32 | NodeKey(seq) | 0x80000000
	panic("TODO")
}

func NewBranchNodeKey(version uint32, seq uint32) NodeKey {
	//return NodeKey(uint64(version))<<32 | NodeKey(seq&0x7FFFFFFF)
	panic("TODO")
}

type NodeReader interface {
	Load(*NodePointer) (*Node, error)
}

type NodeFactory interface {
	NodeReader
	NewLeafNode(key, value []byte) *Node
	NewBranchNode() *Node
	MutateLeafNode(node *Node, newValue []byte) *Node
	MutateBranchNode(*Node) *Node
	DropNode(node *Node)
}

type NodeWriter interface {
	NodeReader
	NodeKeyGenerator
	SaveNode(*Node) error
	DeleteNode(version int64, deleteKey NodeKey, node *Node) error
	SaveRoot(version int64, root *Node) error
}

type Node struct {
	key           []byte
	value         []byte
	hash          []byte
	version       uint32
	nodeKey       NodeKey
	size          int64
	subtreeHeight uint8
	left          *NodePointer
	right         *NodePointer
}

func NewNode() *Node {
	return &Node{}
}

type NodePointer struct {
	ptr atomic.Pointer[Node]
	key NodeKey
}

func (n *NodePointer) Get(store NodeReader) (*Node, error) {
	node := n.ptr.Load()
	if node != nil {
		return node, nil
	}
	return store.Load(n)
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
		leftNode, err := node.left.Get(store)
		if err != nil {
			return 0, nil, err
		}

		return leftNode.get(store, key)
	}

	rightNode, err := node.right.Get(store)
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

func (node *Node) calcBalance(store NodeReader) (int, error) {
	leftNode, err := node.left.Get(store)
	if err != nil {
		return 0, err
	}

	rightNode, err := node.right.Get(store)
	if err != nil {
		return 0, err
	}

	return int(leftNode.subtreeHeight) - int(rightNode.subtreeHeight), nil
}

func newLeafNode(key, value []byte) *Node {
	node := NewNode()
	node.key = key
	node.value = value
	node.size = 1
	return node
}
