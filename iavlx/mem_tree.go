package iavlx

import (
	"bytes"
	"fmt"
)

type Tree struct {
	root     *Node
	store    NodeWriter
	zeroCopy bool
}

func NewTree(root *Node, store NodeWriter, zeroCopy bool) *Tree {
	return &Tree{root: root, store: store, zeroCopy: zeroCopy}
}

func NewMemTree() *Tree {
	return &Tree{
		store: &MemStore{},
	}
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	if t.root == nil {
		return nil, nil
	}
	_, value, err := t.root.get(t.store, key)
	if err != nil {
		return nil, err
	}
	if !t.zeroCopy && value != nil {
		value = bytes.Clone(value)
	}
	return value, nil
}

func (t *Tree) Set(key, value []byte) error {
	if value == nil {
		return fmt.Errorf("nil value is not allowed")
	}
	var err error
	t.root, _, err = setRecursive(t.store, t.root, key, value)
	return err
}

func (t *Tree) Remove(key []byte) error {
	var err error
	_, t.root, _, err = removeRecursive(t.store, t.root, key)
	return err
}

type MemStore struct{}

func (m MemStore) Load(*NodePointer) (*Node, error) {
	return nil, fmt.Errorf("MemStore does not support Load")
}

func (m MemStore) NewLeafNode(key, value []byte) *Node {
	return newLeafNode(key, value)
}

func (m MemStore) CopyLeafNode(node *Node, newValue []byte) *Node {
	newNode := node.copy()
	newNode.value = newValue
	return newNode
}

func (m MemStore) CopyNode(node *Node) *Node {
	return node.copy()
}

func (m MemStore) NewBranchNode() *Node {
	return NewNode()
}

func (m MemStore) DeleteNode(*Node) {}

var _ NodeWriter = (*MemStore)(nil)
