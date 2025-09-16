package iavlx

import (
	"bytes"
	"fmt"

	corestore "cosmossdk.io/core/store"
)

type Tree struct {
	root     *Node
	store    NodeWriter
	zeroCopy bool
}

func NewTree(root *Node, store NodeWriter, zeroCopy bool) *Tree {
	return &Tree{root: root, store: store, zeroCopy: zeroCopy}
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

func (t *Tree) Iterator(start, end []byte, ascending bool) (corestore.Iterator, error) {
	return NewIterator(t.store, start, end, ascending, t.root, t.zeroCopy), nil
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

type NullStore struct{}

func (m NullStore) Load(*NodePointer) (*Node, error) {
	return nil, fmt.Errorf("NullStore does not support Load")
}

func (m NullStore) NewLeafNode(key, value []byte) *Node {
	return newLeafNode(key, value)
}

func (m NullStore) CopyLeafNode(node *Node, newValue []byte) *Node {
	newNode := node.copy()
	newNode.value = newValue
	return newNode
}

func (m NullStore) CopyNode(node *Node) *Node {
	return node.copy()
}

func (m NullStore) NewBranchNode() *Node {
	return NewNode()
}

func (m NullStore) DeleteNode(*Node) {}

var _ NodeWriter = (*NullStore)(nil)
