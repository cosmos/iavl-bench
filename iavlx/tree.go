package iavlx

import (
	"bytes"
	"fmt"

	corestore "cosmossdk.io/core/store"
)

type Tree struct {
	root     *NodePointer
	store    NodeFactory
	zeroCopy bool
}

func NewTree(root *NodePointer, store NodeFactory, zeroCopy bool) *Tree {
	return &Tree{root: root, store: store, zeroCopy: zeroCopy}
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	if t.root == nil {
		return nil, nil
	}
	root, err := t.root.Get(t.store)
	if err != nil {
		return nil, err
	}
	_, value, err := root.get(t.store, key)
	if err != nil {
		return nil, err
	}
	if !t.zeroCopy && value != nil {
		value = bytes.Clone(value)
	}
	return value, nil
}

func (t *Tree) Iterator(start, end []byte, ascending bool) (corestore.Iterator, error) {
	if t.root == nil {
		return NewIterator(t.store, start, end, ascending, nil, t.zeroCopy), nil
	}
	root, err := t.root.Get(t.store)
	if err != nil {
		return nil, err
	}
	return NewIterator(t.store, start, end, ascending, root, t.zeroCopy), nil
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
