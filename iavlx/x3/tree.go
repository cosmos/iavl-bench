package x3

import corestore "cosmossdk.io/core/store"

type Tree struct {
	origRoot    *NodePointer
	root        *NodePointer
	updateBatch *KVUpdateBatch
	zeroCopy    bool
}

func NewTree(root *NodePointer, updateBatch *KVUpdateBatch, zeroCopy bool) *Tree {
	return &Tree{origRoot: root, root: root, updateBatch: updateBatch, zeroCopy: zeroCopy}
}

func (tree *Tree) Get(key []byte) ([]byte, error) {
	if tree.root == nil {
		return nil, nil
	}

	root, err := tree.root.Resolve()
	if err != nil {
		return nil, err
	}

	value, _, err := root.Get(key)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (tree *Tree) Set(key, value []byte) error {
	leafNode := &MemNode{
		height:  0,
		size:    1,
		version: tree.updateBatch.StagedVersion,
		key:     key,
		value:   value,
	}
	newRoot, _, err := setRecursive(tree.root, leafNode, MutationContext{Version: tree.updateBatch.StagedVersion})
	if err != nil {
		return err
	}

	tree.root = newRoot
	tree.updateBatch.Updates = append(tree.updateBatch.Updates, KVUpdate{
		SetNode: leafNode,
	})
	return nil
}

func (tree *Tree) Remove(key []byte) error {
	_, newRoot, _, err := removeRecursive(tree.root, key, MutationContext{Version: tree.updateBatch.StagedVersion})
	if err != nil {
		return err
	}
	tree.root = newRoot
	tree.updateBatch.Updates = append(tree.updateBatch.Updates, KVUpdate{
		DeleteKey: key,
	})
	return nil
}

func (tree *Tree) Iterator(start, end []byte, ascending bool) (corestore.Iterator, error) {
	return NewIterator(start, end, ascending, tree.root, tree.zeroCopy), nil
}
