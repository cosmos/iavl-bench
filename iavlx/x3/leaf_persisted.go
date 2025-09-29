package x3

import (
	"bytes"
	"fmt"
)

type LeafPersisted struct {
	store   NodeStore
	selfIdx uint32
	layout  LeafLayout
}

func (node *LeafPersisted) Height() uint8 {
	//TODO implement me
	panic("implement me")
}

func (node *LeafPersisted) IsLeaf() bool {
	return true
}

func (node *LeafPersisted) Size() int64 {
	return 1
}

func (node *LeafPersisted) Version() uint64 {
	return node.layout.id.Version()
}

func (node *LeafPersisted) Key() ([]byte, error) {
	return node.store.ReadK(node.layout.keyOffset)
}

func (node *LeafPersisted) Value() ([]byte, error) {
	_, v, err := node.store.ReadKV(node.layout.keyOffset)
	return v, err
}

func (node *LeafPersisted) Left() *NodePointer {
	return nil
}

func (node *LeafPersisted) Right() *NodePointer {
	return nil
}

func (node *LeafPersisted) Hash() []byte {
	return node.layout.hash[:]
}

func (node *LeafPersisted) SafeHash() []byte {
	// TODO how do we make this safe?
	return node.layout.hash[:]
}

func (node *LeafPersisted) MutateBranch(ctx *MutationContext) (*MemNode, error) {
	return nil, fmt.Errorf("leaf nodes should not get mutated this way")
}

func (node *LeafPersisted) MarkOrphan(ctx *MutationContext) error {
	if node.layout.orphanVersion != 0 {
		return nil // already orphaned
	}

	// write the orphan version in memory
	node.layout.orphanVersion = ctx.Version

	// write the orphan version in the store
	layoutPtr, err := node.store.ResolveLeaf(node.layout.id, node.selfIdx)
	if err != nil {
		return err
	}
	layoutPtr.orphanVersion = node.layout.orphanVersion

	// add to the context
	ctx.Orphans = append(ctx.Orphans, node.layout.id)

	return nil
}

func (node *LeafPersisted) Get(key []byte) (value []byte, index int64, err error) {
	nodeKey, err := node.Key()
	if err != nil {
		return nil, 0, err
	}
	switch bytes.Compare(nodeKey, key) {
	case -1:
		return nil, 1, nil
	case 1:
		return nil, 0, nil
	default:
		value, err := node.Value()
		if err != nil {
			return nil, 0, err
		}
		return value, 0, nil
	}
}

func (node *LeafPersisted) String() string {
	//TODO implement me
	panic("implement me")
}

var _ Node = (*LeafPersisted)(nil)
