package internal

import (
	"bytes"
	"fmt"
)

type BranchPersisted struct {
	store NodeStore
	BranchData
}

type BranchData struct {
	layout          BranchLayout
	selfOffset      int64
	leftId, rightId NodeID // cached for convenience if not present in the layout
}

func (p BranchPersisted) Hash() []byte {
	return p.layout.Hash()
}

func (p BranchPersisted) Height() uint8 {
	return p.layout.Height()
}

func (p BranchPersisted) IsLeaf() bool {
	return false
}

func (p BranchPersisted) Size() int64 {
	return int64(p.layout.Size())
}

func (p BranchPersisted) Version() uint64 {
	return p.layout.NodeID().Version()
}

func (p BranchPersisted) Key() ([]byte, error) {
	keyRef := p.layout.KeyRef()
	if keyRef.IsNodeID() {
		return nil, fmt.Errorf("resolving node ID key refs not implemented")
	}
	walRef := keyRef.WALRef()
	n, overflow := walRef.Length()
	if overflow {
		return nil, fmt.Errorf("overflow key support not implemented")
	}
	return p.store.Read(walRef.Offset(), n)
}

func (p BranchPersisted) Value() ([]byte, error) {
	return nil, nil // non-leaf nodes do not have values
}

func (p BranchPersisted) Left() *NodePointer {
	return p.resolveNodePointer(p.layout.Left(), p.leftId)
}

func (p BranchPersisted) Right() *NodePointer {
	return p.resolveNodePointer(p.layout.Right(), p.rightId)
}

func (p BranchPersisted) resolveNodePointer(ref NodeRef, cachedId NodeID) *NodePointer {
	np := &NodePointer{
		store: p.store,
	}
	if ref.IsRelativePointer() {
		np.fileIdx = p.selfOffset + ref.AsRelativePointer().Offset()
		np.id = cachedId
	} else {
		np.id = ref.AsNodeID()
	}
	return np
}

func (p BranchPersisted) SafeHash() []byte {
	return p.layout.Hash()
}

func (p BranchPersisted) MutateBranch(ctx MutationContext) (*MemNode, error) {
	key, err := p.Key()
	if err != nil {
		return nil, err
	}
	memNode := &MemNode{
		height:  p.Height(),
		size:    p.Size(),
		version: ctx.Version,
		key:     key,
		left:    p.Left(),
		right:   p.Right(),
		_keyRef: p.layout.KeyRef(),
	}
	return memNode, nil
}

func (p BranchPersisted) Get(key []byte) (value []byte, index int64, err error) {
	nodeKey, err := p.Key()
	if err != nil {
		return nil, 0, err
	}

	if bytes.Compare(key, nodeKey) < 0 {
		leftNode, err := p.Left().Resolve()
		if err != nil {
			return nil, 0, err
		}

		return leftNode.Get(key)
	}

	rightNode, err := p.Right().Resolve()
	if err != nil {
		return nil, 0, err
	}

	value, index, err = rightNode.Get(key)
	if err != nil {
		return nil, 0, err
	}

	index += p.Size() - rightNode.Size()
	return value, index, nil
}

var _ Node = BranchPersisted{}
