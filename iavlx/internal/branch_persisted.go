package internal

import (
	"bytes"
	"fmt"
)

type BranchPersisted struct {
	layout     BranchLayout
	store      NodeStore
	selfOffset int64
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
	return p.resolveNodePointer(p.layout.Left())
}

func (p BranchPersisted) Right() *NodePointer {
	return p.resolveNodePointer(p.layout.Right())
}

func (p BranchPersisted) resolveNodePointer(ref NodeRef) *NodePointer {
	np := &NodePointer{
		store: p.store,
	}
	if ref.IsRelativePointer() {
		np.fileIdx = p.selfOffset + ref.AsRelativePointer().Offset()
		// TODO should we traverse to the root to find the node ID and then set it here?
	} else {
		np.id = ref.AsNodeID()
	}
	return np
}

func (p BranchPersisted) Hash() ([]byte, error) {
	return p.layout.Hash(), nil
}

func (p BranchPersisted) SafeHash() ([]byte, error) {
	return p.layout.Hash(), nil
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
