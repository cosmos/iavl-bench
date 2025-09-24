package internal

import (
	"bytes"
	"fmt"
)

// BranchPersistedInline wraps BranchLayoutInline with NodeStore access
// for resolving child nodes
type BranchPersistedInline struct {
	store      NodeStore
	layout     BranchLayoutInline
	selfOffset uint64 // position in file for calculating relative offsets
}

func (node BranchPersistedInline) Height() uint8 {
	return node.layout.Height()
}

func (node BranchPersistedInline) IsLeaf() bool {
	return false
}

func (node BranchPersistedInline) Size() int64 {
	return int64(node.layout.Size())
}

func (node BranchPersistedInline) Version() uint64 {
	return node.layout.NodeID().Version()
}

func (node BranchPersistedInline) Key() ([]byte, error) {
	// Key is stored inline, no external lookup needed
	return node.layout.Key(), nil
}

func (node BranchPersistedInline) Value() ([]byte, error) {
	return nil, nil // non-leaf nodes do not have values
}

func (node BranchPersistedInline) Left() *NodePointer {
	leftOffset := node.layout.LeftOffset()
	leftID := node.layout.LeftID()

	np := &NodePointer{
		store: node.store,
		id:    leftID,
	}

	// If offset is non-zero, use it for direct file access
	if leftOffset > 0 {
		np.fileIdx = node.selfOffset + leftOffset
	}
	// Otherwise will resolve by ID

	return np
}

func (node BranchPersistedInline) Right() *NodePointer {
	rightOffset := node.layout.RightOffset()
	rightID := node.layout.RightID()

	np := &NodePointer{
		store: node.store,
		id:    rightID,
	}

	// If offset is non-zero, use it for direct file access
	if rightOffset > 0 {
		np.fileIdx = node.selfOffset + rightOffset
	}
	// Otherwise will resolve by ID

	return np
}

func (node BranchPersistedInline) Hash() []byte {
	return node.layout.Hash()
}

func (node BranchPersistedInline) SafeHash() []byte {
	return node.layout.Hash()
}

func (node BranchPersistedInline) MutateBranch(ctx MutationContext) (*MemNode, error) {
	key, err := node.Key()
	if err != nil {
		return nil, err
	}

	memNode := &MemNode{
		height:  node.Height(),
		size:    node.Size(),
		version: ctx.Version,
		key:     key,
		left:    node.Left(),
		right:   node.Right(),
		_keyRef: KeyRef(node.layout.NodeID()), // Use node ID as key ref
	}
	return memNode, nil
}

func (node BranchPersistedInline) Get(key []byte) (value []byte, index int64, err error) {
	nodeKey, err := node.Key()
	if err != nil {
		return nil, 0, err
	}

	if bytes.Compare(key, nodeKey) < 0 {
		leftNode, err := node.Left().Resolve()
		if err != nil {
			return nil, 0, err
		}
		return leftNode.Get(key)
	}

	rightNode, err := node.Right().Resolve()
	if err != nil {
		return nil, 0, err
	}

	value, index, err = rightNode.Get(key)
	if err != nil {
		return nil, 0, err
	}

	index += node.Size() - rightNode.Size()
	return value, index, nil
}

func (node BranchPersistedInline) String() string {
	return fmt.Sprintf("BranchPersistedInline{%s, selfOffset:%d}",
		node.layout.String(), node.selfOffset)
}

func (node BranchPersistedInline) toKeyRef() KeyRef {
	// Return node ID as KeyRef (though not used with inline system)
	return KeyRef(node.layout.NodeID())
}

// Verify interface compliance
var _ Node = BranchPersistedInline{}
var _ keyRefLink = BranchPersistedInline{}
