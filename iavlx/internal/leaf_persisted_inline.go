package internal

import (
	"bytes"
	"fmt"
)

// LeafPersistedInline implements Node interface directly on inline leaf data
// No external KVData lookups needed since key and value are stored inline
type LeafPersistedInline struct {
	LeafLayoutInline
}

func (node LeafPersistedInline) Height() uint8 {
	return 0 // leaves have height 0
}

func (node LeafPersistedInline) IsLeaf() bool {
	return true
}

func (node LeafPersistedInline) Size() int64 {
	return 1 // leaves have size 1
}

func (node LeafPersistedInline) Version() uint64 {
	return node.NodeID().Version()
}

func (node LeafPersistedInline) Key() ([]byte, error) {
	// Key is stored inline, no external lookup needed
	return node.LeafLayoutInline.Key(), nil
}

func (node LeafPersistedInline) Value() ([]byte, error) {
	// Value is stored inline, no external lookup needed
	return node.LeafLayoutInline.Value(), nil
}

func (node LeafPersistedInline) Left() *NodePointer {
	return nil // leaves have no children
}

func (node LeafPersistedInline) Right() *NodePointer {
	return nil // leaves have no children
}

func (node LeafPersistedInline) Hash() []byte {
	return node.LeafLayoutInline.Hash()
}

func (node LeafPersistedInline) SafeHash() []byte {
	return node.LeafLayoutInline.Hash()
}

func (node LeafPersistedInline) MutateBranch(MutationContext) (*MemNode, error) {
	panic("leaves don't get mutated this way!")
}

func (node LeafPersistedInline) Get(key []byte) ([]byte, int64, error) {
	nodeKey, err := node.Key()
	if err != nil {
		return nil, 0, err
	}
	switch bytes.Compare(nodeKey, key) {
	case -1: // nodeKey < key
		return nil, 1, nil
	case 1: // nodeKey > key
		return nil, 0, nil
	default: // nodeKey == key
		value, err := node.Value()
		if err != nil {
			return nil, 0, err
		}
		return value, 0, nil
	}
}

func (node LeafPersistedInline) String() string {
	return fmt.Sprintf("LeafPersistedInline{%s}", node.LeafLayoutInline.String())
}

func (node LeafPersistedInline) toKeyRef() KeyRef {
	// Return node ID as KeyRef (though not used with inline system)
	return KeyRef(node.NodeID())
}

// Verify interface compliance
var _ Node = LeafPersistedInline{}
var _ keyRefLink = LeafPersistedInline{}
