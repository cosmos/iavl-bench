package internal

import (
	"bytes"
)

type LeafPersisted struct {
	layout LeafLayout
	kvData KVData
}

func (node LeafPersisted) Hash() []byte {
	return node.layout.Hash()
}

func (node LeafPersisted) Height() uint8 {
	return 0
}

func (node LeafPersisted) Size() int64 {
	return 1
}

func (node LeafPersisted) Version() uint64 {
	return node.layout.NodeID().Version()
}

func (node LeafPersisted) Key() ([]byte, error) {
	return node.kvData.Read(node.layout.KeyOffset(), node.layout.KeyLength())
}

func (node LeafPersisted) Value() ([]byte, error) {
	valueOffset := node.layout.KeyOffset() + uint64(node.layout.KeyLength())
	bz, _, err := node.kvData.ReadVarintBytes(valueOffset)
	return bz, err
}

func (node LeafPersisted) Left() *NodePointer {
	return nil
}

func (node LeafPersisted) Right() *NodePointer {
	return nil
}

func (node LeafPersisted) SafeHash() []byte {
	return node.layout.Hash()
}

func (node LeafPersisted) MutateBranch(MutationContext) (*MemNode, error) {
	panic("leaves don't get mutated this way!")
}

func (node LeafPersisted) Get(key []byte) ([]byte, int64, error) {
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

func (node LeafPersisted) IsLeaf() bool {
	return true
}

func (node LeafPersisted) String() string {
	return node.layout.String()
}

var _ Node = LeafPersisted{}
