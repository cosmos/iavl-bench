package internal

import "bytes"

type PersistedLeaf struct {
	layout LeafLayout
	kvData KVData
}

func (node PersistedLeaf) Height() uint8 {
	return 0
}

func (node PersistedLeaf) Size() int64 {
	return 1
}

func (node PersistedLeaf) Version() uint64 {
	return node.layout.NodeID().Version()
}

func (node PersistedLeaf) Key() ([]byte, error) {
	return node.kvData.Read(node.layout.KeyOffset(), node.layout.KeyLength())
}

func (node PersistedLeaf) Value() ([]byte, error) {
	valueOffset := node.layout.KeyOffset() + uint64(node.layout.KeyLength())
	bz, _, err := node.kvData.ReadVarintBytes(valueOffset)
	return bz, err
}

func (node PersistedLeaf) Left() *NodePointer {
	return nil
}

func (node PersistedLeaf) Right() *NodePointer {
	return nil
}

func (node PersistedLeaf) SafeHash() ([]byte, error) {
	return node.layout.Hash(), nil
}

func (node PersistedLeaf) MutateBranch(MutationContext) (*MemNode, error) {
	panic("leaves don't get mutated this way!")
}

func (node PersistedLeaf) Get(key []byte) ([]byte, int64, error) {
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

func (node PersistedLeaf) IsLeaf() bool {
	return true
}

func (node PersistedLeaf) Hash() ([]byte, error) {
	return node.layout.Hash(), nil
}

var _ Node = PersistedLeaf{}
