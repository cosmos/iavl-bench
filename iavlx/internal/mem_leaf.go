package internal

import "bytes"

type MemLeaf struct {
	version   uint64
	key       []byte
	value     []byte
	hash      []byte
	walOffset int64
	persisted *PersistedNode
}

func (m *MemLeaf) Height() uint8 {
	return 0
}

func (m *MemLeaf) IsLeaf() bool {
	return true
}

func (m *MemLeaf) Size() int64 {
	return 1
}

func (m *MemLeaf) Version() uint64 {
	return m.version
}

func (m *MemLeaf) Key() []byte {
	return m.key
}

func (m *MemLeaf) Value() []byte {
	return m.value
}

func (m *MemLeaf) Left() Node {
	return nil
}

func (m *MemLeaf) Right() Node {
	return nil
}

func (m *MemLeaf) Hash() []byte {
	return m.hash
}

func (m *MemLeaf) SafeHash() []byte {
	return m.hash
}

func (m *MemLeaf) MutateBranch(uint64) *MemBranch {
	panic("cannot mutate a leaf to branch")
}

func (m *MemLeaf) Get(key []byte) ([]byte, uint64, error) {
	switch bytes.Compare(m.key, key) {
	case -1:
		return nil, 1, nil
	case 1:
		return nil, 0, nil
	default:
		return m.value, 0, nil
	}
}

var _ Node = &MemLeaf{}
