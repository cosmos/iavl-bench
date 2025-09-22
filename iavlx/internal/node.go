package internal

type Node interface {
	Height() uint8
	IsLeaf() bool
	Size() int64
	Version() uint64
	Key() ([]byte, error)
	Value() ([]byte, error)
	Left() *NodePointer
	Right() *NodePointer
	Hash() []byte
	SafeHash() []byte
	MutateBranch(MutationContext) (*MemNode, error)
	Get(key []byte) (value []byte, index int64, err error)
}
