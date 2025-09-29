package x3

import "fmt"

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
	// MutateBranch should always call MarkOrphan.
	MutateBranch(*MutationContext) (*MemNode, error)
	MarkOrphan(*MutationContext) error
	Get(key []byte) (value []byte, index int64, err error)

	fmt.Stringer
}

type MutationContext struct {
	Version uint32
	Orphans []NodeID
}
