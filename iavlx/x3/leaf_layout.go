package x3

import (
	"fmt"
	"unsafe"
)

func init() {
	if unsafe.Sizeof(LeafLayout{}) != SizeLeaf {
		panic(fmt.Sprintf("invalid LeafLayout size: got %d, want %d", unsafe.Sizeof(LeafLayout{}), SizeLeaf))
	}
}

const (
	SizeLeaf = 48
)

type LeafLayout struct {
	id            NodeID
	keyOffset     uint32
	orphanVersion uint32 // TODO 5 bytes?
	hash          [32]byte
}

func (l LeafLayout) ID() NodeID {
	return l.id
}
