package x3

import (
	"fmt"
	"unsafe"
)

func init() {
	if unsafe.Sizeof(BranchLayout{}) != SizeBranch {
		panic(fmt.Sprintf("invalid BranchLayout size: got %d, want %d", unsafe.Sizeof(BranchLayout{}), SizeBranch))
	}
}

const (
	SizeBranch = 72
)

type BranchLayout struct {
	id            NodeID
	left          NodeRef
	right         NodeRef
	keyOffset     uint32
	keyLoc        uint8
	height        uint8
	size          uint32 // TODO 5 bytes?
	orphanVersion uint32 // TODO 5 bytes?
	hash          [32]byte
}

func (b BranchLayout) ID() NodeID {
	return b.id
}
