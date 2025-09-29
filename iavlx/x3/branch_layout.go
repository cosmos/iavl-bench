package x3

import (
	"fmt"
	"unsafe"
)

func init() {
	if unsafe.Sizeof(BranchLayout{}) != BranchSize {
		panic(fmt.Sprintf("invalid BranchLayout size: got %d, want %d", unsafe.Sizeof(BranchLayout{}), BranchSize))
	}
}

const (
	BranchSize = 72
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

type Branches struct {
	branches []BranchLayout
}

func NewBranches(buf []byte) (Branches, error) {
	// check alignment and size of the buffer
	p := unsafe.Pointer(unsafe.SliceData(buf))
	if uintptr(p)%unsafe.Alignof(BranchLayout{}) != 0 {
		return Branches{}, fmt.Errorf("input buffer is not aligned: %p", p)
	}
	size := int(unsafe.Sizeof(BranchLayout{}))
	if len(buf)%size != 0 {
		return Branches{}, fmt.Errorf("input buffer size is not a multiple of leaf size: %d %% %d != 0", len(buf), size)
	}
	branches := unsafe.Slice((*BranchLayout)(p), len(buf)/size)
	return Branches{branches}, nil
}

func (branches Branches) Branch(i uint32) *BranchLayout {
	return &branches.branches[i]
}
