package x3

import (
	"fmt"
	"unsafe"
)

// check little endian at init time
func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	if buf != [2]byte{0xCD, 0xAB} {
		panic("native byte order is not little endian, please build without nativebyteorder")
	}
}

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

type Leaves struct {
	leaves []LeafLayout
}

func NewLeaves(buf []byte) (Leaves, error) {
	// check alignment and size of the buffer
	p := unsafe.Pointer(unsafe.SliceData(buf))
	if uintptr(p)%unsafe.Alignof(LeafLayout{}) != 0 {
		return Leaves{}, fmt.Errorf("input buffer is not aligned: %p", p)
	}
	size := int(unsafe.Sizeof(LeafLayout{}))
	if len(buf)%size != 0 {
		return Leaves{}, fmt.Errorf("input buffer size is not a multiple of leaf size: %d %% %d != 0", len(buf), size)
	}
	leaves := unsafe.Slice((*LeafLayout)(p), len(buf)/size)
	return Leaves{leaves}, nil
}

func (leaves Leaves) Leaf(i uint32) *LeafLayout {
	return &leaves.leaves[i]
}
