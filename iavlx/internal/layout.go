package internal

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const (
	SizeNodeID    = 8
	SizeKeyOffset = 5
	SizeKeyLen    = 3
	SizeHash      = sha256.Size

	OffsetLeafNodeID    = 0
	OffsetLeafKeyLen    = OffsetLeafNodeID + SizeNodeID
	KeyLenMax           = 0xFFFFFF // 3 bytes
	OffsetLeafKeyOffset = OffsetLeafKeyLen + SizeKeyLen
	KeyOffsetMax        = 0xFFFFFFFFFF // 5 bytes
	OffsetLeafHash      = OffsetLeafKeyOffset + SizeKeyOffset
	SizeLeafWithoutHash = OffsetLeafHash
	SizeLeaf            = SizeLeafWithoutHash + SizeHash

	OffsetBranchNodeID      = 0
	OffsetBranchLeft        = OffsetBranchNodeID + SizeNodeID
	OffsetBranchRight       = OffsetBranchLeft + SizeNodeID
	OffsetBranchKeyRef      = OffsetBranchRight + SizeNodeID
	SizeKeyRef              = SizeNodeID
	OffsetBranchHeight      = OffsetBranchKeyRef + SizeKeyRef
	SizeBranchHeight        = 1
	OffsetBranchSubtreeSize = OffsetBranchHeight + SizeBranchHeight
	SizeBranchSubtreeSize   = 3
	OffsetBranchSize        = OffsetBranchSubtreeSize + SizeBranchSubtreeSize
	SizeBranchSize          = 5
	BranchSizeMax           = 0xFFFFFFFFFF // 5 bytes
	OffsetBranchHash        = OffsetBranchSize + SizeBranchSize
	SizeBranchWithoutHash   = OffsetBranchHash
	SizeBranch              = SizeBranchWithoutHash + SizeHash
)

type Branches struct {
	data []byte
}

func NewBranches(data []byte) Branches {
	return Branches{data}
}

func (nodes Branches) Branch(i uint64) BranchLayout {
	offset := int(i) * SizeBranch
	return BranchLayout{data: (*[SizeBranch]byte)(nodes.data[offset : offset+SizeBranch])}
}

type BranchLayout struct {
	data *[SizeBranch]byte
}

func (branch BranchLayout) NodeID() NodeID {
	return NodeID(binary.LittleEndian.Uint64(branch.data[OffsetBranchNodeID : OffsetBranchNodeID+SizeNodeID]))
}

func (branch BranchLayout) Left() NodeRef {
	return NodeRef(binary.LittleEndian.Uint64(branch.data[OffsetBranchLeft : OffsetBranchLeft+SizeNodeID]))
}

func (branch BranchLayout) Right() NodeRef {
	return NodeRef(binary.LittleEndian.Uint64(branch.data[OffsetBranchRight : OffsetBranchRight+SizeNodeID]))
}

func (branch BranchLayout) KeyRef() KeyRef {
	return KeyRef(binary.LittleEndian.Uint64(branch.data[OffsetBranchKeyRef : OffsetBranchKeyRef+SizeKeyRef]))
}

func (branch BranchLayout) Height() uint8 {
	return branch.data[OffsetBranchHeight]
}

func (branch BranchLayout) SubtreeSize() uint32 {
	size := branch.data[OffsetBranchSubtreeSize : OffsetBranchSubtreeSize+SizeBranchSubtreeSize]
	return uint32LE3(size)
}

func (branch BranchLayout) Size() uint64 {
	size := branch.data[OffsetBranchSize : OffsetBranchSize+SizeBranchSize]
	return uint64LE5(size)
}

func (branch BranchLayout) Hash() []byte {
	return branch.data[OffsetBranchHash : OffsetBranchHash+32]
}

type Leaves struct {
	data []byte
}

func NewLeaves(data []byte) Leaves {
	return Leaves{data}
}

func (leaves Leaves) Leaf(i uint64) LeafLayout {
	offset := int(i) * SizeLeaf
	return LeafLayout{data: (*[SizeLeaf]byte)(leaves.data[offset : offset+SizeLeaf])}
}

type LeafLayout struct {
	data *[SizeLeaf]byte
}

func (leaf LeafLayout) NodeID() NodeID {
	return NodeID(binary.LittleEndian.Uint64(leaf.data[OffsetLeafNodeID : OffsetLeafNodeID+SizeNodeID]))
}

func (leaf LeafLayout) KeyLength() uint32 {
	keyLen := leaf.data[OffsetLeafKeyLen : OffsetLeafKeyLen+SizeKeyLen]
	return uint32LE3(keyLen)
}

func (leaf LeafLayout) KeyOffset() uint64 {
	offset := leaf.data[OffsetLeafKeyOffset : OffsetLeafKeyOffset+SizeKeyOffset]
	return uint64LE5(offset)
}

func (leaf LeafLayout) Hash() []byte {
	return leaf.data[OffsetLeafHash : OffsetLeafHash+32]
}

func uint32LE3(b []byte) uint32 {
	_ = b[2] // bounds check hint to compiler; see golang.org/issue/14808
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}

func uint64LE5(b []byte) uint64 {
	_ = b[4] // bounds check hint to compiler; see golang.org/issue/14808
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 | uint64(b[4])<<32
}

func encodeLeafNode(node *MemNode, buf [SizeLeaf]byte, nodeId NodeID) error {
	binary.LittleEndian.PutUint64(buf[OffsetLeafNodeID:OffsetLeafNodeID+SizeNodeID], uint64(nodeId))
	keyLen := len(node.key)
	if keyLen > KeyLenMax {
		return fmt.Errorf("key length %d exceeds maximum of %d", keyLen, KeyLenMax)
	}
	buf[OffsetLeafKeyLen] = byte(keyLen)
	buf[OffsetLeafKeyLen+1] = byte(keyLen >> 8)
	buf[OffsetLeafKeyLen+2] = byte(keyLen >> 16)
	walOffset := node._walOffset
	if walOffset > KeyOffsetMax {
		return fmt.Errorf("key offset %d exceeds maximum of %d", walOffset, KeyOffsetMax)
	}
	buf[OffsetLeafKeyOffset] = byte(walOffset)
	buf[OffsetLeafKeyOffset+1] = byte(walOffset >> 8)
	buf[OffsetLeafKeyOffset+2] = byte(walOffset >> 16)
	buf[OffsetLeafKeyOffset+3] = byte(walOffset >> 24)
	buf[OffsetLeafKeyOffset+4] = byte(walOffset >> 32)
	copy(buf[OffsetLeafHash:OffsetLeafHash+SizeHash], node.hash)
	return nil
}

func encodeBranchNode(node *MemNode, buf [SizeBranch]byte, nodeId NodeID, left, right NodeRef, keyRef KeyRef) error {
	binary.LittleEndian.PutUint64(buf[OffsetBranchNodeID:OffsetBranchNodeID+SizeNodeID], uint64(nodeId))
	binary.LittleEndian.PutUint64(buf[OffsetBranchLeft:OffsetBranchLeft+SizeNodeID], uint64(left))
	binary.LittleEndian.PutUint64(buf[OffsetBranchRight:OffsetBranchRight+SizeNodeID], uint64(right))
	binary.LittleEndian.PutUint64(buf[OffsetBranchKeyRef:OffsetBranchKeyRef+SizeKeyRef], uint64(keyRef))

	buf[OffsetBranchHeight] = node.height
	size := node.size
	if size > BranchSizeMax {
		return fmt.Errorf("branch node size %d exceeds maximum of %d", size, BranchSizeMax)
	}

	panic("TODO")
}
