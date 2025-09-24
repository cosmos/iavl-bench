package internal

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const (
	SizeNodeID          = 8
	SizeKeyOffset       = 5
	SizeKeyLen          = 3
	SizeHash            = sha256.Size
	OffsetLeafNodeID    = 0
	OffsetLeafKeyLen    = OffsetLeafNodeID + SizeNodeID
	KeyLenMax           = 0xFFFFFF // 3 bytes
	OffsetLeafKeyOffset = OffsetLeafKeyLen + SizeKeyLen
	KeyOffsetMax        = 0xFFFFFFFFFF // 5 bytes
	OffsetLeafHash      = OffsetLeafKeyOffset + SizeKeyOffset
	SizeLeafWithoutHash = OffsetLeafHash
	SizeLeaf            = SizeLeafWithoutHash + SizeHash
)

type LeafLayout struct {
	data [SizeLeaf]byte
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

func (leaf LeafLayout) String() string {
	return fmt.Sprintf("Leaf{NodeID:%s, KeyLen:%d, KeyOffset:%d, Hash:%x}", leaf.NodeID(), leaf.KeyLength(), leaf.KeyOffset(), leaf.Hash())
}

func uint32LE3(b []byte) uint32 {
	_ = b[2] // bounds check hint to compiler; see golang.org/issue/14808
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}

func uint64LE5(b []byte) uint64 {
	_ = b[4] // bounds check hint to compiler; see golang.org/issue/14808
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 | uint64(b[4])<<32
}

func encodeLeafNode(node *MemNode, buf *[SizeLeaf]byte, nodeId NodeID) error {
	binary.LittleEndian.PutUint64(buf[OffsetLeafNodeID:OffsetLeafNodeID+SizeNodeID], uint64(nodeId))
	keyLen := len(node.key)
	if keyLen > KeyLenMax {
		return fmt.Errorf("key length %d exceeds maximum of %d", keyLen, KeyLenMax)
	}
	buf[OffsetLeafKeyLen] = byte(keyLen)
	buf[OffsetLeafKeyLen+1] = byte(keyLen >> 8)
	buf[OffsetLeafKeyLen+2] = byte(keyLen >> 16)
	walRef, ok := node._keyRef.(WALRef)
	if !ok {
		return fmt.Errorf("expected WALRef for leaf node key reference, got %T", node._keyRef)
	}
	walOffset := walRef.Offset()
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
