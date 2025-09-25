package internal

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	OffsetLeafInlineID       = 0
	OffsetLeafInlineKeyLen   = OffsetLeafInlineID + SizeNodeID
	OffsetLeafInlineValueLen = OffsetLeafInlineKeyLen + 4 // KeyLength is 3 bytes + 1 padding byte
	OffsetLeafInlineHash     = OffsetLeafInlineValueLen + 4
	OffsetLeafInlineData     = OffsetLeafInlineHash + SizeHash
	SizeLeafInlineHeader     = OffsetLeafInlineData // 48 bytes fixed header
)

type LeafLayoutInline struct {
	header []byte
	key    []byte
	value  []byte
}

func (leaf LeafLayoutInline) NodeID() NodeID {
	return NodeID(binary.LittleEndian.Uint64(leaf.header[OffsetLeafInlineID : OffsetLeafInlineID+SizeNodeID]))
}

// KeyLen reads the key length (3 bytes with 1 padding byte for alignment)
// The 4th byte (padding) should always be 0
func (leaf LeafLayoutInline) KeyLen() uint32 {
	// Read as 4-byte little-endian, padding byte is high byte (should be 0)
	return binary.LittleEndian.Uint32(leaf.header[OffsetLeafInlineKeyLen : OffsetLeafInlineKeyLen+4])
}

func (leaf LeafLayoutInline) ValueLen() uint32 {
	return binary.LittleEndian.Uint32(leaf.header[OffsetLeafInlineValueLen : OffsetLeafInlineValueLen+4])
}

func (leaf LeafLayoutInline) Hash() []byte {
	return leaf.header[OffsetLeafInlineHash : OffsetLeafInlineHash+SizeHash]
}

func (leaf LeafLayoutInline) KeyLength() uint32 {
	return uint32LE3(leaf.header[OffsetLeafInlineKeyLen : OffsetLeafInlineKeyLen+3])
}

func (leaf LeafLayoutInline) ValueLength() uint32 {
	return binary.LittleEndian.Uint32(leaf.header[OffsetLeafInlineValueLen : OffsetLeafInlineValueLen+4])
}

// Key returns the inline key data
func (leaf LeafLayoutInline) Key() []byte {
	return leaf.key
}

// Value returns the inline value data
func (leaf LeafLayoutInline) Value() []byte {
	return leaf.value
}

func (leaf LeafLayoutInline) String() string {
	return fmt.Sprintf("LeafInline{NodeID:%s, KeyLength:%d, ValueLen:%d, Hash:%x}",
		leaf.NodeID(), leaf.KeyLen(), leaf.ValueLen(), leaf.Hash())
}

// encodeLeafNodeInline encodes a MemNode into inline format with key and value data
func encodeLeafNodeInline(w io.Writer, node *MemNode, nodeId NodeID) error {
	keyLen := uint32(len(node.key))
	if keyLen > KeyLenMax {
		return fmt.Errorf("key length %d exceeds maximum of %d", keyLen, KeyLenMax)
	}

	valueLen := uint32(len(node.value))

	// Use a small fixed buffer for the header
	var header [SizeLeafInlineHeader]byte

	// Write NodeID (8 bytes)
	binary.LittleEndian.PutUint64(header[OffsetLeafInlineID:], uint64(nodeId))

	// Write KeyLength as 3 bytes + 1 padding byte (4 bytes total for alignment)
	// The padding byte (high byte) is automatically 0
	binary.LittleEndian.PutUint32(header[OffsetLeafInlineKeyLen:], keyLen)

	// Write ValueLen (4 bytes)
	binary.LittleEndian.PutUint32(header[OffsetLeafInlineValueLen:], valueLen)

	// Write Hash (32 bytes)
	copy(header[OffsetLeafInlineHash:], node.Hash())

	// Write header
	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	// Write inline key
	if _, err := w.Write(node.key); err != nil {
		return err
	}

	// Write inline value
	if _, err := w.Write(node.value); err != nil {
		return err
	}

	return nil
}
