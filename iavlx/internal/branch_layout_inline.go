package internal

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	OffsetBranchInlineID           = 0
	OffsetBranchInlineLeftOffset   = OffsetBranchInlineID + SizeNodeID      // 8
	OffsetBranchInlineRightOffset  = OffsetBranchInlineLeftOffset + 5       // 13
	OffsetBranchInlineLeftID       = OffsetBranchInlineRightOffset + 5      // 18
	OffsetBranchInlineRightID      = OffsetBranchInlineLeftID + SizeNodeID  // 26
	OffsetBranchInlineKeyLenHeight = OffsetBranchInlineRightID + SizeNodeID // 34 (packed: 3 bytes key + 1 byte height)
	OffsetBranchInlineSize         = OffsetBranchInlineKeyLenHeight + 4     // 38
	OffsetBranchInlineSpan         = OffsetBranchInlineSize + 5             // 43
	OffsetBranchInlineHash         = OffsetBranchInlineSpan + 5             // 48
	OffsetBranchInlineData         = OffsetBranchInlineHash + SizeHash      // 80
	SizeBranchInlineHeader         = OffsetBranchInlineData                 // 80 bytes fixed header
)

type BranchLayoutInline struct {
	header []byte
	key    []byte
}

func (branch BranchLayoutInline) NodeID() NodeID {
	return NodeID(binary.LittleEndian.Uint64(branch.header[OffsetBranchInlineID : OffsetBranchInlineID+SizeNodeID]))
}

func (branch BranchLayoutInline) LeftOffset() uint64 {
	return uint64LE5(branch.header[OffsetBranchInlineLeftOffset : OffsetBranchInlineLeftOffset+5])
}

func (branch BranchLayoutInline) RightOffset() uint64 {
	return uint64LE5(branch.header[OffsetBranchInlineRightOffset : OffsetBranchInlineRightOffset+5])
}

func (branch BranchLayoutInline) LeftID() NodeID {
	return NodeID(binary.LittleEndian.Uint64(branch.header[OffsetBranchInlineLeftID : OffsetBranchInlineLeftID+SizeNodeID]))
}

func (branch BranchLayoutInline) RightID() NodeID {
	return NodeID(binary.LittleEndian.Uint64(branch.header[OffsetBranchInlineRightID : OffsetBranchInlineRightID+SizeNodeID]))
}

// KeyLength returns the key length (low 3 bytes of the packed field)
func (branch BranchLayoutInline) KeyLength() uint32 {
	packed := binary.LittleEndian.Uint32(branch.header[OffsetBranchInlineKeyLenHeight : OffsetBranchInlineKeyLenHeight+4])
	return packed & 0xFFFFFF // Low 3 bytes
}

// KeyLength returns the key length (low 3 bytes of the packed field)
func (branch BranchLayoutInline) Key() []byte {
	return branch.key
}

// Height returns the height (high byte of the packed field)
func (branch BranchLayoutInline) Height() uint8 {
	return branch.header[OffsetBranchInlineKeyLenHeight+3] // 4th byte is height
}

func (branch BranchLayoutInline) Size() uint64 {
	return uint64LE5(branch.header[OffsetBranchInlineSize : OffsetBranchInlineSize+5])
}

func (branch BranchLayoutInline) Span() uint64 {
	return uint64LE5(branch.header[OffsetBranchInlineSpan : OffsetBranchInlineSpan+5])
}

func (branch BranchLayoutInline) Hash() []byte {
	return branch.header[OffsetBranchInlineHash : OffsetBranchInlineHash+SizeHash]
}

func (branch BranchLayoutInline) String() string {
	return fmt.Sprintf("BranchInline{NodeID:%s, LeftOff:%d, RightOff:%d, LeftID:%s, RightID:%s, KeyLength:%d, Height:%d, Size:%d, Span:%d, Hash:%x}",
		branch.NodeID(), branch.LeftOffset(), branch.RightOffset(),
		branch.LeftID(), branch.RightID(),
		branch.KeyLength(), branch.Height(), branch.Size(), branch.Span(), branch.Hash())
}

// encodeBranchNodeInline encodes a MemNode into inline format with key data
func encodeBranchNodeInline(w io.Writer, node *MemNode, nodeId NodeID,
	leftOffset, rightOffset uint64, leftID, rightID NodeID, size, span uint64) error {

	keyLen := uint32(len(node.key))
	if keyLen > KeyLenMax {
		return fmt.Errorf("key length %d exceeds maximum of %d", keyLen, KeyLenMax)
	}

	if leftOffset > 0xFFFFFFFFFF {
		return fmt.Errorf("left offset %d exceeds maximum 5-byte value", leftOffset)
	}
	if rightOffset > 0xFFFFFFFFFF {
		return fmt.Errorf("right offset %d exceeds maximum 5-byte value", rightOffset)
	}
	if size > 0xFFFFFFFFFF {
		return fmt.Errorf("size %d exceeds maximum 5-byte value", size)
	}
	if span > 0xFFFFFFFFFF {
		return fmt.Errorf("span %d exceeds maximum 5-byte value", span)
	}

	// Use a fixed buffer for the header
	var header [SizeBranchInlineHeader]byte

	// Write NodeID (8 bytes)
	binary.LittleEndian.PutUint64(header[OffsetBranchInlineID:], uint64(nodeId))

	// Write LeftOffset (5 bytes)
	header[OffsetBranchInlineLeftOffset] = byte(leftOffset)
	header[OffsetBranchInlineLeftOffset+1] = byte(leftOffset >> 8)
	header[OffsetBranchInlineLeftOffset+2] = byte(leftOffset >> 16)
	header[OffsetBranchInlineLeftOffset+3] = byte(leftOffset >> 24)
	header[OffsetBranchInlineLeftOffset+4] = byte(leftOffset >> 32)

	// Write RightOffset (5 bytes)
	header[OffsetBranchInlineRightOffset] = byte(rightOffset)
	header[OffsetBranchInlineRightOffset+1] = byte(rightOffset >> 8)
	header[OffsetBranchInlineRightOffset+2] = byte(rightOffset >> 16)
	header[OffsetBranchInlineRightOffset+3] = byte(rightOffset >> 24)
	header[OffsetBranchInlineRightOffset+4] = byte(rightOffset >> 32)

	// Write LeftID (8 bytes)
	binary.LittleEndian.PutUint64(header[OffsetBranchInlineLeftID:], uint64(leftID))

	// Write RightID (8 bytes)
	binary.LittleEndian.PutUint64(header[OffsetBranchInlineRightID:], uint64(rightID))

	// Write packed KeyLength+Height (4 bytes: 3 bytes key length, 1 byte height)
	packed := keyLen | (uint32(node.height) << 24)
	binary.LittleEndian.PutUint32(header[OffsetBranchInlineKeyLenHeight:], packed)

	// Write Size (5 bytes)
	header[OffsetBranchInlineSize] = byte(size)
	header[OffsetBranchInlineSize+1] = byte(size >> 8)
	header[OffsetBranchInlineSize+2] = byte(size >> 16)
	header[OffsetBranchInlineSize+3] = byte(size >> 24)
	header[OffsetBranchInlineSize+4] = byte(size >> 32)

	// Write Span (5 bytes)
	header[OffsetBranchInlineSpan] = byte(span)
	header[OffsetBranchInlineSpan+1] = byte(span >> 8)
	header[OffsetBranchInlineSpan+2] = byte(span >> 16)
	header[OffsetBranchInlineSpan+3] = byte(span >> 24)
	header[OffsetBranchInlineSpan+4] = byte(span >> 32)

	// Write Hash (32 bytes)
	copy(header[OffsetBranchInlineHash:], node.Hash())

	// Write header
	if _, err := w.Write(header[:]); err != nil {
		return err
	}

	// Write inline key
	if _, err := w.Write(node.key); err != nil {
		return err
	}

	return nil
}
