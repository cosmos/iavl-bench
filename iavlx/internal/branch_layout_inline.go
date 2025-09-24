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
	SizeBranchInlineFixed          = OffsetBranchInlineData                 // 80 bytes fixed header
)

type BranchLayoutInline struct {
	data []byte
}

func (branch BranchLayoutInline) NodeID() NodeID {
	return NodeID(binary.LittleEndian.Uint64(branch.data[OffsetBranchInlineID : OffsetBranchInlineID+SizeNodeID]))
}

func (branch BranchLayoutInline) LeftOffset() uint64 {
	return uint64LE5(branch.data[OffsetBranchInlineLeftOffset : OffsetBranchInlineLeftOffset+5])
}

func (branch BranchLayoutInline) RightOffset() uint64 {
	return uint64LE5(branch.data[OffsetBranchInlineRightOffset : OffsetBranchInlineRightOffset+5])
}

func (branch BranchLayoutInline) LeftID() NodeID {
	return NodeID(binary.LittleEndian.Uint64(branch.data[OffsetBranchInlineLeftID : OffsetBranchInlineLeftID+SizeNodeID]))
}

func (branch BranchLayoutInline) RightID() NodeID {
	return NodeID(binary.LittleEndian.Uint64(branch.data[OffsetBranchInlineRightID : OffsetBranchInlineRightID+SizeNodeID]))
}

// KeyLen returns the key length (low 3 bytes of the packed field)
func (branch BranchLayoutInline) KeyLen() uint32 {
	packed := binary.LittleEndian.Uint32(branch.data[OffsetBranchInlineKeyLenHeight : OffsetBranchInlineKeyLenHeight+4])
	return packed & 0xFFFFFF // Low 3 bytes
}

// Height returns the height (high byte of the packed field)
func (branch BranchLayoutInline) Height() uint8 {
	return branch.data[OffsetBranchInlineKeyLenHeight+3] // 4th byte is height
}

func (branch BranchLayoutInline) Size() uint64 {
	return uint64LE5(branch.data[OffsetBranchInlineSize : OffsetBranchInlineSize+5])
}

func (branch BranchLayoutInline) Span() uint64 {
	return uint64LE5(branch.data[OffsetBranchInlineSpan : OffsetBranchInlineSpan+5])
}

func (branch BranchLayoutInline) Hash() []byte {
	return branch.data[OffsetBranchInlineHash : OffsetBranchInlineHash+SizeHash]
}

// Key returns the inline key data
func (branch BranchLayoutInline) Key() []byte {
	keyLen := branch.KeyLen()
	return branch.data[OffsetBranchInlineData : OffsetBranchInlineData+keyLen]
}

func (branch BranchLayoutInline) String() string {
	return fmt.Sprintf("BranchInline{NodeID:%s, LeftOff:%d, RightOff:%d, LeftID:%s, RightID:%s, KeyLen:%d, Height:%d, Size:%d, Span:%d, Hash:%x}",
		branch.NodeID(), branch.LeftOffset(), branch.RightOffset(),
		branch.LeftID(), branch.RightID(),
		branch.KeyLen(), branch.Height(), branch.Size(), branch.Span(), branch.Hash())
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
	var header [SizeBranchInlineFixed]byte

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

	// Write packed KeyLen+Height (4 bytes: 3 bytes key length, 1 byte height)
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
