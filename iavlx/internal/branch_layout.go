package internal

import (
	"encoding/binary"
	"fmt"
)

const (
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

func encodeBranchNode(node *MemNode, buf [SizeBranch]byte, nodeId NodeID, left, right NodeRef, keyRef KeyRef, subtreeSize uint32) error {
	// write node ID (8 bytes)
	binary.LittleEndian.PutUint64(buf[OffsetBranchNodeID:OffsetBranchNodeID+SizeNodeID], uint64(nodeId))

	// write left and right child refs (8 bytes each)
	binary.LittleEndian.PutUint64(buf[OffsetBranchLeft:OffsetBranchLeft+SizeNodeID], uint64(left))
	binary.LittleEndian.PutUint64(buf[OffsetBranchRight:OffsetBranchRight+SizeNodeID], uint64(right))

	// write key ref (8 bytes)
	binary.LittleEndian.PutUint64(buf[OffsetBranchKeyRef:OffsetBranchKeyRef+SizeKeyRef], uint64(keyRef))

	// write height (1 byte)
	buf[OffsetBranchHeight] = node.height

	// write subtree size (3 bytes, little-endian)
	if subtreeSize >= 0xFFFFFF {
		// write max value to indicate overflow
		buf[OffsetBranchSubtreeSize+0] = 0xFF
		buf[OffsetBranchSubtreeSize+1] = 0xFF
		buf[OffsetBranchSubtreeSize+2] = 0xFF
	} else {
		buf[OffsetBranchSubtreeSize+0] = byte(subtreeSize)
		buf[OffsetBranchSubtreeSize+1] = byte(subtreeSize >> 8)
		buf[OffsetBranchSubtreeSize+2] = byte(subtreeSize >> 16)
	}

	// write size (5 bytes, little-endian)
	size := node.size
	if size > BranchSizeMax {
		return fmt.Errorf("branch node size %d exceeds maximum of %d", size, BranchSizeMax)
	}
	buf[OffsetBranchSize+0] = byte(size)
	buf[OffsetBranchSize+1] = byte(size >> 8)
	buf[OffsetBranchSize+2] = byte(size >> 16)
	buf[OffsetBranchSize+3] = byte(size >> 24)
	buf[OffsetBranchSize+4] = byte(size >> 32)

	// write hash (32 bytes)
	hash, err := HashNode(node)
	if err != nil {
		return err
	}
	copy(buf[OffsetBranchHash:OffsetBranchHash+32], hash)

	return nil
}
