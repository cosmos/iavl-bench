package internal

// bit 63 indicates whether this is a node ID (0) or relative pointer (1)
// a valid NodeID should always have bit 63 as 0
// bit 62 indicates whether this is a leaf (1) or branch (0)
// bits 61-23 (39 bits) are for version
// bits 22-0 (23 bits) are for index
type NodeID uint64

func (id NodeID) IsLeaf() bool {
	// check if second highest bit is set
	return id&(1<<62) != 0
}

func (id NodeID) Version() uint64 {
	return (uint64(id) >> 23) & 0x7FFFFF
}

func (id NodeID) Index() uint32 {
	return uint32(id & 0x7FFFFF)
}

// bit 63 indicates whether this is a node ID (0) or relative pointer (1)
type NodeRef uint64

func (ref NodeRef) IsRelativePointer() bool {
	return ref&(1<<63) != 0
}

func (ref NodeRef) IsNodeID() bool {
	return ref&(1<<63) == 1
}

func (ref NodeRef) IsLeaf() bool {
	return ref&(1<<62) != 0
}

func (ref NodeRef) AsNodeID() NodeID {
	return NodeID(ref)
}

func (ref NodeRef) AsRelativePointer() NodeRelativePointer {
	return NodeRelativePointer(ref)
}

// bit 63 indicates whether this is a node ID (0) or relative pointer (1)
// a valid NodeRelativePointer should always have bit 63 as 0
// bit 62 indicates whether this is a leaf (1) or branch (0)
// bits 61-0 (62 bits) are for signed offset
type NodeRelativePointer uint64

func (ptr NodeRelativePointer) IsLeaf() bool {
	// check if second highest bit is set
	return ptr&(1<<62) != 0
}

func (ptr NodeRelativePointer) Offset() int64 {
	// get lower 61 bits and interpret as signed int64
	offset := int64(ptr &^ (3 << 62))
	// if bit 61 is set, it's negative
	if ptr&(1<<61) != 0 {
		offset *= -1
	}
	return offset
}
