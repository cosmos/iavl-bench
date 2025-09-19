package internal

// bit 63 indicates whether this is a node ID (0) or WAL offset + len (1)
type KeyRef uint64

func (ref KeyRef) IsNodeID() bool {
	return ref&(1<<63) == 0
}

func (ref KeyRef) IsWALRef() bool {
	return ref&(1<<63) != 0
}

func (ref KeyRef) AsNodeID() NodeID {
	return NodeID(ref)
}

func (ref KeyRef) WALRef() WALRef {
	return WALRef(ref)
}

// bit 63 indicates whether this is a node ID (0) or WAL offset + len (1)
// bits 62-40 (23 bits) are for length
// bits 39-0 (40 bits) are for offset
type WALRef uint64

func NewWALRef(length uint32, offset uint64) WALRef {
	if length >= 0x7FFFFF {
		length = 0x7FFFFF // max length that can be stored
	}
	if offset >= 0x10000000000 {
		panic("offset too large for WALRef")
	}
	return WALRef((1 << 63) | (uint64(length&0x7FFFFF) << 40) | (offset & 0xFFFFFFFFFF)) // 40 bits for offset
}

func (ref WALRef) Length() (len uint32, overflow bool) {
	len = uint32((ref >> 40) & 0x7FFFFF)
	if len == 0x7FFFFF {
		overflow = true
	}
	return
}

func (ref WALRef) Offset() uint64 {
	return uint64(ref & 0xFFFFFFFFFF) // 40 bits for offset
}
