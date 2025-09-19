package x1

type Pointer uint64

func (p Pointer) IsLeaf() bool {
	// check if the highest bit is set
	return p&(1<<63) != 0
}

func (p Pointer) IsLocal() bool {
	// check if the second highest bit is off
	return p&(1<<62) == 0
}

func (p Pointer) Offset() int64 {
	return int64(p &^ (3 << 62))
}

func (p Pointer) NodeID() uint64 {
	return uint64(p &^ (3 << 62))
}

// 39 bits after the highest 2 bits
func (p Pointer) Version() uint64 {
	return (uint64(p) >> 22) & 0x7FFFFF
}

// 23 bits after the highest 2 bits and 39 bits of version
func (p Pointer) Index() uint32 {
	return uint32(p & 0x3FFFFF)
}
