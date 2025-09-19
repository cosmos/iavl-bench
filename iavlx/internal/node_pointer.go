package internal

import "sync/atomic"

type NodePointer struct {
	mem     atomic.Pointer[MemNode]
	fileIdx int64
	store   NodeStore
	id      NodeID
	// when we store a node to disk we may clear its mem pointer to save memory
	// branch nodes need some way to get the key for the node in the WAL (or wherever it's stored).
	// on leaf nodes this should be set with a reference to the key in the KV storage when leaf nodes are serialized
	_keyRef keyRefLink // used for linking new nodes to the position of the key in the kv storage
}

func NewNodePointer(memNode *MemNode) *NodePointer {
	n := &NodePointer{}
	n.mem.Store(memNode)
	return n
}

func (p *NodePointer) Resolve() (Node, error) {
	mem := p.mem.Load()
	if mem != nil {
		return mem, nil
	}
	panic("TODO")
}
