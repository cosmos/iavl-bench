package internal

import "sync/atomic"

type NodePointer struct {
	mem     atomic.Pointer[MemNode]
	fileIdx int64
	store   NodeStore
	id      NodeID
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
