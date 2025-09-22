package internal

import "sync/atomic"

type NodePointer struct {
	mem     atomic.Pointer[MemNode]
	fileIdx int64
	store   NodeStore
	id      NodeID
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
