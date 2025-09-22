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
	if p.id.IsLeaf() {
		layout, err := p.store.ResolveLeaf(p.id, p.fileIdx)
		if err != nil {
			return nil, err
		}
		return LeafPersisted{
			kvData: p.store,
			layout: layout,
		}, nil
	} else {
		data, err := p.store.ResolveBranch(p.id, p.fileIdx)
		if err != nil {
			return nil, err
		}
		return BranchPersisted{
			store:      p.store,
			BranchData: data,
		}, nil
	}
}
