package x3

type NodeStore interface {
	KVData
	ResolveLeaf(nodeId NodeID, fileIdx uint32) (*LeafLayout, error)
	ResolveBranch(nodeId NodeID, fileIdx uint32) (*BranchLayout, error)
	ResolveNodeRef(nodeRef NodeRef, selfIdx uint32) *NodePointer
	Resolve(nodeId NodeID, fileIdx uint32) (Node, error)
}
