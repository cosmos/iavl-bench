package internal

type NodeStore interface {
	KVData
	ResolveLeaf(nodeId NodeID, fileIdx int64) (LeafLayout, error)
	ResolveBranch(nodeId NodeID, fileIdx int64) (BranchData, error)
}
