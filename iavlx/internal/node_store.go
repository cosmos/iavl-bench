package internal

type NodeStore interface {
	KVData
	Get(ref NodeRef) (Node, error)
}
