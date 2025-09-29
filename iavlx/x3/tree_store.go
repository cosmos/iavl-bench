package x3

type TreeStore struct {
	currentChangeset *Changeset
}

func (ts *TreeStore) SaveRoot(root *NodePointer, version uint64) error {
	return ts.currentChangeset.SaveRoot(root, version)
}
