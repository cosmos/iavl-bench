package x3

import (
	"fmt"

	"github.com/tidwall/btree"
)

type TreeStore struct {
	currentChangeset  *Changeset
	changesets        btree.Map[uint32, *Changeset]
	rolloverThreshold uint64
}

func (ts *TreeStore) getChangesetForVersion(version uint32) *Changeset {
	var res *Changeset
	ts.changesets.Ascend(version, func(key uint32, cs *Changeset) bool {
		res = cs
		return false
	})
	return res
}

func (ts *TreeStore) ResolveLeaf(nodeId NodeID) (*LeafLayout, error) {
	cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
	if cs == nil {
		return nil, fmt.Errorf("no changeset found for version %d", nodeId.Version())
	}
	return cs.ResolveLeaf(nodeId, 0)
}

func (ts *TreeStore) ResolveBranch(nodeId NodeID) (*BranchLayout, error) {
	cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
	if cs == nil {
		return nil, fmt.Errorf("no changeset found for version %d", nodeId.Version())
	}
	return cs.ResolveBranch(nodeId, 0)
}

func (ts *TreeStore) SaveRoot(root *NodePointer, version uint32) error {
	err := ts.currentChangeset.SaveRoot(root, version)
	if err != nil {
		return err
	}

	changesetSize := ts.currentChangeset.TotalBytes()
	if changesetSize > ts.rolloverThreshold {
		ts.currentChangeset.sealed = true
		// TODO finalize current changeset and create a new one
	}
	return nil
}

func (ts *TreeStore) MarkOrphans(version uint32, nodeIds []NodeID) error {
	// TODO add locking
	for _, nodeId := range nodeIds {
		if nodeId.IsLeaf() {
			leaf, err := ts.ResolveLeaf(nodeId)
			if err != nil {
				return err
			}
			if leaf.orphanVersion == 0 {
				leaf.orphanVersion = version
			}
		} else {
			branch, err := ts.ResolveBranch(nodeId)
			if err != nil {
				return err
			}
			if branch.orphanVersion == 0 {
				branch.orphanVersion = version
			}
		}
	}

	// TODO flush changes to disk

	return nil
}
