package x3

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"

	"github.com/tidwall/btree"
)

type TreeStore struct {
	logger                 *slog.Logger
	dir                    string
	currentChangeset       *Changeset
	changesets             *btree.Map[uint32, *Changeset]
	rolloverThresholdBytes uint64
	savedVersion           atomic.Uint32
}

type TreeStoreOptions struct {
	RolloverThresholdBytes uint64
}

func NewTreeStore(dir string, options TreeStoreOptions, logger *slog.Logger) (*TreeStore, error) {
	rolloverThresholdBytes := options.RolloverThresholdBytes
	if rolloverThresholdBytes == 0 {
		rolloverThresholdBytes = 1024 * 1024 * 1024 // 1 GiB default
	}

	ts := &TreeStore{
		dir:                    dir,
		changesets:             &btree.Map[uint32, *Changeset]{},
		rolloverThresholdBytes: rolloverThresholdBytes,
		logger:                 logger,
	}
	cs, err := NewChangeset(filepath.Join(dir, "0"), 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create initial changeset: %w", err)
	}
	ts.currentChangeset = cs
	ts.changesets.Set(0, cs)
	return ts, nil
}

func (ts *TreeStore) getChangesetForVersion(version uint32) *Changeset {
	var res *Changeset
	ts.changesets.Ascend(version, func(key uint32, cs *Changeset) bool {
		res = cs
		return false
	})
	return res
}

func (ts *TreeStore) ResolveLeaf(nodeId NodeID) (LeafLayout, error) {
	cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
	if cs == nil {
		return LeafLayout{}, fmt.Errorf("no changeset found for version %d", nodeId.Version())
	}
	return cs.ResolveLeaf(nodeId, 0)
}

func (ts *TreeStore) ResolveBranch(nodeId NodeID) (BranchLayout, error) {
	cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
	if cs == nil {
		return BranchLayout{}, fmt.Errorf("no changeset found for version %d", nodeId.Version())
	}
	return cs.ResolveBranch(nodeId, 0)
}

func (ts *TreeStore) SavedVersion() uint32 {
	return ts.savedVersion.Load()
}

func (ts *TreeStore) SaveRoot(root *NodePointer, version uint32, totalLeaves, totalBranches uint32) error {
	ts.logger.Debug("saving root", "version", version)
	err := ts.currentChangeset.SaveRoot(root, version, totalLeaves, totalBranches)
	if err != nil {
		return err
	}

	ts.savedVersion.Store(version)

	changesetSize := ts.currentChangeset.TotalBytes()
	if changesetSize > ts.rolloverThresholdBytes {
		// TODO
		//err = ts.rolloverChangeset(version)
		//if err != nil {
		//	return fmt.Errorf("failed to rollover changeset: %w", err)
		//}
	}

	ts.logger.Debug("saved root", "version", version, "changeset_size", changesetSize)
	return nil
}

func (ts *TreeStore) rolloverChangeset(currentVersion uint32) error {
	// Seal the current changeset
	ts.currentChangeset.sealed = true

	nextVersion := currentVersion + 1

	ts.logger.Info("rolling over changeset",
		"start_version", ts.currentChangeset.startVersion,
		"end_version", ts.currentChangeset.endVersion,
		"next_version", nextVersion,
		"changeset_size", ts.currentChangeset.TotalBytes())

	// Create a new changeset for the next version
	newChangeset, err := NewChangeset(
		filepath.Join(ts.dir, fmt.Sprintf("%d", nextVersion)),
		nextVersion,
	)
	if err != nil {
		return fmt.Errorf("failed to create new changeset: %w", err)
	}

	// Update the tree store to use the new changeset
	ts.changesets.Set(nextVersion, newChangeset)
	ts.currentChangeset = newChangeset

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
