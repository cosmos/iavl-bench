package x3

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"

	"github.com/tidwall/btree"
)

type TreeStore struct {
	logger        *slog.Logger
	dir           string
	currentWriter *ChangesetWriter
	changesets    *btree.Map[uint32, *ChangesetReader]
	savedVersion  atomic.Uint32
}

type TreeStoreOptions struct {
}

func NewTreeStore(dir string, options TreeStoreOptions, logger *slog.Logger) (*TreeStore, error) {
	ts := &TreeStore{
		dir:        dir,
		changesets: &btree.Map[uint32, *ChangesetReader]{},
		logger:     logger,
	}

	writer, err := NewChangesetWriter(filepath.Join(dir, "1"), 1, ts)
	if err != nil {
		return nil, fmt.Errorf("failed to create initial changeset: %w", err)
	}
	ts.currentWriter = writer
	return ts, nil
}

func (ts *TreeStore) getChangesetForVersion(version uint32) *ChangesetReader {
	var res *ChangesetReader
	ts.changesets.Ascend(version, func(key uint32, cs *ChangesetReader) bool {
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
	err := ts.currentWriter.SaveRoot(root, version, totalLeaves, totalBranches)
	if err != nil {
		return err
	}

	ts.logger.Debug("saved root", "version", version, "changeset_size", ts.currentWriter.TotalBytes())

	reader, err := ts.currentWriter.Seal()
	if err != nil {
		return fmt.Errorf("failed to seal changeset for version %d: %w", version, err)
	}

	ts.changesets.Set(version, reader)

	ts.savedVersion.Store(version)

	nextVersion := version + 1
	writer, err := NewChangesetWriter(filepath.Join(ts.dir, fmt.Sprintf("%d", nextVersion)), nextVersion, ts)
	if err != nil {
		return fmt.Errorf("failed to create writer for version %d: %w", nextVersion, err)
	}
	ts.currentWriter = writer

	return nil
}

//func (ts *TreeStore) MarkOrphans(version uint32, nodeIds []NodeID) error {
//	// TODO add locking
//	for _, nodeId := range nodeIds {
//		if nodeId.IsLeaf() {
//			leaf, err := ts.ResolveLeaf(nodeId)
//			if err != nil {
//				return err
//			}
//			if leaf.orphanVersion == 0 {
//				leaf.orphanVersion = version
//			}
//		} else {
//			branch, err := ts.ResolveBranch(nodeId)
//			if err != nil {
//				return err
//			}
//			if branch.orphanVersion == 0 {
//				branch.orphanVersion = version
//			}
//		}
//	}
//
//	// TODO flush changes to disk
//
//	return nil
//}
