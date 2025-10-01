package x3

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/tidwall/btree"
)

type TreeStore struct {
	logger            *slog.Logger
	dir               string
	currentWriter     *ChangesetWriter
	changesets        *btree.Map[uint32, *Changeset]
	changesetsMapLock sync.RWMutex
	savedVersion      atomic.Uint32
}

type TreeStoreOptions struct {
}

func NewTreeStore(dir string, options TreeStoreOptions, logger *slog.Logger) (*TreeStore, error) {
	ts := &TreeStore{
		dir:        dir,
		changesets: &btree.Map[uint32, *Changeset]{},
		logger:     logger,
	}

	writer, err := NewChangesetWriter(filepath.Join(dir, "1"), 1, ts)
	if err != nil {
		return nil, fmt.Errorf("failed to create initial changeset: %w", err)
	}
	ts.currentWriter = writer
	return ts, nil
}

func (ts *TreeStore) getChangesetForVersion(version uint32) *Changeset {
	ts.changesetsMapLock.RLock()
	defer ts.changesetsMapLock.RUnlock()

	var res *Changeset
	ts.changesets.Ascend(version, func(key uint32, cs *Changeset) bool {
		res = cs
		return false
	})
	return res
}

func (ts *TreeStore) ReadK(nodeId NodeID, _ uint32) (key []byte, err error) {
	cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
	cs.Pin()
	defer cs.Unpin()

	if cs == nil {
		return nil, fmt.Errorf("no changeset found for version %d", nodeId.Version())
	}

	panic("implement me")
}

func (ts *TreeStore) ReadKV(nodePtr NodeID, offset uint32) (key, value []byte, err error) {
	//TODO implement me
	panic("implement me")
}

func (ts *TreeStore) ResolveLeaf(nodeId NodeID, fileIdx uint32) (LeafLayout, error) {
	cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
	if cs == nil {
		return LeafLayout{}, fmt.Errorf("no changeset found for version %d", nodeId.Version())
	}
	return cs.ResolveLeaf(nodeId, 0)
}

func (ts *TreeStore) ResolveBranch(nodeId NodeID, fileIdx uint32) (BranchLayout, error) {
	cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
	if cs == nil {
		return BranchLayout{}, fmt.Errorf("no changeset found for version %d", nodeId.Version())
	}
	return cs.ResolveBranch(nodeId, 0)
}

func (ts *TreeStore) ResolveNodeRef(nodeRef NodeRef, selfIdx uint32) *NodePointer {
	//TODO implement me
	panic("implement me")
}

func (ts *TreeStore) Resolve(nodeId NodeID, fileIdx uint32) (Node, error) {
	//TODO implement me
	panic("implement me")
}

func (ts *TreeStore) SavedVersion() uint32 {
	return ts.savedVersion.Load()
}

func (ts *TreeStore) WriteWALUpdates(updates []KVUpdate) error {
	return ts.currentWriter.WriteWALUpdates(updates)
}

func (ts *TreeStore) WriteWALCommit(version uint32) error {
	return ts.currentWriter.WriteWALCommit(version)
}

func (ts *TreeStore) SaveRoot(version uint32, root *NodePointer, totalLeaves, totalBranches uint32) error {
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

	ts.changesetsMapLock.Lock()
	ts.changesets.Set(version, reader)
	ts.changesetsMapLock.Unlock()

	ts.savedVersion.Store(version)

	nextVersion := version + 1
	writer, err := NewChangesetWriter(filepath.Join(ts.dir, fmt.Sprintf("%d", nextVersion)), nextVersion, ts)
	if err != nil {
		return fmt.Errorf("failed to create writer for version %d: %w", nextVersion, err)
	}
	ts.currentWriter = writer

	return nil
}

func (ts *TreeStore) MarkOrphans(version uint32, nodeIds [][]NodeID) error {
	for _, nodeSet := range nodeIds {
		for _, nodeId := range nodeSet {
			cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
			if cs == nil {
				return fmt.Errorf("no changeset found for version %d", nodeId.Version())
			}
			err := cs.MarkOrphan(version, nodeId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (ts *TreeStore) Close() error {
	var errs []error
	ts.changesets.Scan(func(version uint32, cs *Changeset) bool {
		errs = append(errs, cs.Close())
		return true
	})
	return errors.Join(errs...)
}
