package x3

import (
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/btree"
)

type TreeStore struct {
	logger *slog.Logger
	dir    string

	currentWriter     *ChangesetWriter
	changesets        *btree.Map[uint32, *changesetEntry]
	changesetsMapLock sync.RWMutex
	savedVersion      atomic.Uint32

	opts TreeStoreOptions

	toDelete          map[*Changeset]string
	cleanupProcDone   chan struct{}
	orphanWriteQueue  []markOrphansReq
	orphanQueueLock   sync.Mutex
	disableCompaction bool
}

type markOrphansReq struct {
	version uint32
	orphans [][]NodeID
}

type changesetEntry struct {
	changeset atomic.Pointer[Changeset]
	compactor atomic.Pointer[Compactor]
}

type TreeStoreOptions struct {
	RetainCriteria         RetainCriteria
	CompactWAL             bool
	CompactOrphanThreshold float64
	CompactOrphanAge       float64
}

func NewTreeStore(dir string, options TreeStoreOptions, logger *slog.Logger) (*TreeStore, error) {
	ts := &TreeStore{
		dir:        dir,
		changesets: &btree.Map[uint32, *changesetEntry]{},
		toDelete:   map[*Changeset]string{},
		logger:     logger,
		opts:       options,
	}

	writer, err := NewChangesetWriter(filepath.Join(dir, "1"), 1, ts)
	if err != nil {
		return nil, fmt.Errorf("failed to create initial changeset: %w", err)
	}
	ts.currentWriter = writer

	ts.cleanupProcDone = make(chan struct{})
	go ts.cleanupProc()
	//ts.disableCompaction = true

	return ts, nil
}

func (ts *TreeStore) getChangesetEntryForVersion(version uint32) *changesetEntry {
	ts.changesetsMapLock.RLock()
	defer ts.changesetsMapLock.RUnlock()

	var res *changesetEntry
	ts.changesets.Ascend(version, func(key uint32, cs *changesetEntry) bool {
		res = cs
		return false
	})
	return res
}

func (ts *TreeStore) getChangesetForVersion(version uint32) *Changeset {
	return ts.getChangesetEntryForVersion(version).changeset.Load()
}

func (ts *TreeStore) ReadK(nodeId NodeID, _ uint32) (key []byte, err error) {
	cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
	cs.Pin()
	defer cs.Unpin()

	if cs == nil {
		return nil, fmt.Errorf("no changeset found for version %d", nodeId.Version())
	}

	var offset uint32
	if nodeId.IsLeaf() {
		leaf, err := cs.ResolveLeaf(nodeId, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve leaf %s: %w", nodeId.String(), err)
		}
		offset = leaf.KeyOffset
	} else {
		branch, err := cs.ResolveBranch(nodeId, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve branch %s: %w", nodeId.String(), err)
		}
		offset = branch.KeyOffset
	}

	return cs.ReadK(nodeId, offset)
}

func (ts *TreeStore) ReadKV(nodeId NodeID, _ uint32) (key, value []byte, err error) {
	cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
	cs.Pin()
	defer cs.Unpin()

	if cs == nil {
		return nil, nil, fmt.Errorf("no changeset found for version %d", nodeId.Version())
	}

	if !nodeId.IsLeaf() {
		return nil, nil, fmt.Errorf("node %s is not a leaf", nodeId.String())
	}

	leaf, err := cs.ResolveLeaf(nodeId, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to resolve leaf %s: %w", nodeId.String(), err)
	}

	return cs.ReadKV(nodeId, leaf.KeyOffset)
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

func (ts *TreeStore) Resolve(nodeId NodeID, _ uint32) (Node, error) {
	cs := ts.getChangesetForVersion(uint32(nodeId.Version()))
	if cs == nil {
		return nil, fmt.Errorf("no changeset found for version %d", nodeId.Version())
	}

	return cs.Resolve(nodeId, 0)
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

	var changesetEntry changesetEntry
	changesetEntry.changeset.Store(reader)

	ts.changesetsMapLock.Lock()
	ts.changesets.Set(version, &changesetEntry)
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

func (ts *TreeStore) MarkOrphans(version uint32, nodeIds [][]NodeID) {
	req := markOrphansReq{
		version: version,
		orphans: nodeIds,
	}

	ts.orphanQueueLock.Lock()
	defer ts.orphanQueueLock.Unlock()

	ts.orphanWriteQueue = append(ts.orphanWriteQueue, req)
}

func (ts *TreeStore) cleanupProc() {
	minCompactorInterval := time.Duration(0)
	var lastCompactorStart time.Time
	for {
		sleepTime := time.Duration(0)
		if time.Since(lastCompactorStart) < minCompactorInterval {
			sleepTime = minCompactorInterval - time.Since(lastCompactorStart)
		}
		select {
		case <-ts.cleanupProcDone:
			return
		case <-time.After(sleepTime):
		}

		lastCompactorStart = time.Now()

		ts.changesetsMapLock.RLock()
		var entries []*changesetEntry
		ts.changesets.Scan(func(version uint32, entry *changesetEntry) bool {
			entries = append(entries, entry)
			return true
		})
		ts.changesetsMapLock.RUnlock()

		for _, entry := range entries {
			select {
			case <-ts.cleanupProcDone:
				return
			default:
			}

			err := ts.doMarkOrphans()
			if err != nil {
				ts.logger.Error("failed to mark orphans", "error", err)
			}

			cs := entry.changeset.Load()
			err = cs.FlushOrphans()
			if err != nil {
				ts.logger.Error("failed to flush orphans", "error", err)
				continue
			}

			if ts.disableCompaction {
				continue
			}

			savedVersion := ts.savedVersion.Load()
			compactOrphanAge := ts.opts.CompactOrphanAge
			if compactOrphanAge <= 0 {
				compactOrphanAge = 3
			}
			compactOrphanThreshold := ts.opts.CompactOrphanThreshold
			if compactOrphanThreshold <= 0 {
				compactOrphanThreshold = 0.6
			}
			ageTarget := float64(savedVersion) - compactOrphanAge
			if !cs.ReadyToCompact(compactOrphanThreshold, ageTarget) {
				continue
			}

			retainCriteria := ts.opts.RetainCriteria
			if retainCriteria == nil {
				retainCriteria = func(createVersion, orphanVersion uint32) bool {
					return orphanVersion == 0 // keep nodes that are not orphaned
				}
			}

			compactor, err := NewCompacter(ts.logger, cs, CompactOptions{
				RetainCriteria: retainCriteria,
				CompactWAL:     ts.opts.CompactWAL,
			}, ts)
			if err != nil {
				ts.logger.Error("failed to create compactor", "error", err)
				continue
			}

			entry.compactor.Store(compactor)

			ts.logger.Info("compacting changeset", "info", cs.info, "size", cs.TotalBytes())
			newCs, err := compactor.Compact()
			if err != nil {
				ts.logger.Error("failed to compact changeset", "error", err)
				entry.compactor.Store(nil)
				continue
			}
			ts.logger.Info("compacted changeset", "info", newCs.info, "new_size", newCs.TotalBytes(), "old_size", cs.TotalBytes())

			entry.changeset.Store(newCs)
			cs.Evict()
			entry.compactor.Store(nil)

			err = compactor.ApplyPendingOrphans(newCs)
			if err != nil {
				ts.logger.Error("failed to apply pending orphans", "error", err)
				continue
			}

			if !cs.IsDisposed() {
				ts.toDelete[cs] = newCs.kvlogPath
			} else {
				// delete all .dat files in old changeset
				err = cs.DeleteFiles(newCs.kvlogPath)
				if err != nil {
					ts.logger.Error("failed to delete old changeset files", "error", err)
				}
			}
		}

		for oldCs, kvlogPath := range ts.toDelete {
			select {
			case <-ts.cleanupProcDone:
				return
			default:
			}

			if !oldCs.IsDisposed() {
				continue
			}

			err := oldCs.DeleteFiles(kvlogPath)
			if err != nil {
				ts.logger.Error("failed to delete old changeset files", "error", err)
			}
			delete(ts.toDelete, oldCs)
		}
	}
}

func (ts *TreeStore) doMarkOrphans() error {
	var orphanQueue []markOrphansReq
	ts.orphanQueueLock.Lock()
	orphanQueue, ts.orphanWriteQueue = ts.orphanWriteQueue, nil
	ts.orphanQueueLock.Unlock()

	for _, req := range orphanQueue {
		for _, nodeSet := range req.orphans {
			for _, nodeId := range nodeSet {
				ce := ts.getChangesetEntryForVersion(uint32(nodeId.Version()))
				if ce == nil {
					return fmt.Errorf("no changeset found for version %d", nodeId.Version())
				}
				if compactor := ce.compactor.Load(); compactor != nil {
					compactor.MarkOrphan(req.version, nodeId)
				} else {
					err := ce.changeset.Load().MarkOrphan(req.version, nodeId)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (ts *TreeStore) Close() error {
	close(ts.cleanupProcDone)

	ts.changesetsMapLock.Lock()

	var errs []error
	ts.changesets.Scan(func(version uint32, entry *changesetEntry) bool {
		errs = append(errs, entry.changeset.Load().Close())
		return true
	})
	return errors.Join(errs...)
}
