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

	opts Options

	closeCleanupProc chan struct{}
	cleanupProcDone  chan struct{}
	orphanWriteQueue []markOrphansReq
	orphanQueueLock  sync.Mutex

	syncQueue chan *Changeset
	syncDone  chan error
}

type markOrphansReq struct {
	version uint32
	orphans [][]NodeID
}

type changesetEntry struct {
	changeset atomic.Pointer[Changeset]
}

func NewTreeStore(dir string, options Options, logger *slog.Logger) (*TreeStore, error) {
	ts := &TreeStore{
		dir:        dir,
		changesets: &btree.Map[uint32, *changesetEntry]{},
		logger:     logger,
		opts:       options,
	}

	writer, err := NewChangesetWriter(filepath.Join(dir, "1"), 1, ts)
	if err != nil {
		return nil, fmt.Errorf("failed to create initial changeset: %w", err)
	}
	ts.currentWriter = writer

	ts.closeCleanupProc = make(chan struct{})
	ts.cleanupProcDone = make(chan struct{})
	go ts.cleanupProc()

	if options.WriteWAL && options.WalSyncBuffer >= 0 {
		bufferSize := options.WalSyncBuffer
		if bufferSize == 0 {
			bufferSize = 1 // Almost synchronous
		}
		ts.syncQueue = make(chan *Changeset, bufferSize)
		ts.syncDone = make(chan error)
		go ts.syncProc()
	}

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

	// Queue changeset for async WAL sync if enabled
	if ts.syncQueue != nil {
		select {
		case err := <-ts.syncDone:
			if err != nil {
				return err
			}
		default:
		}
		reader.needsSync.Store(true)
		ts.syncQueue <- reader
	} else {
		// Otherwise, sync immediately
		err := reader.kvlogReader.file.Sync()
		if err != nil {
			return fmt.Errorf("failed to sync WAL file: %w", err)
		}
	}

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

func (ts *TreeStore) syncProc() {
	defer close(ts.syncDone)
	for cs := range ts.syncQueue {
		if err := cs.kvlogReader.file.Sync(); err != nil {
			ts.syncDone <- fmt.Errorf("failed to sync WAL file: %w", err)
			return
		}
		cs.needsSync.Store(false)
	}
}

func (ts *TreeStore) cleanupProc() {
	defer close(ts.cleanupProcDone)
	minCompactorInterval := time.Second * time.Duration(ts.opts.MinCompactionSeconds)
	var lastCompactorStart time.Time

	toDelete := map[*Changeset]string{}
	for {
		sleepTime := time.Duration(0)
		if time.Since(lastCompactorStart) < minCompactorInterval {
			sleepTime = minCompactorInterval - time.Since(lastCompactorStart)
		}
		select {
		case <-ts.closeCleanupProc:
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

		for i := 0; i < len(entries); i++ {
			entry := entries[i]
			select {
			case <-ts.closeCleanupProc:
				return
			default:
			}

			err := ts.doMarkOrphans()
			if err != nil {
				ts.logger.Error("failed to mark orphans", "error", err)
			}

			cs := entry.changeset.Load()

			// Safety check - skip if evicted or disposed
			if cs.evicted.Load() || cs.disposed.Load() {
				ts.logger.Warn("skipping evicted/disposed changeset", "index", i, "dir", cs.dir)
				continue
			}

			// Safety check - ensure info is valid
			if cs.info == nil {
				ts.logger.Error("changeset has nil info", "index", i, "dir", cs.dir)
				continue
			}

			err = cs.FlushOrphans()
			if err != nil {
				ts.logger.Error("failed to flush orphans", "error", err)
				continue
			}

			if ts.opts.DisableCompaction {
				continue
			}

			// Skip if still pending sync
			if cs.needsSync.Load() {
				continue
			}

			savedVersion := ts.savedVersion.Load()
			retainVersions := ts.opts.RetainVersions
			retentionWindowBottom := savedVersion - retainVersions

			// Skip changesets within retention window
			if cs.info.EndVersion >= retentionWindowBottom {
				continue
			}

			compactOrphanAge := ts.opts.CompactionOrphanAge
			if compactOrphanAge == 0 {
				compactOrphanAge = 10
			}
			compactOrphanThreshold := ts.opts.CompactionOrphanRatio
			if compactOrphanThreshold <= 0 {
				compactOrphanThreshold = 0.6
			}

			// Age target relative to bottom of retention window
			ageTarget := retentionWindowBottom - compactOrphanAge

			// Check orphan-based trigger
			shouldCompact := cs.ReadyToCompact(compactOrphanThreshold, ageTarget)

			// Check size-based joining trigger
			maxSize := uint64(ts.opts.ChangesetMaxTarget)
			if maxSize == 0 {
				maxSize = 1024 * 1024 * 1024 // 1GB default
			}

			canJoin := false
			if !shouldCompact && i+1 < len(entries) && ts.opts.CompactWAL {
				nextEntry := entries[i+1]
				nextCs := nextEntry.changeset.Load()
				if nextCs.info.StartVersion == cs.info.EndVersion+1 {
					if uint64(cs.TotalBytes())+uint64(nextCs.TotalBytes()) <= maxSize {
						canJoin = true
					}
				}
			}

			if !shouldCompact && !canJoin {
				continue
			}

			retainVersion := retentionWindowBottom
			retainCriteria := func(createVersion, orphanVersion uint32) bool {
				// orphanVersion should be non-zero
				if orphanVersion >= retainVersion {
					// keep the orphan if it's in the retain window
					return true
				} else {
					// otherwise, we can remove it
					return false
				}
			}

			ts.logger.Info("compacting changeset", "info", cs.info, "size", cs.TotalBytes())

			compactor, err := NewCompacter(ts.logger, cs, CompactOptions{
				RetainCriteria: retainCriteria,
				CompactWAL:     ts.opts.CompactWAL,
			}, ts)
			if err != nil {
				ts.logger.Error("failed to create compactor", "error", err)
				continue
			}

			processedEntries := []*changesetEntry{entry}

			// Greedily add contiguous changesets if CompactWAL enabled
			if ts.opts.CompactWAL {
				currentCs := cs
				for j := i + 1; j < len(entries); j++ {
					nextEntry := entries[j]
					nextCs := nextEntry.changeset.Load()

					// Check contiguity
					if nextCs.info.StartVersion != currentCs.info.EndVersion+1 {
						break
					}

					// Check size limit
					if compactor.TotalBytes()+uint64(nextCs.TotalBytes()) > maxSize {
						break
					}

					// Skip if pending sync
					if nextCs.needsSync.Load() {
						break
					}

					ts.logger.Info("adding changeset to compaction", "dir", nextCs.dir)
					err = compactor.AddChangeset(nextCs)
					if err != nil {
						ts.logger.Error("failed to add changeset to compaction", "error", err)
						break
					}

					processedEntries = append(processedEntries, nextEntry)
					currentCs = nextCs
					i = j // Skip this entry in outer loop
				}
			}

			newCs, err := compactor.Seal()
			if err != nil {
				ts.logger.Error("failed to seal compacted changeset", "error", err)
				continue
			}

			// Update all processed entries to point to new changeset
			oldSize := uint64(0)
			for _, procEntry := range processedEntries {
				oldCs := procEntry.changeset.Load()
				oldSize += uint64(oldCs.TotalBytes())

				procEntry.changeset.Store(newCs)
				oldCs.Evict()

				if !oldCs.TryDispose() {
					toDelete[oldCs] = newCs.kvlogPath
				} else {
					ts.logger.Info("changeset disposed, deleting files", "path", oldCs.dir)
					err = oldCs.DeleteFiles(newCs.kvlogPath)
					if err != nil {
						ts.logger.Error("failed to delete old changeset files", "error", err, "path", oldCs.dir)
					}
				}
			}

			ts.logger.Info("compacted changeset", "dir", newCs.dir, "new_size", newCs.TotalBytes(), "old_size", oldSize, "joined", len(processedEntries))
		}

		for oldCs, kvlogPath := range toDelete {
			select {
			case <-ts.closeCleanupProc:
				return
			default:
			}

			if !oldCs.TryDispose() {
				ts.logger.Warn("old changeset not disposed, skipping delete", "path", oldCs.dir)
				continue
			}

			ts.logger.Info("deleting old changeset files", "path", oldCs.dir)
			err := oldCs.DeleteFiles(kvlogPath)
			if err != nil {
				ts.logger.Error("failed to delete old changeset files", "error", err)
			}
			delete(toDelete, oldCs)
		}
	}
}

// doMarkOrphans must only be called from the cleanupProc
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
				err := ce.changeset.Load().MarkOrphan(req.version, nodeId)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ts *TreeStore) Close() error {
	close(ts.closeCleanupProc)
	<-ts.cleanupProcDone

	if ts.syncQueue != nil {
		close(ts.syncQueue)
		err := <-ts.syncDone
		if err != nil {
			return err
		}
	}

	ts.changesetsMapLock.Lock()

	var errs []error
	ts.changesets.Scan(func(version uint32, entry *changesetEntry) bool {
		errs = append(errs, entry.changeset.Load().Close())
		return true
	})
	return errors.Join(errs...)
}
