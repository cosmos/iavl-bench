package x3

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
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

	syncQueue chan *Changeset
	syncDone  chan error

	cleanupProc *cleanupProc
}

type markOrphansReq struct {
	version uint32
	orphans [][]NodeID
}

type deleteInfo struct {
	retainKvlogPath string
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

	ts.cleanupProc = newCleanupProc(ts)

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
	ts.cleanupProc.markOrphans(version, nodeIds)
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

func (ts *TreeStore) Close() error {
	ts.cleanupProc.shutdown()

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

type cleanupProc struct {
	*TreeStore
	closeCleanupProc chan struct{}
	cleanupProcDone  chan struct{}
	orphanWriteQueue []markOrphansReq
	orphanQueueLock  sync.Mutex
	toDelete         map[*Changeset]deleteInfo
	activeCompactor  *Compactor
	beingCompacted   []compactionEntry
}

type compactionEntry struct {
	entry *changesetEntry
	cs    *Changeset
}

func newCleanupProc(treeStore *TreeStore) *cleanupProc {
	cp := &cleanupProc{
		TreeStore:        treeStore,
		closeCleanupProc: make(chan struct{}),
		cleanupProcDone:  make(chan struct{}),
		toDelete:         make(map[*Changeset]deleteInfo),
	}
	go cp.run()
	return cp
}

func (cp *cleanupProc) run() {
	defer close(cp.cleanupProcDone)
	minCompactorInterval := time.Second * time.Duration(cp.opts.MinCompactionSeconds)
	var lastCompactorStart time.Time

	for {
		sleepTime := time.Duration(0)
		if time.Since(lastCompactorStart) < minCompactorInterval {
			sleepTime = minCompactorInterval - time.Since(lastCompactorStart)
		}
		select {
		case <-cp.closeCleanupProc:
			return
		case <-time.After(sleepTime):
		}

		lastCompactorStart = time.Now()

		// process any pending orphans at the start of each cycle
		err := cp.doMarkOrphans()
		if err != nil {
			cp.logger.Error("failed to mark orphans at start of cycle", "error", err)
		}

		// collect current entries
		cp.changesetsMapLock.RLock()
		var entries []*changesetEntry
		cp.changesets.Scan(func(version uint32, entry *changesetEntry) bool {
			entries = append(entries, entry)
			return true
		})
		cp.changesetsMapLock.RUnlock()

		for i := 0; i < len(entries); i++ {
			entry := entries[i]
			var nextEntry *changesetEntry
			if i+1 < len(entries) {
				nextEntry = entries[i+1]
			}
			err := cp.processEntry(entry, nextEntry)
			if err != nil {
				cp.logger.Error("failed to process changeset entry", "error", err)
				// on error, clean up any failed compaction and stop processing further entries this round
				cp.cleanupFailedCompaction()
				break
			}
		}
		if cp.activeCompactor != nil {
			err := cp.sealActiveCompactor()
			if err != nil {
				cp.logger.Error("failed to seal active compactor", "error", err)
			}
		}

		cp.processToDelete()
	}
}

func (cp *cleanupProc) markOrphans(version uint32, nodeIds [][]NodeID) {
	req := markOrphansReq{
		version: version,
		orphans: nodeIds,
	}

	cp.orphanQueueLock.Lock()
	defer cp.orphanQueueLock.Unlock()

	cp.orphanWriteQueue = append(cp.orphanWriteQueue, req)
}

// doMarkOrphans must only be called from the cleanupProc
func (cp *cleanupProc) doMarkOrphans() error {
	var orphanQueue []markOrphansReq
	cp.orphanQueueLock.Lock()
	orphanQueue, cp.orphanWriteQueue = cp.orphanWriteQueue, nil
	cp.orphanQueueLock.Unlock()

	for _, req := range orphanQueue {
		for _, nodeSet := range req.orphans {
			for _, nodeId := range nodeSet {
				ce := cp.getChangesetEntryForVersion(uint32(nodeId.Version()))
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

func (cp *cleanupProc) processEntry(entry, nextEntry *changesetEntry) error {
	cs := entry.changeset.Load()

	// safety check - skip if evicted or disposed
	if cs.evicted.Load() || cs.disposed.Load() {
		return fmt.Errorf("evicted/disposed changeset: %s found in queue", cs.dir)
	}

	// safety check - ensure info is valid
	if cs.info == nil {
		return fmt.Errorf("changeset has nil info: %s found in queue", cs.dir)
	}

	err := cs.FlushOrphans()
	if err != nil {
		return fmt.Errorf("failed to flush orphans for changeset %s: %w", cs.dir, err)
	}

	if cp.opts.DisableCompaction {
		return nil
	}

	// skip if still pending sync
	if cs.needsSync.Load() {
		return nil
	}

	if cp.activeCompactor != nil {
		if cp.opts.CompactWAL &&
			cs.TotalBytes()+cp.activeCompactor.TotalBytes() <= int(cp.opts.ChangesetMaxTarget) {
			// add to active compactor
			err = cp.activeCompactor.AddChangeset(cs)
			if err != nil {
				return fmt.Errorf("failed to add changeset to active compactor: %w", err)
			}
			cp.beingCompacted = append(cp.beingCompacted, compactionEntry{entry: entry, cs: cs})
		} else {
			err = cp.sealActiveCompactor()
			if err != nil {
				return fmt.Errorf("failed to seal active compactor: %w", err)
			}
		}
	}

	// mark any pending orphans here when we don't have an active compactor
	err = cp.doMarkOrphans()
	if err != nil {
		cp.logger.Error("failed to mark orphans", "error", err)
	}

	// check if other triggers apply for a new compaction
	savedVersion := cp.savedVersion.Load()
	retainVersions := cp.opts.RetainVersions
	retentionWindowBottom := savedVersion - retainVersions

	// Skip changesets within retention window
	if cs.info.EndVersion >= retentionWindowBottom {
		return nil
	}

	compactOrphanAge := cp.opts.CompactionOrphanAge
	if compactOrphanAge == 0 {
		compactOrphanAge = 10
	}
	compactOrphanThreshold := cp.opts.CompactionOrphanRatio
	if compactOrphanThreshold <= 0 {
		compactOrphanThreshold = 0.6
	}

	// Age target relative to bottom of retention window
	ageTarget := retentionWindowBottom - compactOrphanAge

	// Check orphan-based trigger
	shouldCompact := cs.ReadyToCompact(compactOrphanThreshold, ageTarget)

	// Check size-based joining trigger
	maxSize := uint64(cp.opts.ChangesetMaxTarget)
	if maxSize == 0 {
		maxSize = 1024 * 1024 * 1024 // 1GB default
	}

	canJoin := false
	if !shouldCompact && nextEntry != nil && cp.opts.CompactWAL {
		nextCs := nextEntry.changeset.Load()
		if nextCs.info.StartVersion == cs.info.EndVersion+1 {
			if uint64(cs.TotalBytes())+uint64(nextCs.TotalBytes()) <= maxSize {
				canJoin = true
			}
		}
	}

	if !shouldCompact && !canJoin {
		return nil
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

	cp.logger.Info("compacting changeset", "info", cs.info, "size", cs.TotalBytes())

	cp.activeCompactor, err = NewCompacter(cp.logger, cs, CompactOptions{
		RetainCriteria: retainCriteria,
		CompactWAL:     cp.opts.CompactWAL,
	}, cp.TreeStore)
	if err != nil {
		return fmt.Errorf("failed to create compactor: %w", err)
	}
	cp.beingCompacted = []compactionEntry{{entry: entry, cs: cs}}
	return nil
}

func (cp *cleanupProc) sealActiveCompactor() error {
	// seal compactor and finish
	newCs, err := cp.activeCompactor.Seal()
	if err != nil {
		cp.cleanupFailedCompaction()
		return fmt.Errorf("failed to seal active compactor: %w", err)
	}

	// update all processed entries to point to new changeset
	oldSize := uint64(0)
	for _, procEntry := range cp.beingCompacted {
		oldCs := procEntry.cs
		oldSize += uint64(oldCs.TotalBytes())

		procEntry.entry.changeset.Store(newCs)
		oldCs.Evict()

		// try to delete now or schedule for later
		if !oldCs.TryDispose() {
			cp.toDelete[oldCs] = deleteInfo{newCs.kvlogPath}
		} else {
			cp.logger.Info("changeset disposed, deleting files", "path", oldCs.dir)
			err = oldCs.DeleteFiles(newCs.kvlogPath)
			if err != nil {
				cp.logger.Error("failed to delete old changeset files", "error", err, "path", oldCs.dir)
			}
		}
	}

	cp.logger.Info("compacted changeset", "dir", newCs.dir, "new_size", newCs.TotalBytes(), "old_size", oldSize, "joined", len(cp.beingCompacted))

	// Clear compactor state after successful seal
	cp.cleanupActiveCompactor()
	return nil
}

func (cp *cleanupProc) cleanupActiveCompactor() {
	cp.activeCompactor = nil
	cp.beingCompacted = nil
}

func (cp *cleanupProc) cleanupFailedCompaction() {
	// Clean up any partial compactor state and remove temporary files
	if cp.activeCompactor != nil && cp.activeCompactor.dir != "" {
		cp.logger.Warn("cleaning up failed compaction", "dir", cp.activeCompactor.dir, "changesets_attempted", len(cp.beingCompacted))
		err := os.RemoveAll(cp.activeCompactor.dir)
		if err != nil {
			cp.logger.Error("failed to remove compactor directory", "error", err, "dir", cp.activeCompactor.dir)
		}
	}
	cp.cleanupActiveCompactor()
}

func (cp *cleanupProc) processToDelete() {
	for oldCs, info := range cp.toDelete {
		select {
		case <-cp.closeCleanupProc:
			return
		default:
		}

		if !oldCs.TryDispose() {
			cp.logger.Warn("old changeset not disposed, skipping delete", "path", oldCs.dir)
			continue
		}

		cp.logger.Info("deleting old changeset files", "path", oldCs.dir)
		err := oldCs.DeleteFiles(info.retainKvlogPath)
		if err != nil {
			cp.logger.Error("failed to delete old changeset files", "error", err)
		}
		delete(cp.toDelete, oldCs)
	}
}

func (cp *cleanupProc) shutdown() {
	close(cp.closeCleanupProc)
	<-cp.cleanupProcDone
}
