package x3

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"unsafe"
)

type Changeset struct {
	info *ChangesetInfo

	dir       string
	kvlogPath string

	treeStore *TreeStore

	kvlogReader  *KVLog // TODO make sure we handle compaction here too
	infoReader   *StructReader[ChangesetInfo]
	branchesData *NodeReader[BranchLayout]
	leavesData   *NodeReader[LeafLayout]
	versionsData *StructReader[VersionInfo]

	refCount      atomic.Int32
	evicted       atomic.Bool
	disposed      atomic.Bool
	dirtyBranches atomic.Bool
	dirtyLeaves   atomic.Bool
}

func NewChangeset(dir, kvlogPath string, treeStore *TreeStore) *Changeset {
	return &Changeset{
		treeStore: treeStore,
		dir:       dir,
		kvlogPath: kvlogPath,
	}
}

func (cr *Changeset) Init(
	infoFile *os.File,
	kvDataFile *os.File,
	leavesDataFile *os.File,
	branchesDataFile *os.File,
	versionsDataFile *os.File,
) error {
	var err error
	cr.infoReader, err = NewStructReader[ChangesetInfo](infoFile)
	if err != nil {
		return fmt.Errorf("failed to open changeset info: %w", err)
	}

	if cr.infoReader.Count() == 0 {
		return fmt.Errorf("changeset info file is empty")
	}
	cr.info = cr.infoReader.UnsafeItem(0)

	cr.kvlogReader, err = NewKVLog(kvDataFile)
	if err != nil {
		return fmt.Errorf("failed to open KV data store: %w", err)
	}

	cr.leavesData, err = NewNodeReader[LeafLayout](leavesDataFile)
	if err != nil {
		return fmt.Errorf("failed to open leaves data file: %w", err)
	}

	cr.branchesData, err = NewNodeReader[BranchLayout](branchesDataFile)
	if err != nil {
		return fmt.Errorf("failed to open branches data file: %w", err)
	}

	cr.versionsData, err = NewStructReader[VersionInfo](versionsDataFile)
	if err != nil {
		return fmt.Errorf("failed to open versions data file: %w", err)
	}

	return nil
}

func (cr *Changeset) getVersionInfo(version uint32) (*VersionInfo, error) {
	if version < cr.info.StartVersion || version >= cr.info.StartVersion+uint32(cr.versionsData.Count()) {
		return nil, fmt.Errorf("version %d out of range for changeset (have %d..%d)", version, cr.info.StartVersion, cr.info.StartVersion+uint32(cr.versionsData.Count())-1)
	}
	return cr.versionsData.UnsafeItem(version - cr.info.StartVersion), nil
}

func (cr *Changeset) ReadK(nodeId NodeID, offset uint32) (key []byte, err error) {
	if cr.evicted.Load() {
		cr.tryDispose()
		return cr.treeStore.ReadK(nodeId, offset)
	}
	cr.Pin()
	defer cr.Unpin()

	k, err := cr.kvlogReader.UnsafeReadK(offset)
	if err != nil {
		return nil, err
	}
	copyKey := make([]byte, len(k))
	copy(copyKey, k)
	return copyKey, nil
}

func (cr *Changeset) ReadKV(nodeId NodeID, offset uint32) (key, value []byte, err error) {
	if cr.evicted.Load() {
		cr.tryDispose()
		return cr.treeStore.ReadKV(nodeId, offset)
	}
	cr.Pin()
	defer cr.Unpin()

	// TODO add an optimization when we only want to read and copy value
	k, v, err := cr.kvlogReader.ReadKV(offset)
	if err != nil {
		return nil, nil, err
	}
	copyKey := make([]byte, len(k))
	copy(copyKey, k)
	copyValue := make([]byte, len(v))
	copy(copyValue, v)
	return copyKey, copyValue, nil
}

func (cr *Changeset) ResolveLeaf(nodeId NodeID, fileIdx uint32) (LeafLayout, error) {
	if cr.evicted.Load() {
		cr.tryDispose()
		return cr.treeStore.ResolveLeaf(nodeId)
	}
	cr.Pin()
	defer cr.Unpin()

	if fileIdx == 0 {
		version := uint32(nodeId.Version())
		vi, err := cr.getVersionInfo(version)
		if err != nil {
			return LeafLayout{}, err
		}
		leaf, err := cr.leavesData.FindByID(nodeId, &vi.Leaves)
		if err != nil {
			return LeafLayout{}, err
		}
		return *leaf, nil
	} else {
		fileIdx-- // convert to 0-based index
		return *cr.leavesData.UnsafeItem(fileIdx), nil
	}
}

func (cr *Changeset) ResolveBranch(nodeId NodeID, fileIdx uint32) (BranchLayout, error) {
	if cr.evicted.Load() {
		cr.tryDispose()
		return cr.treeStore.ResolveBranch(nodeId)
	}

	layout, _, err := cr.resolveBranchWithIdx(nodeId, fileIdx)
	return layout, err
}

func (cr *Changeset) resolveBranchWithIdx(nodeId NodeID, fileIdx uint32) (BranchLayout, uint32, error) {
	cr.Pin()
	defer cr.Unpin()

	if fileIdx == 0 {
		version := uint32(nodeId.Version())
		vi, err := cr.getVersionInfo(version)
		if err != nil {
			return BranchLayout{}, 0, err
		}
		branch, err := cr.branchesData.FindByID(nodeId, &vi.Branches)
		if err != nil {
			return BranchLayout{}, 0, err
		}
		// Compute the actual file index from the pointer
		itemIdx := uint32((uintptr(unsafe.Pointer(branch)) - uintptr(unsafe.Pointer(&cr.branchesData.items[0]))) / uintptr(cr.branchesData.size))
		return *branch, itemIdx + 1, nil // +1 to convert back to 1-based
	} else {
		itemIdx := fileIdx - 1                                    // convert to 0-based index
		return *cr.branchesData.UnsafeItem(itemIdx), fileIdx, nil // return original fileIdx
	}
}

func (cr *Changeset) resolveNodeRef(nodeRef NodeRef, selfIdx uint32) *NodePointer {
	if nodeRef.IsNodeID() {
		id := nodeRef.AsNodeID()
		return &NodePointer{
			id:    id,
			store: cr.treeStore.getChangesetForVersion(uint32(id.Version())),
		}
	}
	relPtr := nodeRef.AsRelativePointer()
	offset := relPtr.Offset()
	if nodeRef.IsLeaf() {
		if offset < 1 {
			panic(fmt.Sprintf("invalid leaf offset: %d", offset))
		}
		itemIdx := uint32(offset - 1)
		if itemIdx >= uint32(cr.leavesData.Count()) {
			panic(fmt.Sprintf("leaf offset %d out of bounds (have %d leaves)", offset, cr.leavesData.Count()))
		}
		layout := cr.leavesData.UnsafeItem(itemIdx)
		return &NodePointer{
			id:      layout.Id,
			store:   cr,
			fileIdx: uint32(offset),
		}
	} else {
		idx := int64(selfIdx) + offset
		if idx < 1 {
			panic(fmt.Sprintf("invalid branch index: %d (selfIdx=%d, offset=%d)", idx, selfIdx, offset))
		}
		itemIdx := uint32(idx - 1)
		if itemIdx >= uint32(cr.branchesData.Count()) {
			panic(fmt.Sprintf("branch index %d out of bounds (have %d branches)", idx, cr.branchesData.Count()))
		}
		layout := cr.branchesData.UnsafeItem(itemIdx)
		return &NodePointer{
			id:      layout.Id,
			store:   cr,
			fileIdx: uint32(idx),
		}
	}
}

func (cr *Changeset) Resolve(nodeId NodeID, fileIdx uint32) (Node, error) {
	if cr.evicted.Load() {
		cr.tryDispose()
		return cr.treeStore.Resolve(nodeId, fileIdx)
	}
	cr.Pin()
	defer cr.Unpin()

	if nodeId.IsLeaf() {
		layout, err := cr.ResolveLeaf(nodeId, fileIdx)
		if err != nil {
			return nil, err
		}
		return &LeafPersisted{layout: layout, store: cr}, nil
	} else {
		layout, actualIdx, err := cr.resolveBranchWithIdx(nodeId, fileIdx)
		if err != nil {
			return nil, err
		}

		leftPtr := cr.resolveNodeRef(layout.Left, actualIdx)
		rightPtr := cr.resolveNodeRef(layout.Right, actualIdx)

		return &BranchPersisted{
			layout:   layout,
			store:    cr,
			selfIdx:  actualIdx,
			leftPtr:  leftPtr,
			rightPtr: rightPtr,
		}, nil
	}
}

func (cr *Changeset) MarkOrphan(version uint32, nodeId NodeID) error {
	cr.Pin()
	defer cr.Unpin()

	nodeVersion := uint32(nodeId.Version())
	vi, err := cr.getVersionInfo(nodeVersion)
	if err != nil {
		return err
	}

	if nodeId.IsLeaf() {
		leaf, err := cr.leavesData.FindByID(nodeId, &vi.Leaves)
		if err != nil {
			return err
		}

		if leaf.OrphanVersion == 0 {
			leaf.OrphanVersion = version
			cr.info.LeafOrphans++
			cr.info.LeafOrphanVersionTotal += uint64(version)
			cr.dirtyLeaves.Store(true)
		}
	} else {
		branch, err := cr.branchesData.FindByID(nodeId, &vi.Branches)
		if err != nil {
			return err
		}

		if branch.OrphanVersion == 0 {
			branch.OrphanVersion = version
			cr.info.BranchOrphans++
			cr.info.BranchOrphanVersionTotal += uint64(version)
			cr.dirtyBranches.Store(true)
		}
	}

	return nil
}

func (cr *Changeset) ReadyToCompact(orphanPercentTarget, orphanAgeTarget float64) bool {
	leafOrphanCount := float64(cr.info.LeafOrphans)
	leafOrphanPercent := leafOrphanCount / float64(cr.leavesData.Count())
	leafOrphanAge := float64(cr.info.LeafOrphanVersionTotal) / leafOrphanCount

	if leafOrphanPercent >= orphanPercentTarget && leafOrphanAge <= orphanAgeTarget {
		return true
	}

	branchOrphanCount := float64(cr.info.BranchOrphans)
	branchOrphanPercent := branchOrphanCount / float64(cr.branchesData.Count())
	branchOrphanAge := float64(cr.info.BranchOrphanVersionTotal) / branchOrphanCount
	if branchOrphanPercent >= orphanPercentTarget && branchOrphanAge <= orphanAgeTarget {
		return true
	}

	return false
}

func (cr *Changeset) FlushOrphans() error {
	cr.Pin()
	defer cr.Unpin()

	if cr.dirtyLeaves.Load() {
		err := cr.leavesData.Flush()
		if err != nil {
			return fmt.Errorf("failed to flush leaf data: %w", err)
		}
	}
	if cr.dirtyBranches.Load() {
		err := cr.branchesData.Flush()
		if err != nil {
			return fmt.Errorf("failed to flush branch data: %w", err)
		}
	}
	return nil
}

func (cr *Changeset) Close() error {
	return errors.Join(
		cr.kvlogReader.Close(),
		cr.leavesData.Close(),
		cr.branchesData.Close(),
		cr.versionsData.Close(),
		cr.infoReader.Close(),
	)
}

func (cr *Changeset) Pin() {
	cr.refCount.Add(1)
}

func (cr *Changeset) Unpin() {
	cr.refCount.Add(-1)
}

func (cr *Changeset) Evict() {
	cr.evicted.Store(true)
}

func (cr *Changeset) tryDispose() {
	if cr.disposed.Load() {
		return
	}
	if cr.refCount.Load() <= 0 {
		if cr.disposed.CompareAndSwap(false, true) {
			_ = cr.Close()
			cr.versionsData = nil
			cr.branchesData = nil
			cr.leavesData = nil
			cr.kvlogReader = nil
			cr.infoReader = nil
		}
	}
}

func (cr *Changeset) IsDisposed() bool {
	return cr.disposed.Load()
}

func (cr *Changeset) DeleteFiles(saveKVLogPath string) error {
	var errs []error
	if cr.kvlogPath != saveKVLogPath {
		errs = append(errs, os.Remove(cr.kvlogPath))
	}
	errs = append(errs, os.Remove(filepath.Join(cr.dir, "leaves.dat")))
	errs = append(errs, os.Remove(filepath.Join(cr.dir, "branches.dat")))
	errs = append(errs, os.Remove(filepath.Join(cr.dir, "versions.dat")))
	errs = append(errs, os.Remove(filepath.Join(cr.dir, "info.dat")))
	return errors.Join(errs...)
}

func (cr *Changeset) TotalBytes() any {
	return cr.leavesData.TotalBytes() +
		cr.branchesData.TotalBytes() +
		cr.kvlogReader.TotalBytes() +
		cr.versionsData.TotalBytes()
}
