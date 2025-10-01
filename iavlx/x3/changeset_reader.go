package x3

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"unsafe"
)

type ChangesetReader struct {
	dir       string
	info      *ChangesetInfo
	treeStore *TreeStore

	*KVDataReader // TODO make sure we handle compaction here too
	infoReader    *StructReader[ChangesetInfo]
	branchesData  *NodeReader[BranchLayout]
	leavesData    *NodeReader[LeafLayout]
	versionsData  *StructReader[VersionInfo]

	compacted     *ChangesetReader
	refCount      atomic.Int32
	disposed      atomic.Bool
	dirtyBranches atomic.Bool
	dirtyLeaves   atomic.Bool
}

func NewChangesetReader(dir string, treeStore *TreeStore) *ChangesetReader {
	return &ChangesetReader{
		treeStore: treeStore,
		dir:       dir,
	}
}

func (cr *ChangesetReader) Open() error {
	var err error
	cr.infoReader, err = NewStructReader[ChangesetInfo](filepath.Join(cr.dir, "info.dat"))
	if err != nil {
		return fmt.Errorf("failed to open changeset info: %w", err)
	}

	if cr.infoReader.Count() == 0 {
		return fmt.Errorf("changeset info file is empty")
	}
	cr.info = cr.infoReader.UnsafeItem(0)

	cr.KVDataReader, err = NewKVDataReader(filepath.Join(cr.dir, "kv.dat"))
	if err != nil {
		return fmt.Errorf("failed to open KV data store: %w", err)
	}

	cr.leavesData, err = NewNodeReader[LeafLayout](filepath.Join(cr.dir, "leaves.dat"))
	if err != nil {
		return fmt.Errorf("failed to open leaves data file: %w", err)
	}

	cr.branchesData, err = NewNodeReader[BranchLayout](filepath.Join(cr.dir, "branches.dat"))
	if err != nil {
		return fmt.Errorf("failed to open branches data file: %w", err)
	}

	cr.versionsData, err = NewStructReader[VersionInfo](filepath.Join(cr.dir, "versions.dat"))
	if err != nil {
		return fmt.Errorf("failed to open versions data file: %w", err)
	}

	return nil
}

func (cr *ChangesetReader) getVersionInfo(version uint32) (*VersionInfo, error) {
	if version < cr.info.StartVersion || version >= cr.info.StartVersion+uint32(cr.versionsData.Count()) {
		return nil, fmt.Errorf("version %d out of range for changeset (have %d..%d)", version, cr.info.StartVersion, cr.info.StartVersion+uint32(cr.versionsData.Count())-1)
	}
	return cr.versionsData.UnsafeItem(version - cr.info.StartVersion), nil
}

func (cr *ChangesetReader) ResolveLeaf(nodeId NodeID, fileIdx uint32) (LeafLayout, error) {
	if compacted := cr.compacted; compacted != nil {
		cr.tryDispose()
		return compacted.ResolveLeaf(nodeId, fileIdx)
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

func (cr *ChangesetReader) ResolveBranch(nodeId NodeID, fileIdx uint32) (BranchLayout, error) {
	if compacted := cr.compacted; compacted != nil {
		cr.tryDispose()
		return compacted.ResolveBranch(nodeId, fileIdx)
	}

	layout, _, err := cr.resolveBranchWithIdx(nodeId, fileIdx)
	return layout, err
}

func (cr *ChangesetReader) resolveBranchWithIdx(nodeId NodeID, fileIdx uint32) (BranchLayout, uint32, error) {
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

func (cr *ChangesetReader) ResolveNodeRef(nodeRef NodeRef, selfIdx uint32) *NodePointer {
	if compacted := cr.compacted; compacted != nil {
		cr.tryDispose()
		return compacted.ResolveNodeRef(nodeRef, selfIdx)
	}
	cr.Pin()
	defer cr.Unpin()

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

func (cr *ChangesetReader) Resolve(nodeId NodeID, fileIdx uint32) (Node, error) {
	if compacted := cr.compacted; compacted != nil {
		cr.tryDispose()
		return compacted.Resolve(nodeId, fileIdx)
	}

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
		return &BranchPersisted{layout: layout, store: cr, selfIdx: actualIdx}, nil
	}
}

func (cr *ChangesetReader) MarkOrphan(version uint32, nodeId NodeID) error {
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

func (cr *ChangesetReader) Close() error {
	return errors.Join(
		cr.KVDataReader.Close(),
		cr.leavesData.Close(),
		cr.branchesData.Close(),
		cr.versionsData.Close(),
		cr.infoReader.Close(),
	)
}

func (cr *ChangesetReader) Pin() {
	cr.refCount.Add(1)
}

func (cr *ChangesetReader) Unpin() {
	cr.refCount.Add(-1)
}

func (cr *ChangesetReader) tryDispose() {
	if cr.disposed.Load() {
		return
	}
	if cr.refCount.Load() <= 0 {
		if cr.disposed.CompareAndSwap(false, true) {
			// TODO delete all files and close everything
		}
	}
}

var _ NodeStore = (*ChangesetReader)(nil)
