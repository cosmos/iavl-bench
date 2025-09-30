package x3

import (
	"errors"
	"fmt"
	"path/filepath"
)

type ChangesetReader struct {
	info ChangesetInfo

	treeStore *TreeStore
	dir       string
	*KVDataReader
	branchesData *NodeReader[BranchLayout]
	leavesData   *NodeReader[LeafLayout]
	versionsData *StructReader[VersionInfo]
}

func NewChangesetReader(dir string, treeStore *TreeStore) *ChangesetReader {
	return &ChangesetReader{
		treeStore: treeStore,
		dir:       dir,
	}
}

func (cr *ChangesetReader) Open() error {
	infoReader, err := NewStructReader[ChangesetInfo](filepath.Join(cr.dir, "info.dat"))
	if err != nil {
		return fmt.Errorf("failed to open changeset info: %w", err)
	}
	defer infoReader.Close()

	if infoReader.Count() == 0 {
		return fmt.Errorf("changeset info file is empty")
	}
	cr.info = *infoReader.UnsafeItem(0)

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
	if fileIdx == 0 {
		version := uint32(nodeId.Version())
		vi, err := cr.getVersionInfo(version)
		if err != nil {
			return BranchLayout{}, err
		}
		branch, err := cr.branchesData.FindByID(nodeId, &vi.Branches)
		if err != nil {
			return BranchLayout{}, err
		}
		return *branch, nil
	} else {
		fileIdx-- // convert to 0-based index
		return *cr.branchesData.UnsafeItem(fileIdx), nil
	}
}

func (cr *ChangesetReader) ResolveNodeRef(nodeRef NodeRef, selfIdx uint32) *NodePointer {
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
	if nodeId.IsLeaf() {
		layout, err := cr.ResolveLeaf(nodeId, fileIdx)
		if err != nil {
			return nil, err
		}
		return &LeafPersisted{layout: layout, store: cr}, nil
	} else {
		layout, err := cr.ResolveBranch(nodeId, fileIdx)
		if err != nil {
			return nil, err
		}
		return &BranchPersisted{layout: layout, store: cr, selfIdx: fileIdx}, nil
	}
}

func (cr *ChangesetReader) Close() error {
	return errors.Join(
		cr.KVDataReader.Close(),
		cr.leavesData.Close(),
		cr.branchesData.Close(),
		cr.versionsData.Close(),
	)
}

var _ NodeStore = (*ChangesetReader)(nil)
