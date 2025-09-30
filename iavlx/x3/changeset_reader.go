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

func (cr *ChangesetReader) ResolveLeaf(nodeId NodeID, fileIdx uint32) (LeafLayout, error) {
	if fileIdx == 0 {
		return LeafLayout{}, fmt.Errorf("NodeID resolution not implemented for leaves")
	} else {
		fileIdx-- // convert to 0-based index
		return *cr.leavesData.UnsafeItem(fileIdx), nil
	}
}

func (cr *ChangesetReader) ResolveBranch(nodeId NodeID, fileIdx uint32) (BranchLayout, error) {
	if fileIdx == 0 {
		return BranchLayout{}, fmt.Errorf("NodeID resolution not implemented for branches")
	} else {
		fileIdx-- // convert to 0-based index
		return *cr.branchesData.UnsafeItem(fileIdx), nil
	}
}

func (cr *ChangesetReader) ResolveNodeRef(nodeRef NodeRef, selfIdx uint32) *NodePointer {
	if nodeRef.IsNodeID() {
		// Cross-changeset reference - use TreeStore to route to correct changeset
		return &NodePointer{
			id:    nodeRef.AsNodeID(),
			store: cr.treeStore,
		}
	}
	offset := nodeRef.AsRelativePointer().Offset()
	if nodeRef.IsLeaf() {
		if offset < 1 {
			// TODO: return error instead?
			return nil
		}
		layout := cr.leavesData.UnsafeItem(uint32(offset - 1)) // convert to 0-based index
		return &NodePointer{
			id:      layout.id,
			store:   cr,
			fileIdx: uint32(offset),
		}
	} else {
		idx := int64(selfIdx) + offset
		if idx < 1 {
			// TODO: return error instead?
			return nil
		}
		layout := cr.branchesData.UnsafeItem(uint32(idx - 1)) // convert to 0-based index
		return &NodePointer{
			id:      layout.id,
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
