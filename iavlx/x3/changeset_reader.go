package x3

import (
	"fmt"
	"path/filepath"
)

type ChangesetReader struct {
	info ChangesetInfo

	dir string
	*KVDataReader
	branchesData *NodeReader[BranchLayout]
	leavesData   *NodeReader[LeafLayout]
	versionsData *StructReader[VersionInfo]
}

func (cr *ChangesetReader) Open(dir string) error {
	cr.dir = dir

	infoReader, err := NewStructReader[ChangesetInfo](filepath.Join(dir, "info.dat"))
	if err != nil {
		return fmt.Errorf("failed to open changeset info: %w", err)
	}
	defer infoReader.Close()
	cr.info = *infoReader.UnsafeItem(0)

	cr.KVDataReader, err = NewKVDataReader(filepath.Join(dir, "kv.dat"))
	if err != nil {
		return fmt.Errorf("failed to open KV data store: %w", err)
	}

	cr.leavesData, err = NewNodeReader[LeafLayout](filepath.Join(dir, "leaves.dat"))
	if err != nil {
		return fmt.Errorf("failed to open leaves data file: %w", err)
	}

	cr.branchesData, err = NewNodeReader[BranchLayout](filepath.Join(dir, "branches.dat"))
	if err != nil {
		return fmt.Errorf("failed to open branches data file: %w", err)
	}

	cr.versionsData, err = NewStructReader[VersionInfo](filepath.Join(dir, "versions.dat"))
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
		return &NodePointer{
			id: nodeRef.AsNodeID(),
			// TODO should we find the actual store for this version to speed up lookups, or make that lazy?
			store: cr,
		}
	}
	offset := nodeRef.AsRelativePointer().Offset()
	if nodeRef.IsLeaf() {
		layout := cr.leavesData.UnsafeItem(uint32(offset - 1)) // convert to 0-based index
		return &NodePointer{
			id:      layout.id,
			store:   cr,
			fileIdx: uint32(offset),
		}
	} else {
		idx := int64(selfIdx) + offset
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

var _ NodeStore = (*ChangesetReader)(nil)
