package internal

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
)

// RollingDiffInline implements NodeStore with inline node storage
// All nodes are stored in a single file with keys/values inline
// Spans and offsets are measured in bytes (not node counts)
type RollingDiffInline struct {
	*NodeStoreInline
	stagedVersion      uint64
	savedVersion       atomic.Uint64
	versionStartOffset uint64 // Byte offset at start of current version
}

// NewRollingDiffInline creates a new inline rolling diff
func NewRollingDiffInline(dir string, startVersion uint64) (*RollingDiffInline, error) {
	nodesFile := filepath.Join(dir, "nodes_inline.dat")
	nodesData, err := NewMmapFile(nodesFile)
	if err != nil {
		return nil, err
	}

	nodeStore := NewNodeStoreInline(nodesData)

	rd := &RollingDiffInline{
		NodeStoreInline:    nodeStore,
		stagedVersion:      startVersion + 1,
		versionStartOffset: uint64(nodeStore.nodesFile.Offset()),
	}

	return rd, nil
}

// writeRoot writes the root node and commits the version
func (rd *RollingDiffInline) writeRoot(version uint64, root *NodePointer, lastBranchIdx uint32) error {
	if version != rd.stagedVersion {
		return fmt.Errorf("version mismatch: expected %d, got %d", rd.stagedVersion, version)
	}

	if root != nil {
		// Write the entire tree
		_, err := rd.writeNode(root)
		if err != nil {
			return err
		}

		// Save and remap the nodes file
		err = rd.nodesFile.SaveAndRemap()
		if err != nil {
			return fmt.Errorf("failed to save nodes data: %w", err)
		}
	}

	// Update version tracking
	rd.savedVersion.Store(rd.stagedVersion)
	rd.stagedVersion++
	rd.versionStartOffset = uint64(rd.nodesFile.Offset())

	return nil
}

// writeNode writes a node and returns the number of bytes written
func (rd *RollingDiffInline) writeNode(np *NodePointer) (bytesWritten uint64, err error) {
	memNode := np.mem.Load()
	if memNode == nil {
		return 0, nil // Already persisted
	}
	if memNode.version != rd.stagedVersion {
		return 0, nil // Not part of this version
	}

	if memNode.IsLeaf() {
		return rd.writeLeaf(np, memNode)
	} else {
		return rd.writeBranch(np, memNode)
	}
}

// writeLeaf writes a leaf node and returns bytes written
func (rd *RollingDiffInline) writeLeaf(np *NodePointer, node *MemNode) (uint64, error) {
	nodeId := np.id
	startOffset := uint64(rd.nodesFile.Offset())

	err := encodeLeafNodeInline(rd.nodesFile, node, nodeId)
	if err != nil {
		return 0, err
	}

	// Update tracking
	np.fileIdx = startOffset + 1 // fileIdx is 1-based (0 means unresolved)
	np.store = rd

	bytesWritten := uint64(rd.nodesFile.Offset()) - startOffset
	return bytesWritten, nil
}

// writeBranch writes a branch node and returns bytes written
func (rd *RollingDiffInline) writeBranch(np *NodePointer, node *MemNode) (uint64, error) {
	nodeId := np.id

	// Track where children are written
	var leftOffset, rightOffset uint64
	var leftID, rightID NodeID

	// Write left child first (post-order traversal)
	leftOffset = uint64(rd.nodesFile.Offset())
	leftBytes, err := rd.writeNode(node.left)
	if err != nil {
		return 0, err
	}
	leftID = node.left.id

	// Write right child
	rightOffset = uint64(rd.nodesFile.Offset())
	rightBytes, err := rd.writeNode(node.right)
	if err != nil {
		return 0, err
	}
	rightID = node.right.id

	// Now write the branch node itself
	branchStartOffset := uint64(rd.nodesFile.Offset())

	// Calculate size (number of nodes in subtree - same as original)
	// This is based on NodeID indexes, not bytes
	size := uint64(node.size)
	span := leftBytes + rightBytes

	err = encodeBranchNodeInline(rd.nodesFile, node, nodeId, leftOffset, rightOffset,
		leftID, rightID, size, span)
	if err != nil {
		return 0, err
	}

	// Update tracking
	np.fileIdx = branchStartOffset + 1 // fileIdx is 1-based
	np.store = rd

	return span, nil
}

var _ NodeStore = (*RollingDiffInline)(nil)
