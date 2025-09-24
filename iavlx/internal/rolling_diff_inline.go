package internal

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync/atomic"
)

// RollingDiffInline implements NodeStore with inline node storage
// All nodes are stored in a single file with keys/values inline
// Spans and offsets are measured in bytes (not node counts)
type RollingDiffInline struct {
	*NodeStoreInline
	nodesData          *MmapFile // Direct reference to the nodes file
	stagedVersion      uint64
	savedVersion       atomic.Uint64
	currentOffset      uint64 // Current byte offset in nodes file
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

	// Get current file size as starting offset
	nodesData.flushLock.RLock()
	currentSize := uint64(len(nodesData.handle))
	nodesData.flushLock.RUnlock()

	rd := &RollingDiffInline{
		NodeStoreInline:    nodeStore,
		nodesData:          nodesData,
		stagedVersion:      startVersion + 1,
		currentOffset:      currentSize,
		versionStartOffset: currentSize,
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
		err = rd.nodesData.SaveAndRemap()
		if err != nil {
			return fmt.Errorf("failed to save nodes data: %w", err)
		}
	}

	// Update version tracking
	rd.savedVersion.Store(rd.stagedVersion)
	rd.stagedVersion++
	rd.versionStartOffset = rd.currentOffset

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
	startOffset := rd.currentOffset

	// Create a buffer to write to
	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, node, nodeId)
	if err != nil {
		return 0, err
	}

	// Write to the nodes file
	n, err := rd.nodesData.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}
	bytesWritten := uint64(n)

	// Update tracking
	rd.currentOffset += bytesWritten
	np.fileIdx = startOffset + 1 // fileIdx is 1-based (0 means unresolved)
	np.store = rd

	return bytesWritten, nil
}

// writeBranch writes a branch node and returns bytes written
func (rd *RollingDiffInline) writeBranch(np *NodePointer, node *MemNode) (uint64, error) {
	nodeId := np.id

	// Track where children are written
	var leftOffset, rightOffset uint64
	var leftID, rightID NodeID

	// Write left child first (post-order traversal)
	leftStartOffset := rd.currentOffset
	leftBytes, err := rd.writeNode(node.left)
	if err != nil {
		return 0, err
	}
	if leftBytes > 0 {
		leftOffset = leftStartOffset // Absolute byte offset where left child starts
	}
	leftID = node.left.id

	// Write right child
	rightStartOffset := rd.currentOffset
	rightBytes, err := rd.writeNode(node.right)
	if err != nil {
		return 0, err
	}
	if rightBytes > 0 {
		rightOffset = rightStartOffset // Absolute byte offset where right child starts
	}
	rightID = node.right.id

	// Now write the branch node itself
	branchStartOffset := rd.currentOffset

	// Calculate size (number of nodes in subtree - same as original)
	// This is based on NodeID indexes, not bytes
	size := uint64(node.size)

	// Calculate span in BYTES (total bytes of this subtree)
	// This will be: left subtree bytes + right subtree bytes + this branch node bytes
	// We'll calculate the branch node size first
	var buf bytes.Buffer
	tempSpan := uint64(0) // Temporary value, will update after we know branch size

	err = encodeBranchNodeInline(&buf, node, nodeId, leftOffset, rightOffset,
		leftID, rightID, size, tempSpan)
	if err != nil {
		return 0, err
	}
	branchBytes := uint64(buf.Len())

	// Now calculate actual span
	span := leftBytes + rightBytes + branchBytes

	// Re-encode with correct span
	buf.Reset()
	err = encodeBranchNodeInline(&buf, node, nodeId, leftOffset, rightOffset,
		leftID, rightID, size, span)
	if err != nil {
		return 0, err
	}

	// Write to file
	n, err := rd.nodesData.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}
	bytesWritten := uint64(n)

	// Update tracking
	rd.currentOffset += bytesWritten
	np.fileIdx = branchStartOffset + 1 // fileIdx is 1-based
	np.store = rd

	return span, nil
}

var _ NodeStore = (*RollingDiffInline)(nil)
