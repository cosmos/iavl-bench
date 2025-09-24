package internal

import (
	"encoding/binary"
	"fmt"
)

// NodeStoreInline implements NodeStore for inline node storage
// All nodes are stored in a single file with keys/values inline
type NodeStoreInline struct {
	nodesFile *MmapFile
}

// NewNodeStoreInline creates a new inline node store
func NewNodeStoreInline(nodesFile *MmapFile) *NodeStoreInline {
	return &NodeStoreInline{
		nodesFile: nodesFile,
	}
}

// ResolveNode resolves a node by file index (byte offset)
// NodeID resolution (fileIdx == 0) is not supported
func (ns *NodeStoreInline) ResolveNode(nodeId NodeID, fileIdx uint64) (Node, error) {
	if fileIdx == 0 {
		return nil, fmt.Errorf("NodeID resolution not supported in inline mode, only file indexes")
	}

	// Convert to 0-based offset
	offset := fileIdx - 1

	// Ensure we can at least read the NodeID
	ns.nodesFile.flushLock.RLock()
	defer ns.nodesFile.flushLock.RUnlock()

	if offset+8 > uint64(len(ns.nodesFile.handle)) {
		return nil, fmt.Errorf("cannot read NodeID at offset %d, file size %d", offset, len(ns.nodesFile.handle))
	}

	// Read the NodeID to determine node type
	readNodeId := NodeID(binary.LittleEndian.Uint64(ns.nodesFile.handle[offset : offset+8]))

	// Verify the NodeID matches if provided (non-zero)
	if nodeId != 0 && readNodeId != nodeId {
		return nil, fmt.Errorf("NodeID mismatch at offset %d: expected %s, got %s", offset, nodeId, readNodeId)
	}

	if readNodeId.IsLeaf() {
		return ns.resolveLeaf(offset)
	} else {
		return ns.resolveBranch(offset)
	}
}

// resolveLeaf reads an inline leaf node at the given byte offset
func (ns *NodeStoreInline) resolveLeaf(offset uint64) (Node, error) {
	// Read fixed header first to determine sizes
	if offset+SizeLeafInlineFixed > uint64(len(ns.nodesFile.handle)) {
		return nil, fmt.Errorf("insufficient data for leaf header at offset %d", offset)
	}

	// Create temporary layout to read sizes
	tempLayout := LeafLayoutInline{data: ns.nodesFile.handle[offset : offset+SizeLeafInlineFixed]}
	keyLen := tempLayout.KeyLen()
	valueLen := tempLayout.ValueLen()

	// Calculate total size including variable data
	totalSize := SizeLeafInlineFixed + keyLen + valueLen
	if offset+uint64(totalSize) > uint64(len(ns.nodesFile.handle)) {
		return nil, fmt.Errorf("insufficient data for full leaf at offset %d (needs %d bytes)", offset, totalSize)
	}

	// Create the full layout with complete data
	layout := LeafLayoutInline{data: ns.nodesFile.handle[offset : offset+uint64(totalSize)]}
	return LeafPersistedInline{LeafLayoutInline: layout}, nil
}

// resolveBranch reads an inline branch node at the given byte offset
func (ns *NodeStoreInline) resolveBranch(offset uint64) (Node, error) {
	// Read fixed header first to determine key size
	if offset+SizeBranchInlineFixed > uint64(len(ns.nodesFile.handle)) {
		return nil, fmt.Errorf("insufficient data for branch header at offset %d", offset)
	}

	// Create temporary layout to read key length
	tempLayout := BranchLayoutInline{data: ns.nodesFile.handle[offset : offset+SizeBranchInlineFixed]}
	keyLen := tempLayout.KeyLen()

	// Calculate total size including variable data
	totalSize := SizeBranchInlineFixed + keyLen
	if offset+uint64(totalSize) > uint64(len(ns.nodesFile.handle)) {
		return nil, fmt.Errorf("insufficient data for full branch at offset %d (needs %d bytes)", offset, totalSize)
	}

	// Create the full layout with complete data
	layout := BranchLayoutInline{data: ns.nodesFile.handle[offset : offset+uint64(totalSize)]}
	return BranchPersistedInline{
		store:      ns,
		layout:     layout,
		selfOffset: offset,
	}, nil
}

// Read implements KVData interface - not used in inline mode
func (ns *NodeStoreInline) Read(offset uint64, size uint32) ([]byte, error) {
	return nil, fmt.Errorf("KVData.Read not supported in inline mode - keys/values are stored inline")
}

// ReadVarintBytes implements KVData interface - not used in inline mode
func (ns *NodeStoreInline) ReadVarintBytes(offset uint64) ([]byte, int, error) {
	return nil, 0, fmt.Errorf("KVData.ReadVarintBytes not supported in inline mode - values are stored inline")
}

// Verify interface compliance
var _ NodeStore = (*NodeStoreInline)(nil)
