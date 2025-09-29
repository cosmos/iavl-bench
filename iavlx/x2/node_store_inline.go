package x2

import (
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

	if nodeId.IsLeaf() {
		return ns.resolveLeaf(offset)
	} else {
		return ns.resolveBranch(offset)
	}
}

// resolveLeaf reads an inline leaf node at the given byte offset
func (ns *NodeStoreInline) resolveLeaf(offset uint64) (Node, error) {
	header, err := ns.nodesFile.SliceExact(int(offset), SizeLeafInlineHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to read leaf header at offset %d: %w", offset, err)
	}

	layout := LeafLayoutInline{header: header}
	keyLen := layout.KeyLength()
	valueLen := layout.ValueLength()
	dataSize := int(keyLen + valueLen)

	// Read the inline key and value data
	data, err := ns.nodesFile.SliceExact(int(offset)+SizeLeafInlineHeader, dataSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read leaf data at offset %d: %w", offset+SizeLeafInlineHeader, err)
	}

	layout.key = data[:keyLen]
	layout.value = data[keyLen:]

	return LeafPersistedInline{layout}, nil
}

// resolveBranch reads an inline branch node at the given byte offset
func (ns *NodeStoreInline) resolveBranch(offset uint64) (Node, error) {
	header, err := ns.nodesFile.SliceExact(int(offset), SizeBranchInlineHeader)
	if err != nil {
		return nil, fmt.Errorf("failed to read branch header at offset %d: %w", offset, err)
	}

	layout := BranchLayoutInline{header: header}

	keyLen := layout.KeyLength()
	key, err := ns.nodesFile.SliceExact(int(offset)+OffsetBranchInlineData, int(keyLen))
	if err != nil {
		return nil, fmt.Errorf("failed to read branch key at offset %d: %w", offset+OffsetBranchInlineData, err)
	}

	layout.key = key

	return BranchPersistedInline{
		store:      ns,
		selfOffset: offset,
		layout:     layout,
	}, nil
}

// Read implements KVData interface - not used in inline mode
func (ns *NodeStoreInline) Read(uint64, uint32) ([]byte, error) {
	return nil, fmt.Errorf("KVData.Read not supported in inline mode - keys/values are stored inline")
}

// ReadVarintBytes implements KVData interface - not used in inline mode
func (ns *NodeStoreInline) ReadVarintBytes(uint64) ([]byte, int, error) {
	return nil, 0, fmt.Errorf("KVData.ReadVarintBytes not supported in inline mode - values are stored inline")
}

// Verify interface compliance
var _ NodeStore = (*NodeStoreInline)(nil)
