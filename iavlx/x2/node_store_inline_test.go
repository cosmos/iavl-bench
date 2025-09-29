package x2

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestNodeStoreInlineResolveLeaf(t *testing.T) {
	// Create a test leaf node
	nodeID := NewNodeID(true, 1, 100)
	key := []byte("test-key")
	value := []byte("test-value-data")
	hash := sha256.Sum256(append(key, value...))

	memNode := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	// Encode to buffer
	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, memNode, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode leaf: %v", err)
	}

	// Create a mock mmap file with the data
	mmapFile := &MmapFile{
		handle: buf.Bytes(),
	}

	// Create NodeStoreInline
	store := NewNodeStoreInline(mmapFile)

	// Test resolution by file index
	node, err := store.ResolveNode(nodeID, 1) // fileIdx 1 = offset 0
	if err != nil {
		t.Fatalf("Failed to resolve node: %v", err)
	}

	// Verify it's a leaf
	if !node.IsLeaf() {
		t.Error("Expected leaf node")
	}

	// Check type assertion
	leaf, ok := node.(LeafPersistedInline)
	if !ok {
		t.Fatalf("Expected LeafPersistedInline, got %T", node)
	}

	// Verify data
	gotKey, err := leaf.Key()
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Errorf("Key mismatch: got %q, want %q", gotKey, key)
	}

	gotValue, err := leaf.Value()
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if !bytes.Equal(gotValue, value) {
		t.Errorf("Value mismatch: got %q, want %q", gotValue, value)
	}
}

func TestNodeStoreInlineResolveBranch(t *testing.T) {
	// Create a test branch node
	nodeID := NewNodeID(false, 2, 200)
	leftID := NewNodeID(true, 2, 201)
	rightID := NewNodeID(true, 2, 202)
	key := []byte("branch-key")
	height := uint8(3)
	size := uint64(1000)
	span := uint64(5000)
	hash := sha256.Sum256(key)

	memNode := &MemNode{
		key:    key,
		height: height,
		hash:   hash[:],
	}

	// Encode to buffer
	var buf bytes.Buffer
	err := encodeBranchNodeInline(&buf, memNode, nodeID, 100, 200, leftID, rightID, size, span)
	if err != nil {
		t.Fatalf("Failed to encode branch: %v", err)
	}

	// Create a mock mmap file with the data
	mmapFile := &MmapFile{
		handle: buf.Bytes(),
	}

	// Create NodeStoreInline
	store := NewNodeStoreInline(mmapFile)

	// Test resolution by file index
	node, err := store.ResolveNode(nodeID, 1) // fileIdx 1 = offset 0
	if err != nil {
		t.Fatalf("Failed to resolve node: %v", err)
	}

	// Verify it's a branch
	if node.IsLeaf() {
		t.Error("Expected branch node")
	}

	// Check type assertion
	branch, ok := node.(BranchPersistedInline)
	if !ok {
		t.Fatalf("Expected BranchPersistedInline, got %T", node)
	}

	// Verify data
	gotKey, err := branch.Key()
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Errorf("Key mismatch: got %q, want %q", gotKey, key)
	}

	if branch.Height() != height {
		t.Errorf("Height mismatch: got %d, want %d", branch.Height(), height)
	}
}

func TestNodeStoreInlineNodeIDResolution(t *testing.T) {
	mmapFile := &MmapFile{
		handle: make([]byte, 100),
	}
	store := NewNodeStoreInline(mmapFile)

	// Test that NodeID resolution (fileIdx == 0) returns error
	_, err := store.ResolveNode(NewNodeID(true, 1, 1), 0)
	if err == nil {
		t.Error("Expected error for NodeID resolution")
	}
	if err.Error() != "NodeID resolution not supported in inline mode, only file indexes" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestNodeStoreInlineKVDataMethods(t *testing.T) {
	store := &NodeStoreInline{}

	// Test Read returns error
	_, err := store.Read(0, 100)
	if err == nil {
		t.Error("Expected error from Read")
	}

	// Test ReadVarintBytes returns error
	_, _, err = store.ReadVarintBytes(0)
	if err == nil {
		t.Error("Expected error from ReadVarintBytes")
	}
}

func TestNodeStoreInlineMultipleNodes(t *testing.T) {
	var buf bytes.Buffer

	// Write multiple nodes sequentially
	// First: a leaf
	leafID := NewNodeID(true, 1, 1)
	leafHash := sha256.Sum256([]byte("leaf1value1"))
	leafNode := &MemNode{
		key:   []byte("leaf1"),
		value: []byte("value1"),
		hash:  leafHash[:],
	}
	err := encodeLeafNodeInline(&buf, leafNode, leafID)
	if err != nil {
		t.Fatalf("Failed to encode leaf: %v", err)
	}
	leafOffset := uint64(1) // fileIdx starts at 1

	leafSize := buf.Len()

	// Second: a branch
	branchID := NewNodeID(false, 1, 2)
	branchHash := sha256.Sum256([]byte("branch1"))
	branchNode := &MemNode{
		key:    []byte("branch1"),
		height: 2,
		hash:   branchHash[:],
	}
	err = encodeBranchNodeInline(&buf, branchNode, branchID, 10, 20, leafID, leafID, 100, 200)
	if err != nil {
		t.Fatalf("Failed to encode branch: %v", err)
	}
	branchOffset := uint64(leafSize + 1) // fileIdx for branch

	// Create store with the data
	mmapFile := &MmapFile{
		handle: buf.Bytes(),
	}
	store := NewNodeStoreInline(mmapFile)

	// Resolve leaf
	node1, err := store.ResolveNode(0, leafOffset)
	if err != nil {
		t.Fatalf("Failed to resolve leaf: %v", err)
	}
	if !node1.IsLeaf() {
		t.Error("Expected first node to be leaf")
	}

	// Resolve branch
	node2, err := store.ResolveNode(0, branchOffset)
	if err != nil {
		t.Fatalf("Failed to resolve branch: %v", err)
	}
	if node2.IsLeaf() {
		t.Error("Expected second node to be branch")
	}
}

func TestNodeStoreInlineOutOfBounds(t *testing.T) {
	mmapFile := &MmapFile{
		handle: make([]byte, 10), // Small file
	}
	store := NewNodeStoreInline(mmapFile)

	// Try to read beyond file bounds
	_, err := store.ResolveNode(0, 100)
	if err == nil {
		t.Error("Expected error for out of bounds access")
	}
}

func TestNodeStoreInlineNodeIDMismatch(t *testing.T) {
	// Create a leaf node
	nodeID := NewNodeID(true, 1, 100)
	wrongID := NewNodeID(true, 1, 200) // Different ID

	hash := sha256.Sum256([]byte("keyvalue"))
	memNode := &MemNode{
		key:   []byte("key"),
		value: []byte("value"),
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, memNode, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	mmapFile := &MmapFile{
		handle: buf.Bytes(),
	}
	store := NewNodeStoreInline(mmapFile)

	// Try to resolve with wrong NodeID
	_, err = store.ResolveNode(wrongID, 1)
	if err == nil {
		t.Error("Expected error for NodeID mismatch")
	}
	if err != nil && !bytes.Contains([]byte(err.Error()), []byte("NodeID mismatch")) {
		t.Errorf("Unexpected error: %v", err)
	}

	// Resolve with NodeID 0 (don't verify) should work
	_, err = store.ResolveNode(0, 1)
	if err != nil {
		t.Errorf("Failed to resolve with NodeID 0: %v", err)
	}
}
