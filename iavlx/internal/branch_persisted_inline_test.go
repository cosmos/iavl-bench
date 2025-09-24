package internal

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

// Mock NodeStore for testing
type mockNodeStore struct {
	nodes map[NodeID]Node
}

func (m *mockNodeStore) Read(offset uint64, size uint32) ([]byte, error) {
	return nil, nil
}

func (m *mockNodeStore) ReadVarintBytes(offset uint64) ([]byte, int, error) {
	return nil, 0, nil
}

func (m *mockNodeStore) ResolveNode(nodeId NodeID, fileIdx uint64) (Node, error) {
	panic("not implemented for test")
}

func TestBranchPersistedInlineNode(t *testing.T) {
	// Create test data
	nodeID := NewNodeID(false, 5, 1) // Branch node, Version 5, index 1
	leftID := NewNodeID(true, 1, 111)
	rightID := NewNodeID(true, 1, 222)
	key := []byte("branch-key")
	height := uint8(3)
	size := uint64(1000)
	span := uint64(5000)
	hash := sha256.Sum256(key)

	// Create a test MemNode and encode it
	memNode := &MemNode{
		key:    key,
		height: height,
		hash:   hash[:],
	}

	var buf bytes.Buffer
	err := encodeBranchNodeInline(&buf, memNode, nodeID, 100, 200, leftID, rightID, size, span)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// Create BranchPersistedInline
	store := &mockNodeStore{nodes: make(map[NodeID]Node)}
	node := BranchPersistedInline{
		store:      store,
		layout:     BranchLayoutInline{data: buf.Bytes()},
		selfOffset: 1000,
	}

	// Test Height
	if h := node.Height(); h != height {
		t.Errorf("Height() = %v, want %v", h, height)
	}

	// Test IsLeaf
	if node.IsLeaf() {
		t.Error("IsLeaf() = true, want false")
	}

	// Test Size
	if s := node.Size(); s != int64(size) {
		t.Errorf("Size() = %v, want %v", s, size)
	}

	// Test Version
	if v := node.Version(); v != 5 {
		t.Errorf("Version() = %v, want 5", v)
	}

	// Test Key
	gotKey, err := node.Key()
	if err != nil {
		t.Fatalf("Key() error: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Errorf("Key() = %q, want %q", gotKey, key)
	}

	// Test Value (should be nil for branch)
	gotValue, err := node.Value()
	if err != nil {
		t.Fatalf("Value() error: %v", err)
	}
	if gotValue != nil {
		t.Errorf("Value() = %q, want nil", gotValue)
	}

	// Test Left pointer
	left := node.Left()
	if left == nil {
		t.Fatal("Left() returned nil")
	}
	if left.id != leftID {
		t.Errorf("Left().id = %v, want %v", left.id, leftID)
	}
	if left.fileIdx != 1100 { // selfOffset(1000) + leftOffset(100)
		t.Errorf("Left().fileIdx = %v, want 1100", left.fileIdx)
	}

	// Test Right pointer
	right := node.Right()
	if right == nil {
		t.Fatal("Right() returned nil")
	}
	if right.id != rightID {
		t.Errorf("Right().id = %v, want %v", right.id, rightID)
	}
	if right.fileIdx != 1200 { // selfOffset(1000) + rightOffset(200)
		t.Errorf("Right().fileIdx = %v, want 1200", right.fileIdx)
	}

	// Test Hash
	if !bytes.Equal(node.Hash(), hash[:]) {
		t.Errorf("Hash() mismatch")
	}
	if !bytes.Equal(node.SafeHash(), hash[:]) {
		t.Errorf("SafeHash() mismatch")
	}

	// Test toKeyRef
	keyRef := node.toKeyRef()
	if KeyRef(nodeID) != keyRef {
		t.Errorf("toKeyRef() = %v, want %v", keyRef, KeyRef(nodeID))
	}
}

func TestBranchPersistedInlineZeroOffsets(t *testing.T) {
	// Test when offsets are zero (resolve by ID only)
	nodeID := NewNodeID(false, 3, 1)
	leftID := NewNodeID(true, 1, 111)
	rightID := NewNodeID(true, 1, 222)
	key := []byte("key")
	height := uint8(2)
	hash := sha256.Sum256(key)

	memNode := &MemNode{
		key:    key,
		height: height,
		hash:   hash[:],
	}

	var buf bytes.Buffer
	// Zero offsets
	err := encodeBranchNodeInline(&buf, memNode, nodeID, 0, 0, leftID, rightID, 100, 500)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	store := &mockNodeStore{nodes: make(map[NodeID]Node)}
	node := BranchPersistedInline{
		store:      store,
		layout:     BranchLayoutInline{data: buf.Bytes()},
		selfOffset: 1000,
	}

	// Test Left pointer with zero offset
	left := node.Left()
	if left == nil {
		t.Fatal("Left() returned nil")
	}
	if left.id != leftID {
		t.Errorf("Left().id = %v, want %v", left.id, leftID)
	}
	if left.fileIdx != 0 { // Zero offset means resolve by ID
		t.Errorf("Left().fileIdx = %v, want 0", left.fileIdx)
	}

	// Test Right pointer with zero offset
	right := node.Right()
	if right == nil {
		t.Fatal("Right() returned nil")
	}
	if right.id != rightID {
		t.Errorf("Right().id = %v, want %v", right.id, rightID)
	}
	if right.fileIdx != 0 { // Zero offset means resolve by ID
		t.Errorf("Right().fileIdx = %v, want 0", right.fileIdx)
	}
}

func TestBranchPersistedInlineMutateBranch(t *testing.T) {
	nodeID := NewNodeID(false, 2, 1)
	key := []byte("mutate-key")
	height := uint8(4)
	size := uint64(2000)
	hash := sha256.Sum256(key)

	memNode := &MemNode{
		key:    key,
		height: height,
		hash:   hash[:],
	}

	var buf bytes.Buffer
	err := encodeBranchNodeInline(&buf, memNode, nodeID, 100, 200, 10, 20, size, 1000)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	store := &mockNodeStore{nodes: make(map[NodeID]Node)}
	node := BranchPersistedInline{
		store:      store,
		layout:     BranchLayoutInline{data: buf.Bytes()},
		selfOffset: 5000,
	}

	ctx := MutationContext{Version: 10}
	mutated, err := node.MutateBranch(ctx)
	if err != nil {
		t.Fatalf("MutateBranch() error: %v", err)
	}

	if mutated.height != height {
		t.Errorf("Mutated height = %v, want %v", mutated.height, height)
	}
	if mutated.size != int64(size) {
		t.Errorf("Mutated size = %v, want %v", mutated.size, size)
	}
	if mutated.version != 10 {
		t.Errorf("Mutated version = %v, want 10", mutated.version)
	}
	if !bytes.Equal(mutated.key, key) {
		t.Errorf("Mutated key = %q, want %q", mutated.key, key)
	}
	if mutated._keyRef != KeyRef(nodeID) {
		t.Errorf("Mutated _keyRef = %v, want %v", mutated._keyRef, KeyRef(nodeID))
	}
}

func TestBranchPersistedInlineString(t *testing.T) {
	nodeID := NewNodeID(false, 1, 100)
	key := []byte("stringkey")
	height := uint8(1)
	hash := sha256.Sum256(key)

	memNode := &MemNode{
		key:    key,
		height: height,
		hash:   hash[:],
	}

	var buf bytes.Buffer
	err := encodeBranchNodeInline(&buf, memNode, nodeID, 50, 100, 1, 2, 500, 1000)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	store := &mockNodeStore{nodes: make(map[NodeID]Node)}
	node := BranchPersistedInline{
		store:      store,
		layout:     BranchLayoutInline{data: buf.Bytes()},
		selfOffset: 2000,
	}

	str := node.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}

	// Should contain "BranchPersistedInline"
	if !bytes.Contains([]byte(str), []byte("BranchPersistedInline")) {
		t.Errorf("String() should contain 'BranchPersistedInline': %s", str)
	}

	// Should contain selfOffset
	if !bytes.Contains([]byte(str), []byte("2000")) {
		t.Errorf("String() should contain selfOffset '2000': %s", str)
	}
}
