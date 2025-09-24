package internal

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestLeafPersistedInlineNode(t *testing.T) {
	// Create test data
	nodeID := NewNodeID(true, 1, 1) // Leaf node, Version 1, index 1
	key := []byte("test-key")
	value := []byte("test-value")
	hash := sha256.Sum256(append(key, value...))

	// Create a test MemNode and encode it
	memNode := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, memNode, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// Create LeafPersistedInline
	node := LeafPersistedInline{
		LeafLayoutInline: LeafLayoutInline{data: buf.Bytes()},
	}

	// Test Height
	if h := node.Height(); h != 0 {
		t.Errorf("Height() = %v, want 0", h)
	}

	// Test IsLeaf
	if !node.IsLeaf() {
		t.Error("IsLeaf() = false, want true")
	}

	// Test Size
	if s := node.Size(); s != 1 {
		t.Errorf("Size() = %v, want 1", s)
	}

	// Test Version
	if v := node.Version(); v != 1 {
		t.Errorf("Version() = %v, want 1", v)
	}

	// Test Key
	gotKey, err := node.Key()
	if err != nil {
		t.Fatalf("Key() error: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Errorf("Key() = %q, want %q", gotKey, key)
	}

	// Test Value
	gotValue, err := node.Value()
	if err != nil {
		t.Fatalf("Value() error: %v", err)
	}
	if !bytes.Equal(gotValue, value) {
		t.Errorf("Value() = %q, want %q", gotValue, value)
	}

	// Test Left/Right (should be nil for leaves)
	if node.Left() != nil {
		t.Error("Left() should return nil for leaf")
	}
	if node.Right() != nil {
		t.Error("Right() should return nil for leaf")
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

func TestLeafPersistedInlineGet(t *testing.T) {
	nodeID := NewNodeID(true, 1, 1)
	key := []byte("mykey")
	value := []byte("myvalue")
	hash := sha256.Sum256(append(key, value...))

	memNode := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, memNode, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	node := LeafPersistedInline{
		LeafLayoutInline: LeafLayoutInline{data: buf.Bytes()},
	}

	tests := []struct {
		name      string
		searchKey []byte
		wantValue []byte
		wantIndex int64
		wantFound bool
	}{
		{
			name:      "exact match",
			searchKey: []byte("mykey"),
			wantValue: value,
			wantIndex: 0,
			wantFound: true,
		},
		{
			name:      "key less than node",
			searchKey: []byte("aaa"),
			wantValue: nil,
			wantIndex: 0,
			wantFound: false,
		},
		{
			name:      "key greater than node",
			searchKey: []byte("zzz"),
			wantValue: nil,
			wantIndex: 1,
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotValue, gotIndex, err := node.Get(tt.searchKey)
			if err != nil {
				t.Fatalf("Get() error: %v", err)
			}

			if tt.wantFound {
				if !bytes.Equal(gotValue, tt.wantValue) {
					t.Errorf("Get() value = %q, want %q", gotValue, tt.wantValue)
				}
			} else {
				if gotValue != nil {
					t.Errorf("Get() value = %q, want nil", gotValue)
				}
			}

			if gotIndex != tt.wantIndex {
				t.Errorf("Get() index = %v, want %v", gotIndex, tt.wantIndex)
			}
		})
	}
}

func TestLeafPersistedInlineMutateBranch(t *testing.T) {
	nodeID := NewNodeID(true, 1, 1)
	key := []byte("key")
	value := []byte("value")
	hash := sha256.Sum256(append(key, value...))

	memNode := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, memNode, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	node := LeafPersistedInline{
		LeafLayoutInline: LeafLayoutInline{data: buf.Bytes()},
	}

	// MutateBranch should panic for leaves
	defer func() {
		if r := recover(); r == nil {
			t.Error("MutateBranch() should panic for leaves")
		}
	}()

	ctx := MutationContext{Version: 2}
	node.MutateBranch(ctx)
}

func TestLeafPersistedInlineString(t *testing.T) {
	nodeID := NewNodeID(true, 1, 100)
	key := []byte("stringkey")
	value := []byte("stringvalue")
	hash := sha256.Sum256(append(key, value...))

	memNode := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, memNode, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	node := LeafPersistedInline{
		LeafLayoutInline: LeafLayoutInline{data: buf.Bytes()},
	}

	str := node.String()
	if str == "" {
		t.Error("String() should not return empty string")
	}

	// Should contain "LeafPersistedInline"
	if !bytes.Contains([]byte(str), []byte("LeafPersistedInline")) {
		t.Errorf("String() should contain 'LeafPersistedInline': %s", str)
	}
}
