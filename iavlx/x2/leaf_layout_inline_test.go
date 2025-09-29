package x2

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"
)

func TestLeafLayoutInline(t *testing.T) {
	// Create test data
	nodeID := NodeID(12345)
	key := []byte("test-key")
	value := []byte("test-value-data")
	hash := sha256.Sum256(append(key, value...))

	// Create a test MemNode
	node := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	// Encode to buffer
	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, node, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// Create LeafLayoutInline from encoded data
	leaf := LeafLayoutInline{header: buf.Bytes()}

	// Test NodeID
	if got := leaf.NodeID(); got != nodeID {
		t.Errorf("NodeID() = %v, want %v", got, nodeID)
	}

	// Test KeyLength
	if got := leaf.KeyLen(); got != uint32(len(key)) {
		t.Errorf("KeyLength() = %v, want %v", got, len(key))
	}

	// Test ValueLen
	if got := leaf.ValueLen(); got != uint32(len(value)) {
		t.Errorf("ValueLen() = %v, want %v", got, len(value))
	}

	// Test Hash
	if got := leaf.Hash(); !bytes.Equal(got, hash[:]) {
		t.Errorf("Hash() = %x, want %x", got, hash[:])
	}

	// Test Key
	if got := leaf.Key(); !bytes.Equal(got, key) {
		t.Errorf("Key() = %q, want %q", got, key)
	}

	// Test Value
	if got := leaf.Value(); !bytes.Equal(got, value) {
		t.Errorf("Value() = %q, want %q", got, value)
	}
}

func TestLeafLayoutInlineAlignment(t *testing.T) {
	// Verify that the padding byte is correctly placed and set to 0
	nodeID := NodeID(999)
	key := []byte("abc") // 3 byte key
	value := []byte("defg")
	hash := sha256.Sum256([]byte("test"))

	node := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, node, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	data := buf.Bytes()

	// Check that the padding byte (at offset 11) is 0
	if data[11] != 0 {
		t.Errorf("Padding byte at offset 11 should be 0, got %v", data[11])
	}

	// Verify that KeyLength can be read as a 4-byte aligned integer
	keyLenBytes := data[8:12]
	keyLen := binary.LittleEndian.Uint32(keyLenBytes)
	if keyLen != 3 {
		t.Errorf("KeyLength read as aligned uint32 = %v, want 3", keyLen)
	}

	// Verify ValueLen is at a 4-byte aligned offset (12)
	if OffsetLeafInlineValueLen%4 != 0 {
		t.Errorf("ValueLen offset %v is not 4-byte aligned", OffsetLeafInlineValueLen)
	}

	valueLenBytes := data[12:16]
	valueLen := binary.LittleEndian.Uint32(valueLenBytes)
	if valueLen != 4 {
		t.Errorf("ValueLen = %v, want 4", valueLen)
	}
}

func TestLeafLayoutInlineMaxKeyLen(t *testing.T) {
	// Test with maximum key length
	nodeID := NodeID(1)
	key := make([]byte, KeyLenMax) // Maximum size key
	value := []byte("v")
	hash := sha256.Sum256([]byte("test"))

	node := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, node, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode max key: %v", err)
	}

	leaf := LeafLayoutInline{header: buf.Bytes()}
	if got := leaf.KeyLen(); got != KeyLenMax {
		t.Errorf("KeyLength() = %v, want %v", got, KeyLenMax)
	}
}

func TestLeafLayoutInlineKeyTooLarge(t *testing.T) {
	// Test with key exceeding maximum length
	nodeID := NodeID(1)
	key := make([]byte, KeyLenMax+1) // One byte too large
	value := []byte("v")
	hash := sha256.Sum256([]byte("test"))

	node := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, node, nodeID)
	if err == nil {
		t.Error("Expected error for key exceeding maximum length")
	}
}

func TestLeafLayoutInlineEmptyKeyValue(t *testing.T) {
	// Test with empty key and value
	nodeID := NodeID(42)
	key := []byte{}
	value := []byte{}
	hash := sha256.Sum256([]byte{})

	node := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, node, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode empty key/value: %v", err)
	}

	leaf := LeafLayoutInline{header: buf.Bytes()}

	if got := leaf.KeyLen(); got != 0 {
		t.Errorf("KeyLength() = %v, want 0", got)
	}

	if got := leaf.ValueLen(); got != 0 {
		t.Errorf("ValueLen() = %v, want 0", got)
	}

	if got := leaf.Key(); len(got) != 0 {
		t.Errorf("Key() returned non-empty slice: %v", got)
	}

	if got := leaf.Value(); len(got) != 0 {
		t.Errorf("Value() returned non-empty slice: %v", got)
	}
}

func TestLeafLayoutInlineString(t *testing.T) {
	nodeID := NodeID(100)
	key := []byte("mykey")
	value := []byte("myvalue")
	hash := sha256.Sum256([]byte("test"))

	node := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, node, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	leaf := LeafLayoutInline{header: buf.Bytes()}
	str := leaf.String()

	// Check that string representation contains expected values
	if str == "" {
		t.Error("String() returned empty string")
	}

	// Should contain NodeID, KeyLength, ValueLen, and Hash
	expected := "LeafInline{NodeID:100, KeyLength:5, ValueLen:7, Hash:"
	if len(str) < len(expected) {
		t.Errorf("String() output too short: %q", str)
	}
}

func TestLeafLayoutInlineLargeValue(t *testing.T) {
	// Test with a large value (multiple KB)
	nodeID := NodeID(5000)
	key := []byte("large-value-key")
	value := make([]byte, 10000) // 10KB value
	for i := range value {
		value[i] = byte(i % 256)
	}
	hash := sha256.Sum256(append(key, value...))

	node := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, node, nodeID)
	if err != nil {
		t.Fatalf("Failed to encode large value: %v", err)
	}

	leaf := LeafLayoutInline{header: buf.Bytes()}

	// Verify all fields
	if got := leaf.NodeID(); got != nodeID {
		t.Errorf("NodeID() = %v, want %v", got, nodeID)
	}

	if got := leaf.KeyLen(); got != uint32(len(key)) {
		t.Errorf("KeyLength() = %v, want %v", got, len(key))
	}

	if got := leaf.ValueLen(); got != uint32(len(value)) {
		t.Errorf("ValueLen() = %v, want %v", got, len(value))
	}

	if got := leaf.Key(); !bytes.Equal(got, key) {
		t.Errorf("Key() mismatch")
	}

	if got := leaf.Value(); !bytes.Equal(got, value) {
		t.Errorf("Value() mismatch, got len %v, want len %v", len(got), len(value))
	}
}

func BenchmarkLeafLayoutInlineEncode(b *testing.B) {
	nodeID := NodeID(1000)
	key := []byte("benchmark-key-123")
	value := []byte("benchmark-value-with-some-data")
	hash := sha256.Sum256(append(key, value...))

	node := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		err := encodeLeafNodeInline(&buf, node, nodeID)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLeafLayoutInlineRead(b *testing.B) {
	nodeID := NodeID(1000)
	key := []byte("benchmark-key-123")
	value := []byte("benchmark-value-with-some-data")
	hash := sha256.Sum256(append(key, value...))

	node := &MemNode{
		key:   key,
		value: value,
		hash:  hash[:],
	}

	var buf bytes.Buffer
	err := encodeLeafNodeInline(&buf, node, nodeID)
	if err != nil {
		b.Fatal(err)
	}

	leaf := LeafLayoutInline{header: buf.Bytes()}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = leaf.NodeID()
		_ = leaf.KeyLen()
		_ = leaf.ValueLen()
		_ = leaf.Hash()
		_ = leaf.Key()
		_ = leaf.Value()
	}
}
