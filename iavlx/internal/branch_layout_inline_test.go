package internal

//func TestBranchLayoutInline(t *testing.T) {
//	// Create test data
//	nodeID := NodeID(54321)
//	leftOffset := int64(1000)
//	rightOffset := int64(2000)
//	leftID := NodeID(111)
//	rightID := NodeID(222)
//	key := []byte("branch-test-key")
//	height := uint8(5)
//	size := uint64(100000)
//	span := uint64(500000)
//	hash := sha256.Sum256(key)
//
//	// Create a test MemNode
//	node := &MemNode{
//		key:    key,
//		height: height,
//		hash:   hash[:],
//	}
//
//	// Encode to buffer
//	var buf bytes.Buffer
//	err := encodeBranchNodeInline(&buf, node, nodeID, leftOffset, rightOffset, leftID, rightID, size, span)
//	if err != nil {
//		t.Fatalf("Failed to encode: %v", err)
//	}
//
//	// Create BranchLayoutInline from encoded data
//	branch := BranchLayoutInline{header: buf.Bytes()}
//
//	// Test NodeID
//	if got := branch.NodeID(); got != nodeID {
//		t.Errorf("NodeID() = %v, want %v", got, nodeID)
//	}
//
//	// Test LeftOffset
//	if got := branch.LeftOffset(); got != leftOffset {
//		t.Errorf("LeftOffset() = %v, want %v", got, leftOffset)
//	}
//
//	// Test RightOffset
//	if got := branch.RightOffset(); got != rightOffset {
//		t.Errorf("RightOffset() = %v, want %v", got, rightOffset)
//	}
//
//	// Test LeftID
//	if got := branch.LeftID(); got != leftID {
//		t.Errorf("LeftID() = %v, want %v", got, leftID)
//	}
//
//	// Test RightID
//	if got := branch.RightID(); got != rightID {
//		t.Errorf("RightID() = %v, want %v", got, rightID)
//	}
//
//	// Test KeyLength
//	if got := branch.KeyLength(); got != uint32(len(key)) {
//		t.Errorf("KeyLength() = %v, want %v", got, len(key))
//	}
//
//	// Test Height
//	if got := branch.Height(); got != height {
//		t.Errorf("Height() = %v, want %v", got, height)
//	}
//
//	// Test Size
//	if got := branch.Size(); got != size {
//		t.Errorf("Size() = %v, want %v", got, size)
//	}
//
//	// Test Span
//	if got := branch.Span(); got != span {
//		t.Errorf("Span() = %v, want %v", got, span)
//	}
//
//	// Test Hash
//	if got := branch.Hash(); !bytes.Equal(got, hash[:]) {
//		t.Errorf("Hash() = %x, want %x", got, hash[:])
//	}
//
//	// Test Key
//	if got := branch.Key(); !bytes.Equal(got, key) {
//		t.Errorf("Key() = %q, want %q", got, key)
//	}
//}
//
//func TestBranchLayoutInlinePackedFields(t *testing.T) {
//	// Test the packed KeyLength+Height field
//	nodeID := NodeID(1)
//	key := []byte("abc") // 3 byte key
//	height := uint8(42)
//	hash := sha256.Sum256([]byte("test"))
//
//	node := &MemNode{
//		key:    key,
//		height: height,
//		hash:   hash[:],
//	}
//
//	var buf bytes.Buffer
//	err := encodeBranchNodeInline(&buf, node, nodeID, 0, 0, 0, 0, 0, 0)
//	if err != nil {
//		t.Fatalf("Failed to encode: %v", err)
//	}
//
//	data := buf.Bytes()
//
//	// Verify the packed field directly
//	packedBytes := data[OffsetBranchInlineKeyLenHeight : OffsetBranchInlineKeyLenHeight+4]
//	packed := binary.LittleEndian.Uint32(packedBytes)
//
//	// Check key length (low 3 bytes)
//	keyLen := packed & 0xFFFFFF
//	if keyLen != 3 {
//		t.Errorf("Packed key length = %v, want 3", keyLen)
//	}
//
//	// Check height (high byte)
//	heightFromPacked := uint8(packed >> 24)
//	if heightFromPacked != 42 {
//		t.Errorf("Packed height = %v, want 42", heightFromPacked)
//	}
//
//	// Test via the methods
//	branch := BranchLayoutInline{header: buf.Bytes()}
//	if got := branch.KeyLength(); got != 3 {
//		t.Errorf("KeyLength() = %v, want 3", got)
//	}
//	if got := branch.Height(); got != 42 {
//		t.Errorf("Height() = %v, want 42", got)
//	}
//}
//
//func TestBranchLayoutInlineLargeOffsets(t *testing.T) {
//	// Test with maximum 5-byte values
//	maxOffset := int64(0xFFFFFFFFFF) // Max 5-byte value
//	nodeID := NodeID(1)
//	key := []byte("key")
//	hash := sha256.Sum256([]byte("test"))
//
//	node := &MemNode{
//		key:    key,
//		height: 10,
//		hash:   hash[:],
//	}
//
//	var buf bytes.Buffer
//	err := encodeBranchNodeInline(&buf, node, nodeID, maxOffset, maxOffset-1, 0, 0, uint64(maxOffset), uint64(maxOffset-2))
//	if err != nil {
//		t.Fatalf("Failed to encode max offsets: %v", err)
//	}
//
//	branch := BranchLayoutInline{header: buf.Bytes()}
//
//	if got := branch.LeftOffset(); got != maxOffset {
//		t.Errorf("LeftOffset() = %v, want %v", got, maxOffset)
//	}
//
//	if got := branch.RightOffset(); got != maxOffset-1 {
//		t.Errorf("RightOffset() = %v, want %v", got, maxOffset-1)
//	}
//
//	if got := branch.Size(); got != uint64(maxOffset) {
//		t.Errorf("Size() = %v, want %v", got, maxOffset)
//	}
//
//	if got := branch.Span(); got != uint64(maxOffset-2) {
//		t.Errorf("Span() = %v, want %v", got, maxOffset-2)
//	}
//}
//
//func TestBranchLayoutInlineOffsetOverflow(t *testing.T) {
//	// Test that values exceeding 5 bytes cause an error
//	overflowValue := int64(0x10000000000) // Exceeds 5-byte max
//	nodeID := NodeID(1)
//	key := []byte("key")
//	hash := sha256.Sum256([]byte("test"))
//
//	node := &MemNode{
//		key:    key,
//		height: 1,
//		hash:   hash[:],
//	}
//
//	tests := []struct {
//		name        string
//		leftOffset  int64
//		rightOffset int64
//		size        uint64
//		span        uint64
//		shouldError bool
//	}{
//		{"left overflow", overflowValue, 0, 0, 0, true},
//		{"right overflow", 0, overflowValue, 0, 0, true},
//		{"size overflow", 0, 0, overflowValue, 0, true},
//		{"span overflow", 0, 0, 0, overflowValue, true},
//		{"all valid", 0xFFFFFFFFFF, 0xFFFFFFFFFF, 0xFFFFFFFFFF, 0xFFFFFFFFFF, false},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			var buf bytes.Buffer
//			err := encodeBranchNodeInline(&buf, node, nodeID, tt.leftOffset, tt.rightOffset, 0, 0, tt.size, tt.span)
//			if tt.shouldError && err == nil {
//				t.Error("Expected error for overflow value")
//			}
//			if !tt.shouldError && err != nil {
//				t.Errorf("Unexpected error: %v", err)
//			}
//		})
//	}
//}
//
//func TestBranchLayoutInlineMaxKeyLen(t *testing.T) {
//	// Test with maximum key length
//	nodeID := NodeID(1)
//	key := make([]byte, KeyLenMax) // Maximum size key
//	hash := sha256.Sum256([]byte("test"))
//
//	node := &MemNode{
//		key:    key,
//		height: 1,
//		hash:   hash[:],
//	}
//
//	var buf bytes.Buffer
//	err := encodeBranchNodeInline(&buf, node, nodeID, 0, 0, 0, 0, 0, 0)
//	if err != nil {
//		t.Fatalf("Failed to encode max key: %v", err)
//	}
//
//	branch := BranchLayoutInline{header: buf.Bytes()}
//	if got := branch.KeyLength(); got != KeyLenMax {
//		t.Errorf("KeyLength() = %v, want %v", got, KeyLenMax)
//	}
//}
//
//func TestBranchLayoutInlineKeyTooLarge(t *testing.T) {
//	// Test with key exceeding maximum length
//	nodeID := NodeID(1)
//	key := make([]byte, KeyLenMax+1) // One byte too large
//	hash := sha256.Sum256([]byte("test"))
//
//	node := &MemNode{
//		key:    key,
//		height: 1,
//		hash:   hash[:],
//	}
//
//	var buf bytes.Buffer
//	err := encodeBranchNodeInline(&buf, node, nodeID, 0, 0, 0, 0, 0, 0)
//	if err == nil {
//		t.Error("Expected error for key exceeding maximum length")
//	}
//}
//
//func TestBranchLayoutInlineEmptyKey(t *testing.T) {
//	// Test with empty key
//	nodeID := NodeID(42)
//	key := []byte{}
//	height := uint8(3)
//	hash := sha256.Sum256([]byte{})
//
//	node := &MemNode{
//		key:    key,
//		height: height,
//		hash:   hash[:],
//	}
//
//	var buf bytes.Buffer
//	err := encodeBranchNodeInline(&buf, node, nodeID, 100, 200, 10, 20, 1000, 2000)
//	if err != nil {
//		t.Fatalf("Failed to encode empty key: %v", err)
//	}
//
//	branch := BranchLayoutInline{header: buf.Bytes()}
//
//	if got := branch.KeyLength(); got != 0 {
//		t.Errorf("KeyLength() = %v, want 0", got)
//	}
//
//	if got := branch.Key(); len(got) != 0 {
//		t.Errorf("Key() returned non-empty slice: %v", got)
//	}
//
//	// Verify other fields still work
//	if got := branch.Height(); got != height {
//		t.Errorf("Height() = %v, want %v", got, height)
//	}
//}
//
//func TestBranchLayoutInlineString(t *testing.T) {
//	nodeID := NodeID(100)
//	key := []byte("mykey")
//	height := uint8(7)
//	hash := sha256.Sum256([]byte("test"))
//
//	node := &MemNode{
//		key:    key,
//		height: height,
//		hash:   hash[:],
//	}
//
//	var buf bytes.Buffer
//	err := encodeBranchNodeInline(&buf, node, nodeID, 1000, 2000, 10, 20, 5000, 10000)
//	if err != nil {
//		t.Fatalf("Failed to encode: %v", err)
//	}
//
//	branch := BranchLayoutInline{header: buf.Bytes()}
//	str := branch.String()
//
//	// Check that string representation contains expected values
//	if str == "" {
//		t.Error("String() returned empty string")
//	}
//
//	// Should contain all the important fields
//	expected := "BranchInline{NodeID:100"
//	if len(str) < len(expected) {
//		t.Errorf("String() output too short: %q", str)
//	}
//}
//
//func TestBranchLayoutInlineOffsets(t *testing.T) {
//	// Verify all offsets are correct
//	tests := []struct {
//		name     string
//		offset   int
//		expected int
//	}{
//		{"NodeID", OffsetBranchInlineID, 0},
//		{"LeftOffset", OffsetBranchInlineLeftOffset, 8},
//		{"RightOffset", OffsetBranchInlineRightOffset, 13},
//		{"LeftID", OffsetBranchInlineLeftID, 18},
//		{"RightID", OffsetBranchInlineRightID, 26},
//		{"KeyLenHeight", OffsetBranchInlineKeyLenHeight, 34},
//		{"Size", OffsetBranchInlineSize, 38},
//		{"Span", OffsetBranchInlineSpan, 43},
//		{"Hash", OffsetBranchInlineHash, 48},
//		{"Data", OffsetBranchInlineData, 80},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if tt.offset != tt.expected {
//				t.Errorf("%s offset = %v, want %v", tt.name, tt.offset, tt.expected)
//			}
//		})
//	}
//
//	// Verify total size
//	if SizeBranchInlineHeader != 80 {
//		t.Errorf("SizeBranchInlineHeader = %v, want 80", SizeBranchInlineHeader)
//	}
//}
//
//func BenchmarkBranchLayoutInlineEncode(b *testing.B) {
//	nodeID := NodeID(1000)
//	key := []byte("benchmark-branch-key")
//	height := uint8(5)
//	hash := sha256.Sum256(key)
//
//	node := &MemNode{
//		key:    key,
//		height: height,
//		hash:   hash[:],
//	}
//
//	var buf bytes.Buffer
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		buf.Reset()
//		err := encodeBranchNodeInline(&buf, node, nodeID, 1000, 2000, 10, 20, 5000, 10000)
//		if err != nil {
//			b.Fatal(err)
//		}
//	}
//}
//
//func BenchmarkBranchLayoutInlineRead(b *testing.B) {
//	nodeID := NodeID(1000)
//	key := []byte("benchmark-branch-key")
//	height := uint8(5)
//	hash := sha256.Sum256(key)
//
//	node := &MemNode{
//		key:    key,
//		height: height,
//		hash:   hash[:],
//	}
//
//	var buf bytes.Buffer
//	err := encodeBranchNodeInline(&buf, node, nodeID, 1000, 2000, 10, 20, 5000, 10000)
//	if err != nil {
//		b.Fatal(err)
//	}
//
//	branch := BranchLayoutInline{header: buf.Bytes()}
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		_ = branch.NodeID()
//		_ = branch.LeftOffset()
//		_ = branch.RightOffset()
//		_ = branch.LeftID()
//		_ = branch.RightID()
//		_ = branch.KeyLength()
//		_ = branch.Height()
//		_ = branch.Size()
//		_ = branch.Span()
//		_ = branch.Hash()
//		_ = branch.Key()
//	}
//}
