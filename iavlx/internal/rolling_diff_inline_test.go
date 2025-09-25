package internal

//
//func TestRollingDiffInlineWriteLeaf(t *testing.T) {
//	// Create temp directory
//	tmpDir := t.TempDir()
//
//	// Create RollingDiffInline
//	rd, err := NewRollingDiffInline(tmpDir, 0)
//	if err != nil {
//		t.Fatalf("Failed to create RollingDiffInline: %v", err)
//	}
//
//	// Create a leaf node
//	nodeID := NewNodeID(true, 1, 1)
//	key := []byte("test-key")
//	value := []byte("test-value")
//	hash := sha256.Sum256(append(key, value...))
//
//	memNode := &MemNode{
//		key:     key,
//		value:   value,
//		hash:    hash[:],
//		version: 1, // stagedVersion
//	}
//
//	np := &NodePointer{
//		id:    nodeID,
//		store: rd,
//	}
//	np.mem.Store(memNode)
//
//	// Write the leaf
//	bytesWritten, err := rd.writeLeaf(np, memNode)
//	if err != nil {
//		t.Fatalf("Failed to write leaf: %v", err)
//	}
//
//	// Flush to make data visible for reading
//	err = rd.nodesFile.SaveAndRemap()
//	if err != nil {
//		t.Fatalf("Failed to save and remap: %v", err)
//	}
//
//	// Verify bytes written
//	expectedSize := SizeLeafInlineHeader + uint64(len(key)) + uint64(len(value))
//	if bytesWritten != expectedSize {
//		t.Errorf("Bytes written = %d, want %d", bytesWritten, expectedSize)
//	}
//
//	// Verify fileIdx was set
//	if np.fileIdx != 1 { // Should be 1 (1-based, starting at offset 0)
//		t.Errorf("fileIdx = %d, want 1", np.fileIdx)
//	}
//
//	// Verify we can read it back
//	node, err := rd.ResolveNode(nodeID, np.fileIdx)
//	if err != nil {
//		t.Fatalf("Failed to resolve node: %v", err)
//	}
//
//	leaf, ok := node.(LeafPersistedInline)
//	if !ok {
//		t.Fatalf("Expected LeafPersistedInline, got %T", node)
//	}
//
//	// Verify data
//	gotKey, _ := leaf.Key()
//	if string(gotKey) != string(key) {
//		t.Errorf("Key = %q, want %q", gotKey, key)
//	}
//}
//
//func TestRollingDiffInlineWriteBranch(t *testing.T) {
//	// Create temp directory
//	tmpDir := t.TempDir()
//
//	rd, err := NewRollingDiffInline(tmpDir, 0)
//	if err != nil {
//		t.Fatalf("Failed to create RollingDiffInline: %v", err)
//	}
//
//	// Create leaf nodes
//	leftID := NewNodeID(true, 1, 10)
//	leftKey := []byte("left")
//	leftValue := []byte("leftval")
//	leftHash := sha256.Sum256(append(leftKey, leftValue...))
//	leftMem := &MemNode{
//		key:     leftKey,
//		value:   leftValue,
//		hash:    leftHash[:],
//		version: 1,
//		size:    1,
//	}
//	leftNP := &NodePointer{
//		id: leftID,
//	}
//	leftNP.mem.Store(leftMem)
//
//	rightID := NewNodeID(true, 1, 11)
//	rightKey := []byte("right")
//	rightValue := []byte("rightval")
//	rightHash := sha256.Sum256(append(rightKey, rightValue...))
//	rightMem := &MemNode{
//		key:     rightKey,
//		value:   rightValue,
//		hash:    rightHash[:],
//		version: 1,
//		size:    1,
//	}
//	rightNP := &NodePointer{
//		id: rightID,
//	}
//	rightNP.mem.Store(rightMem)
//
//	// Create branch node
//	branchID := NewNodeID(false, 1, 5)
//	branchKey := []byte("branch")
//	branchHash := sha256.Sum256(branchKey)
//	branchMem := &MemNode{
//		key:     branchKey,
//		height:  1,
//		hash:    branchHash[:],
//		version: 1,
//		size:    3, // branch + 2 leaves
//		left:    leftNP,
//		right:   rightNP,
//	}
//	branchNP := &NodePointer{
//		id: branchID,
//	}
//	branchNP.mem.Store(branchMem)
//
//	// Write the branch (which will write children first)
//	bytesWritten, err := rd.writeBranch(branchNP, branchMem)
//	if err != nil {
//		t.Fatalf("Failed to write branch: %v", err)
//	}
//
//	// Flush to make data visible for reading
//	err = rd.nodesFile.SaveAndRemap()
//	if err != nil {
//		t.Fatalf("Failed to save and remap: %v", err)
//	}
//
//	// Verify span is the total bytes of the subtree
//	if bytesWritten == 0 {
//		t.Error("Expected non-zero bytes written")
//	}
//
//	// Verify we can resolve all nodes
//	// Left leaf
//	if leftNP.fileIdx == 0 {
//		t.Error("Left leaf fileIdx not set")
//	}
//	leftNode, err := rd.ResolveNode(leftID, leftNP.fileIdx)
//	if err != nil {
//		t.Fatalf("Failed to resolve left leaf: %v", err)
//	}
//	if !leftNode.IsLeaf() {
//		t.Error("Expected left node to be leaf")
//	}
//
//	// Right leaf
//	if rightNP.fileIdx == 0 {
//		t.Error("Right leaf fileIdx not set")
//	}
//	rightNode, err := rd.ResolveNode(rightID, rightNP.fileIdx)
//	if err != nil {
//		t.Fatalf("Failed to resolve right leaf: %v", err)
//	}
//	if !rightNode.IsLeaf() {
//		t.Error("Expected right node to be leaf")
//	}
//
//	// Branch
//	if branchNP.fileIdx == 0 {
//		t.Error("Branch fileIdx not set")
//	}
//	branchNode, err := rd.ResolveNode(branchID, branchNP.fileIdx)
//	if err != nil {
//		t.Fatalf("Failed to resolve branch: %v", err)
//	}
//	if branchNode.IsLeaf() {
//		t.Error("Expected branch node to not be leaf")
//	}
//
//	// Verify branch has correct child references
//	branch := branchNode.(BranchPersistedInline)
//	if branch.layout.LeftID() != leftID {
//		t.Errorf("Left ID = %v, want %v", branch.layout.LeftID(), leftID)
//	}
//	if branch.layout.RightID() != rightID {
//		t.Errorf("Right ID = %v, want %v", branch.layout.RightID(), rightID)
//	}
//
//	// Verify span is total byte size
//	if branch.layout.Span() != bytesWritten {
//		t.Errorf("Span = %d, want %d", branch.layout.Span(), bytesWritten)
//	}
//}
//
//func TestRollingDiffInlineWriteRoot(t *testing.T) {
//	// Create temp directory
//	tmpDir := t.TempDir()
//
//	rd, err := NewRollingDiffInline(tmpDir, 0)
//	if err != nil {
//		t.Fatalf("Failed to create RollingDiffInline: %v", err)
//	}
//
//	// Create a simple tree
//	leafID := NewNodeID(true, 1, 1)
//	leafHash := sha256.Sum256([]byte("leafvalue"))
//	leafMem := &MemNode{
//		key:     []byte("leaf"),
//		value:   []byte("value"),
//		hash:    leafHash[:],
//		version: 1,
//		size:    1,
//	}
//	leafNP := &NodePointer{
//		id: leafID,
//	}
//	leafNP.mem.Store(leafMem)
//
//	// Write root
//	err = rd.writeRoot(1, leafNP, 1)
//	if err != nil {
//		t.Fatalf("Failed to write root: %v", err)
//	}
//
//	// Verify version was updated
//	if rd.stagedVersion != 2 {
//		t.Errorf("stagedVersion = %d, want 2", rd.stagedVersion)
//	}
//
//	// Verify saved version
//	if rd.savedVersion.Load() != 1 {
//		t.Errorf("savedVersion = %d, want 1", rd.savedVersion.Load())
//	}
//
//	// Verify file was saved
//	nodesPath := filepath.Join(tmpDir, "nodes_inline.dat")
//	info, err := os.Stat(nodesPath)
//	if err != nil {
//		t.Fatalf("Failed to stat nodes file: %v", err)
//	}
//	if info.Size() == 0 {
//		t.Error("Nodes file is empty")
//	}
//}
//
//func TestRollingDiffInlineVersionMismatch(t *testing.T) {
//	tmpDir := t.TempDir()
//	rd, err := NewRollingDiffInline(tmpDir, 0)
//	if err != nil {
//		t.Fatalf("Failed to create RollingDiffInline: %v", err)
//	}
//
//	// Create node with wrong version
//	nodeHash := sha256.Sum256([]byte("keyvalue"))
//	memNode := &MemNode{
//		key:     []byte("key"),
//		value:   []byte("value"),
//		hash:    nodeHash[:],
//		version: 2, // Wrong version (should be 1)
//		size:    1,
//	}
//	np := &NodePointer{
//		id: NewNodeID(true, 2, 1),
//	}
//	np.mem.Store(memNode)
//
//	// Should not write (version mismatch)
//	bytesWritten, err := rd.writeNode(np)
//	if err != nil {
//		t.Fatalf("Unexpected error: %v", err)
//	}
//	if bytesWritten != 0 {
//		t.Error("Expected 0 bytes written for version mismatch")
//	}
//}
//
//func TestRollingDiffInlineByteOffsets(t *testing.T) {
//	tmpDir := t.TempDir()
//	rd, err := NewRollingDiffInline(tmpDir, 0)
//	if err != nil {
//		t.Fatalf("Failed to create RollingDiffInline: %v", err)
//	}
//
//	// Write multiple nodes and verify offsets
//	var nodes []*NodePointer
//	var expectedOffsets []uint64
//
//	for i := 0; i < 3; i++ {
//		nodeID := NewNodeID(true, 1, uint32(i+1))
//		key := []byte(fmt.Sprintf("key%d", i))
//		value := []byte(fmt.Sprintf("value%d", i))
//		hash := sha256.Sum256(append(key, value...))
//
//		memNode := &MemNode{
//			key:     key,
//			value:   value,
//			hash:    hash[:],
//			version: 1,
//			size:    1,
//		}
//
//		np := &NodePointer{
//			id: nodeID,
//		}
//		np.mem.Store(memNode)
//
//		expectedOffsets = append(expectedOffsets, uint64(rd.nodesFile.Offset()+1)) // +1 for 1-based
//
//		bytesWritten, err := rd.writeLeaf(np, memNode)
//		if err != nil {
//			t.Fatalf("Failed to write leaf %d: %v", i, err)
//		}
//
//		if bytesWritten == 0 {
//			t.Errorf("Node %d: expected non-zero bytes written", i)
//		}
//
//		nodes = append(nodes, np)
//	}
//
//	// Flush to make data visible for reading
//	err = rd.nodesFile.SaveAndRemap()
//	if err != nil {
//		t.Fatalf("Failed to save and remap: %v", err)
//	}
//
//	// Verify all offsets are correct
//	for i, np := range nodes {
//		if np.fileIdx != expectedOffsets[i] {
//			t.Errorf("Node %d: fileIdx = %d, want %d", i, np.fileIdx, expectedOffsets[i])
//		}
//
//		// Verify we can resolve at that offset
//		node, err := rd.ResolveNode(np.id, np.fileIdx)
//		if err != nil {
//			t.Errorf("Node %d: failed to resolve: %v", i, err)
//		}
//		if node == nil {
//			t.Errorf("Node %d: resolved to nil", i)
//		}
//	}
//}
