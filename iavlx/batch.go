package iavlx

type BatchStore struct {
	NodeReader
	batchNodes   []*Node
	batchOrphans []*Node
	batchNodeMap map[*Node]int
}

func (b *BatchStore) NewLeafNode(key, value []byte) *Node {
	node := newLeafNode(key, value)
	n := len(b.batchNodes)
	b.batchNodes = append(b.batchNodes, node)
	b.batchNodeMap[node] = n
	return node
}

func (b *BatchStore) NewBranchNode() *Node {
	return b.trackNode(&Node{})
}

func (b *BatchStore) CopyLeafNode(node *Node, newValue []byte) *Node {
	newNode := node.copy()
	newNode.value = newValue
	return b.trackNode(newNode)
}

func (b *BatchStore) CopyNode(node *Node) *Node {
	return b.trackNode(node.copy())
}

func (b *BatchStore) trackNode(node *Node) *Node {
	n := len(b.batchNodes)
	b.batchNodes = append(b.batchNodes, node)
	b.batchNodeMap[node] = n
	return node
}

func (b *BatchStore) DeleteNode(node *Node) {
	if n, exists := b.batchNodeMap[node]; exists {
		b.batchNodes[n] = nil
		delete(b.batchNodeMap, node)
	} else {
		b.batchOrphans = append(b.batchOrphans, node)
	}
}

func (b *BatchStore) ApplyBatch(other *BatchStore) {
	for _, node := range other.batchNodes {
		if node != nil {
			b.trackNode(node)
		}
	}
	for _, node := range other.batchOrphans {
		b.DeleteNode(node)
	}
}

var _ NodeWriter = (*BatchStore)(nil)
