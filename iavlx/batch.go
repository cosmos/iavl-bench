package iavlx

type BatchStore struct {
	NodeReader
	// TODO should/can we separate branches and leaves easily here so that it's easier to apply just leaves as a WAL if the original root differs?
	batchUpdates []*nodeUpdate
	batchNodeMap map[*Node]int
}

type nodeUpdate struct {
	*Node
	deleted bool
}

func NewBatchStore(nodeReader NodeReader) *BatchStore {
	return &BatchStore{
		NodeReader:   nodeReader,
		batchNodeMap: make(map[*Node]int),
	}
}

func (b *BatchStore) NewLeafNode(key, value []byte) *Node {
	node := newLeafNode(key, value)
	n := len(b.batchUpdates)
	b.batchUpdates = append(b.batchUpdates, &nodeUpdate{Node: node})
	b.batchNodeMap[node] = n
	return node
}

func (b *BatchStore) NewBranchNode() *Node {
	return b.trackNode(NewNode())
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
	n := len(b.batchUpdates)
	b.batchUpdates = append(b.batchUpdates, &nodeUpdate{Node: node})
	b.batchNodeMap[node] = n
	return node
}

func (b *BatchStore) DeleteNode(node *Node) {
	if n, exists := b.batchNodeMap[node]; exists {
		b.batchUpdates[n] = nil
		delete(b.batchNodeMap, node)
	} else {
		b.batchUpdates = append(b.batchUpdates, &nodeUpdate{Node: node, deleted: true})
	}
}

func (b *BatchStore) ApplyBatch(other *BatchStore) {
	for _, node := range other.batchUpdates {
		if node != nil {
			if node.deleted {
				b.DeleteNode(node.Node)
			} else {
				b.trackNode(node.Node)
			}
		}
	}
}

var _ NodeFactory = (*BatchStore)(nil)

type BatchTree struct {
	origRoot *Node
	store    *BatchStore
	*Tree
}

func NewBatchTree(root *Node, reader NodeReader, zeroCopy bool) *BatchTree {
	store := NewBatchStore(reader)
	return &BatchTree{
		origRoot: root,
		store:    store,
		Tree:     NewTree(root, store, zeroCopy),
	}
}
