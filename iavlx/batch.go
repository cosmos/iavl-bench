package iavlx

type Batch struct {
	NodeReader
	leafNodes   *nodeTracker
	branchNodes *nodeTracker
}

type nodeTracker struct {
	updates []*nodeUpdate
	nodeMap map[*Node]int
}

func newNodeTracker() *nodeTracker {
	return &nodeTracker{
		nodeMap: make(map[*Node]int),
	}
}

func (nt *nodeTracker) trackNode(node *Node) *Node {
	n := len(nt.updates)
	nt.updates = append(nt.updates, &nodeUpdate{Node: node})
	nt.nodeMap[node] = n
	return node
}

func (nt *nodeTracker) dropNode(node *Node) {
	if n, exists := nt.nodeMap[node]; exists {
		nt.updates[n] = nil
		delete(nt.nodeMap, node)
	} else {
		nt.updates = append(nt.updates, &nodeUpdate{Node: node, deleted: true})
	}
}
func (nt *nodeTracker) apply(other *nodeTracker) {
	for _, node := range other.updates {
		if node != nil {
			if node.deleted {
				nt.dropNode(node.Node)
			} else {
				nt.trackNode(node.Node)
			}
		}
	}

}

type nodeUpdate struct {
	*Node
	deleted bool
}

func NewBatch(nodeReader NodeReader) *Batch {
	return &Batch{
		NodeReader:  nodeReader,
		leafNodes:   newNodeTracker(),
		branchNodes: newNodeTracker(),
	}
}

func (b *Batch) NewLeafNode(key, value []byte) *Node {
	node := newLeafNode(key, value)
	return b.trackNode(node)
}

func (b *Batch) NewBranchNode() *Node {
	return b.trackNode(NewNode())
}

func (b *Batch) MutateLeafNode(node *Node, newValue []byte) *Node {
	b.DropNode(node)
	newNode := node.copy()
	newNode.value = newValue
	return b.trackNode(newNode)
}

func (b *Batch) MutateBranchNode(node *Node) *Node {
	b.DropNode(node)
	return b.trackNode(node.copy())
}

func (b *Batch) trackNode(node *Node) *Node {
	if node.isLeaf() {
		b.leafNodes.trackNode(node)
	} else {
		b.branchNodes.trackNode(node)
	}
	return node
}

func (b *Batch) DropNode(node *Node) {
	if node.isLeaf() {
		b.leafNodes.dropNode(node)
	} else {
		b.branchNodes.dropNode(node)
	}
}

func (b *Batch) ApplyBatch(other *Batch) {
	b.leafNodes.apply(other.leafNodes)
	b.branchNodes.apply(other.branchNodes)
}

var _ NodeFactory = (*Batch)(nil)

type BatchTree struct {
	origRoot *NodePointer
	store    *Batch
	*Tree
}

func NewBatchTree(root *NodePointer, reader NodeReader, zeroCopy bool) *BatchTree {
	store := NewBatch(reader)
	return &BatchTree{
		origRoot: root,
		store:    store,
		Tree:     NewTree(root, store, zeroCopy),
	}
}
