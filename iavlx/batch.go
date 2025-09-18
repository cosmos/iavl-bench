package iavlx

type ChangeBatch struct {
	NodeReader
	stagedVersion uint32
	leafUpdates   []*leafUpdate
}

type leafUpdate struct {
	*Node
	deleted   bool
	deleteKey NodeKey
}

func NewChangeBatch(stagedVersion uint32, nodeReader NodeReader) *ChangeBatch {
	return &ChangeBatch{
		stagedVersion: stagedVersion,
		NodeReader:    nodeReader,
	}
}

func (b *ChangeBatch) NewLeafNode(key, value []byte) *Node {
	node := newLeafNode(key, value)
	node.version = b.stagedVersion
	b.leafUpdates = append(b.leafUpdates, &leafUpdate{Node: node})
	return node
}

func (b *ChangeBatch) NewBranchNode() *Node {
	node := NewNode()
	node.version = b.stagedVersion
	return node
}

func (b *ChangeBatch) MutateLeafNode(node *Node, newValue []byte) *Node {
	b.DropNode(node)
	newNode := node.copy()
	newNode.value = newValue
	newNode.hash = nil
	newNode.version = b.stagedVersion
	return newNode
}

func (b *ChangeBatch) MutateBranchNode(node *Node) *Node {
	newNode := node.copy()
	newNode.hash = nil
	newNode.version = b.stagedVersion
	return newNode
}

func (b *ChangeBatch) DropNode(node *Node) {
	if node.isLeaf() {
		b.leafUpdates = append(b.leafUpdates, &leafUpdate{
			Node:      node,
			deleted:   true,
			deleteKey: node.nodeKey,
		})
	}
}

var _ NodeFactory = (*ChangeBatch)(nil)
