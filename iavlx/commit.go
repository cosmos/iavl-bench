package iavlx

type CommitStore struct {
	version   uint32
	leafSeq   uint32
	branchSeq uint32
}

func (c *CommitStore) NewLeafNode(key, value []byte) *Node {
	node := newLeafNode(key, value)
	node.version = c.version
	node.leafID = c.leafSeq
	c.leafSeq++
	return node
}

func (c *CommitStore) CopyLeafNode(node *Node, newValue []byte) *Node {
	//TODO implement me
	panic("implement me")
}

func (c *CommitStore) GetLeft(node *Node) (*Node, error) {
	//TODO implement me
	panic("implement me")
}

func (c *CommitStore) GetRight(node *Node) (*Node, error) {
	//TODO implement me
	panic("implement me")
}

func (c *CommitStore) NewBranchNode() *Node {
	//TODO implement me
	panic("implement me")
}

func (c *CommitStore) CopyNode(node *Node) *Node {
	//TODO implement me
	panic("implement me")
}

func (c *CommitStore) DeleteNode(node *Node) {
	//TODO implement me
	panic("implement me")
}

var _ NodeWriter = (*CommitStore)(nil)
