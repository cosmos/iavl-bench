package iavlx

import "fmt"

type NullStore struct {
	NodeKeyGenerator
}

func NewNullStore(nodeKeyGenerator NodeKeyGenerator) *NullStore {
	return &NullStore{NodeKeyGenerator: nodeKeyGenerator}
}

func (m NullStore) SaveNode(node *Node) error {
	return nil
}

func (m NullStore) DeleteNode(node *Node) error {
	return nil
}

func (m NullStore) SaveRoot(version int64, root *Node) error {
	return nil
}

func (m NullStore) Load(*NodePointer) (*Node, error) {
	return nil, fmt.Errorf("NullStore does not support Load")
}

var _ NodeWriter = (*NullStore)(nil)
