package iavlx

import "bytes"

type BranchPersisted struct {
	store             *Changeset
	selfIdx           uint32
	layout            BranchLayout
	leftPtr, rightPtr *NodePointer
}

func (node *BranchPersisted) ID() NodeID {
	return node.layout.Id
}

func (node *BranchPersisted) Height() uint8 {
	return node.layout.Height
}

func (node *BranchPersisted) IsLeaf() bool {
	return false
}

func (node *BranchPersisted) Size() int64 {
	return int64(node.layout.Size)
}

func (node *BranchPersisted) Version() uint32 {
	return uint32(node.layout.Id.Version())
}

func (node *BranchPersisted) Key() ([]byte, error) {
	return node.store.ReadK(node.layout.Id, node.layout.KeyOffset)
}

func (node *BranchPersisted) Value() ([]byte, error) {
	return nil, nil
}

func (node *BranchPersisted) makeNodePointer(ref NodeRef, maybeId NodeID) *NodePointer {
	if ref.IsNodeID() {
		return &NodePointer{
			id:    ref.AsNodeID(),
			store: node.store,
		}
	} else {
		relPtr := ref.AsRelativePointer()
		if relPtr.IsLeaf() {
			return &NodePointer{
				id:      maybeId,
				store:   node.store,
				fileIdx: uint32(relPtr.Offset()) + 1, // +1 because offset is 1-based
			}
		} else {
			return &NodePointer{
				id:      maybeId,
				store:   node.store,
				fileIdx: uint32(int64(node.selfIdx) + relPtr.Offset()),
			}
		}
	}
}

func (node *BranchPersisted) Left() *NodePointer {
	return node.leftPtr
}

func (node *BranchPersisted) Right() *NodePointer {
	return node.rightPtr
}

func (node *BranchPersisted) Hash() []byte {
	return node.layout.Hash[:]
}

func (node *BranchPersisted) SafeHash() []byte {
	return node.layout.Hash[:]
}

func (node *BranchPersisted) MutateBranch(version uint32) (*MemNode, error) {
	key, err := node.Key()
	if err != nil {
		return nil, err
	}
	memNode := &MemNode{
		height:  node.Height(),
		size:    node.Size(),
		version: version,
		key:     key,
		left:    node.Left(),
		right:   node.Right(),
	}
	return memNode, err
}

func (node *BranchPersisted) Get(key []byte) (value []byte, index int64, err error) {
	nodeKey, err := node.Key()
	if err != nil {
		return nil, 0, err
	}

	if bytes.Compare(key, nodeKey) < 0 {
		leftNode, err := node.Left().Resolve()
		if err != nil {
			return nil, 0, err
		}

		return leftNode.Get(key)
	}

	rightNode, err := node.Right().Resolve()
	if err != nil {
		return nil, 0, err
	}

	value, index, err = rightNode.Get(key)
	if err != nil {
		return nil, 0, err
	}

	index += node.Size() - rightNode.Size()
	return value, index, nil
}

func (node *BranchPersisted) String() string {
	//TODO implement me
	panic("implement me")
}

var _ Node = (*BranchPersisted)(nil)
