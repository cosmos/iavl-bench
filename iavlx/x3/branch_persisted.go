package x3

import "bytes"

type BranchPersisted struct {
	store   NodeStore
	selfIdx uint32
	layout  BranchLayout
}

func (node *BranchPersisted) Height() uint8 {
	return node.layout.height
}

func (node *BranchPersisted) IsLeaf() bool {
	return false
}

func (node *BranchPersisted) Size() int64 {
	return int64(node.layout.size)
}

func (node *BranchPersisted) Version() uint64 {
	return node.layout.id.Version()
}

func (node *BranchPersisted) Key() ([]byte, error) {
	return node.store.ReadK(node.layout.keyOffset)
}

func (node *BranchPersisted) Value() ([]byte, error) {
	return nil, nil
}

func (node *BranchPersisted) resolveNodePointer(ref NodeRef) *NodePointer {
	if ref.IsRelativePointer() {
		id, err := node.store.ResolveNodeID(ref, node.selfIdx)
		if err != nil {
			panic(err)
		}
		return &NodePointer{
			store:   node.store,
			id:      id,
			fileIdx: uint32(int64(node.selfIdx) + ref.AsRelativePointer().Offset()),
		}
	} else {
		return &NodePointer{
			store: node.store,
			id:    ref.AsNodeID(),
		}
	}
}

func (node *BranchPersisted) Left() *NodePointer {
	return node.resolveNodePointer(node.layout.left)
}

func (node *BranchPersisted) Right() *NodePointer {
	return node.resolveNodePointer(node.layout.right)
}

func (node *BranchPersisted) Hash() []byte {
	return node.layout.hash[:]
}

func (node *BranchPersisted) SafeHash() []byte {
	return node.layout.hash[:]
}

func (node *BranchPersisted) MutateBranch(ctx *MutationContext) (*MemNode, error) {
	err := node.MarkOrphan(ctx)
	if err != nil {
		return nil, err
	}

	key, err := node.Key()
	if err != nil {
		return nil, err
	}
	memNode := &MemNode{
		height:  node.Height(),
		size:    node.Size(),
		version: ctx.Version,
		key:     key,
		left:    node.Left(),
		right:   node.Right(),
	}
	return memNode, err
}

func (node *BranchPersisted) MarkOrphan(ctx *MutationContext) error {
	if node.layout.orphanVersion != 0 {
		// already orphaned
		return nil
	}

	// write the orphan version in memory
	node.layout.orphanVersion = ctx.Version

	// write the orphan version in the store
	layoutPtr, err := node.store.ResolveBranch(node.layout.id, node.selfIdx)
	if err != nil {
		return err
	}
	layoutPtr.orphanVersion = node.layout.orphanVersion

	// add to the context
	ctx.Orphans = append(ctx.Orphans, node.layout.id)

	return nil
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
