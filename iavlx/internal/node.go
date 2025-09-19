package internal

import "sync/atomic"

type Node interface {
	Height() uint8
	IsLeaf() bool
	Size() int64
	Version() uint64
	Key() ([]byte, error)
	Value() ([]byte, error)
	Left() *NodePointer
	Right() *NodePointer
	Hash() []byte
	SafeHash() []byte
	MutateBranch(MutationContext) (*MemNode, error)
	Get(key []byte) (value []byte, index int64, err error)
}

type NodePointer struct {
	mem        atomic.Pointer[MemNode]
	fileOffset int64
	store      NodeStore
	id         NodeID
}

func NewNodePointer(memNode *MemNode) *NodePointer {
	n := &NodePointer{}
	n.mem.Store(memNode)
	return n
}

func (p *NodePointer) Resolve() (Node, error) {
	mem := p.mem.Load()
	if mem != nil {
		return mem, nil
	}
	panic("TODO")
}

type MemNode struct {
	height  uint8
	size    int64
	version uint64
	key     []byte
	value   []byte
	left    *NodePointer
	right   *NodePointer
	hash    []byte

	_walOffset int    // only valid for leaf nodes
	_keyRef    KeyRef // used when copying branch nodes
}

func (node *MemNode) Height() uint8 {
	return node.height
}

func (node *MemNode) Size() int64 {
	return node.size
}

func (node *MemNode) Version() uint64 {
	return node.version
}

func (node *MemNode) Key() ([]byte, error) {
	return node.key, nil
}

func (node *MemNode) Value() ([]byte, error) {
	return node.value, nil
}

func (node *MemNode) Left() *NodePointer {
	return node.left
}

func (node *MemNode) Right() *NodePointer {
	return node.right
}

func (node *MemNode) SafeHash() []byte {
	panic("TODO")
}

func (node *MemNode) MutateBranch(context MutationContext) (*MemNode, error) {
	//TODO implement me
	panic("implement me")
}

func (node *MemNode) Get(key []byte) (value []byte, index int64, err error) {
	//TODO implement me
	panic("implement me")
}

func (node *MemNode) IsLeaf() bool {
	return node.height == 0
}

func (node *MemNode) Hash() []byte {
	panic("TODO")
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func (node *MemNode) updateHeightSize() error {
	leftNode, err := node.left.Resolve()
	if err != nil {
		return err
	}

	rightNode, err := node.right.Resolve()
	if err != nil {
		return err
	}

	node.height = maxUint8(leftNode.Height(), rightNode.Height()) + 1
	node.size = leftNode.Size() + rightNode.Size()
	return nil
}

func maxUint8(a, b uint8) uint8 {
	if a > b {
		return a
	}
	return b
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func (node *MemNode) reBalance(ctx MutationContext) (*MemNode, error) {
	balance, err := calcBalance(node)
	if err != nil {
		return nil, err
	}
	switch {
	case balance > 1:
		left, err := node.left.Resolve()
		if err != nil {
			return nil, err
		}

		leftBalance, err := calcBalance(left)
		if err != nil {
			return nil, err
		}

		if leftBalance >= 0 {
			// left left
			return node.rotateRight(ctx)
		}

		// left right
		newLeft, err := left.MutateBranch(ctx)
		if err != nil {
			return nil, err
		}
		newLeft, err = newLeft.rotateLeft(ctx)
		if err != nil {
			return nil, err
		}
		node.left = NewNodePointer(newLeft)
		return node.rotateRight(ctx)
	case balance < -1:
		right, err := node.right.Resolve()
		if err != nil {
			return nil, err
		}

		rightBalance, err := calcBalance(right)
		if err != nil {
			return nil, err
		}

		if rightBalance <= 0 {
			// right right
			return node.rotateLeft(ctx)
		}

		// right left
		newRight, err := right.MutateBranch(ctx)
		if err != nil {
			return nil, err
		}
		newRight, err = newRight.rotateRight(ctx)
		node.right = NewNodePointer(newRight)
		return node.rotateLeft(ctx)
	default:
		// nothing changed
		return node, err
	}
}

func calcBalance(node Node) (int, error) {
	leftNode, err := node.Left().Resolve()
	if err != nil {
		return 0, err
	}

	rightNode, err := node.Right().Resolve()
	if err != nil {
		return 0, err
	}

	return int(leftNode.Height()) - int(rightNode.Height()), nil
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func (node *MemNode) rotateRight(ctx MutationContext) (*MemNode, error) {
	left, err := node.left.Resolve()
	if err != nil {
		return nil, err
	}
	newSelf, err := left.MutateBranch(ctx)
	if err != nil {
		return nil, err
	}
	node.left = left.Right()
	newSelf.right = NewNodePointer(node)

	err = node.updateHeightSize()
	if err != nil {
		return nil, err
	}
	err = newSelf.updateHeightSize()
	if err != nil {
		return nil, err
	}

	return newSelf, nil
}

// IMPORTANT: nodes called with this method must be new or copies first.
// Code reviewers should use find usages to ensure that all callers follow this rule!
func (node *MemNode) rotateLeft(ctx MutationContext) (*MemNode, error) {
	right, err := node.right.Resolve()
	if err != nil {
		return nil, err
	}

	newSelf, err := right.MutateBranch(ctx)
	if err != nil {
		return nil, err
	}

	node.right = right.Left()
	newSelf.left = NewNodePointer(node)

	err = node.updateHeightSize()
	if err != nil {
		return nil, err
	}

	err = newSelf.updateHeightSize()
	if err != nil {
		return nil, err
	}

	return newSelf, nil
}

var _ Node = &MemNode{}
