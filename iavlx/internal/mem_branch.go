package internal

import "sync/atomic"

type MemBranch struct {
	height    uint8
	size      int64
	version   uint64
	key       []byte
	value     []byte
	left      atomic.Pointer[Node]
	right     atomic.Pointer[Node]
	hash      []byte
	persisted *PersistedNode
}

func (node *MemBranch) MutateBranch(version uint64) *MemBranch {
	newNode := &MemBranch{
		height:  node.height,
		size:    node.size,
		key:     node.key,
		value:   node.value,
		version: version,
	}
	newNode.left.Store(node.left.Load())
	newNode.right.Store(node.right.Load())
	return newNode
}

func (node *MemBranch) Height() uint8 {
	return node.height
}

func (node *MemBranch) IsLeaf() bool {
	return node.height == 0
}

func (node *MemBranch) Size() int64 {
	return node.size
}

func (node *MemBranch) Version() uint64 {
	return node.version
}

func (node *MemBranch) Key() []byte {
	return node.key
}

func (node *MemBranch) Value() []byte {
	return node.value
}

func (node *MemBranch) Left() Node {
	return *node.left.Load()
}

func (node *MemBranch) Right() Node {
	return *node.right.Load()
}

func (node *MemBranch) SafeHash() []byte {
	return node.Hash()
}

// Hash Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *MemBranch) Hash() []byte {
	if node == nil {
		return nil
	}
	if node.hash != nil {
		return node.hash
	}
	node.hash = HashNode(node)
	return node.hash
}

func (node *MemBranch) updateHeightSize() {
	node.height = max(node.Left().Height(), node.Right().Height()) + 1
	node.size = node.Left().Size() + node.Right().Size()
}

func (node *MemBranch) calcBalance() int {
	return int(node.Left().Height()) - int(node.Right().Height())
}

func calcBalance(node Node) int {
	return int(node.Left().Height()) - int(node.Right().Height())
}

// Invariant: node is returned by `Mutate(version)`.
//
//	   S               L
//	  / \      =>     / \
//	 L                   S
//	/ \                 / \
//	  LR               LR
func (node *MemBranch) rotateRight(version uint64) *MemBranch {
	newSelf := node.Left().MutateBranch(version)
	leftRight := node.Left().Right()
	node.left.Store(&leftRight)
	//newSelf.right.Store(node)
	node.updateHeightSize()
	newSelf.updateHeightSize()
	return newSelf
}

// Invariant: node is returned by `Mutate(version, cowVersion)`.
//
//	 S              R
//	/ \     =>     / \
//	    R         S
//	   / \       / \
//	 RL             RL
func (node *MemBranch) rotateLeft(version, cowVersion uint32) *MemBranch {
	newSelf := node.right.Mutate(version, cowVersion)
	node.right = node.right.Left()
	newSelf.left = node
	node.updateHeightSize()
	newSelf.updateHeightSize()
	return newSelf
}

// Invariant: node is returned by `Mutate(version, cowVersion)`.
func (node *MemBranch) reBalance(version, cowVersion uint32) *MemBranch {
	balance := node.calcBalance()
	switch {
	case balance > 1:
		leftBalance := calcBalance(node.left)
		if leftBalance >= 0 {
			// left left
			return node.rotateRight(version, cowVersion)
		}
		// left right
		node.left = node.left.Mutate(version, cowVersion).rotateLeft(version, cowVersion)
		return node.rotateRight(version, cowVersion)
	case balance < -1:
		rightBalance := calcBalance(node.right)
		if rightBalance <= 0 {
			// right right
			return node.rotateLeft(version, cowVersion)
		}
		// right left
		node.right = node.right.Mutate(version, cowVersion).rotateRight(version, cowVersion)
		return node.rotateLeft(version, cowVersion)
	default:
		// nothing changed
		return node
	}
}

var _ Node = &MemBranch{}
