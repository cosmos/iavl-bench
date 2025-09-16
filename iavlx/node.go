package iavlx

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sync"
	"sync/atomic"
)

// NodeKey uniquely represents nodes.
// For now the first 32 bits represent the version and the last 32 bits represent
// order in which the node was created in that version with the high bit distinguishing
// between leaf and internal nodes.
type NodeKey uint64

func (k NodeKey) Version() int64 {
	return int64(uint32(k >> 32))
}

func NewLeafNodeKey(version uint32, seq uint32) NodeKey {
	return NodeKey(uint64(version))<<32 | NodeKey(seq) | 0x80000000
}

func NewBranchNodeKey(version uint32, seq uint32) NodeKey {
	return NodeKey(uint64(version))<<32 | NodeKey(seq&0x7FFFFFFF)
}

type NodeReader interface {
	Load(*NodePointer) (*Node, error)
}

type NodeFactory interface {
	NodeReader
	NewLeafNode(key, value []byte) *Node
	NewBranchNode() *Node
	CopyLeafNode(node *Node, newValue []byte) *Node
	CopyNode(*Node) *Node
	DeleteNode(node *Node)
}

type NodeWriter interface {
	NodeReader
	SaveNode(*Node) error
	DeleteNode(*Node) error
}

type nodeStatic struct {
	key           []byte
	value         []byte
	hash          []byte
	nodeKey       NodeKey
	size          int64
	subtreeHeight int8
}

type Node struct {
	nodeStatic
	left  *NodePointer
	right *NodePointer
}

func NewNode() *Node {
	return &Node{
		left:  &NodePointer{},
		right: &NodePointer{},
	}
}

type NodePointer struct {
	ptr atomic.Pointer[Node]
	key NodeKey
}

func (n *NodePointer) Get(store NodeReader) (*Node, error) {
	node := n.ptr.Load()
	if node != nil {
		return node, nil
	}
	return store.Load(n)
}

func (n *NodePointer) Set(node *Node) {
	n.ptr.Store(node)
	n.key = node.nodeKey
}

// TODO instead of copying like this, can we just copy the pointer and then replace it instead of setting so that eviction is simpler?
func (n *NodePointer) CopyFrom(ptr *NodePointer) {
	n.ptr.Store(ptr.ptr.Load())
	n.key = ptr.key
}

func (node *Node) get(store NodeReader, key []byte) (index int64, value []byte, err error) {
	if node.isLeaf() {
		switch bytes.Compare(node.key, key) {
		case -1:
			return 1, nil, nil
		case 1:
			return 0, nil, nil
		default:
			return 0, node.value, nil
		}
	}

	if bytes.Compare(key, node.key) < 0 {
		leftNode, err := node.left.Get(store)
		if err != nil {
			return 0, nil, err
		}

		return leftNode.get(store, key)
	}

	rightNode, err := node.right.Get(store)
	if err != nil {
		return 0, nil, err
	}

	index, value, err = rightNode.get(store, key)
	if err != nil {
		return 0, nil, err
	}

	index += node.size - rightNode.size
	return index, value, nil
}

func (node *Node) isLeaf() bool {
	return node.subtreeHeight == 0
}

func (node *Node) calcBalance(store NodeReader) (int, error) {
	leftNode, err := node.left.Get(store)
	if err != nil {
		return 0, err
	}

	rightNode, err := node.right.Get(store)
	if err != nil {
		return 0, err
	}

	return int(leftNode.subtreeHeight) - int(rightNode.subtreeHeight), nil
}

// IMPORTANT: nodes that call this method must be new or copies first
func (node *Node) balance(store NodeFactory) (*Node, error) {
	balance, err := node.calcBalance(store)
	if err != nil {
		return nil, err
	}
	switch {
	case balance > 1:
		left, err := node.left.Get(store)
		if err != nil {
			return nil, err
		}

		leftBalance, err := left.calcBalance(store)
		if err != nil {
			return nil, err
		}

		if leftBalance >= 0 {
			// left left
			return node.rotateRight(store)
		}

		// left right
		left, err = store.CopyNode(left).rotateLeft(store)
		if err != nil {
			return nil, err
		}
		node.left.Set(left)
		return node.rotateRight(store)
	case balance < -1:
		right, err := node.right.Get(store)
		if err != nil {
			return nil, err
		}

		rightBalance, err := right.calcBalance(store)
		if err != nil {
			return nil, err
		}

		if rightBalance <= 0 {
			// right right
			return node.rotateLeft(store)
		}

		// right left
		right, err = store.CopyNode(right).rotateRight(store)
		if err != nil {
			return nil, err
		}
		node.right.Set(right)
		return node.rotateLeft(store)
	default:
		// nothing changed
		return node, err
	}
}

// IMPORTANT: nodes that call this method must be new or copies first
func (node *Node) rotateRight(store NodeFactory) (*Node, error) {
	left, err := node.left.Get(store)
	if err != nil {
		return nil, err
	}
	newSelf := store.CopyNode(left)
	leftRight, err := left.right.Get(store)
	if err != nil {
		return nil, err
	}
	node.left.Set(leftRight)
	newSelf.right.Set(node)

	err = node.updateHeightSize(store)
	if err != nil {
		return nil, err
	}
	err = newSelf.updateHeightSize(store)
	if err != nil {
		return nil, err
	}

	return newSelf, nil
}

// IMPORTANT: nodes that call this method must be new or copies first
func (node *Node) rotateLeft(store NodeFactory) (*Node, error) {
	right, err := node.right.Get(store)
	if err != nil {
		return nil, err
	}
	newSelf := store.CopyNode(right)
	rightLeft, err := right.left.Get(store)
	if err != nil {
		return nil, err
	}
	node.right.Set(rightLeft)
	newSelf.left.Set(node)

	err = node.updateHeightSize(store)
	if err != nil {
		return nil, err
	}

	err = newSelf.updateHeightSize(store)
	if err != nil {
		return nil, err
	}

	return newSelf, nil
}

// IMPORTANT: nodes that call this method must be new or copies first
func (node *Node) updateHeightSize(store NodeFactory) error {
	leftNode, err := node.left.Get(store)
	if err != nil {
		return err
	}

	rightNode, err := node.right.Get(store)
	if err != nil {
		return err
	}

	node.subtreeHeight = maxInt8(leftNode.subtreeHeight, rightNode.subtreeHeight) + 1
	node.size = leftNode.size + rightNode.size
	return nil
}

// Computes the hash of the node without computing its descendants. Must be
// called on nodes which have descendant node hashes already computed.
func (node *Node) Hash(store NodeReader) ([]byte, error) {
	if node.hash != nil {
		return node.hash, nil
	}

	h := hashPool.Get().(hash.Hash)
	if err := node.writeHashBytes(h, store); err != nil {
		return nil, err
	}
	node.hash = h.Sum(nil)
	h.Reset()
	hashPool.Put(h)

	return node.hash, nil
}

var (
	hashPool = &sync.Pool{
		New: func() any {
			return sha256.New()
		},
	}
	emptyHash = sha256.New().Sum(nil)
)

// Writes the node's hash to the given `io.Writer`. This function recursively calls
// children to update hashes.
func (node *Node) writeHashBytes(w io.Writer, store NodeReader) error {
	var (
		n   int
		buf [binary.MaxVarintLen64]byte
	)

	n = binary.PutVarint(buf[:], int64(node.subtreeHeight))
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing height, %w", err)
	}
	n = binary.PutVarint(buf[:], node.size)
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing size, %w", err)
	}
	n = binary.PutVarint(buf[:], node.nodeKey.Version())
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing version, %w", err)
	}

	// Key is not written for inner nodes, unlike writeBytes.

	if node.isLeaf() {
		if err := encodeBytes(w, node.key); err != nil {
			return fmt.Errorf("writing key, %w", err)
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		valueHash := sha256.Sum256(node.value)

		if err := encodeBytes(w, valueHash[:]); err != nil {
			return fmt.Errorf("writing value, %w", err)
		}
	} else {
		left, err := node.left.Get(store)
		if err != nil {
			return fmt.Errorf("getting left node, %w", err)
		}

		leftHash, err := left.Hash(store)
		if err != nil {
			return fmt.Errorf("getting left hash, %w", err)
		}

		if err := encodeBytes(w, leftHash); err != nil {
			return fmt.Errorf("writing left hash, %w", err)
		}

		right, err := node.right.Get(store)
		if err != nil {
			return fmt.Errorf("getting right node, %w", err)
		}

		rightHash, err := right.Hash(store)
		if err != nil {
			return fmt.Errorf("getting right hash, %w", err)
		}

		if err := encodeBytes(w, rightHash); err != nil {
			return fmt.Errorf("writing right hash, %w", err)
		}
	}

	return nil
}

func encodeBytes(w io.Writer, bz []byte) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(bz)))
	if _, err := w.Write(buf[0:n]); err != nil {
		return err
	}
	_, err := w.Write(bz)
	return err
}

func (node *Node) copy() *Node {
	newNode := NewNode()
	newNode.nodeStatic = node.nodeStatic
	newNode.left.CopyFrom(node.left)
	newNode.right.CopyFrom(node.right)
	return newNode
}

func maxInt8(a, b int8) int8 {
	if a > b {
		return a
	}
	return b
}

// setRecursive do set operation.
// returns if it's an update or insertion, if update, the tree height and balance is not changed.
func setRecursive(store NodeFactory, node *Node, key, value []byte) (*Node, bool, error) {
	if node == nil {
		return store.NewLeafNode(key, value), true, nil
	}

	if node.isLeaf() {
		cmp := bytes.Compare(key, node.key)
		if cmp == 0 {
			// just updating value
			return store.CopyLeafNode(node, value), true, nil
		}

		// need to create a new internal node
		newNode := store.NewBranchNode()
		newNode.subtreeHeight = 1
		newNode.size = 2
		switch cmp {
		case -1:
			newNode.key = node.key
			newNode.left.Set(store.NewLeafNode(key, value))
			newNode.right.Set(node)
		case 1:
			newNode.key = key
			newNode.left.Set(node)
			newNode.right.Set(store.NewLeafNode(key, value))
		default:
			panic("unreachable")
		}
		return newNode, false, nil
	} else {
		var (
			newChild *Node
			updated  bool
		)
		newNode := store.CopyNode(node)
		if bytes.Compare(key, node.key) == -1 {
			left, err := node.left.Get(store)
			if err != nil {
				return nil, false, err
			}
			newChild, updated, err = setRecursive(store, left, key, value)
			if err != nil {
				return nil, false, err
			}
			newNode.left.Set(newChild)
		} else {
			right, err := node.right.Get(store)
			if err != nil {
				return nil, false, err
			}
			newChild, updated, err = setRecursive(store, right, key, value)
			if err != nil {
				return nil, false, err
			}
			newNode.right.Set(newChild)
		}

		if !updated {
			err := newNode.updateHeightSize(store)
			if err != nil {
				return nil, false, err
			}
			newNode, err = newNode.balance(store)
			if err != nil {
				return nil, false, err
			}
		}

		return newNode, updated, nil
	}
}

func newLeafNode(key, value []byte) *Node {
	node := NewNode()
	node.key = key
	node.value = value
	node.size = 1
	return node
}

// removeRecursive returns:
// - (nil, origNode, nil, nil) -> nothing changed in subtree
// - (value, nil, newKey, nil) -> leaf node is removed
// - (value, new node, newKey, nil) -> subtree changed
func removeRecursive(store NodeFactory, node *Node, key []byte) (value []byte, newNode *Node, newKey []byte, err error) {
	if node == nil {
		return nil, nil, nil, nil
	}

	if node.isLeaf() {
		if bytes.Equal(node.key, key) {
			return node.value, nil, nil, nil
		}
		return nil, node, nil, nil
	}

	if bytes.Compare(key, node.key) == -1 {
		left, err := node.left.Get(store)
		if err != nil {
			return nil, nil, nil, err
		}
		value, newLeft, newKey, err := removeRecursive(store, left, key)
		if err != nil {
			return nil, nil, nil, err
		}
		if value == nil {
			return nil, node, nil, nil
		}
		if newLeft == nil {
			right, err := node.right.Get(store)
			if err != nil {
				return nil, nil, nil, err
			}
			return value, right, node.key, nil
		}

		newNode := store.CopyNode(node)
		newNode.left.Set(newLeft)
		err = newNode.updateHeightSize(store)
		if err != nil {
			return nil, nil, nil, err
		}
		newNode, err = newNode.balance(store)
		if err != nil {
			return nil, nil, nil, err
		}

		return value, newNode, newKey, nil
	}

	right, err := node.right.Get(store)
	if err != nil {
		return nil, nil, nil, err
	}
	value, newRight, newKey, err := removeRecursive(store, right, key)
	if err != nil {
		return nil, nil, nil, err
	}

	if value == nil {
		return nil, node, nil, nil
	}
	left, err := node.left.Get(store)
	if err != nil {
		return nil, nil, nil, err
	}
	if newRight == nil {
		return value, left, nil, nil
	}

	newNode = store.CopyNode(node)
	newNode.right.Set(newRight)
	if newKey != nil {
		newNode.key = newKey
	}
	err = newNode.updateHeightSize(store)
	if err != nil {
		return nil, nil, nil, err
	}

	newNode, err = newNode.balance(store)
	if err != nil {
		return nil, nil, nil, err
	}

	return value, newNode, nil, nil
}
