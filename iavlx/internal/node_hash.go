package internal

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sync"
)

// ComputeHashAndAssignIDs computes the hash of the node pointed to by `np` and assigns
// node IDs for the current staged version.
// These two are done in one traversal to improve performance because node ID assignment
// is based on pre-order traversal which is the order we traverse here.
// If `assigner` is nil, node IDs are not assigned.
func ComputeHashAndAssignIDs(np *NodePointer, assigner *NodeIDAssigner) ([]byte, error) {
	memNode := np.mem.Load()
	if memNode != nil {
		// if we've already computed the hash, return it
		if memNode.hash != nil {
			return memNode.hash, nil
		}

		// first assign node ID if needed (visit root first then children for pre-order traversal)
		if assigner != nil && memNode.version == assigner.version {
			if memNode.IsLeaf() {
				assigner.leafNodeIdx++
				np.id = NewNodeID(true, assigner.version, assigner.leafNodeIdx)
			} else {
				assigner.branchNodeIdx++
				np.id = NewNodeID(false, assigner.version, assigner.branchNodeIdx)
			}
		}

		// now compute hash which also causes left and right sub-trees to have their node IDs assigned
		hasher := hashPool.Get().(hash.Hash)
		if err := writeHashBytes(memNode, hasher, assigner); err != nil {
			return nil, err
		}
		hasher.Reset()
		hashPool.Put(hasher)
		h := hasher.Sum(nil)
		memNode.hash = h

		return h, nil
	}
	node, err := np.Resolve()
	if err != nil {
		return nil, err
	}
	return node.Hash(), nil
}

// Writes the node's hash to the given `io.Writer`. This function recursively calls
// children to update hashes.
func writeHashBytes(node Node, w io.Writer, assigner *NodeIDAssigner) error {
	var (
		n   int
		buf [binary.MaxVarintLen64]byte
	)

	n = binary.PutVarint(buf[:], int64(node.Height()))
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing height, %w", err)
	}
	n = binary.PutVarint(buf[:], node.Size())
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing size, %w", err)
	}
	n = binary.PutVarint(buf[:], int64(node.Version()))
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing version, %w", err)
	}

	// Key is not written for inner nodes, unlike writeBytes.

	if node.IsLeaf() {
		key, err := node.Key()
		if err != nil {
			return fmt.Errorf("getting key, %w", err)
		}

		if err := EncodeBytes(w, key); err != nil {
			return fmt.Errorf("writing key, %w", err)
		}

		// Indirection needed to provide proofs without values.
		// (e.g. ProofLeafNode.ValueHash)
		value, err := node.Value()
		if err != nil {
			return fmt.Errorf("getting value, %w", err)
		}
		valueHash := sha256.Sum256(value)

		if err := EncodeBytes(w, valueHash[:]); err != nil {
			return fmt.Errorf("writing value, %w", err)
		}
	} else {
		leftHash, err := ComputeHashAndAssignIDs(node.Left(), assigner)
		if err != nil {
			return fmt.Errorf("getting left hash, %w", err)
		}

		rightHash, err := ComputeHashAndAssignIDs(node.Right(), assigner)
		if err != nil {
			return fmt.Errorf("getting right hash, %w", err)
		}

		if err := EncodeBytes(w, leftHash); err != nil {
			return fmt.Errorf("writing left hash, %w", err)
		}
		if err := EncodeBytes(w, rightHash); err != nil {
			return fmt.Errorf("writing right hash, %w", err)
		}
	}

	return nil
}

var (
	hashPool = &sync.Pool{
		New: func() any {
			return sha256.New()
		},
	}
	emptyHash = sha256.New().Sum(nil)
)

type NodeIDAssigner struct {
	version       uint64
	branchNodeIdx uint32
	leafNodeIdx   uint32
}

// EncodeBytes writes a varint length-prefixed byte slice to the writer,
// it's used for hash computation, must be compactible with the official IAVL implementation.
func EncodeBytes(w io.Writer, bz []byte) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], uint64(len(bz)))
	if _, err := w.Write(buf[0:n]); err != nil {
		return err
	}
	_, err := w.Write(bz)
	return err
}
