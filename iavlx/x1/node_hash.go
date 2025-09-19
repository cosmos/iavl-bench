package x1

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sync"
)

// Hash computes the hash of the node.
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
	n = binary.PutVarint(buf[:], int64(node.version))
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
