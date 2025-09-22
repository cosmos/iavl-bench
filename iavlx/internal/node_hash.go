package internal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sync"
)

// Writes the node's hash to the given `io.Writer`. This function recursively calls
// children to update hashes.
func writeHashBytes(node Node, w io.Writer) error {
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
		left, err := node.Left().Resolve()
		if err != nil {
			return fmt.Errorf("resolving left, %w", err)
		}

		leftHash, err := left.Hash()
		if err != nil {
			return fmt.Errorf("getting left hash, %w", err)
		}

		right, err := node.Right().Resolve()
		if err != nil {
			return fmt.Errorf("resolving right, %w", err)
		}

		rightHash, err := right.Hash()
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

// HashNode computes the hash of the node.
func HashNode(node Node) ([]byte, error) {
	if node == nil {
		return nil, nil
	}
	h := hashPool.Get().(hash.Hash)
	if err := writeHashBytes(node, h); err != nil {
		return nil, err
	}
	h.Reset()
	hashPool.Put(h)
	return h.Sum(nil), nil
}

// VerifyHash compare node's cached hash with computed one
func VerifyHash(node Node) (bool, error) {
	hash, err := HashNode(node)
	if err != nil {
		return false, err
	}

	nodeHash, err := node.Hash()
	if err != nil {
		return false, err
	}

	return bytes.Equal(hash, nodeHash), nil
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
