package x1

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type RollingDiffWriter struct {
	file        *os.File
	branchIndex uint32
	leafIndex   uint32
}

func (r *RollingDiffWriter) WriteRoot(root *Node) {

}

func (r *RollingDiffWriter) writeNodePointer(ptr *NodePointer) (start int64, end int64, err error) {
	if ptr == nil {
		return 0, 0, fmt.Errorf("ptr is nil")
	}
	node := ptr.ptr.Load()
	if node == nil {
		return 0, 0, fmt.Errorf("ptr.ptr is nil")
	}

	return r.writeNode(node)
}

func (r *RollingDiffWriter) writeNode(node *Node) (start int64, end int64, err error) {
	if !node.isLeaf() {
		return r.writeBranch(node)
	} else {
		panic("not implemented")
	}
}

func (r *RollingDiffWriter) writeBranch(node *Node) (start int64, end int64, err error) {
	start, err = r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}

	_, err = r.file.Write([]byte{byte(node.subtreeHeight)}) // branch height
	if err != nil {
		return
	}

	// skip 24 bytes (self size, left pointer, right pointer)
	var padding [24]byte
	_, err = r.file.Write(padding[:])
	if err != nil {
		return
	}

	// write version as varint
	var varintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(varintBuf[:], uint64(node.version))
	_, err = r.file.Write(varintBuf[:n])
	if err != nil {
		return
	}

	// write size as varint
	n = binary.PutUvarint(varintBuf[:], uint64(node.size))
	_, err = r.file.Write(varintBuf[:n])
	if err != nil {
		return
	}

	// write key length as varint
	n = binary.PutUvarint(varintBuf[:], uint64(len(node.key)))
	_, err = r.file.Write(varintBuf[:n])
	if err != nil {
		return
	}

	// write key
	_, err = r.file.Write(node.key)
	if err != nil {
		return
	}

	// write hash (32 bytes)
	if len(node.hash) != 32 {
		return 0, 0, fmt.Errorf("invalid hash length: %d", len(node.hash))
	}
	_, err = r.file.Write(node.hash)
	if err != nil {
		return
	}

	end, err = r.file.Seek(0, io.SeekCurrent)

	// write left child
	leftStart, _, err := r.writeNodePointer(node.left)
	if err != nil {
		return
	}

	// write right child
	rightStart, rightEnd, err := r.writeNodePointer(node.right)
	if err != nil {
		return
	}

	size := rightEnd - start
	// go back and write size, left pointer, right pointer
	_, err = r.file.Seek(start+1, io.SeekStart)
	if err != nil {
		return
	}

	// write size as 64-bit little endian
	err = binary.Write(r.file, binary.LittleEndian, uint64(size))
	if err != nil {
		return
	}

	// write left pointer as 64-bit little endian
	err = binary.Write(r.file, binary.LittleEndian, uint64(leftStart))
	if err != nil {
		return
	}

	// write right pointer as 64-bit little endian
	err = binary.Write(r.file, binary.LittleEndian, uint64(rightStart))
	if err != nil {
		return
	}

	return
}
