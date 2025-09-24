package internal

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

type MmapFile struct {
	flushLock    sync.RWMutex
	file         *os.File
	writer       *bufio.Writer
	handle       mmap.MMap
	bytesWritten int
	bytesFlushed int
}

func NewMmapFile(path string) (*MmapFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	// check file size
	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to stat file %s: %w", path, err)
	}

	writer := bufio.NewWriter(file)

	res := &MmapFile{
		file:   file,
		writer: writer,
	}

	if fi.Size() == 0 {
		return res, nil
	}

	handle, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to mmap file %s: %w", path, err)
	}

	res.handle = handle
	return res, nil
}

func (m *MmapFile) SliceVar(offset, maxSize int) (int, []byte, error) {
	m.flushLock.RLock()
	defer m.flushLock.RUnlock()

	if offset >= len(m.handle) {
		return 0, nil, fmt.Errorf("trying to read beyond mapped data: %d >= %d", offset, len(m.handle))
	}
	if offset+maxSize > len(m.handle) {
		maxSize = len(m.handle) - offset
	}
	data := m.handle[offset : offset+maxSize]
	// make a copy of the data to avoid data being changed after remap
	copied := make([]byte, maxSize)
	copy(copied, data)
	return maxSize, copied, nil
}

func (m *MmapFile) SliceExact(offset, size int) ([]byte, error) {
	m.flushLock.RLock()
	defer m.flushLock.RUnlock()

	//var buf = make([]byte, size)
	//n, err := m.file.ReadAt(buf, int64(offset))
	//if err != nil {
	//	return nil, fmt.Errorf("failed to read file at offset %d size %d: %w", offset, size, err)
	//}
	//if n != size {
	//	return nil, fmt.Errorf("short read file at offset %d size %d: read %d bytes", offset, size, n)
	//}
	//return buf, nil

	if offset+size > len(m.handle) {
		return nil, fmt.Errorf("trying to read beyond mapped data: %d + %d >= %d", offset, size, len(m.handle))
	}
	data := m.handle[offset : offset+size]
	// make a copy of the data to avoid data being changed after remap
	copied := make([]byte, size)
	copy(copied, data)
	return copied, nil
}

func (m *MmapFile) Offset() int {
	if m.handle == nil {
		return 0
	}
	return len(m.handle)
}

func (m *MmapFile) Write(p []byte) (n int, err error) {
	m.bytesWritten += len(p)
	return m.writer.Write(p)
}

func (m *MmapFile) SaveAndRemap() error {
	if err := m.flush(); err != nil {
		return err
	}

	m.flushLock.Lock()
	defer m.flushLock.Unlock()

	// unmap existing mapping
	if m.handle != nil {
		if err := m.handle.Unmap(); err != nil {
			return fmt.Errorf("failed to unmap existing mapping: %w", err)
		}
		m.handle = nil
	}

	// remap file
	fi, err := m.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	if fi.Size() > 0 {
		handle, err := mmap.Map(m.file, mmap.RDONLY, 0)
		if err != nil {
			return fmt.Errorf("failed to remap file: %w", err)
		}
		m.handle = handle
	}

	m.bytesFlushed = m.bytesWritten

	return nil
}

func (m *MmapFile) flush() error {
	// flush writer buffer
	if err := m.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	// sync file to disk
	if err := m.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}
	return nil
}

func (m *MmapFile) Close() error {
	err := m.flush()
	if err != nil {
		_ = m.file.Close()
		return err
	}

	if m.handle != nil {
		if err := m.handle.Unmap(); err != nil {
			_ = m.file.Close()
			return err
		}
	}

	return m.file.Close()
}

var _ io.Writer = &MmapFile{}
var _ io.Closer = &MmapFile{}

type LeavesFile struct {
	*MmapFile
}

func (leaves LeavesFile) Leaf(i uint64) (LeafLayout, error) {
	leaves.flushLock.RLock()
	defer leaves.flushLock.RUnlock()

	offset := int(i) * SizeLeaf
	if offset+SizeLeaf > len(leaves.handle) {
		return LeafLayout{}, fmt.Errorf("trying to read beyond mapped data: %d + %d >= %d", offset, SizeLeaf, len(leaves.handle))
	}
	return LeafLayout{data: ([SizeLeaf]byte)(leaves.handle[offset : offset+SizeLeaf])}, nil
}

func (leaves LeavesFile) Count() uint64 {
	return uint64(len(leaves.handle)) / SizeLeaf
}

type BranchesFile struct {
	*MmapFile
}

func (branches BranchesFile) Branch(i uint64) (BranchLayout, error) {
	branches.flushLock.RLock()
	defer branches.flushLock.RUnlock()

	offset := int(i) * SizeBranch
	if offset+SizeBranch > len(branches.handle) {
		return BranchLayout{}, fmt.Errorf("trying to read beyond mapped data: %d + %d >= %d", offset, SizeBranch, len(branches.handle))
	}
	return BranchLayout{data: ([SizeBranch]byte)(branches.handle[offset : offset+SizeBranch])}, nil
}

func (branches BranchesFile) Count() uint64 {
	return uint64(len(branches.handle)) / SizeBranch
}
