package x3

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
)

type MmapFile struct {
	file   *os.File
	handle mmap.MMap
}

func NewMmapFile(path string) (*MmapFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	// Check file size - cannot mmap empty files
	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to stat file %s: %w", path, err)
	}
	if fi.Size() == 0 {
		_ = file.Close()
		return nil, fmt.Errorf("cannot mmap empty file: %s", path)
	}

	res := &MmapFile{
		file: file,
	}

	// maybe we can make read/write configurable? not sure if the OS optimizes read-only mapping
	handle, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to mmap file %s: %w", path, err)
	}

	res.handle = handle
	return res, nil
}

func (m *MmapFile) UnsafeSliceVar(offset, maxSize int) (int, []byte, error) {
	if offset >= len(m.handle) {
		return 0, nil, fmt.Errorf("trying to read beyond mapped data: %d >= %d", offset, len(m.handle))
	}
	if offset+maxSize > len(m.handle) {
		maxSize = len(m.handle) - offset
	}
	data := m.handle[offset : offset+maxSize]
	// make a copy of the data to avoid data being changed after remap
	return maxSize, data, nil
}

func (m *MmapFile) UnsafeSliceExact(offset, size int) ([]byte, error) {
	if offset+size > len(m.handle) {
		return nil, fmt.Errorf("trying to read beyond mapped data: %d + %d >= %d", offset, size, len(m.handle))
	}
	bz := m.handle[offset : offset+size]
	return bz, nil
}

func (m *MmapFile) Data() []byte {
	return m.handle
}

func (m *MmapFile) Flush() error {
	if err := m.handle.Flush(); err != nil {
		return fmt.Errorf("failed to flush mmap: %w", err)
	}
	if err := m.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}
	return nil
}

func (m *MmapFile) Close() error {
	var unmapErr, closeErr error
	if m.handle != nil {
		unmapErr = m.handle.Unmap()
		m.handle = nil
	}
	if m.file != nil {
		closeErr = m.file.Close()
		m.file = nil
	}
	return errors.Join(unmapErr, closeErr)
}

var _ io.Closer = &MmapFile{}
