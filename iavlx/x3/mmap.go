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

func NewMmapFile(file *os.File) (*MmapFile, error) {
	// Check file size
	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	res := &MmapFile{
		file: file,
	}

	// Empty files are valid - just don't mmap them
	if fi.Size() == 0 {
		return res, nil
	}

	// maybe we can make read/write configurable? not sure if the OS optimizes read-only mapping
	handle, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to mmap file: %w", err)
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
	if m.handle != nil {
		if err := m.handle.Flush(); err != nil {
			return fmt.Errorf("failed to flush mmap: %w", err)
		}
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
