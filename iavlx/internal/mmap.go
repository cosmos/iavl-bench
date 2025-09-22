package internal

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/edsrzf/mmap-go"
)

type MmapFile struct {
	flushLock   sync.RWMutex
	writeLock   sync.Mutex
	file        *os.File
	handle      mmap.MMap
	writeBuffer []byte
}

func NewMmapFile(path string) (*MmapFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	handle, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return &MmapFile{
		file:   file,
		handle: handle,
	}, nil
}

func (m *MmapFile) Slice(offset, size int) ([]byte, error) {
	m.flushLock.RLock()
	defer m.flushLock.RUnlock()

	if offset >= len(m.handle) {
		// read from write buffer
		offset -= len(m.handle)
		if offset+size >= len(m.writeBuffer) {
			return nil, fmt.Errorf("trying to read beyond write buffer: %d + %d >= %d", offset, size, len(m.writeBuffer))
		}
		return m.writeBuffer[offset : offset+size], nil
	} else {
		if offset+size >= len(m.handle) {
			return nil, fmt.Errorf("trying to read beyond mapped data: %d + %d >= %d", offset, size, len(m.handle))
		}
		return m.handle[offset : offset+size], nil
	}
}

func (m *MmapFile) Offset() int {
	return len(m.handle) + len(m.writeBuffer)
}

func (m *MmapFile) Write(p []byte) (n int, err error) {
	m.writeLock.Lock()
	defer m.writeLock.Unlock()

	m.writeBuffer = append(m.writeBuffer, p...)
	return len(p), nil
}

func (m *MmapFile) SaveAndRemap() error {
	m.flushLock.Lock()
	defer m.flushLock.Unlock()
	m.writeLock.Lock()
	defer m.writeLock.Unlock()

	if len(m.writeBuffer) == 0 {
		return nil
	}

	// unmap existing mapping
	if err := m.handle.Unmap(); err != nil {
		return err
	}

	// extend file
	if _, err := m.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	if _, err := m.file.Write(m.writeBuffer); err != nil {
		return err
	}

	// sync file to disk
	if err := m.file.Sync(); err != nil {
		return err
	}

	// remap file
	handle, err := mmap.Map(m.file, mmap.RDONLY, 0)
	if err != nil {
		return err
	}

	m.handle = handle
	m.writeBuffer = nil
	return nil
}

func (m *MmapFile) Close() error {
	//TODO implement me
	panic("implement me")
}

var _ io.Writer = &MmapFile{}
var _ io.Closer = &MmapFile{}
