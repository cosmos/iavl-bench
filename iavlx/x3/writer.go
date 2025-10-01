package x3

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"unsafe"
)

type FileWriter struct {
	file    *os.File
	writer  *bufio.Writer
	written int
}

func NewFileWriter(path string) (*FileWriter, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	return &FileWriter{
		file:   file,
		writer: bufio.NewWriter(file),
	}, nil
}

func (f *FileWriter) Write(p []byte) (n int, err error) {
	n, err = f.writer.Write(p)
	f.written += n
	return n, err
}

func (f *FileWriter) Flush() error {
	if err := f.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}
	return nil
}

func (f *FileWriter) Size() int {
	return f.written
}

func (f *FileWriter) Dispose() (*os.File, error) {
	if err := f.Flush(); err != nil {
		_ = f.file.Close()
		return nil, err
	}
	return f.file, nil
}

var _ io.Writer = (*FileWriter)(nil)

type StructWriter[T any] struct {
	size int
	*FileWriter
}

func NewStructWriter[T any](path string) (*StructWriter[T], error) {
	fw, err := NewFileWriter(path)
	if err != nil {
		return nil, err
	}

	return &StructWriter[T]{
		size:       int(unsafe.Sizeof(*new(T))),
		FileWriter: fw,
	}, nil
}

func (sw *StructWriter[T]) Append(x *T) error {
	_, err := sw.Write(unsafe.Slice((*byte)(unsafe.Pointer(x)), sw.size))
	return err
}

func (sw *StructWriter[T]) Count() int {
	return sw.written / sw.size
}
