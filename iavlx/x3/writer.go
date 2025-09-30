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
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
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
	if err := f.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}
	return nil
}

func (f *FileWriter) Size() int {
	return f.written
}

func (f *FileWriter) Close() error {
	if err := f.Flush(); err != nil {
		_ = f.file.Close()
		return err
	}
	return f.file.Close()
}

var _ io.Closer = (*FileWriter)(nil)
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
