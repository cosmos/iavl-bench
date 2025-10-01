package x3

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
)

type KVData interface {
	ReadK(offset uint32) (key []byte, err error)
	ReadKV(offset uint32) (key, value []byte, err error)
}

type KVDataWriter struct {
	*FileWriter
}

func NewKVDataWriter(filename string) (*KVDataWriter, error) {
	file, err := NewFileWriter(filename)
	if err != nil {
		return nil, err
	}
	return &KVDataWriter{
		FileWriter: file,
	}, nil

}

type KVDataReader struct {
	*MmapFile
}

func NewKVDataReader(file *os.File) (*KVDataReader, error) {
	mmap, err := NewMmapFile(file)
	if err != nil {
		return nil, err
	}
	return &KVDataReader{
		MmapFile: mmap,
	}, nil
}

func (kvs *KVDataReader) ReadK(offset uint32) (key []byte, err error) {
	bz, err := kvs.UnsafeSliceExact(int(offset), 4)
	if err != nil {
		return nil, err
	}
	lenKey := binary.LittleEndian.Uint32(bz)

	return kvs.UnsafeSliceExact(int(offset)+4, int(lenKey))
}

func (kvs *KVDataReader) ReadKV(offset uint32) (key, value []byte, err error) {
	key, err = kvs.ReadK(offset)
	if err != nil {
		return nil, nil, err
	}

	value, err = kvs.ReadK(offset + 4 + uint32(len(key)))
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

var _ KVData = (*KVDataReader)(nil)

func (kvs *KVDataWriter) WriteK(key []byte) (offset uint32, err error) {
	lenKey := len(key)
	if lenKey > math.MaxUint32 {
		return 0, fmt.Errorf("key too large: %d bytes", lenKey)
	}

	offset = uint32(kvs.Size())

	// write little endian uint32 length prefix
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(lenKey))
	_, err = kvs.Write(lenBuf[:])
	if err != nil {
		return 0, err
	}

	// write key bytes
	_, err = kvs.Write(key)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func (kvs *KVDataWriter) WriteKV(key, value []byte) (offset uint32, err error) {
	offset, err = kvs.WriteK(key)
	if err != nil {
		return 0, err
	}
	_, err = kvs.WriteK(value)
	return offset, err
}
