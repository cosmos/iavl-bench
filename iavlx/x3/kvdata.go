package x3

import (
	"encoding/binary"
	"fmt"
	"math"
)

type KVData interface {
	ReadK(offset uint32) (key []byte, err error)
	ReadKV(offset uint32) (key, value []byte, err error)
}

type KVDataStore struct {
	file *MmapFile
}

func NewKVDataStore(filename string) (*KVDataStore, error) {
	file, err := NewMmapFile(filename)
	if err != nil {
		return nil, err
	}
	return &KVDataStore{
		file: file,
	}, nil
}

func (kvs *KVDataStore) ReadK(offset uint32) (key []byte, err error) {
	bz, err := kvs.file.SliceExact(int(offset), 4)
	if err != nil {
		return nil, err
	}
	lenKey := binary.LittleEndian.Uint32(bz)

	return kvs.file.SliceExact(int(offset)+4, int(lenKey))
}

func (kvs *KVDataStore) ReadKV(offset uint32) (key, value []byte, err error) {
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

var _ KVData = (*KVDataStore)(nil)

func (kvs *KVDataStore) WriteK(key []byte) (offset uint32, err error) {
	lenKey := len(key)
	if lenKey > math.MaxUint32 {
		return 0, fmt.Errorf("key too large: %d bytes", lenKey)
	}

	offset = uint32(kvs.file.Offset())

	// write little endian uint32 length prefix
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(lenKey))
	_, err = kvs.file.Write(lenBuf[:])
	if err != nil {
		return 0, err
	}

	// write key bytes
	_, err = kvs.file.Write(key)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func (kvs *KVDataStore) WriteKV(key, value []byte) (offset uint32, err error) {
	offset, err = kvs.WriteK(key)
	if err != nil {
		return 0, err
	}
	_, err = kvs.WriteK(value)
	return offset, err
}

func (kvs *KVDataStore) SaveAndRemap() error {
	return kvs.file.SaveAndRemap()
}
