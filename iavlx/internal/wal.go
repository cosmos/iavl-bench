package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type WAL struct {
	walData    *MmapFile
	commitFile *os.File
	curOffset  int
	version    uint64
}

func OpenWAL(dir string, startVersion uint64) (*WAL, error) {
	walFilename := filepath.Join(dir, "wal.log")
	walData, err := NewMmapFile(walFilename)
	if err != nil {
		return nil, err
	}

	commitFilename := filepath.Join(dir, "wal.commit")
	commitFile, err := os.OpenFile(commitFilename, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		walData:    walData,
		commitFile: commitFile,
		curOffset:  0,
		version:    startVersion,
	}, nil
}

type WALEntryType byte

const (
	WALEntryTypeSet WALEntryType = iota
	WALEntryTypeDelete
	WALEntryTypeCommit
)

func (w *WAL) WriteUpdates(updates *KVUpdateBatch) error {
	var varintBytes [binary.MaxVarintLen64]byte
	for _, update := range updates.Updates {
		if setNode := update.SetNode; setNode != nil {
			n, err := w.walData.Write([]byte{byte(WALEntryTypeSet)})
			if err != nil {
				return err
			}
			w.curOffset += n

			// varint prefix len(key) + key + varint len(value) + value
			key := setNode.key
			lenKey := uint64(len(key))
			n = binary.PutUvarint(varintBytes[:], lenKey)
			n, err = w.walData.Write(varintBytes[:n])
			if err != nil {
				return err
			}
			w.curOffset += n

			// save the offset of the key for when leaf nodes are written to the leaves file
			// TODO handle case where key could be larger than 2^23-1 bytes - we need to set offset to start of varint
			setNode._keyRef = NewWALRef(uint32(lenKey), uint64(w.curOffset))

			n, err = w.walData.Write(key)
			if err != nil {
				return err
			}
			w.curOffset += n

			value := setNode.value
			lenValue := uint64(len(value))
			n = binary.PutUvarint(varintBytes[:], lenValue)
			n, err = w.walData.Write(varintBytes[:n])
			if err != nil {
				return err
			}
			w.curOffset += n

			n, err = w.walData.Write(value)
			if err != nil {
				return err
			}

			w.curOffset += n
		} else {
			n, err := w.walData.Write([]byte{byte(WALEntryTypeDelete)})
			if err != nil {
				return err
			}
			w.curOffset += n

			// varint prefix len(key) + key + varint len(value) + value
			key := update.DeleteKey
			lenKey := uint64(len(key))
			n = binary.PutUvarint(varintBytes[:], lenKey)
			n, err = w.walData.Write(varintBytes[:n])
			if err != nil {
				return err
			}
			w.curOffset += n

			n, err = w.walData.Write(key)
			if err != nil {
				return err
			}
			w.curOffset += n
		}
	}
	return nil
}

func (w *WAL) CommitNoSync() error {
	// write commit entry to WAL
	n, err := w.walData.Write([]byte{byte(WALEntryTypeCommit)})
	if err != nil {
		return err
	}
	w.curOffset += n
	w.version++

	var varintBytes [binary.MaxVarintLen64]byte
	n = binary.PutUvarint(varintBytes[:], w.version)
	n, err = w.walData.Write(varintBytes[:n])
	if err != nil {
		return err
	}
	w.curOffset += n

	// write version to commit file
	var bz [8]byte
	binary.LittleEndian.PutUint64(bz[:], w.version)
	_, err = w.commitFile.Write(bz[:])
	if err != nil {
		return err
	}

	// write offset to commit file
	binary.LittleEndian.PutUint64(bz[:], uint64(w.curOffset))
	_, err = w.commitFile.Write(bz[:])
	if err != nil {
		return err
	}

	return nil
}

func (w *WAL) CommitSync() error {
	err := w.CommitNoSync()
	if err != nil {
		return err
	}

	return w.walData.SaveAndRemap()
}

type KVData interface {
	Read(offset uint64, size uint32) ([]byte, error)
	ReadVarintBytes(offset uint64) (bz []byte, newOffset int, err error)
}

func (w *WAL) Read(offset uint64, size uint32) ([]byte, error) {
	return w.walData.SliceExact(int(offset), int(size))
}

func (w *WAL) ReadVarintBytes(offset uint64) (bz []byte, newOffset int, err error) {
	_, bz, err = w.walData.SliceVar(int(offset), binary.MaxVarintLen64)
	if err != nil {
		return nil, 0, err
	}
	length, n := binary.Uvarint(bz)
	if n <= 0 {
		return nil, 0, err
	}
	bz, err = w.walData.SliceExact(int(offset)+n, int(length))
	if err != nil {
		return nil, 0, err
	}
	return bz, int(offset) + n + int(length), nil
}

func (w *WAL) DebugDump(writer io.Writer) error {
	it := &WALIterator{wal: w, offset: 0}
	for {
		update, offset, err := it.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(writer, "%d: %s\n", offset, update.String())
	}
}

type WALIterator struct {
	wal    *WAL
	offset int
}

func (it *WALIterator) Next() (update Update, entryOffset int, err error) {
	if it.offset >= len(it.wal.walData.handle) {
		err = io.EOF
		return
	}
	bz, err := it.wal.walData.SliceExact(it.offset, 1)
	if err == io.EOF {
		return
	} else if err != nil {
		return
	}

	entryOffset = it.offset
	entryType := WALEntryType(bz[0])
	it.offset += 1
	switch entryType {
	case WALEntryTypeSet:
		var key, value []byte
		var n int
		key, n, err = it.wal.ReadVarintBytes(uint64(it.offset))
		if err != nil {
			return
		}
		it.offset = n
		value, n, err = it.wal.ReadVarintBytes(uint64(it.offset))
		if err != nil {
			return
		}
		it.offset = n
		return Update{Key: key, Value: value}, entryOffset, nil
	case WALEntryTypeDelete:
		var key []byte
		var n int
		key, n, err = it.wal.ReadVarintBytes(uint64(it.offset))
		if err != nil {
			return
		}
		it.offset = n
		return Update{Key: key, Delete: true}, entryOffset, nil
	case WALEntryTypeCommit:
		var versionBz []byte
		_, versionBz, err = it.wal.walData.SliceVar(it.offset, binary.MaxVarintLen64)
		if err != nil {
			return
		}
		version, n := binary.Uvarint(versionBz)
		if n <= 0 {
			return Update{}, 0, fmt.Errorf("corrupted wal commit entry at offset %d", entryOffset)
		}
		it.offset += n
		return Update{Commit: version}, entryOffset, nil
	default:
		return Update{}, 0, fmt.Errorf("corrupted wal entry at offset %d: unknown entry type %d", entryOffset, entryType)
	}
}

type Update struct {
	Key, Value []byte
	Delete     bool
	Commit     uint64
}

func (u Update) String() string {
	if u.Delete {
		return fmt.Sprintf("DEL %X (%d)", u.Key, len(u.Key))
	} else if u.Commit != 0 {
		return fmt.Sprintf("COMMIT %d", u.Commit)
	} else {
		return fmt.Sprintf("SET %X=%X (%d, %d)", u.Key, u.Value, len(u.Key), len(u.Value))
	}
}
