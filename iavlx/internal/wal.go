package internal

import (
	"encoding/binary"
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
			setNode._walOffset = uint64(w.curOffset)

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
	return w.walData.Slice(int(offset), int(size))
}

func (w *WAL) ReadVarintBytes(offset uint64) (bz []byte, newOffset int, err error) {
	panic("TODO")
}
