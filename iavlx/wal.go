package iavlx

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type WALWriter struct {
	dir           string
	info          *walInfo
	activeVersion uint64
	file          *os.File
	writer        *bufio.Writer
}

type walInfo struct {
	FirstVersion uint64 `json:"first_version"`
	LastVersion  uint64 `json:"last_version"`
}

const infoFileName = "wal_info.json"

func OpenWALWriter(dir string) (*WALWriter, error) {
	wal := &WALWriter{dir: dir}
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, err
	}
	infoPath := filepath.Join(dir, infoFileName)
	infoData, err := os.ReadFile(infoPath)
	if err == nil {
		wal.info = &walInfo{}
		err = json.Unmarshal(infoData, wal.info)
		if err != nil {
			return nil, err
		}
		wal.activeVersion = wal.info.LastVersion + 1
	} else if os.IsNotExist(err) {
		// no existing wal, create new
		wal.info = &walInfo{FirstVersion: 0, LastVersion: 0}
		wal.activeVersion = 1
	} else {
		return nil, err
	}
	err = wal.initWalFile()
	return wal, err
}

func (w *WALWriter) WriteUpdates(updates []*leafUpdate) error {
	for _, update := range updates {
		err := w.writeUpdate(update)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *WALWriter) initWalFile() error {
	var err error
	w.file, err = os.Create(filepath.Join(w.dir, walFileName(w.activeVersion)))
	w.writer = bufio.NewWriter(w.file)
	return err
}

func (w *WALWriter) CommitVersion() (committedVersion uint64, err error) {
	err = w.writer.Flush()
	if err != nil {
		return 0, err
	}
	err = w.file.Sync()
	if err != nil {
		return 0, err
	}
	err = w.file.Close()
	if err != nil {
		return 0, err
	}
	committedVersion = w.activeVersion
	w.info.LastVersion = committedVersion
	if w.info.FirstVersion == 0 && committedVersion == 1 {
		w.info.FirstVersion = 1
	}
	infoPath := filepath.Join(w.dir, infoFileName)
	infoData, err := json.MarshalIndent(w.info, "", "  ")
	if err != nil {
		return 0, err
	}
	err = os.WriteFile(infoPath, infoData, 0o644)
	if err != nil {
		return 0, err
	}
	w.activeVersion++
	err = w.initWalFile()
	return committedVersion, err
}

func walFileName(version uint64) string {
	return fmt.Sprintf("wal_%020d.log", version)
}

func (w *WALWriter) writeUpdate(update *leafUpdate) error {
	var err error
	if update.deleted {
		_, err = w.writer.Write([]byte{1})
	} else {
		_, err = w.writer.Write([]byte{0})
	}
	if err != nil {
		return err
	}

	keyLen := len(update.key)
	// write varint
	var varintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(varintBuf[:], uint64(keyLen))
	_, err = w.writer.Write(varintBuf[:n])
	if err != nil {
		return err
	}
	_, err = w.writer.Write(update.key)
	if err != nil {
		return err
	}

	if !update.deleted {
		valueLen := len(update.value)
		// write varint
		n = binary.PutUvarint(varintBuf[:], uint64(valueLen))
		_, err = w.writer.Write(varintBuf[:n])
		if err != nil {
			return err
		}

		_, err = w.writer.Write(update.value)
		if err != nil {
			return err
		}
	}
	return nil
}
