package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	dbm "github.com/cosmos/cosmos-db"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

const memtableMode = "adaptive"

// TreeDBAdapter adapts a TreeDB instance to the cosmos-db interface (used by IAVL v0.21.x).
type TreeDBAdapter struct {
	db *treedb.DB
	kv *treedbadapter.DB
}

func NewTreeDBAdapter(dir string, name string) (*TreeDBAdapter, error) {
	dbPath := filepath.Join(dir, name)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("error creating treedb directory: %w", err)
	}

	openOpts := treedb.Options{
		Dir:          dbPath,
		Mode:         treedb.ModeCached,
		MemtableMode: memtableMode,

		// --- "Unsafe" Performance Options ---
		DisableWAL:          false,
		RelaxedSync:         false,
		DisableReadChecksum: false,

		// --- Tuning for High-Throughput & Large Values ---
		FlushThreshold:        64 * 1024 * 1024,
		FlushBuildConcurrency: 4,
		ChunkSize:             64 * 1024 * 1024,

		PreferAppendAlloc:             false,
		KeepRecent:                    1,
		BackgroundIndexVacuumInterval: 15 * time.Second,

		// Add Value Log Compaction
		//BackgroundCompactionInterval:  1 * time.Second,
		//BackgroundCompactionDeadRatio: 0.1,
	}

	tdb, err := treedb.Open(openOpts)
	if err != nil {
		return nil, err
	}

	return &TreeDBAdapter{
		db: tdb,
		kv: treedbadapter.Wrap(tdb),
	}, nil
}

func (d *TreeDBAdapter) Get(key []byte) ([]byte, error) {
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	value, err := d.kv.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	// TreeDB may return a read-only view into mmaped pages/slabs; iavl's DB
	// contract treats returned bytes as read-only, so returning the view avoids
	// a high allocation rate from per-Get copying.
	return value, nil
}

func (d *TreeDBAdapter) Has(key []byte) (bool, error) {
	if d.kv == nil {
		return false, treedb.ErrClosed
	}
	return d.kv.Has(key)
}

func (d *TreeDBAdapter) Set(key, value []byte) error {
	if d.kv == nil {
		return treedb.ErrClosed
	}
	if value == nil {
		value = []byte{}
	}
	return d.kv.Set(key, value)
}

func (d *TreeDBAdapter) SetSync(key, value []byte) error {
	return d.Set(key, value)
}

func (d *TreeDBAdapter) Delete(key []byte) error {
	if d.kv == nil {
		return treedb.ErrClosed
	}
	return d.kv.Delete(key)
}

func (d *TreeDBAdapter) DeleteSync(key []byte) error {
	return d.Delete(key)
}

func (d *TreeDBAdapter) Iterator(start, end []byte) (dbm.Iterator, error) {
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.kv.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &treeDBIterator{iter: it, start: start, end: end}, nil
}

func (d *TreeDBAdapter) ReverseIterator(start, end []byte) (dbm.Iterator, error) {
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.kv.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &treeDBIterator{iter: it, start: start, end: end}, nil
}

func (d *TreeDBAdapter) Close() error {
	if d.db == nil {
		return nil
	}
	err := d.db.Close()
	d.db = nil
	d.kv = nil
	return err
}

func (d *TreeDBAdapter) NewBatch() dbm.Batch {
	return d.NewBatchWithSize(16)
}

func (d *TreeDBAdapter) NewBatchWithSize(size int) dbm.Batch {
	if size <= 0 {
		size = 16
	}
	b := &treeDBBatch{db: d}
	if d.kv != nil {
		kb, err := d.kv.NewBatch()
		if err == nil {
			b.kb = kb
		}
	}
	return b
}

func (d *TreeDBAdapter) Print() error { return nil }

func (d *TreeDBAdapter) Stats() map[string]string {
	if d.kv == nil {
		return nil
	}
	return d.kv.Stats()
}

func (d *TreeDBAdapter) Checkpoint() error {
	if d.kv == nil {
		return treedb.ErrClosed
	}
	return d.kv.Checkpoint()
}

func (d *TreeDBAdapter) FragmentationReport() (map[string]string, error) {
	if d.db == nil {
		return nil, treedb.ErrClosed
	}
	return d.db.FragmentationReport()
}

type treeDBBatch struct {
	db   *TreeDBAdapter
	kb   kvstore.Batch
	done bool
}

func (b *treeDBBatch) Set(key, value []byte) error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	if value == nil {
		value = []byte{}
	}
	return b.kb.Set(key, value)
}

func (b *treeDBBatch) Delete(key []byte) error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	return b.kb.Delete(key)
}

func (b *treeDBBatch) Write() error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	b.done = true
	return b.kb.Commit()
}

func (b *treeDBBatch) WriteSync() error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	b.done = true
	return b.kb.CommitSync()
}

func (b *treeDBBatch) Close() error {
	if b.kb == nil {
		b.done = true
		return nil
	}
	err := b.kb.Close()
	b.kb = nil
	b.done = true
	return err
}

type treeDBIterator struct {
	iter  kvstore.Iterator
	start []byte
	end   []byte
}

func (it *treeDBIterator) Domain() (start, end []byte) { return it.start, it.end }
func (it *treeDBIterator) Valid() bool                 { return it.iter.Valid() }
func (it *treeDBIterator) Next()                       { it.iter.Next() }
func (it *treeDBIterator) Key() []byte                 { return it.iter.Key() }
func (it *treeDBIterator) Value() []byte               { return it.iter.Value() }
func (it *treeDBIterator) Error() error                { return it.iter.Error() }
func (it *treeDBIterator) Close() error                { return it.iter.Close() }

var _ dbm.DB = (*TreeDBAdapter)(nil)
var _ dbm.Batch = (*treeDBBatch)(nil)
var _ dbm.Iterator = (*treeDBIterator)(nil)
