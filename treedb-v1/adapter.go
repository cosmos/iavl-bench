package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	corestore "cosmossdk.io/core/store"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/slab"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/kvstore"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

const memtableMode = "adaptive"

const (
	envDisableWAL          = "TREEDB_BENCH_DISABLE_WAL"
	envDisableJournal      = "TREEDB_BENCH_DISABLE_JOURNAL"
	envDisableBG           = "TREEDB_BENCH_DISABLE_BG"
	envRelaxedSync         = "TREEDB_BENCH_RELAXED_SYNC"
	envDisableValueLog     = "TREEDB_BENCH_DISABLE_VALUE_LOG"
	envSplitValueLog       = "TREEDB_BENCH_SPLIT_VALUE_LOG"
	envMemtableVlogPtrs    = "TREEDB_BENCH_MEMTABLE_VALUE_LOG_POINTERS"
	envDisableReadChecksum = "TREEDB_BENCH_DISABLE_READ_CHECKSUM"
	envAllowUnsafe         = "TREEDB_BENCH_ALLOW_UNSAFE"
	envVlogDictTrainBytes  = "TREEDB_BENCH_VLOG_DICT_TRAIN_BYTES"
	envMode                = "TREEDB_BENCH_MODE"
	envPinSnapshot         = "TREEDB_BENCH_PIN_SNAPSHOT"
	envProfile             = "TREEDB_BENCH_PROFILE"
	envReuseReads          = "TREEDB_BENCH_REUSE_READS"
	envSlabCompression     = "TREEDB_SLAB_COMPRESSION"
	envLeafPrefix          = "TREEDB_LEAF_PREFIX_COMPRESSION"
	envForcePointers       = "TREEDB_FORCE_VALUE_POINTERS"
	envJournalLanes        = "TREEDB_JOURNAL_LANES"
	envIndexColumnarLeaves = "TREEDB_INDEX_COLUMNAR_LEAVES"
	envIndexBaseDelta      = "TREEDB_INDEX_INTERNAL_BASE_DELTA"
	envAllowView           = "TREEDB_BENCH_ALLOW_VIEW"
	envAllowUnsafeReads    = "TREEDB_BENCH_ALLOW_UNSAFE_READS"
	envVerifyOnRead        = "TREEDB_BENCH_VERIFY_ON_READ"
	envTraceWrites         = "TREEDB_BENCH_TRACE_WRITES"
	envTraceOpsLimit       = "TREEDB_BENCH_TRACE_OPS"
	envTraceBytesLimit     = "TREEDB_BENCH_TRACE_BYTES"
	envTraceStream         = "TREEDB_BENCH_TRACE_STREAM"
)

// TreeDBAdapter adapts TreeDB to the IAVL v1 db.DB interface.
type TreeDBAdapter struct {
	db               *treedb.DB
	kv               *treedbadapter.DB
	snap             *treedb.Snapshot
	reuseReads       bool
	readBuf          []byte
	allowView        bool
	allowUnsafeReads bool
	storeName        string
	storeDir         string
	trace            *traceRecorder
	stream           *traceStream
}

func (d *TreeDBAdapter) PinSnapshot() {
	if d.snap != nil {
		d.snap.Close()
	}
	d.snap = d.db.AcquireSnapshot()
}

func (d *TreeDBAdapter) UnpinSnapshot() {
	if d.snap != nil {
		d.snap.Close()
		d.snap = nil
	}
}

type keyArena struct {
	buf []byte
}

func newKeyArena(capacity int) keyArena {
	if capacity <= 0 {
		capacity = 64 * 1024
	}
	return keyArena{buf: make([]byte, 0, capacity)}
}

func (a *keyArena) Copy(key []byte) ([]byte, bool) {
	if len(key) > cap(a.buf)-len(a.buf) {
		return nil, false
	}
	off := len(a.buf)
	a.buf = append(a.buf, key...)
	return a.buf[off : off+len(key)], true
}

func envBool(name string, defaultValue bool) bool {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	v = strings.TrimSpace(strings.ToLower(v))
	if v == "" {
		return true
	}
	switch v {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	}
	if n, err := strconv.Atoi(v); err == nil {
		return n != 0
	}
	return defaultValue
}

func envInt64(name string, defaultValue int64) int64 {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return defaultValue
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return defaultValue
	}
	return n
}

func envString(name string, defaultValue string) string {
	v, ok := os.LookupEnv(name)
	if !ok {
		return defaultValue
	}
	v = strings.TrimSpace(v)
	if v == "" {
		return defaultValue
	}
	return v
}

func setBoolOption(opts *treedb.Options, name string, value bool) {
	v := reflect.ValueOf(opts).Elem()
	field := v.FieldByName(name)
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Bool {
		return
	}
	field.SetBool(value)
}

func setIntOption(opts *treedb.Options, name string, value int) {
	v := reflect.ValueOf(opts).Elem()
	field := v.FieldByName(name)
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.Int {
		return
	}
	field.SetInt(int64(value))
}

func parseSlabCompression(value string) slab.CompressionOptions {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "zstd", "zstandard":
		return slab.CompressionOptions{Kind: slab.CompressionZSTD}
	case "none", "off", "":
		return slab.CompressionOptions{Kind: slab.CompressionNone}
	default:
		return slab.CompressionOptions{Kind: slab.CompressionNone}
	}
}

func setAllowUnsafe(opts *treedb.Options, allow bool) {
	setBoolOption(opts, "AllowUnsafe", allow)
}

func applyProfile(opts *treedb.Options, profile string) {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "durable":
		treedb.ApplyProfile(opts, treedb.ProfileDurable)
	case "fast":
		treedb.ApplyProfile(opts, treedb.ProfileFast)
	case "bench":
		treedb.ApplyProfile(opts, treedb.ProfileBench)
	}
}

func NewTreeDBAdapter(dir string, name string) (*TreeDBAdapter, error) {
	dbPath := filepath.Join(dir, name)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("error creating treedb directory: %w", err)
	}

	disableWAL := envBool(envDisableWAL, false)
	disableJournal := envBool(envDisableJournal, false)
	disableBG := envBool(envDisableBG, false)
	pinSnapshot := envBool(envPinSnapshot, false)
	reuseReads := envBool(envReuseReads, false)
	relaxedSync := envBool(envRelaxedSync, true)
	disableValueLog := envBool(envDisableValueLog, false)
	splitValueLog := envBool(envSplitValueLog, false)
	memtableVlogPtrs := envBool(envMemtableVlogPtrs, false)
	disableReadChecksum := envBool(envDisableReadChecksum, true)
	verifyOnRead := envBool(envVerifyOnRead, false)
	_, leafPrefixSet := os.LookupEnv(envLeafPrefix)
	_, forcePointersSet := os.LookupEnv(envForcePointers)
	slabCompression, slabCompressionSet := os.LookupEnv(envSlabCompression)
	journalLanes := int(envInt64(envJournalLanes, 0))
	indexColumnar := envBool(envIndexColumnarLeaves, false)
	indexBaseDelta := envBool(envIndexBaseDelta, false)
	_, allowUnsafeSet := os.LookupEnv(envAllowUnsafe)
	allowUnsafe := envBool(envAllowUnsafe, false)
	if !allowUnsafeSet && (disableWAL || disableJournal || relaxedSync || disableReadChecksum) {
		allowUnsafe = true
	}

	mode := treedb.ModeCached
	switch strings.ToLower(envString(envMode, "cached")) {
	case "backend", "raw", "uncached":
		mode = treedb.ModeBackend
	}

	openOpts := treedb.Options{
		Dir:          dbPath,
		Mode:         mode,
		MemtableMode: memtableMode,
	}
	openOpts.VerifyOnRead = verifyOnRead

	applyProfile(&openOpts, envString(envProfile, ""))
	if _, ok := os.LookupEnv(envVlogDictTrainBytes); ok {
		openOpts.ValueLogDictTrain.TrainBytes = int(envInt64(envVlogDictTrainBytes, 0))
	}

	// --- "Unsafe" Performance Options ---
	openOpts.DisableWAL = disableWAL
	openOpts.DisableValueLog = disableValueLog
	openOpts.RelaxedSync = relaxedSync
	openOpts.DisableReadChecksum = disableReadChecksum
	setBoolOption(&openOpts, "DisableJournal", disableJournal)
	setBoolOption(&openOpts, "SplitValueLog", splitValueLog)
	setBoolOption(&openOpts, "MemtableValueLogPointers", memtableVlogPtrs)

	// --- Tuning for High-Throughput & Large Values ---
	openOpts.FlushThreshold = 64 * 1024 * 1024
	openOpts.FlushBuildConcurrency = 4
	openOpts.ChunkSize = 64 * 1024 * 1024
	openOpts.PreferAppendAlloc = false
	openOpts.KeepRecent = 1
	openOpts.BackgroundIndexVacuumInterval = 15 * time.Second
	if leafPrefixSet {
		openOpts.LeafPrefixCompression = envBool(envLeafPrefix, false)
	}
	if forcePointersSet {
		openOpts.ForceValuePointers = envBool(envForcePointers, false)
	}
	if slabCompressionSet {
		openOpts.SlabCompression = parseSlabCompression(slabCompression)
	}
	setIntOption(&openOpts, "JournalLanes", journalLanes)
	setBoolOption(&openOpts, "IndexColumnarLeaves", indexColumnar)
	setBoolOption(&openOpts, "IndexInternalBaseDelta", indexBaseDelta)

	// Add Value Log Compaction
	//openOpts.BackgroundCompactionInterval = 1 * time.Second
	//openOpts.BackgroundCompactionDeadRatio = 0.1
	setAllowUnsafe(&openOpts, allowUnsafe)

	if disableBG {
		// Background tasks can dominate profile lock/wait time and obscure the
		// hot path; disable them for tighter profiling loops.
		openOpts.BackgroundIndexVacuumInterval = -1
		openOpts.BackgroundCheckpointInterval = -1
		openOpts.MaxWALBytes = -1
		openOpts.BackgroundCheckpointIdleDuration = -1
	}

	tdb, err := treedb.Open(openOpts)
	if err != nil {
		return nil, err
	}

	adapter := &TreeDBAdapter{
		db:               tdb,
		kv:               treedbadapter.Wrap(tdb),
		reuseReads:       reuseReads,
		allowView:        envBool(envAllowView, false),
		allowUnsafeReads: envBool(envAllowUnsafeReads, false),
		storeName:        name,
		storeDir:         dbPath,
	}
	if envBool(envTraceWrites, false) {
		traceOps := int(envInt64(envTraceOpsLimit, 200_000))
		traceBytes := int(envInt64(envTraceBytesLimit, 64*1024*1024))
		adapter.trace = newTraceRecorder(traceOps, traceBytes)
		if envBool(envTraceStream, false) {
			if stream, err := newTraceStream(adapter.storeDir, adapter.storeName); err == nil {
				adapter.stream = stream
			}
		}
	}
	if pinSnapshot {
		adapter.PinSnapshot()
	}
	return adapter, nil
}

func (d *TreeDBAdapter) Get(key []byte) ([]byte, error) {
	if d.snap != nil {
		val, err := d.snap.GetUnsafe(key)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		return val, nil
	}
	if d.db == nil {
		return nil, treedb.ErrClosed
	}
	if d.reuseReads {
		val, err := d.db.GetAppend(key, d.readBuf[:0])
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		d.readBuf = val[:0]
		return val, nil
	}
	if d.allowUnsafeReads {
		return d.kv.GetUnsafe(key)
	}
	return d.kv.Get(key)
}

func (d *TreeDBAdapter) Has(key []byte) (bool, error) {
	if d.snap != nil {
		return d.snap.Has(key)
	}
	if d.kv == nil {
		return false, treedb.ErrClosed
	}
	return d.kv.Has(key)
}

func (d *TreeDBAdapter) Iterator(start, end []byte) (corestore.Iterator, error) {
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.kv.Iterator(start, end)
	if err != nil {
		return nil, err
	}
	return &coreIterator{iter: it, start: start, end: end}, nil
}

func (d *TreeDBAdapter) ReverseIterator(start, end []byte) (corestore.Iterator, error) {
	if d.kv == nil {
		return nil, treedb.ErrClosed
	}
	it, err := d.kv.ReverseIterator(start, end)
	if err != nil {
		return nil, err
	}
	return &coreIterator{iter: it, start: start, end: end}, nil
}

func (d *TreeDBAdapter) Close() error {
	if d.db == nil {
		return nil
	}
	d.UnpinSnapshot()
	if d.stream != nil {
		_ = d.stream.Close()
		d.stream = nil
	}
	err := d.db.Close()
	d.db = nil
	d.kv = nil
	return err
}

func (d *TreeDBAdapter) NewBatch() corestore.Batch {
	return d.NewBatchWithSize(16)
}

func (d *TreeDBAdapter) NewBatchWithSize(size int) corestore.Batch {
	if size <= 0 {
		size = 16
	}
	b := &coreBatch{db: d}
	if d.kv != nil {
		kb, err := d.kv.NewBatch()
		if err == nil {
			b.kb = kb
			if d.allowView {
				if sv, ok := kb.(interface{ SetView(key, value []byte) error }); ok {
					b.setView = sv.SetView
				}
				if dv, ok := kb.(interface{ DeleteView(key []byte) error }); ok {
					b.deleteView = dv.DeleteView
				}
			}
		}
	}
	return b
}

func (d *TreeDBAdapter) Checkpoint() error {
	if d.kv == nil {
		return treedb.ErrClosed
	}
	return d.kv.Checkpoint()
}

func (d *TreeDBAdapter) Stats() map[string]string {
	if d.kv == nil {
		return nil
	}
	return d.kv.Stats()
}

func (d *TreeDBAdapter) FragmentationReport() (map[string]string, error) {
	if d.db == nil {
		return nil, treedb.ErrClosed
	}
	return d.db.FragmentationReport()
}

func (d *TreeDBAdapter) DumpTrace(path string, version int64) error {
	if d.trace == nil {
		return nil
	}
	dump := d.trace.snapshot()
	if len(dump) == 0 {
		return nil
	}
	data := traceDump{
		Version: version,
		Store:   d.storeName,
		Ops:     dump,
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&data); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0644)
}

func (d *TreeDBAdapter) DumpTraceOnError(reason string) {
	if d.trace == nil || d.storeDir == "" {
		return
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	if reason != "" {
		suffix = reason + "-" + suffix
	}
	path := filepath.Join(d.storeDir, fmt.Sprintf("bench-trace-%s-%s.gob", d.storeName, suffix))
	_ = d.DumpTrace(path, 0)
	if d.stream != nil {
		_ = d.stream.Flush()
	}
}

type coreBatch struct {
	db         *TreeDBAdapter
	kb         kvstore.Batch
	setView    func(key, value []byte) error
	deleteView func(key []byte) error
	size       int
	done       bool
}

func (b *coreBatch) Set(key, value []byte) error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	if value == nil {
		value = []byte{}
	}
	if b.db != nil && b.db.trace != nil {
		b.db.trace.record(traceOp{Type: traceOpSet, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	}
	if b.db != nil && b.db.stream != nil {
		b.db.stream.record(traceOp{Type: traceOpSet, Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	}
	if b.setView != nil {
		if err := b.setView(key, value); err != nil {
			if b.db != nil {
				b.db.DumpTraceOnError("set")
			}
			return err
		}
	} else if err := b.kb.Set(key, value); err != nil {
		if b.db != nil {
			b.db.DumpTraceOnError("set")
		}
		return err
	}
	b.size += len(key) + len(value)
	return nil
}

func (b *coreBatch) Delete(key []byte) error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	if b.db != nil && b.db.trace != nil {
		b.db.trace.record(traceOp{Type: traceOpDelete, Key: append([]byte(nil), key...)})
	}
	if b.db != nil && b.db.stream != nil {
		b.db.stream.record(traceOp{Type: traceOpDelete, Key: append([]byte(nil), key...)})
	}
	if b.deleteView != nil {
		if err := b.deleteView(key); err != nil {
			if b.db != nil {
				b.db.DumpTraceOnError("del")
			}
			return err
		}
	} else if err := b.kb.Delete(key); err != nil {
		if b.db != nil {
			b.db.DumpTraceOnError("del")
		}
		return err
	}
	b.size += len(key)
	return nil
}

func (b *coreBatch) Write() error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	if b.db != nil && b.db.trace != nil {
		b.db.trace.record(traceOp{Type: traceOpCommit})
	}
	if b.db != nil && b.db.stream != nil {
		b.db.stream.record(traceOp{Type: traceOpCommit})
	}
	b.done = true
	if err := b.kb.Commit(); err != nil {
		if b.db != nil {
			b.db.DumpTraceOnError("commit")
		}
		return err
	}
	return nil
}

func (b *coreBatch) WriteSync() error {
	if b.done || b.kb == nil {
		return treedb.ErrClosed
	}
	if b.db != nil && b.db.trace != nil {
		b.db.trace.record(traceOp{Type: traceOpCommit})
	}
	if b.db != nil && b.db.stream != nil {
		b.db.stream.record(traceOp{Type: traceOpCommit})
	}
	b.done = true
	if err := b.kb.CommitSync(); err != nil {
		if b.db != nil {
			b.db.DumpTraceOnError("commit")
		}
		return err
	}
	return nil
}

func (b *coreBatch) Close() error {
	if b.kb == nil {
		b.done = true
		return nil
	}
	err := b.kb.Close()
	b.kb = nil
	b.done = true
	return err
}

func (b *coreBatch) GetByteSize() (int, error) {
	return b.size, nil
}

type coreIterator struct {
	iter  kvstore.Iterator
	start []byte
	end   []byte

	keyArena keyArena
	valArena keyArena
}

func (it *coreIterator) Domain() (start, end []byte) { return it.start, it.end }
func (it *coreIterator) Valid() bool                 { return it.iter.Valid() }
func (it *coreIterator) Next()                       { it.iter.Next() }

func (it *coreIterator) Key() []byte {
	if it.keyArena.buf == nil {
		it.keyArena = newKeyArena(64 * 1024)
	}
	key := it.iter.Key()
	out, ok := it.keyArena.Copy(key)
	if ok {
		return out
	}
	out = make([]byte, len(key))
	copy(out, key)
	return out
}

func (it *coreIterator) Value() []byte {
	if it.valArena.buf == nil {
		it.valArena = newKeyArena(256 * 1024)
	}
	val := it.iter.Value()
	out, ok := it.valArena.Copy(val)
	if ok {
		return out
	}
	out = make([]byte, len(val))
	copy(out, val)
	return out
}

func (it *coreIterator) Error() error { return it.iter.Error() }
func (it *coreIterator) Close() error { return it.iter.Close() }

const (
	traceOpSet    = "set"
	traceOpDelete = "del"
	traceOpCommit = "commit"
)

type traceOp struct {
	Type  string
	Key   []byte
	Value []byte
}

type traceDump struct {
	Version int64
	Store   string
	Ops     []traceOp
}

type traceStreamHeader struct {
	Version int64
	Store   string
	Created int64
}

type traceRecorder struct {
	mu       sync.Mutex
	maxOps   int
	maxBytes int
	bytes    int
	ops      []traceOp
}

func newTraceRecorder(maxOps, maxBytes int) *traceRecorder {
	if maxOps <= 0 {
		maxOps = 200_000
	}
	if maxBytes <= 0 {
		maxBytes = 64 * 1024 * 1024
	}
	return &traceRecorder{
		maxOps:   maxOps,
		maxBytes: maxBytes,
		ops:      make([]traceOp, 0, maxOps),
	}
}

func (r *traceRecorder) record(op traceOp) {
	r.mu.Lock()
	defer r.mu.Unlock()
	opBytes := len(op.Key) + len(op.Value)
	r.ops = append(r.ops, op)
	r.bytes += opBytes
	for (r.maxOps > 0 && len(r.ops) > r.maxOps) || (r.maxBytes > 0 && r.bytes > r.maxBytes) {
		drop := r.ops[0]
		r.ops = r.ops[1:]
		r.bytes -= len(drop.Key) + len(drop.Value)
		if r.bytes < 0 {
			r.bytes = 0
		}
	}
}

func (r *traceRecorder) snapshot() []traceOp {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]traceOp, len(r.ops))
	copy(out, r.ops)
	return out
}

type traceStream struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	enc    *gob.Encoder
}

func newTraceStream(dir, store string) (*traceStream, error) {
	if dir == "" || store == "" {
		return nil, fmt.Errorf("trace stream requires store dir and name")
	}
	path := filepath.Join(dir, fmt.Sprintf("bench-trace-stream-%s-%d.gob", store, time.Now().UnixNano()))
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	enc := gob.NewEncoder(w)
	header := traceStreamHeader{
		Version: 0,
		Store:   store,
		Created: time.Now().UnixNano(),
	}
	if err := enc.Encode(&header); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &traceStream{file: f, writer: w, enc: enc}, nil
}

func (s *traceStream) record(op traceOp) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.enc == nil {
		return
	}
	_ = s.enc.Encode(&op)
}

func (s *traceStream) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writer == nil {
		return nil
	}
	return s.writer.Flush()
}

func (s *traceStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writer != nil {
		_ = s.writer.Flush()
	}
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		s.writer = nil
		s.enc = nil
		return err
	}
	return nil
}

var _ corestore.Batch = (*coreBatch)(nil)
var _ corestore.Iterator = (*coreIterator)(nil)
