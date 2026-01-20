package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"cosmossdk.io/log"
	"github.com/cosmos/iavl"
	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/bench/util"
)

const (
	envDiagEnabled = "TREEDB_BENCH_LOG_DIAG"
	envDiagEvery   = "TREEDB_BENCH_LOG_DIAG_EVERY"
	envIAVLSync    = "TREEDB_BENCH_IAVL_SYNC"
	envDumpOnError = "TREEDB_BENCH_DUMP_ON_ERROR"
	envPanicOnErr  = "TREEDB_BENCH_PANIC_ON_ERROR"
	// envCheckpointEvery triggers a TreeDB checkpoint every N committed versions
	// (0 disables).
	envCheckpointEvery = "TREEDB_BENCH_CHECKPOINT_EVERY"
)

type Options struct {
	SkipFastStorageUpgrade bool `json:"skip_fast_storage_upgrade"`
	CacheSize              int  `json:"cache_size"`
}

// MultiTreeWrapper wraps multiple IAVL trees sharing a single DB backend.
// This is identical to the logic in iavl-v0/main.go but uses TreeDBAdapter.
type MultiTreeWrapper struct {
	dbDir   string
	version int64
	trees   map[string]*iavl.MutableTree
	dbs     map[string]*TreeDBAdapter

	logger      *slog.Logger
	diagEnabled bool
	diagEvery   int64

	checkpointEvery int64
	pinSnapshots    bool

	dumpOnError  bool
	panicOnError bool
}

func (m *MultiTreeWrapper) Close() error {
	for _, d := range m.dbs {
		if err := d.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *MultiTreeWrapper) Version() int64 {
	return m.version
}

func (m *MultiTreeWrapper) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
	tree, ok := m.trees[storeKey]
	if !ok {
		return fmt.Errorf("store key %s not found", storeKey)
	}
	if delete {
		_, _, err := tree.Remove(key)
		return err
	} else {
		_, err := tree.Set(key, value)
		return err
	}
}

func (m *MultiTreeWrapper) Commit() error {
	for _, tree := range m.trees {
		_, _, err := tree.SaveVersion()
		if err != nil {
			if m.dumpOnError {
				m.dumpCommitError(tree, err)
			}
			if m.panicOnError {
				panic(err)
			}
			return err
		}
	}

	m.version++

	if err := util.SaveVersion(m.dbDir, m.version); err != nil {
		return err
	}

	if m.diagEnabled && (m.diagEvery <= 1 || (m.version%m.diagEvery) == 0) {
		m.writeDiagReports(m.version)
	}
	if m.checkpointEvery > 0 && (m.version%m.checkpointEvery) == 0 {
		for _, d := range m.dbs {
			if err := d.Checkpoint(); err != nil {
				return err
			}
		}
	}
	if m.pinSnapshots {
		for _, d := range m.dbs {
			d.PinSnapshot()
		}
	}
	return nil
}

var _ bench.Tree = &MultiTreeWrapper{}

func main() {
	bench.Run("iavl-treedb-v1", bench.RunConfig{
		OptionsType: &Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			opts := params.TreeOptions.(*Options)
			if opts == nil {
				opts = &Options{}
			}

			dbDir := params.TreeDir
			version, err := util.LoadVersion(dbDir)
			if err != nil {
				return nil, err
			}
			trees := make(map[string]*iavl.MutableTree)
			dbs := make(map[string]*TreeDBAdapter)
			diagEnabled, diagEvery := loadDiagConfig()
			pinSnapshots := envBool(envPinSnapshot, false)
			iavlSyncEnabled := envBool(envIAVLSync, false)
			checkpointEvery := envInt64(envCheckpointEvery, 0)
			dumpOnError := envBool(envDumpOnError, true)
			panicOnError := envBool(envPanicOnErr, false)

			// Initialize our TreeDB Adapter
			// NOTE: In iavl-v0, they create a new DB for EACH store name using NewGoLevelDBWithOpts.
			// This means they are creating separate LevelDB instances (folders) for "storeA", "storeB".
			// We should mimic this behavior.

			logger := log.NewNopLogger()
			for _, storeName := range params.StoreNames {
				// Create the DB adapter for this specific store
				d, err := NewTreeDBAdapter(dbDir, storeName)
				if err != nil {
					return nil, fmt.Errorf("error creating treedb for %s: %w", storeName, err)
				}
				dbs[storeName] = d

				tree := iavl.NewMutableTree(
					d,
					opts.CacheSize,
					opts.SkipFastStorageUpgrade,
					logger,
					iavl.SyncOption(iavlSyncEnabled),
				)
				if version != 0 {
					_, err := tree.LoadVersion(version)
					if err != nil {
						return nil, fmt.Errorf("loading store %s at version %d: %w", storeName, version, err)
					}
				}
				trees[storeName] = tree
			}
			if pinSnapshots {
				for _, d := range dbs {
					d.PinSnapshot()
				}
			}
			return &MultiTreeWrapper{
					trees:   trees,
					dbs:     dbs,
					version: version,
					dbDir:   dbDir,
					logger:  params.Logger,

					diagEnabled:     diagEnabled,
					diagEvery:       diagEvery,
					checkpointEvery: checkpointEvery,
					pinSnapshots:    pinSnapshots,
					dumpOnError:     dumpOnError,
					panicOnError:    panicOnError,
				},
				nil
		},
	})
}

func loadDiagConfig() (enabled bool, every int64) {
	enabled = false
	if v := strings.TrimSpace(os.Getenv(envDiagEnabled)); v != "" && v != "0" && !strings.EqualFold(v, "false") {
		enabled = true
	}

	every = 1
	if raw := strings.TrimSpace(os.Getenv(envDiagEvery)); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			every = n
		}
	}
	return enabled, every
}

func (m *MultiTreeWrapper) writeDiagReports(version int64) {
	for storeName, d := range m.dbs {
		stats := d.Stats()
		frag, fragErr := d.FragmentationReport()

		dbPath := filepath.Join(m.dbDir, storeName)
		path := filepath.Join(dbPath, fmt.Sprintf("bench-diag-v%05d.txt", version))

		var b strings.Builder
		b.WriteString("time=")
		b.WriteString(time.Now().Format(time.RFC3339Nano))
		b.WriteString("\n")
		b.WriteString("version=")
		b.WriteString(strconv.FormatInt(version, 10))
		b.WriteString("\n")

		b.WriteString("\nStats:\n")
		writeSortedMap(&b, stats)

		b.WriteString("\nFragmentation:\n")
		if fragErr != nil {
			b.WriteString("  error=")
			b.WriteString(fragErr.Error())
			b.WriteString("\n")
		} else {
			writeSortedMap(&b, frag)
		}

		if err := os.WriteFile(path, []byte(b.String()), 0644); err != nil {
			if m.logger != nil {
				m.logger.Warn("failed to write treedb diag report", "store", storeName, "path", path, "error", err)
			}
			continue
		}

		if m.logger != nil {
			m.logger.Info("wrote treedb diag report", "store", storeName, "path", path, "version", version)
		}
	}
}

func (m *MultiTreeWrapper) dumpCommitError(tree *iavl.MutableTree, err error) {
	storeName := ""
	for name, t := range m.trees {
		if t == tree {
			storeName = name
			break
		}
	}
	d := m.dbs[storeName]
	stats := map[string]string{}
	if d != nil {
		if s := d.Stats(); s != nil {
			for k, v := range s {
				stats[k] = v
			}
		}
		if frag, fragErr := d.FragmentationReport(); fragErr == nil {
			for k, v := range frag {
				stats["frag."+k] = v
			}
		} else {
			stats["frag.error"] = fragErr.Error()
		}
		if d.db != nil {
			for k, v := range d.db.Stats() {
				stats[k] = v
			}
		}
	}

	dbPath := m.dbDir
	if storeName != "" {
		dbPath = filepath.Join(m.dbDir, storeName)
	}
	path := filepath.Join(dbPath, fmt.Sprintf("bench-error-v%05d.txt", m.version))
	tracePath := filepath.Join(dbPath, fmt.Sprintf("bench-trace-%s-v%05d.gob", storeName, m.version))

	var b strings.Builder
	b.WriteString("time=")
	b.WriteString(time.Now().Format(time.RFC3339Nano))
	b.WriteString("\n")
	b.WriteString("version=")
	b.WriteString(strconv.FormatInt(m.version, 10))
	b.WriteString("\n")
	b.WriteString("store=")
	b.WriteString(storeName)
	b.WriteString("\n")
	b.WriteString("error=")
	b.WriteString(err.Error())
	b.WriteString("\n\n")
	b.WriteString("Stats:\n")
	writeSortedMap(&b, stats)

	_ = os.WriteFile(path, []byte(b.String()), 0644)
	if d != nil {
		_ = d.DumpTrace(tracePath, m.version)
	}
	if m.logger != nil {
		m.logger.Error("treedb commit failed", "store", storeName, "error", err, "path", path)
	}
}

func writeSortedMap(b *strings.Builder, m map[string]string) {
	if len(m) == 0 {
		return
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString("  ")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(m[k])
		b.WriteString("\n")
	}
}
