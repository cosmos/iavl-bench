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

	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/iavl"
	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/bench/util"
)

const (
	envDiagEnabled = "TREEDB_BENCH_LOG_DIAG"
	envDiagEvery   = "TREEDB_BENCH_LOG_DIAG_EVERY"
)

// MultiTreeWrapper wraps multiple IAVL trees sharing a single DB backend.
// This mirrors the original iavl-v0 benchmark structure, but uses TreeDB as the
// backing DB implementation (via cosmos-db).
type MultiTreeWrapper struct {
	dbDir   string
	version int64
	trees   map[string]*iavl.MutableTree
	dbs     map[string]dbm.DB

	logger      *slog.Logger
	diagEnabled bool
	diagEvery   int64
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
	}
	_, err := tree.Set(key, value)
	return err
}

func (m *MultiTreeWrapper) Commit() error {
	for _, tree := range m.trees {
		if _, _, err := tree.SaveVersion(); err != nil {
			return err
		}
	}

	for _, d := range m.dbs {
		if tdb, ok := d.(*TreeDBAdapter); ok {
			if err := tdb.Checkpoint(); err != nil {
				return fmt.Errorf("error checkpointing treedb: %w", err)
			}
		}
	}

	m.version++

	if err := util.SaveVersion(m.dbDir, m.version); err != nil {
		return err
	}

	if m.diagEnabled && (m.diagEvery <= 1 || (m.version%m.diagEvery) == 0) {
		m.writeDiagReports(m.version)
	}
	return nil
}

var _ bench.Tree = &MultiTreeWrapper{}

func main() {
	bench.Run("iavl-treedb", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			dbDir := params.TreeDir
			version, err := util.LoadVersion(dbDir)
			if err != nil {
				return nil, err
			}

			trees := make(map[string]*iavl.MutableTree)
			dbs := make(map[string]dbm.DB)
			diagEnabled, diagEvery := loadDiagConfig()

			for _, storeName := range params.StoreNames {
				d, err := NewTreeDBAdapter(dbDir, storeName)
				if err != nil {
					return nil, fmt.Errorf("error creating treedb for %s: %w", storeName, err)
				}
				dbs[storeName] = d

				tree, err := iavl.NewMutableTree(d, 10_000, true)
				if err != nil {
					return nil, fmt.Errorf("error creating store %s: %w", storeName, err)
				}
				if version != 0 {
					if _, err := tree.LoadVersion(version); err != nil {
						return nil, fmt.Errorf("loading store %s at version %d: %w", storeName, version, err)
					}
				}
				trees[storeName] = tree
			}

			return &MultiTreeWrapper{
				trees:   trees,
				dbs:     dbs,
				version: version,
				dbDir:   dbDir,
				logger:  params.Logger,

				diagEnabled: diagEnabled,
				diagEvery:   diagEvery,
			}, nil
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
		tdb, ok := d.(*TreeDBAdapter)
		if !ok {
			continue
		}

		stats := tdb.Stats()
		frag, fragErr := tdb.FragmentationReport()

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
