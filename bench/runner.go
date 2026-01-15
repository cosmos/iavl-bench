package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/cosmos/cosmos-sdk/telemetry"
	"github.com/dustin/go-humanize"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

type MultiTree interface {
	// Version should return the last committed version. If no version has been committed, it should return 0.
	Version() int64
	// Commit should persist all changes made since the last commit and return the new version's hash.
	Commit(updates MultiStoreUpdates) error
	// Tree should return a TreeReader for the given store name.
	Tree(storeName string) TreeReader
	ForceToDisk() error
	io.Closer
}

type TreeReader interface {
	Get(key []byte) ([]byte, error)
	Size() int64
}

type LoaderParams struct {
	TreeDir     string
	TreeOptions interface{}
	StoreNames  []string
	Logger      *slog.Logger
}

type TreeLoader func(params LoaderParams) (MultiTree, error)

type RunConfig struct {
	TreeLoader  TreeLoader
	OptionsType interface{}
}

func Run(treeType string, cfg RunConfig) {
	NewRunner(treeType, cfg).Run()
}

type Runner struct {
	*cobra.Command
}

func (r Runner) Run() {
	err := r.Command.Execute()
	if err != nil {
		slog.Error("error running benchmarks", "error", err)
		os.Exit(1)
	}
}

func NewRunner(treeType string, cfg RunConfig) Runner {
	var treeDir string
	var treeOptions string
	var genOptions string
	var logHandlerType string
	var logFile string
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Runs benchmarks for the tree implementation.",
	}
	cmd.Flags().StringVar(&treeDir, "db-dir", "", "Directory for the db's data.")
	cmd.Flags().StringVar(&treeOptions, "db-options", "", "Implementation specific options for the db, in JSON format.")
	cmd.Flags().StringVar(&genOptions, "gen-options", "", "Changeset generator params, in JSON format.")
	cmd.Flags().StringVar(&logHandlerType, "log-type", "text", "Log handler type. One of 'text' or 'json'.")
	cmd.Flags().StringVar(&logFile, "log-file", "", "If set, log output will be written to this file instead of stdout.")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if treeDir == "" {
			return fmt.Errorf("tree-dir is required")
		}

		var genParams SimParams
		if genOptions == "" {
			return fmt.Errorf("gen-options is required")
		}
		decoder := json.NewDecoder(bytes.NewReader([]byte(genOptions)))
		// we disallow unknown fields to catch typos with generator options
		decoder.DisallowUnknownFields()
		err := decoder.Decode(&genParams)
		if err != nil {
			return fmt.Errorf("error unmarshaling gen-options: %w", err)
		}

		// decode db options from json
		var opts interface{}
		if cfg.OptionsType != nil {
			opts = reflect.New(reflect.TypeOf(cfg.OptionsType).Elem()).Interface()
			if treeOptions != "" {
				if cfg.OptionsType == nil {
					return fmt.Errorf("db-options provided but no OptionsType set in RunConfig")
				}
				decoder := json.NewDecoder(bytes.NewReader([]byte(treeOptions)))
				// we disallow unknown fields to catch typos with database options
				decoder.DisallowUnknownFields()
				err := decoder.Decode(opts)
				if err != nil {
					return fmt.Errorf("error unmarshaling db-options: %w", err)
				}
			}
		}

		logOut := os.Stdout
		if logFile != "" {
			logOut, err = os.Create(logFile)
			if err != nil {
				return fmt.Errorf("error creating log file: %w", err)
			}
			defer func() {
				err := logOut.Close()
				if err != nil {
					slog.Error("error closing log file", "error", err)
				}
			}()
		}

		var handler slog.Handler
		switch logHandlerType {
		case "text":
			handler = slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: slog.LevelDebug})
		case "json":
			handler = slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: slog.LevelDebug})
		default:
			return fmt.Errorf("unknown log handler type: %s", logHandlerType)
		}

		// Create a separate handler for tree logger at info level
		var treeHandler slog.Handler
		switch logHandlerType {
		case "text":
			treeHandler = slog.NewTextHandler(logOut, &slog.HandlerOptions{Level: slog.LevelInfo})
		case "json":
			treeHandler = slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: slog.LevelInfo, AddSource: true})
		default:
			return fmt.Errorf("unknown log handler type: %s", logHandlerType)
		}

		logger := slog.New(handler).With("module", "runner")
		treeLogger := slog.New(treeHandler)
		logger.Info("Starting benchmark run, loading tree")

		var storeNames []string
		for _, store := range genParams.Stores {
			storeNames = append(storeNames, store.Name)
		}

		loaderParams := LoaderParams{
			TreeDir:     treeDir,
			TreeOptions: opts,
			StoreNames:  storeNames,
			Logger:      treeLogger.With("module", treeType),
		}

		tree, err := cfg.TreeLoader(loaderParams)
		if err != nil {
			return fmt.Errorf("error loading tree: %w", err)
		}

		return run(tree, genParams, runParams{
			TreeType:     treeType,
			Logger:       logger,
			LoaderParams: loaderParams,
		})
	}

	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(cmd)
	return Runner{Command: rootCmd}
}

type runParams struct {
	Logger       *slog.Logger
	LoaderParams LoaderParams
	TreeType     string
}

func run(tree MultiTree, genParams SimParams, params runParams) error {
	logger := params.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// capture exceptions and log stack trace
	defer func() {
		if r := recover(); r != nil {
			logger.Error("panic occurred", "error", r, "stack", string(debug.Stack()))
		}
	}()

	version := tree.Version()
	logger.Info("starting run",
		"start_version", version,
		"gen_params", genParams,
		"db_dir", params.LoaderParams.TreeDir,
		"db_options", params.LoaderParams.TreeOptions,
		"tree_type", params.TreeType,
	)

	captureSystemInfo(logger)

	closeCh := make(chan struct{})
	currentVersion := atomic.Int64{}
	currentVersion.Store(version)
	doneCh := measureBackgroundStats(logger, &currentVersion, params.LoaderParams.TreeDir, closeCh)

	sim := GenSimulation(genParams)
	for phase := range sim {
		logger.Info("starting phase", "phase", phase.Params)
		if phase.Params.ForceToDisk {
			logger.Info("forcing tree to disk before starting phase")
			err := tree.ForceToDisk()
			if err != nil {
				return fmt.Errorf("error forcing tree to disk: %w", err)
			}
		}
		for version, versionSim := range phase.Versions {
			currentVersion.Store(int64(version))
			if phase.Params.ForceToDisk {
				// Evict all data from the OS page cache before each version
				// so reads actually hit disk rather than serving from cached mmap pages
				err := EvictFromPageCache(params.LoaderParams.TreeDir)
				if err != nil {
					return fmt.Errorf("error evicting from page cache: %w", err)
				}
			}
			err := applyVersion(logger, tree, versionSim, int64(version))
			if err != nil {
				return fmt.Errorf("error applying version %d: %w", version, err)
			}
		}
	}

	err := tree.Close()
	if err != nil {
		return fmt.Errorf("error closing tree: %w", err)
	}
	logger.Info("closed tree")

	logger.Info(
		"benchmark run complete",
		"versions_applied", currentVersion.Load(),
	)

	close(closeCh)
	<-doneCh

	err = telemetry.Shutdown(context.Background())
	if err != nil {
		logger.Warn("error shutting down telemetry", "error", err)
		return err
	}

	return nil
}

func captureSystemInfo(logger *slog.Logger) {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		logger.Warn("could not read build info")
	}

	cpuInfo, err := cpu.Info()
	if err != nil {
		logger.Warn("could not read cpu info", "error", err)
	}

	memInfo, err := mem.VirtualMemory()
	if err != nil {
		logger.Warn("could not read memory info", "error", err)
	}

	hostInfo, err := host.Info()
	if err != nil {
		logger.Warn("could not read host info", "error", err)
	}

	diskInfo, err := disk.Usage("/")
	if err != nil {
		logger.Warn("could not read disk info", "error", err)
	}

	logger.Debug("system info",
		"build_info", buildInfo.String(),
		"cpu_info", cpuInfo,
		"mem_info", memInfo,
		"host_info", hostInfo,
		"disk_info", diskInfo,
	)

	// capture initial disk IO state
	initialDiskCounters, err := disk.IOCounters()
	if err != nil {
		logger.Warn("could not read initial disk io counters", "error", err)
	} else {
		logger.Debug("initial disk io counters", "disk_io_counters", initialDiskCounters)
	}

	// initialize CPU tracking - first call establishes baseline
	initialCPUTimes, err := cpu.Times(true)
	if err != nil {
		logger.Warn("could not read initial cpu times", "error", err)
	} else {
		logger.Debug("initial cpu times", "cpu_times", initialCPUTimes)
	}

	// call cpu.Percent to establish baseline for subsequent calls
	_, _ = cpu.Percent(0, true)
}

func applyVersion(logger *slog.Logger, tree MultiTree, versionSim VersionSim, version int64) error {
	logger.Info("simulating reads", "version", version, "concurrent_readers", len(versionSim.ReaderOps))

	startReadTime := time.Now()

	var readCount atomic.Uint64
	g := new(errgroup.Group)
	for _, reader := range versionSim.ReaderOps {
		g.Go(func() error {
			for op := range reader {
				_, err := tree.Tree(op.Store).Get(op.Key)
				if err != nil {
					return fmt.Errorf("reading key from store %s: %w", op.Store, err)
				}
				readCount.Add(1)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	readDuration := time.Since(startReadTime)
	logger.Info("completed reads",
		"version", version,
		"total_reads", readCount.Load(),
		"duration", readDuration,
		"concurrent_readers", len(versionSim.ReaderOps),
	)

	logger.Info("applying changeset", "version", version)
	startTime := time.Now()

	// TODO add gets

	err := tree.Commit(versionSim.Updates)
	if err != nil {
		return fmt.Errorf("error committing version %d: %w", version, err)
	}

	if tree.Version() != version {
		return fmt.Errorf("committed version %d does not match expected version %d", tree.Version(), version)
	}

	duration := time.Since(startTime)
	count := uint32(0)
	totalSize := int64(0)
	for storeName, storeUpdates := range versionSim.Updates {
		count += storeUpdates.TotalOps
		totalSize += tree.Tree(storeName).Size()
	}
	opsPerSec := float64(count) / duration.Seconds()

	// get mem stats

	logger.Info(
		"committed version",
		"version", version,
		"duration", duration,
		"count", count,
		"ops_per_sec", opsPerSec,
		"total_size", totalSize,
	)

	return nil
}

func measureBackgroundStats(logger *slog.Logger, currentVersion *atomic.Int64, path string, closeCh <-chan struct{}) <-chan struct{} {
	doneChan := make(chan struct{})
	go func() {
		fastTicker := time.NewTicker(1 * time.Second)
		slowTicker := time.NewTicker(10 * time.Second)
		defer fastTicker.Stop()
		defer slowTicker.Stop()
		for {
			select {
			case <-fastTicker.C:
				// capture mem stats
				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)
				logger.Info("mem stats", "version", currentVersion.Load(),
					"alloc", humanize.Bytes(memStats.Alloc),
					"total_alloc", humanize.Bytes(memStats.TotalAlloc),
					"sys", humanize.Bytes(memStats.Sys),
					"num_gc", memStats.NumGC,
					"gc_sys", humanize.Bytes(memStats.GCSys),
					"heap_sys", humanize.Bytes(memStats.HeapSys),
					"heap_idle", humanize.Bytes(memStats.HeapIdle),
					"heap_inuse", humanize.Bytes(memStats.HeapInuse),
					"heap_released", humanize.Bytes(memStats.HeapReleased),
					"heap_objects", memStats.HeapObjects,
					"gc_pause_total", memStats.PauseTotalNs,
					"gc_cpu_fraction", memStats.GCCPUFraction,
				)

				// get cpu utilization data
				cpuPercents, err := cpu.Percent(0, true)
				if err != nil {
					logger.Warn("could not read cpu percent", "error", err)
				}

				cpuTimes, err := cpu.Times(true)
				if err != nil {
					logger.Warn("could not read cpu times", "error", err)
				}
				logger.Info("cpu usage", "version", currentVersion.Load(), "cpu_percents", cpuPercents, "cpu_times", cpuTimes)

				// get disk io stats
				diskIOCounters, err := disk.IOCounters()
				if err != nil {
					logger.Warn("could not read disk io counters", "error", err)
				}
				logger.Info("disk io counters", "version", currentVersion.Load(), "disk_io_counters", diskIOCounters)

			case <-slowTicker.C:
				// capture disk usage (expensive operation)
				size := getDirSize(logger, path)
				logger.Info("disk usage", "version", currentVersion.Load(), "size", humanize.Bytes(size))

			case <-closeCh:
				close(doneChan)
				return
			}
		}
	}()
	return doneChan
}

func getDirSize(logger *slog.Logger, path string) uint64 {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			// we don't want to fail the whole operation if there's an error walking a path
			// just log it and continue, the files may change in the meantime
			logger.Warn("error walking path", "path", path, "error", err)
			return nil
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		logger.Warn("error getting dir size", "path", path, "error", err)
	}
	return uint64(size)
}
