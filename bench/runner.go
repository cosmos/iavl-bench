package bench

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"time"

	storev1beta1 "cosmossdk.io/api/cosmos/store/v1beta1"
	"github.com/dustin/go-humanize"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protodelim"
)

// Tree is a generic interface wrapping a multi-store tree structure.
type Tree interface {
	// Version should return the last committed version. If no version has been committed, it should return 0.
	Version() int64
	// ApplyUpdate should apply a single set or delete to the tree.
	ApplyUpdate(storeKey string, key, value []byte, delete bool) error
	// Commit should persist all changes made since the last commit and return the new version's hash.
	Commit() error
}

type LoaderParams struct {
	TreeDir     string
	TreeOptions interface{}
	StoreNames  []string
	Logger      *slog.Logger
}

type TreeLoader func(params LoaderParams) (Tree, error)

type RunConfig struct {
	TreeLoader  TreeLoader
	OptionsType interface{}
}

func Run(treeType string, cfg RunConfig) {
	var treeDir string
	var treeOptions string
	var changesetDir string
	var targetVersion int64
	var logHandlerType string
	var logFile string
	cmd := &cobra.Command{
		Use:   "bench",
		Short: "Runs benchmarks for the tree implementation.",
	}
	cmd.Flags().StringVar(&treeDir, "db-dir", "", "Directory for the db's data.")
	cmd.Flags().StringVar(&treeOptions, "db-options", "", "Implementation specific options for the db, in JSON format.")
	cmd.Flags().StringVar(&changesetDir, "changeset-dir", "", "Directory containing the changeset files.")
	cmd.Flags().Int64Var(&targetVersion, "target-version", 0, "Target version to apply changesets up to. If this is empty or 0, all remaining versions in the changeset-dir will be applied.")
	cmd.Flags().StringVar(&logHandlerType, "log-type", "text", "Log handler type. One of 'text' or 'json'.")
	cmd.Flags().StringVar(&logFile, "log-file", "", "If set, log output will be written to this file instead of stdout.")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		if treeDir == "" {
			return fmt.Errorf("tree-dir is required")
		}

		if changesetDir == "" {
			return fmt.Errorf("changeset-dir is required")
		}

		info, err := readInfoFile(changesetDir)
		if err != nil {
			return fmt.Errorf("error reading changeset info file: %w", err)
		}

		if targetVersion <= 0 {
			targetVersion = info.Versions
		}

		// decode db options from json
		var opts interface{}
		if cfg.OptionsType != nil {
			opts = reflect.New(reflect.TypeOf(cfg.OptionsType).Elem()).Interface()
			if treeOptions != "" {
				if cfg.OptionsType == nil {
					return fmt.Errorf("db-options provided but no OptionsType set in RunConfig")
				}
				err := json.Unmarshal([]byte(treeOptions), opts)
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
			handler = slog.NewTextHandler(logOut, nil)
		case "json":
			handler = slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})
		default:
			return fmt.Errorf("unknown log handler type: %s", logHandlerType)
		}

		logger := slog.New(handler)

		loaderParams := LoaderParams{
			TreeDir:     treeDir,
			TreeOptions: opts,
			StoreNames:  info.StoreNames,
			Logger:      logger.With("module", treeType),
		}

		tree, err := cfg.TreeLoader(loaderParams)
		if err != nil {
			return fmt.Errorf("error loading tree: %w", err)
		}

		return run(tree, changesetDir, runParams{
			TreeType:      treeType,
			TargetVersion: targetVersion,
			Logger:        logger,
			LoaderParams:  loaderParams,
		})
	}

	rootCmd := &cobra.Command{}
	rootCmd.AddCommand(cmd)
	err := rootCmd.Execute()
	if err != nil {
		slog.Error("error running benchmarks", "error", err)
		os.Exit(1)
	}
}

type runParams struct {
	TargetVersion int64
	Logger        *slog.Logger
	LoaderParams  LoaderParams
	TreeType      string
}

func run(tree Tree, changesetDir string, params runParams) error {
	logger := params.Logger
	if logger == nil {
		logger = slog.Default()
	}
	version := tree.Version()
	target := params.TargetVersion
	logger.Info("starting run",
		"start_version", version,
		"target_version", target,
		"changeset_dir", changesetDir,
		"db_dir", params.LoaderParams.TreeDir,
		"db_options", params.LoaderParams.TreeOptions,
		"tree_type", params.TreeType,
	)

	captureSystemInfo(logger)

	stats := &totalStats{}
	i := 0
	for version < target {
		version++
		err := applyVersion(logger, tree, changesetDir, params.LoaderParams.TreeDir, version, stats)
		if err != nil {
			return fmt.Errorf("error applying version %d: %w", version, err)
		}
		i++
	}

	opsPerSec := float64(stats.totalOps) / stats.totalTime.Seconds()
	logger.Info(
		"benchmark run complete",
		"versions_applied", i,
		"total_ops", stats.totalOps,
		"total_time", stats.totalTime,
		"ops_per_sec", opsPerSec,
		"max_mem_sys", humanize.Bytes(stats.maxSys),
		"max_disk_usage", humanize.Bytes(stats.maxDiskUsage),
	)

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

type totalStats struct {
	totalOps     uint64
	totalTime    time.Duration
	maxSys       uint64
	maxDiskUsage uint64
}

func applyVersion(logger *slog.Logger, tree Tree, changesetDir string, dbDir string, version int64, stats *totalStats) error {
	dataFilename := dataFilename(changesetDir, version)
	dataFile, err := os.Open(dataFilename)
	if err != nil {
		return fmt.Errorf("error opening changeset file for version %d: %w", version, err)
	}
	defer func() {
		err := dataFile.Close()
		if err != nil {
			panic(err)
		}
	}()
	reader := bufio.NewReader(dataFile)

	logger.Info("applying changeset", "version", version, "file", dataFilename)
	i := 0
	startTime := time.Now()
	for {
		if i%10_000 == 0 && i > 0 {
			logger.Debug("applied changes", "version", version, "count", i)
		}
		var storeKVPair storev1beta1.StoreKVPair
		err := protodelim.UnmarshalFrom(reader, &storeKVPair)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("error at entry %d reading changeset: %w", i, err)
		}

		err = tree.ApplyUpdate(storeKVPair.StoreKey, storeKVPair.Key, storeKVPair.Value, storeKVPair.Delete)
		if err != nil {
			return fmt.Errorf("error at entry %d applying update: %w", i, err)
		}

		i++
	}
	logger.Info("applied all changes, commiting", "version", version, "count", i)

	err = tree.Commit()
	if err != nil {
		return fmt.Errorf("error committing version %d: %w", version, err)
	}

	if tree.Version() != version {
		return fmt.Errorf("committed version %d does not match expected version %d", tree.Version(), version)
	}

	duration := time.Since(startTime)
	opsPerSec := float64(i) / duration.Seconds()

	// get mem stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// get cpu utilization data
	cpuPercents, err := cpu.Percent(0, true)
	if err != nil {
		logger.Warn("could not read cpu percent", "error", err)
	}

	cpuTimes, err := cpu.Times(true)
	if err != nil {
		logger.Warn("could not read cpu times", "error", err)
	}

	// get disk usage and io stats
	dirSize := getDirSize(logger, dbDir)
	diskIOCounters, err := disk.IOCounters()
	if err != nil {
		logger.Warn("could not read disk io counters", "error", err)
	}

	logger.Info(
		"committed version",
		"version", version,
		"duration", duration,
		"ops_per_sec", opsPerSec,
		"mem_allocs", humanize.Bytes(memStats.Alloc),
		"mem_sys", humanize.Bytes(memStats.Sys),
		"mem_heap_in_use", humanize.Bytes(memStats.HeapInuse),
		"mem_num_gc", memStats.NumGC,
		"disk_usage", humanize.Bytes(dirSize),
	)
	logger.Debug("full mem stats", "mem_stats", memStats)
	logger.Debug("disk io counters", "disk_io_counters", diskIOCounters)
	logger.Debug("cpu utilization", "cpu_percents", cpuPercents, "cpu_times", cpuTimes)

	stats.totalOps += uint64(i)
	stats.totalTime += duration
	if memStats.Sys > stats.maxSys {
		stats.maxSys = memStats.Sys
	}
	if dirSize > stats.maxDiskUsage {
		stats.maxDiskUsage = dirSize
	}

	return nil
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
