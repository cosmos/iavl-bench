package bench

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"

	storetypes "cosmossdk.io/store/types"
	"github.com/cosmos/cosmos-sdk/telemetry"
	"github.com/dustin/go-humanize"
	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"golang.org/x/sync/errgroup"
)

type simulator struct {
	rng       *rand.Rand
	store     map[string]*StoreGenerator
	simParams SimParams
	version   atomic.Int64
}

type TreeParams struct {
	TreeLogger  *slog.Logger
	TreeLoader  TreeLoader
	TreeDir     string
	TreeOptions any
	TreeType    string
}

func RunSimulation(logger *slog.Logger, treeParams TreeParams, simParams SimParams) error {
	sim := &simulator{
		rng:       rand.New(rand.NewPCG(0, 0)),
		store:     make(map[string]*StoreGenerator),
		simParams: simParams,
	}
	storeKeys := make([]*storetypes.KVStoreKey, len(simParams.Stores))
	for i, storeParams := range simParams.Stores {
		key := storetypes.NewKVStoreKey(storeParams.Name)
		sim.store[storeParams.Name] = &StoreGenerator{
			kvParams: storeParams,
			seed2:    uint64(i),
			storeKey: key,
		}
		storeKeys[i] = key
	}

	loaderParams := LoaderParams{
		TreeDir:     treeParams.TreeDir,
		TreeOptions: treeParams.TreeOptions,
		StoreKeys:   storeKeys,
		Logger:      treeParams.TreeLogger,
	}

	tree, err := treeParams.TreeLoader(loaderParams)
	if err != nil {
		return fmt.Errorf("failed to load tree: %w", err)
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
		"gen_params", simParams,
		"db_dir", treeParams.TreeDir,
		"db_options", treeParams.TreeOptions,
		"tree_type", treeParams.TreeType,
	)

	captureSystemInfo(logger)

	closeCh := make(chan struct{})
	sim.version.Store(version)
	doneCh := measureBackgroundStats(logger, &sim.version, treeParams.TreeDir, closeCh)

	for _, phase := range simParams.Phases {
		logger.Info("starting phase", "phase", phase)
		for phaseVersion := uint32(1); phaseVersion <= phase.Versions; phaseVersion++ {
			sim.version.Add(1)
			if phase.ClearCaches {
				// Evict all data from the OS page cache before each version
				// so reads actually hit disk rather than serving from cached mmap pages
				err := EvictFromPageCache(treeParams.TreeDir)
				if err != nil {
					return fmt.Errorf("error evicting from page cache: %w", err)
				}
			}
			err := sim.applyVersion(logger, tree, phase)
			if err != nil {
				return fmt.Errorf("error applying version %d: %w", sim.version.Load(), err)
			}
		}
	}

	err = tree.Close()
	if err != nil {
		return fmt.Errorf("error closing tree: %w", err)
	}
	logger.Info("closed tree")

	logger.Info(
		"benchmark run complete",
		"versions_applied", sim.version.Load(),
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

func (sim *simulator) applyVersion(logger *slog.Logger, tree RootMultiTree, phaseParams MultiStorePhase) error {
	version := sim.version.Load()
	logger.Info("simulating reads", "version", version, "concurrent_readers", sim.simParams.ConcurrentReaders)

	startReadTime := time.Now()

	totalReads, err := sim.applyVersionReadOps(phaseParams, tree)
	if err != nil {
		return fmt.Errorf("error applying read ops for version %d: %w", version, err)
	}

	readDuration := time.Since(startReadTime)
	logger.Info("completed reads",
		"version", version,
		"total_reads", totalReads,
		"duration", readDuration,
		"concurrent_readers", sim.simParams.ConcurrentReaders,
	)

	logger.Info("applying updates to cache mutlistore", "version", version)

	cacheMt := tree.CacheMultiTree()
	totalUpdates, err := sim.applyVersionUpdatesToCache(phaseParams, cacheMt)
	if err != nil {
		return fmt.Errorf("error applying updates to cache for version %d: %w", version, err)
	}

	logger.Info("applying changeset", "version", version)
	startTime := time.Now()

	err = tree.Commit(cacheMt)
	if err != nil {
		return fmt.Errorf("error committing version %d: %w", version, err)
	}

	if tree.Version() != version {
		return fmt.Errorf("committed version %d does not match expected version %d", tree.Version(), version)
	}

	duration := time.Since(startTime)
	totalSize := int64(0)
	opsPerSec := float64(totalUpdates) / duration.Seconds()

	// get mem stats

	logger.Info(
		"committed version",
		"version", version,
		"duration", duration,
		"count", totalUpdates,
		"ops_per_sec", opsPerSec,
		"total_size", totalSize,
	)

	return nil
}

func (sim *simulator) applyVersionReadOps(phaseParams MultiStorePhase, tree RootMultiTree) (int64, error) {
	var errGroup errgroup.Group
	var totalReads atomic.Int64
	for i := uint32(0); i < sim.simParams.ConcurrentReaders; i++ {
		sim.applyVersionReadOpsThread(&errGroup, &totalReads, phaseParams, tree, i)
	}
	return totalReads.Load(), errGroup.Wait()
}

func (sim *simulator) applyVersionReadOpsThread(errGroup *errgroup.Group, totalReads *atomic.Int64, phaseParams MultiStorePhase, tree RootMultiTree, idx uint32) {
	remainingCounts := map[string]uint32{}
	for storeName, storePhaseParams := range phaseParams.Stores {
		remainingCounts[storeName] = storePhaseParams.Gets / sim.simParams.ConcurrentReaders
		if idx == 0 {
			// add any remainder to the first reader
			remainingCounts[storeName] += storePhaseParams.Gets % sim.simParams.ConcurrentReaders
		}
	}

	perThreadRng := rand.New(rand.NewPCG(sim.rng.Uint64(), sim.rng.Uint64()))

	errGroup.Go(func() error {
		// each thread gets its own cached tree against the latest state
		cachedTree := tree.CacheMultiTree()
		// naive algorithm simply iterates over all stores and generates gets until all are done
		for len(remainingCounts) > 0 {
			for storeName, count := range remainingCounts {
				if count == 0 {
					delete(remainingCounts, storeName)
					continue
				}
				storeGen := sim.store[storeName]
				key := storeGen.GenGet(perThreadRng)
				if key == nil {
					// no keys to get from this store
					delete(remainingCounts, storeName)
					continue
				}
				remainingCounts[storeName]--
				value := cachedTree.GetKVStore(storeGen.storeKey).Get(key)
				if value == nil {
					return fmt.Errorf("key not found: store=%s key=%x", storeName, key)
				}
				totalReads.Add(1)
			}
		}
		return nil
	})
}

func (sim *simulator) applyVersionUpdatesToCache(phaseParams MultiStorePhase, cachedTree MultiTree) (int64, error) {
	var wg errgroup.Group
	var totalUpdates atomic.Int64
	for storeName, storePhaseParams := range phaseParams.Stores {
		storeGen, exists := sim.store[storeName]
		if !exists {
			return 0, fmt.Errorf("store generator not found: " + storeName)
		}
		perThreadRng := rand.New(rand.NewPCG(sim.rng.Uint64(), sim.rng.Uint64()))
		// we apply updates in parallel to each cached store,
		// but this isn't really applying the updates to iavl, just the cache, so this should be safe
		store := cachedTree.GetKVStore(storeGen.storeKey)
		wg.Go(func() error {
			err := storeGen.ApplyVersionUpdatesToCache(&totalUpdates, storePhaseParams, store, perThreadRng)
			if err != nil {
				return fmt.Errorf("failed to apply updates for store %s: %w", storeName, err)
			}
			return nil
		})
	}
	return totalUpdates.Load(), wg.Wait()
}

func measureBackgroundStats(logger *slog.Logger, currentVersion *atomic.Int64, path string, closeCh <-chan struct{}) <-chan struct{} {
	doneChan := make(chan struct{})
	go func() {
		fastTicker := time.NewTicker(5 * time.Second)
		slowTicker := time.NewTicker(60 * time.Second)
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
