package bench

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync/atomic"

	storetypes "cosmossdk.io/store/types"
	"github.com/cosmos/cosmos-sdk/telemetry"
	"golang.org/x/sync/errgroup"
)

type Simulator struct {
	rng       *rand.Rand
	store     map[string]*StoreGenerator
	simParams SimParams
}

type PhaseSim struct {
	Params MultiStorePhase
	// we use an iterator here to avoid materializing all versions in memory at once
	Versions iter.Seq2[uint32, VersionSim]
}

type VersionSim struct {
	ApplyReads         func(MultiTree) error
	ApplyUpdateToCache func(MultiTree) error
}

type TreeParams struct {
	TreeLogger  *slog.Logger
	TreeLoader  TreeLoader
	TreeDir     string
	TreeOptions any
	TreeType    string
}

func RunSimulation(logger *slog.Logger, treeParams TreeParams, simParams SimParams) error {
	sim := &Simulator{
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
	currentVersion := atomic.Int64{}
	currentVersion.Store(version)
	doneCh := measureBackgroundStats(logger, &currentVersion, treeParams.TreeDir, closeCh)

	for _, phase := range simParams.Phases {
		logger.Info("starting phase", "phase", phase)
		for version := uint32(1); version <= phase.Versions; version++ {
			currentVersion.Store(int64(version))
			if phase.ClearCaches {
				// Evict all data from the OS page cache before each version
				// so reads actually hit disk rather than serving from cached mmap pages
				err := EvictFromPageCache(treeParams.TreeDir)
				if err != nil {
					return fmt.Errorf("error evicting from page cache: %w", err)
				}
			}
			err := sim.applyVersion(logger, tree, int64(version), phase)
			if err != nil {
				return fmt.Errorf("error applying version %d: %w", version, err)
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

func (g *Simulator) ApplyVersionReadOps(phaseParams MultiStorePhase, tree MultiTree) (int64, error) {
	var errGroup errgroup.Group
	var totalReads atomic.Int64
	for i := uint32(0); i < g.simParams.ConcurrentReaders; i++ {
		g.applyOneVersionReadOps(&errGroup, &totalReads, phaseParams, tree, i)
	}
	return totalReads.Load(), errGroup.Wait()
}

func (g *Simulator) applyOneVersionReadOps(errGroup *errgroup.Group, totalReads *atomic.Int64, phaseParams MultiStorePhase, tree MultiTree, idx uint32) {
	remainingCounts := map[string]uint32{}
	for storeName, storePhaseParams := range phaseParams.Stores {
		remainingCounts[storeName] = storePhaseParams.Gets / g.simParams.ConcurrentReaders
		if idx == 0 {
			// add any remainder to the first reader
			remainingCounts[storeName] += storePhaseParams.Gets % g.simParams.ConcurrentReaders
		}
	}

	perThreadRng := rand.New(rand.NewPCG(g.rng.Uint64(), g.rng.Uint64()))

	errGroup.Go(func() error {
		// naive algorithm simply iterates over all stores and generates gets until all are done
		for len(remainingCounts) > 0 {
			for storeName, count := range remainingCounts {
				if count == 0 {
					delete(remainingCounts, storeName)
					continue
				}
				storeGen := g.store[storeName]
				key := storeGen.GenGet(perThreadRng)
				if key == nil {
					// no keys to get from this store
					delete(remainingCounts, storeName)
					continue
				}
				remainingCounts[storeName]--
				value := tree.GetKVStore(storeGen.storeKey).Get(key)
				if value == nil {
					return fmt.Errorf("key not found: store=%s key=%x", storeName, key)
				}
				totalReads.Add(1)
			}
		}
		return nil
	})
}

func (g *Simulator) ApplyVersionUpdatesToCache(phaseParams MultiStorePhase, cachedTree MultiTree) (int64, error) {
	var wg errgroup.Group
	var totalUpdates atomic.Int64
	for storeName, storePhaseParams := range phaseParams.Stores {
		storeGen, exists := g.store[storeName]
		if !exists {
			return 0, fmt.Errorf("store generator not found: " + storeName)
		}
		perThreadRng := rand.New(rand.NewPCG(g.rng.Uint64(), g.rng.Uint64()))
		// we apply updates in parallel to each cached store,
		// but this isn't really applying the updates to iavl, just the cache, so this should be safe
		wg.Go(func() error {
			err := storeGen.ApplyVersionUpdatesToCache(&totalUpdates, storePhaseParams, cachedTree, perThreadRng)
			if err != nil {
				return fmt.Errorf("failed to apply updates for store %s: %w", storeName, err)
			}
			return nil
		})
	}
	return totalUpdates.Load(), wg.Wait()
}

type StoreGenerator struct {
	insertIndex uint64
	deleteIndex uint64
	kvParams    KVParams
	seed2       uint64
	storeKey    *storetypes.KVStoreKey
}

func (g *StoreGenerator) GenGet(rng *rand.Rand) []byte {
	getStartRange := g.deleteIndex
	getEndRange := g.insertIndex
	if getEndRange <= getStartRange {
		// no keys to get
		return nil
	}

	keyIndex := rng.Uint64N(getEndRange-getStartRange) + getStartRange
	return g.kvParams.GenKey(keyIndex, g.seed2)
}

func (g *StoreGenerator) ApplyVersionUpdatesToCache(totalUpdates *atomic.Int64, phaseParams StorePhase, cachedTree MultiTree, rng *rand.Rand) error {
	updateRangeEnd := g.insertIndex
	updatesPerVersion := phaseParams.Inserts + phaseParams.Updates + phaseParams.Deletes
	deleteRatio := float64(phaseParams.Deletes) / float64(updatesPerVersion)
	insertRatio := float64(phaseParams.Inserts) / float64(updatesPerVersion)
	updateRatio := 1.0 - insertRatio - deleteRatio
	store := cachedTree.GetKVStore(g.storeKey)
	for i := uint32(0); i < updatesPerVersion; i++ {
		r := rng.Float64()
		hasOriginalKeys := updateRangeEnd > g.deleteIndex

		if r < deleteRatio && hasOriginalKeys {
			// delete only when we have some original keys
			key := g.kvParams.GenKey(g.deleteIndex, g.seed2)
			store.Delete(key)

			g.deleteIndex++
		} else if r < deleteRatio+updateRatio && hasOriginalKeys {
			// also update only when we have some original keys
			keyIndex := rng.Uint64N(updateRangeEnd-g.deleteIndex) + g.deleteIndex
			key := g.kvParams.GenKey(keyIndex, g.seed2)
			value := g.kvParams.GenValue(rng)
			store.Set(key, value)
		} else {
			// otherwise insert
			key := g.kvParams.GenKey(g.insertIndex, g.seed2)
			value := g.kvParams.GenValue(rng)
			store.Set(key, value)
			g.insertIndex++
		}

		totalUpdates.Add(1)
	}
	return nil
}
