package bench

import (
	"iter"
	"math/rand/v2"
)

type Update = struct {
	Key, Value []byte
	Delete     bool
}

type SimParams struct {
	Name   string            `json:"name"`
	Stores []KVParams        `json:"stores"`
	Phases []MultiStorePhase `json:"phases"`
	// ConcurrentReaders indicates the number of concurrent readers to simulate.
	// The number of gets will be multiplied by this number to determine the total number of read operations.
	ConcurrentReaders uint32 `json:"concurrent_readers"`
}

type MultiStorePhase struct {
	Name     string                `json:"name"`
	Versions uint32                `json:"versions"`
	Stores   map[string]StorePhase `json:"stores"`
	// ForceToDisk indicates whether to force all data to disk before the start of this phase
	ForceToDisk bool `json:"force_to_disk"`
	// ClearCaches indicates whether to clear OS page caches before each version (requires ForceToDisk)
	ClearCaches bool `json:"clear_caches"`
}

type StorePhase struct {
	Inserts uint32 `json:"inserts"`
	Updates uint32 `json:"updates"`
	Deletes uint32 `json:"deletes"`
	Gets    uint32 `json:"gets"`
}

type KVParams struct {
	Name           string  `json:"name"`
	KeyLenMean     float64 `json:"key_len_mean"`
	KeyLenStdDev   float64 `json:"key_len_stddev"`
	ValueLenMean   float64 `json:"value_len_mean"`
	ValueLenStdDev float64 `json:"value_len_stddev"`
}

func (kv KVParams) GenKey(index, seed2 uint64) []byte {
	return genBytes(rand.New(rand.NewPCG(index, seed2)), kv.KeyLenMean, kv.KeyLenStdDev)
}

func (kv KVParams) GenValue(rng *rand.Rand) []byte {
	return genBytes(rng, kv.ValueLenMean, kv.ValueLenStdDev)
}

type MultiStoreGenerator struct {
	store map[string]*StoreGenerator
}

type MultiStoreUpdates = map[string]StoreUpdates

type VersionSim struct {
	// ReaderOps are sequences of read operations to perform before applying updates,
	// these should be run in parallel to simulate concurrent reads.
	ReaderOps []iter.Seq[ReadOp]
	Updates   MultiStoreUpdates
}

type ReadOp struct {
	Store string
	Key   []byte
}

type StoreUpdates struct {
	Updates  iter.Seq[Update]
	TotalOps uint32
}

type PhaseSim struct {
	Params   MultiStorePhase
	Versions iter.Seq2[uint32, VersionSim]
}

func GenSimulation(params SimParams) iter.Seq[PhaseSim] {
	generator := &MultiStoreGenerator{
		store: make(map[string]*StoreGenerator),
	}
	for i, storeParams := range params.Stores {
		generator.store[storeParams.Name] = &StoreGenerator{
			rng:      rand.New(rand.NewPCG(uint64(i), 0)),
			kvParams: storeParams,
			seed2:    uint64(i),
		}
	}
	return func(yield func(sim PhaseSim) bool) {
		var version uint32
		for _, phaseParams := range params.Phases {
			if !yield(PhaseSim{
				Params: phaseParams,
				Versions: func(yield func(uint32, VersionSim) bool) {
					for i := 0; i < int(phaseParams.Versions); i++ {
						version++
						concurrentReaders := params.ConcurrentReaders
						if concurrentReaders == 0 {
							concurrentReaders = 1
						}
						readers := make([]iter.Seq[ReadOp], concurrentReaders)
						for r := uint32(0); r < concurrentReaders; r++ {
							readers[r] = generator.GenVersionReadOps(phaseParams)
						}
						updates := generator.GenVersionUpdates(phaseParams)
						if !yield(version, VersionSim{ReaderOps: readers, Updates: updates}) {
							return
						}
					}
				},
			}) {
				return
			}
		}
	}
}

func (g *MultiStoreGenerator) GenVersionReadOps(phaseParams MultiStorePhase) iter.Seq[ReadOp] {
	remainingCounts := map[string]uint32{}
	for storeName, storePhaseParams := range phaseParams.Stores {
		remainingCounts[storeName] = storePhaseParams.Gets
	}

	return func(yield func(op ReadOp) bool) {
		// naive algorithm simply iterates over all stores and generates gets until all are done
		for len(remainingCounts) > 0 {
			for storeName, count := range remainingCounts {
				if count == 0 {
					delete(remainingCounts, storeName)
					continue
				}
				storeGen := g.store[storeName]
				key := storeGen.GenGet()
				if key == nil {
					// no keys to get from this store
					delete(remainingCounts, storeName)
					continue
				}
				remainingCounts[storeName]--
				if !yield(ReadOp{Store: storeName, Key: key}) {
					return
				}
			}
		}
	}
}

func (g *MultiStoreGenerator) GenVersionUpdates(phaseParams MultiStorePhase) map[string]StoreUpdates {
	result := make(map[string]StoreUpdates)
	for storeName, storePhaseParams := range phaseParams.Stores {
		storeGen, exists := g.store[storeName]
		if !exists {
			panic("store generator not found: " + storeName)
		}
		result[storeName] = storeGen.GenVersionUpdates(storePhaseParams)
	}
	return result
}

type StoreGenerator struct {
	rng         *rand.Rand
	insertIndex uint64
	deleteIndex uint64
	kvParams    KVParams
	seed2       uint64
}

func (g *StoreGenerator) GenGet() []byte {
	getStartRange := g.deleteIndex
	getEndRange := g.insertIndex
	if getEndRange <= getStartRange {
		// no keys to get
		return nil
	}

	keyIndex := g.rng.Uint64N(getEndRange-getStartRange) + getStartRange
	return g.kvParams.GenKey(keyIndex, g.seed2)
}

func (g *StoreGenerator) GenVersionUpdates(phaseParams StorePhase) StoreUpdates {
	updateRangeEnd := g.insertIndex
	updatesPerVersion := phaseParams.Inserts + phaseParams.Updates + phaseParams.Deletes
	deleteRatio := float64(phaseParams.Deletes) / float64(updatesPerVersion)
	insertRatio := float64(phaseParams.Inserts) / float64(updatesPerVersion)
	updateRatio := 1.0 - insertRatio - deleteRatio
	updates := func(yield func(Update) bool) {
		for i := uint32(0); i < updatesPerVersion; i++ {
			r := g.rng.Float64()
			var update Update
			hasOriginalKeys := updateRangeEnd > g.deleteIndex

			if r < deleteRatio && hasOriginalKeys {
				// delete only when we have some original keys
				update = Update{
					Key:    g.kvParams.GenKey(g.deleteIndex, g.seed2),
					Delete: true,
				}
				g.deleteIndex++
			} else if r < deleteRatio+updateRatio && hasOriginalKeys {
				// also update only when we have some original keys
				keyIndex := g.rng.Uint64N(updateRangeEnd-g.deleteIndex) + g.deleteIndex
				update = Update{
					Key:    g.kvParams.GenKey(keyIndex, g.seed2),
					Value:  g.kvParams.GenValue(g.rng),
					Delete: false,
				}
			} else {
				// otherwise insert
				update = Update{
					Key:    g.kvParams.GenKey(g.insertIndex, g.seed2),
					Value:  g.kvParams.GenValue(g.rng),
					Delete: false,
				}
				g.insertIndex++
			}

			if !yield(update) {
				return
			}
		}
	}
	return StoreUpdates{
		Updates:  updates,
		TotalOps: updatesPerVersion,
	}
}
