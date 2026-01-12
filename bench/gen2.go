package bench

import (
	"iter"
	"math/rand/v2"
)

type Update = struct {
	Key, Value []byte
	Delete     bool
}

type GenParams struct {
	Name   string            `json:"name"`
	Stores []KVParams        `json:"stores"`
	Phases []MultiStorePhase `json:"phases"`
}

type MultiStorePhase struct {
	Versions uint32                `json:"versions"`
	Stores   map[string]StorePhase `json:"stores"`
}

type StorePhase struct {
	UpdatesPerVersion uint32  `json:"updates_per_version"`
	InsertRatio       float64 `json:"insert_ratio"`
	DeleteRatio       float64 `json:"delete_ratio"`
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

type MultiStoreUpdates = map[string]iter.Seq[Update]

func GenMultiStoreUpdates(params GenParams) iter.Seq2[uint32, MultiStoreUpdates] {
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
	return func(yield func(uint32, MultiStoreUpdates) bool) {
		var version uint32
		for _, phaseParams := range params.Phases {
			for i := 0; i < int(phaseParams.Versions); i++ {
				version++
				updates := generator.GenVersionUpdates(phaseParams)
				if !yield(version, updates) {
					return
				}
			}
		}
	}
}

func (g *MultiStoreGenerator) GenVersionUpdates(phaseParams MultiStorePhase) map[string]iter.Seq[Update] {
	result := make(map[string]iter.Seq[Update])
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

func (g *StoreGenerator) GenVersionUpdates(phaseParams StorePhase) iter.Seq[Update] {
	updateRangeEnd := g.insertIndex
	return func(yield func(Update) bool) {
		for i := uint32(0); i < phaseParams.UpdatesPerVersion; i++ {
			r := g.rng.Float64()
			var update Update
			hasOriginalKeys := updateRangeEnd > g.deleteIndex
			updateRatio := 1.0 - phaseParams.InsertRatio - phaseParams.DeleteRatio

			if r < phaseParams.DeleteRatio && hasOriginalKeys {
				// delete only when we have some original keys
				update = Update{
					Key:    g.kvParams.GenKey(g.deleteIndex, g.seed2),
					Delete: true,
				}
				g.deleteIndex++
			} else if r < phaseParams.DeleteRatio+updateRatio && hasOriginalKeys {
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
}
