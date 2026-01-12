package bench

import (
	"iter"
	"math/rand/v2"
)

type Update struct {
	Key, Value []byte
	Delete     bool
}

type GenParams struct {
	StoreParams []KVParams        `json:"store_params"`
	Phases      []MultiStorePhase `json:"phases"`
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
	StoreName      string  `json:"store_name"`
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
	for i, storeParams := range params.StoreParams {
		generator.store[storeParams.StoreName] = &StoreGenerator{
			rng:      rand.New(rand.NewPCG(uint64(i), 0)),
			kvParams: storeParams,
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
			if r < phaseParams.InsertRatio || g.insertIndex == g.deleteIndex {
				// either we have selected an insert or we must insert because there are no keys to update/delete
				keyForInsert := g.kvParams.GenKey(g.insertIndex, g.seed2)
				valueForInsert := g.kvParams.GenValue(g.rng)
				update = Update{
					Key:    keyForInsert,
					Value:  valueForInsert,
					Delete: false,
				}
				g.insertIndex++
			} else if r < phaseParams.InsertRatio+phaseParams.DeleteRatio && g.deleteIndex < updateRangeEnd {
				// we have selected a delete and there are keys available to delete
				keyForDelete := g.kvParams.GenKey(g.deleteIndex, g.seed2)
				update = Update{
					Key:    keyForDelete,
					Delete: true,
				}
				g.deleteIndex++
			} else {
				keyIndex := g.rng.Uint64N(updateRangeEnd-g.deleteIndex) + g.deleteIndex
				keyForUpdate := g.kvParams.GenKey(keyIndex, g.seed2)
				valueForUpdate := g.kvParams.GenValue(g.rng)
				update = Update{
					Key:    keyForUpdate,
					Value:  valueForUpdate,
					Delete: false,
				}
			}
			if !yield(update) {
				return
			}
		}
	}
}
