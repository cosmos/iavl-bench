package bench

import (
	"math/rand/v2"
	"sync/atomic"

	storetypes "cosmossdk.io/store/types"
)

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
