package bench

import "math/rand/v2"

type SimParams struct {
	Name   string            `json:"name"`
	Stores []KVParams        `json:"stores"`
	Phases []MultiStorePhase `json:"phases"`
	// ConcurrentReaders indicates the number of concurrent readers to simulate.
	// Gets will be divided evenly among the readers.
	ConcurrentReaders uint32 `json:"concurrent_readers"`
}

type MultiStorePhase struct {
	Name     string                `json:"name"`
	Versions uint32                `json:"versions"`
	Stores   map[string]StorePhase `json:"stores"`
	// ClearCaches indicates whether to clear OS page caches before each version.
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

func genBytes(rng *rand.Rand, mean, stdDev float64) []byte {
	length := int(rng.NormFloat64()*stdDev + mean)
	// length must be at least 1
	// explanation: normal distribution is a poor approximation of certain data sets where std dev is skewed
	// by outliers on the upper bound.  mean - std dev can be negative, which is not a valid length.
	// we could just clamp length at 1, but that would skew the distribution of lengths towards 0 which is
	// not realistic.  instead we just generate again closer to the mean with a std dev of mean / 3.
	// this is not perfect but good enough for test sets.
	if length < 1 {
		length = int(rng.NormFloat64()*(mean/3) + mean)
		// much lower probability of this happening twice, but just in case
		if length < 1 {
			length = 1
		}
	}
	b := make([]byte, length)
	for i := 0; i < length; i++ {
		b[i] = byte(rng.IntN(256))
	}
	return b
}
