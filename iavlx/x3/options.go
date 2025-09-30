package x3

type Options struct {
	ZeroCopy   bool  `json:"zero_copy"`
	EvictDepth uint8 `json:"evict_depth"` // 255 means no eviction
}
