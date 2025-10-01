package x3

type Options struct {
	ZeroCopy              bool    `json:"zero_copy"`
	EvictDepth            uint8   `json:"evict_depth"` // 255 means no eviction
	WriteWAL              bool    `json:"write_wal"`
	CompactWAL            bool    `json:"compact_wal"`
	DisableCompaction     bool    `json:"disable_compaction"`
	CompactionOrphanRatio float64 `json:"compaction_orphan_ratio"`
	CompactionOrphanAge   float64 `json:"compaction_orphan_age"`
	RetainVersions        uint32  `json:"retain_versions"`
	MinCompactionSeconds  uint32  `json:"min_compaction_seconds"`
}
