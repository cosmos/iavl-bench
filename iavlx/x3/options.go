package x3

type Options struct {
	ZeroCopy bool `json:"zero_copy"`
	// EvictDepth defines the depth at which eviction occurs. 255 means no eviction.
	EvictDepth uint8 `json:"evict_depth"`

	// WriteWAL enables write-ahead logging for durability
	WriteWAL bool `json:"write_wal"`

	// WalSyncBuffer controls WAL sync behavior: -1 = blocking fsync, 0 = async sync immediately (buffer=1), >0 = buffer size
	WalSyncBuffer int `json:"wal_sync_buffer"`

	// CompactWAL determines if KV data is copied during compaction (true) or reused (false)
	CompactWAL bool `json:"compact_wal"`
	// DisableCompaction turns off background compaction entirely
	DisableCompaction bool `json:"disable_compaction"`

	// CompactionOrphanRatio is the orphan/total ratio (0-1) that triggers compaction
	CompactionOrphanRatio float64 `json:"compaction_orphan_ratio"`
	// CompactionOrphanAge is the average age of orphans (in versions) at which compaction is triggered
	CompactionOrphanAge uint32 `json:"compaction_orphan_age"`

	// RetainVersions is the number of recent versions to keep uncompacted
	RetainVersions uint32 `json:"retain_versions"`
	// MinCompactionSeconds is the minimum interval between compaction runs
	MinCompactionSeconds uint32 `json:"min_compaction_seconds"`
	// ChangesetMaxTarget is the maximum size of a changeset file when joining changesets.
	ChangesetMaxTarget uint32 `json:"changeset_max_target"`
}
