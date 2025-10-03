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
	// ChangesetMaxTarget is the maximum size of a changeset file when batching or joining changesets
	ChangesetMaxTarget uint32 `json:"changeset_max_target"`
	// CompactAfterVersions is the number of versions after which a full compaction is forced whenever there are orphans
	CompactAfterVersions uint32 `json:"compact_after_versions"`
}

// GetWalSyncBufferSize returns the actual buffer size to use (handling 0 = 1 case)
func (o Options) GetWalSyncBufferSize() int {
	if o.WalSyncBuffer == 0 {
		return 1 // 0 means async sync immediately with buffer of 1
	}
	return o.WalSyncBuffer
}

// GetCompactionOrphanAge returns the orphan age threshold with default
func (o Options) GetCompactionOrphanAge() uint32 {
	if o.CompactionOrphanAge == 0 {
		return 10 // Default to 10 versions
	}
	return o.CompactionOrphanAge
}

// GetCompactionOrphanRatio returns the orphan ratio threshold with default
func (o Options) GetCompactionOrphanRatio() float64 {
	if o.CompactionOrphanRatio <= 0 {
		return 0.6 // Default to 60% orphans
	}
	return o.CompactionOrphanRatio
}

// GetChangesetMaxTarget returns the max changeset size with default
func (o Options) GetChangesetMaxTarget() uint64 {
	if o.ChangesetMaxTarget == 0 {
		return 512 * 1024 * 1024 // 512MB default
	}
	return uint64(o.ChangesetMaxTarget)
}

func (o Options) GetCompactAfterVersions() uint32 {
	if o.CompactAfterVersions == 0 {
		return 500 // default to 500 versions
	}
	return o.CompactAfterVersions
}
