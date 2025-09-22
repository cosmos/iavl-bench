package internal

type KVUpdate struct {
	SetNode   *MemNode
	DeleteKey []byte
}

type KVUpdateBatch struct {
	StagedVersion uint64
	Updates       []KVUpdate
}

func NewKVUpdateBatch(stagedVersion uint64) *KVUpdateBatch {
	return &KVUpdateBatch{StagedVersion: stagedVersion}
}
