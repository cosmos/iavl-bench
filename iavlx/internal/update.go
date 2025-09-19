package internal

type KVUpdate struct {
	SetNode   *MemNode
	DeleteKey []byte
}

type KVUpdateBatch struct {
	StagedVersion uint64
	Updates       []KVUpdate
}
