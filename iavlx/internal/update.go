package internal

type KVUpdate struct {
	SetNode   *MemNode
	DeleteKey []byte
}

type KVUpdateBatch struct {
	Updates []KVUpdate
}
