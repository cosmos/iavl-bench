package x3

type KVUpdate struct {
	SetNode   *MemNode
	DeleteKey []byte
}

type KVUpdateBatch struct {
	StagedVersion uint64
	Updates       []KVUpdate
	Orphans       []NodeID
}

func NewKVUpdateBatch(stagedVersion uint64) *KVUpdateBatch {
	return &KVUpdateBatch{StagedVersion: stagedVersion}
}

type MutationContext struct {
	Version uint32
	Orphans []NodeID
}

func (ctx *MutationContext) MutateBranch(node Node) (*MemNode, error) {
	id := node.ID()
	if id != 0 {
		ctx.Orphans = append(ctx.Orphans, id)
	}
	return node.MutateBranch(ctx.Version)
}

func (ctx *MutationContext) AddOrphan(id NodeID) {
	if id != 0 {
		ctx.Orphans = append(ctx.Orphans, id)
	}
}
