package internal

// RollingStore is the interface for both RollingDiff and RollingDiffInline
type RollingStore interface {
	NodeStore
	writeRoot(version uint64, root *NodePointer, lastBranchIdx uint32) error
	SavedVersion() uint64
}

// SavedVersion returns the last saved version for RollingDiff
func (rd *RollingDiff) SavedVersion() uint64 {
	return rd.savedVersion.Load()
}

// SavedVersion returns the last saved version for RollingDiffInline
func (rd *RollingDiffInline) SavedVersion() uint64 {
	return rd.savedVersion.Load()
}

var _ RollingStore = (*RollingDiff)(nil)
var _ RollingStore = (*RollingDiffInline)(nil)
