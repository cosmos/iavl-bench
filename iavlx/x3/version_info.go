package x3

import (
	"fmt"
	"unsafe"
)

func init() {
	if unsafe.Sizeof(versionInfo{}) != VersionInfoSize {
		panic(fmt.Sprintf("invalid VersionInfo size: got %d, want %d", unsafe.Sizeof(versionInfo{}), VersionInfoSize))
	}
}

const VersionInfoSize = 40

type versionInfo struct {
	Leaves    NodeSetInfo
	Branches  NodeSetInfo
	RootIndex uint32
}

type NodeSetInfo struct {
	StartOffset uint32
	Count       uint32
	StartIndex  uint32
	EndIndex    uint32
}

type VersionInfo = *versionInfo

type Versions struct {
	versions []versionInfo
}

func NewVersions(buf []byte) (Versions, error) {
	// check alignment and size of the buffer
	p := unsafe.Pointer(unsafe.SliceData(buf))
	if uintptr(p)%unsafe.Alignof(BranchLayout{}) != 0 {
		return Versions{}, fmt.Errorf("input buffer is not aligned: %p", p)
	}
	size := int(unsafe.Sizeof(BranchLayout{}))
	if len(buf)%size != 0 {
		return Versions{}, fmt.Errorf("input buffer size is not a multiple of leaf size: %d %% %d != 0", len(buf), size)
	}
	versions := unsafe.Slice((*versionInfo)(p), len(buf)/size)
	return Versions{versions}, nil
}

func (versions Versions) Version(i uint32) VersionInfo {
	return &versions.versions[i]
}
