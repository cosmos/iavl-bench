package x3

import (
	"fmt"
	"unsafe"
)

// check little endian at init time
func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	if buf != [2]byte{0xCD, 0xAB} {
		panic("native byte order is not little endian, please build without nativebyteorder")
	}
}

type StructFile[T any] struct {
	items []T
	file  *MmapFile
	size  int
	align uintptr
}

func NewStructFile[T any](filename string) (*StructFile[T], error) {
	file, err := NewMmapFile(filename)
	if err != nil {
		return nil, err
	}

	var zero T
	df := &StructFile[T]{
		file:  file,
		size:  int(unsafe.Sizeof(zero)),
		align: unsafe.Alignof(zero),
	}

	err = df.updateData(df.file.handle)
	if err != nil {
		_ = df.file.Close()
		return nil, err
	}

	return df, nil
}

func (df *StructFile[T]) SaveAndRemap() error {
	err := df.file.SaveAndRemap()
	if err != nil {
		return err
	}
	return df.updateData(df.file.handle)
}

func (df *StructFile[T]) updateData(buf []byte) error {
	if len(buf) == 0 {
		df.items = nil
		return nil
	}

	// check alignment and size of the buffer
	p := unsafe.Pointer(unsafe.SliceData(buf))
	if uintptr(p)%df.align != 0 {
		return fmt.Errorf("input buffer is not aligned: %p", p)
	}

	size := df.size
	if len(buf)%size != 0 {
		return fmt.Errorf("input buffer size is not a multiple of leaf size: %d %% %d != 0", len(buf), size)
	}
	data := unsafe.Slice((*T)(p), len(buf)/size)
	df.items = data
	return nil
}

func (df *StructFile[T]) Item(i uint32) T {
	df.file.flushLock.RLock()
	defer df.file.flushLock.RUnlock()

	return df.items[i]
}

func (df *StructFile[T]) OnDiskCount() uint32 {
	df.file.flushLock.RLock()
	defer df.file.flushLock.RUnlock()

	return uint32(len(df.items))
}

func (df *StructFile[T]) Append(layout *T) error {
	_, err := df.file.Write(unsafe.Slice((*byte)(unsafe.Pointer(layout)), df.size))
	return err
}

func (df *StructFile[T]) TotalCount() uint32 {
	return uint32(df.file.Offset() / df.size)
}

func (df *StructFile[T]) Close() error {
	return df.file.Close()
}

type NodeLayout interface {
	ID() NodeID
}

type NodeFile[T NodeLayout] struct {
	*StructFile[T]
}

func NewNodeFile[T NodeLayout](filename string) (*NodeFile[T], error) {
	sf, err := NewStructFile[T](filename)
	if err != nil {
		return nil, err
	}
	return &NodeFile[T]{StructFile: sf}, nil
}

func (nf *NodeFile[T]) FindByID(id NodeID, info *NodeSetInfo) (*T, error) {
	// binary search with interpolation
	lowOffset := info.StartOffset
	targetIdx := id.Index()
	lowIdx := info.StartIndex
	highOffset := lowOffset + info.Count - 1
	highIdx := info.EndIndex
	for lowOffset <= highOffset {
		if targetIdx < lowIdx || targetIdx > highIdx {
			return nil, fmt.Errorf("node ID %s not present", id.String())
		}
		// If nodes are contiguous in this range, compute offset directly
		if highIdx-lowIdx == highOffset-lowOffset {
			targetOffset := lowOffset + (targetIdx - lowIdx)
			return &nf.items[targetOffset], nil
		}
		// Interpolation search: estimate position based on target's relative position in index range
		var mid uint32
		if highIdx > lowIdx {
			// Estimate where target should be based on its position in the index range
			fraction := float64(targetIdx-lowIdx) / float64(highIdx-lowIdx)
			mid = lowOffset + uint32(fraction*float64(highOffset-lowOffset))
			// Ensure mid stays within bounds
			if mid < lowOffset {
				mid = lowOffset
			} else if mid > highOffset {
				mid = highOffset
			}
		} else {
			// When indices converge, use simple midpoint
			mid = (lowOffset + highOffset) / 2
		}
		midNode := &nf.items[mid]
		midIdx := (*midNode).ID().Index()
		if midIdx == targetIdx {
			return midNode, nil
		} else if midIdx < targetIdx {
			lowOffset = mid + 1
			lowIdx = midIdx + 1
		} else {
			highOffset = mid - 1
			highIdx = midIdx - 1
		}
	}
	return nil, fmt.Errorf("node ID %s not found", id.String())
}
