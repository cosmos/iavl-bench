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

type StructReader[T any] struct {
	items []T
	file  *MmapFile
	size  int
}

func NewStructReader[T any](filename string) (*StructReader[T], error) {
	file, err := NewMmapFile(filename)
	if err != nil {
		return nil, err
	}

	var zero T
	df := &StructReader[T]{
		file: file,
		size: int(unsafe.Sizeof(zero)),
	}

	buf := file.Data()
	p := unsafe.Pointer(unsafe.SliceData(buf))
	align := unsafe.Alignof(zero)
	if uintptr(p)%align != 0 {
		return nil, fmt.Errorf("input buffer is not aligned: %p", p)
	}

	size := df.size
	if len(buf)%size != 0 {
		return nil, fmt.Errorf("input buffer size is not a multiple of leaf size: %d %% %d != 0", len(buf), size)
	}
	data := unsafe.Slice((*T)(p), len(buf)/size)
	df.items = data

	return df, nil
}

func (df *StructReader[T]) UnsafeItem(i uint32) *T {
	return &df.items[i]
}

func (df *StructReader[T]) Count() int {
	return len(df.items)
}

func (df *StructReader[T]) Close() error {
	return df.file.Close()
}

type NodeLayout interface {
	ID() NodeID
}

type NodeReader[T NodeLayout] struct {
	*StructReader[T]
}

func NewNodeReader[T NodeLayout](filename string) (*NodeReader[T], error) {
	sf, err := NewStructReader[T](filename)
	if err != nil {
		return nil, err
	}
	return &NodeReader[T]{StructReader: sf}, nil
}

func (nf *NodeReader[T]) FindByID(id NodeID, info *NodeSetInfo) (*T, error) {
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
