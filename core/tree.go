package core

type Tree interface {
	Set(key, value []byte) (bool, error)
	Get(key []byte) ([]byte, error)
	Remove(key []byte) ([]byte, bool, error)
	SaveVersion() ([]byte, int64, error)
	Size() int64
	Height() int8
}

type MultiTree struct {
	Trees map[string]Tree
}

func NewMultiTree() *MultiTree {
	return &MultiTree{
		Trees: make(map[string]Tree),
	}
}
