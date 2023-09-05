package core

import (
	"crypto/sha256"
	"fmt"
)

type Tree interface {
	Set(key, value []byte) (bool, error)
	Get(key []byte) ([]byte, error)
	Remove(key []byte) ([]byte, bool, error)
	SaveVersion() ([]byte, int64, error)
	Size() int64
	Height() int8
}

type MultiTree interface {
	GetTree(key string) (Tree, error)
	SaveVersions() ([]byte, error)
}

type NaiveMultiTree struct {
	Trees map[string]Tree
}

func (nmt *NaiveMultiTree) GetTree(key string) (Tree, error) {
	tree, ok := nmt.Trees[key]
	if !ok {
		return nil, fmt.Errorf("tree with key %s not found", key)
	}
	return tree, nil
}

func (nmt *NaiveMultiTree) SaveVersions() ([]byte, error) {
	var hashes []byte
	for _, tree := range nmt.Trees {
		hash, _, err := tree.SaveVersion()
		if err != nil {
			return nil, err
		}
		hashes = append(hashes, hash...)
	}
	h := sha256.Sum256(hashes)
	return h[:], nil
}

func NewMultiTree() *NaiveMultiTree {
	return &NaiveMultiTree{
		Trees: make(map[string]Tree),
	}
}
