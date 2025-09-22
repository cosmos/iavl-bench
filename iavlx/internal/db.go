package internal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/alitto/pond/v2"
)

type DB struct {
	trees       []*CommitTree
	treeNames   []string       // always ordered by tree name
	treesByName map[string]int // index of the trees by name
	version     uint64
	hashPool    pond.ResultPool[[]byte]
}

type DBOptions struct {
	Path      string
	TreeNames []string
	ZeroCopy  bool
}

func LoadDB(opts DBOptions) (*DB, error) {
	n := len(opts.TreeNames)
	trees := make([]*CommitTree, n)
	treesByName := make(map[string]int, n)
	for i, name := range opts.TreeNames {
		if _, exists := treesByName[name]; exists {
			return nil, fmt.Errorf("duplicate tree name: %s", name)
		}
		treesByName[name] = i
		dir := filepath.Join(opts.Path, name)
		err := os.MkdirAll(dir, 0o755)
		if err != nil {
			return nil, fmt.Errorf("failed to create tree dir %s: %w", dir, err)
		}
		wal, err := OpenWAL(dir, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to open WAL for tree %s: %w", name, err)
		}
		trees[i] = NewCommitTree(wal, opts.ZeroCopy)
	}

	db := &DB{
		trees:       trees,
		treeNames:   opts.TreeNames,
		treesByName: treesByName,
		hashPool:    pond.NewResultPool[[]byte](n),
	}
	return db, nil
}

func (db *DB) stagedVersion() uint64 {
	return db.version + 1
}

func (db *DB) LatestVersion() uint64 {
	return db.version
}

func (db *DB) Branch() *MultiTree {
	mt := &MultiTree{
		trees:       make([]*Tree, len(db.trees)),
		treesByName: db.treesByName, // share the map
	}
	for i, root := range db.trees {
		mt.trees[i] = root.Branch()
	}
	return mt
}

func (db *DB) Apply(mt *MultiTree) error {
	if len(mt.trees) != len(db.trees) {
		return fmt.Errorf("mismatched number of trees: %d vs %d", len(mt.trees), len(db.trees))
	}
	for i, tree := range mt.trees {
		err := db.trees[i].Apply(tree)
		if err != nil {
			return fmt.Errorf("failed to apply tree %d: %w", i, err)
		}
	}
	return nil
}

func (db *DB) Commit() error {
	taskGroup := db.hashPool.NewGroup()
	for _, tree := range db.trees {
		t := tree
		taskGroup.SubmitErr(func() ([]byte, error) {
			return t.Commit()
		})
	}
	_, err := taskGroup.Wait()
	if err != nil {
		return fmt.Errorf("failed to commit trees: %w", err)
	}
	db.version++
	return err
}
