package internal

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	storev1beta1 "cosmossdk.io/api/cosmos/store/v1beta1"
	"github.com/alitto/pond/v2"
)

type DB struct {
	trees       []*CommitTree
	treeNames   []string       // always ordered by tree name
	treesByName map[string]int // index of the trees by name
	version     uint64
	hashPool    pond.ResultPool[[]byte]
}

type Options struct {
	ZeroCopy   bool  `json:"zero_copy"`
	Inline     bool  `json:"inline"`
	EvictDepth uint8 `json:"evict_depth"` // 255 means no eviction
}

func LoadDB(path string, treeNames []string, opts *Options, logger *slog.Logger) (*DB, error) {
	n := len(treeNames)
	trees := make([]*CommitTree, n)
	treesByName := make(map[string]int, n)
	for i, name := range treeNames {
		if _, exists := treesByName[name]; exists {
			return nil, fmt.Errorf("duplicate tree name: %s", name)
		}
		treesByName[name] = i
		dir := filepath.Join(path, name)
		err := os.MkdirAll(dir, 0o755)
		if err != nil {
			return nil, fmt.Errorf("failed to create tree dir %s: %w", dir, err)
		}
		trees[i], err = NewCommitTree(dir, *opts, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to load tree %s: %w", name, err)
		}
	}

	db := &DB{
		trees:       trees,
		treeNames:   treeNames,
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

func (db *DB) Commit(logger *slog.Logger) (*storev1beta1.CommitInfo, error) {
	taskGroup := db.hashPool.NewGroup()
	for _, tree := range db.trees {
		t := tree
		taskGroup.SubmitErr(func() ([]byte, error) {
			if t.root == nil {
				logger.Warn("skipping hash of empty tree")
			}
			return t.Commit()
		})
	}
	hashes, err := taskGroup.Wait()
	if err != nil {
		return nil, fmt.Errorf("failed to commit trees: %w", err)
	}
	db.version++
	commitInfo := &storev1beta1.CommitInfo{
		Version:    int64(db.version),
		StoreInfos: make([]*storev1beta1.StoreInfo, len(db.trees)),
	}
	for i, treeName := range db.treeNames {
		if hashes[i] == nil {
			return nil, fmt.Errorf("tree %s returned nil hash", treeName)
		}
		commitInfo.StoreInfos[i] = &storev1beta1.StoreInfo{
			Name: treeName,
			CommitId: &storev1beta1.CommitID{
				Version: int64(db.version),
				Hash:    hashes[i],
			},
		}
	}
	return commitInfo, nil
}

func (db *DB) Close() error {
	for _, tree := range db.trees {
		err := tree.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
