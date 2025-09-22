package internal

import (
	"fmt"
	"sync"
)

type CommitTree struct {
	root         *NodePointer
	zeroCopy     bool
	version      uint64
	writeMutex   sync.Mutex
	wal          *WAL
	walWriteChan chan<- walWriteBatch
	walDone      <-chan error
}

func NewCommitTree(wal *WAL, zeroCopy bool) *CommitTree {
	walWriteChan := make(chan walWriteBatch, 16)
	walDone := make(chan error, 1)
	go func() {
		for batch := range walWriteChan {
			if batch.KVUpdateBatch != nil {
				err := wal.WriteUpdates(batch.KVUpdateBatch)
				if err != nil {
					walDone <- err
					return
				}
			} else if batch.commitVersion != 0 {
				err := wal.CommitSync()
				if err != nil {
					walDone <- err
					return
				}
			}
		}
		walDone <- nil
	}()

	return &CommitTree{
		root:         nil,
		zeroCopy:     zeroCopy,
		version:      0,
		wal:          wal,
		walWriteChan: walWriteChan,
		walDone:      walDone,
	}
}

type walWriteBatch struct {
	*KVUpdateBatch
	commitVersion uint64
}

func (c *CommitTree) ApplyBatch(tree *Tree) error {
	if tree.origRoot != c.root {
		// TODO apply the updates on top of the current root
		//root := wrapNewNode(c.root)
		//for _, update := range batchTree.store.leafNodes.updates {
		//	if update != nil {
		//		var err error
		//		if update.deleted {
		//			_, root, _, err = removeRecursive(c.store, root, update.key)
		//		}
		//	}
		//}
		return fmt.Errorf("batch tree original root does not match current root")
	}
	c.root = tree.root
	// TODO process WAL batch

	return nil
}

func (c *CommitTree) stagedVersion() uint64 {
	return c.version + 1
}

func (c *CommitTree) Branch() *Tree {
	return NewTree(c.root, NewKVUpdateBatch(c.stagedVersion()), c.zeroCopy)
}

func (c *CommitTree) Apply(tree *Tree) error {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	if tree.origRoot != c.root {
		// TODO find a way to apply the changes incrementally when roots don't match
		return fmt.Errorf("tree original root does not match current root")
	}
	c.root = tree.root
	// TODO prevent further writes to the branch tree
	// process WAL batch
	c.walWriteChan <- walWriteBatch{
		KVUpdateBatch: tree.updateBatch,
	}
	return nil
}

func (c *CommitTree) Commit() ([]byte, error) {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	// check WAL errors
	select {
	case err := <-c.walDone:
		if err != nil {
			return nil, fmt.Errorf("WAL error: %w", err)
		}
	default:
	}

	var root Node
	var hash []byte
	if c.root == nil {
		root = nil
		hash = emptyHash
	} else {
		var err error
		root, err = c.root.Resolve()
		if err != nil {
			return nil, err
		}
		hash, err = root.Hash()
		if err != nil {
			return nil, err
		}
	}
	c.version++

	return hash, nil
}

func (c *CommitTree) Close() error {
	close(c.walWriteChan)
	return <-c.walDone
}
