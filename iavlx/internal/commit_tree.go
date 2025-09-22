package internal

import (
	"fmt"
	"sync"
)

type CommitTree struct {
	root          *NodePointer
	zeroCopy      bool
	version       uint64
	writeMutex    sync.Mutex
	wal           *WAL
	walWriteChan  chan<- walWriteBatch
	walDone       <-chan error
	rollingDiff   *RollingDiff
	diffWriteChan chan<- *diffWriteBatch
	diffDone      <-chan error
}

type diffWriteBatch struct {
	version         uint64
	root            *NodePointer
	lastBranchIndex uint32
}

func NewCommitTree(dir string, zeroCopy bool) (*CommitTree, error) {
	wal, err := OpenWAL(dir, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	rollingDiff, err := NewRollingDiff(wal, dir, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open rolling diff: %w", err)
	}

	walWriteChan := make(chan walWriteBatch, 1024)
	walDone := make(chan error, 1)
	diffWriteChan := make(chan *diffWriteBatch, 64)
	diffDone := make(chan error, 1)

	go func() {
		defer close(walDone)
		defer close(diffWriteChan)
		for batch := range walWriteChan {
			if batch.updates != nil {
				err := wal.WriteUpdates(batch.updates)
				if err != nil {
					walDone <- err
					return
				}
			} else if batch.commit != nil {
				err := wal.CommitSync()
				if err != nil {
					walDone <- err
					return
				}
				diffWriteChan <- batch.commit
			}
		}
	}()

	go func() {
		defer close(diffDone)
		for commit := range diffWriteChan {
			err := rollingDiff.writeRoot(commit.root, 0)
			if err != nil {
				diffDone <- err
				return
			}
		}
	}()

	return &CommitTree{
		root:          nil,
		zeroCopy:      zeroCopy,
		version:       0,
		wal:           wal,
		walWriteChan:  walWriteChan,
		walDone:       walDone,
		rollingDiff:   rollingDiff,
		diffWriteChan: diffWriteChan,
		diffDone:      diffDone,
	}, nil
}

type walWriteBatch struct {
	updates *KVUpdateBatch
	commit  *diffWriteBatch
}

func (c *CommitTree) stagedVersion() uint64 {
	return c.version + 1
}

func (c *CommitTree) Branch() *Tree {
	return NewTree(c.root, NewKVUpdateBatch(c.stagedVersion()), c.zeroCopy)
}

func (c *CommitTree) Apply(tree *Tree) error {
	// check errors
	select {
	case err := <-c.walDone:
		if err != nil {
			return fmt.Errorf("WAL error: %w", err)
		}
	case err := <-c.diffDone:
		if err != nil {
			return fmt.Errorf("diff error: %w", err)
		}
	default:
	}
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
		updates: tree.updateBatch,
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

	var hash []byte
	if c.root == nil {
		hash = emptyHash
	} else {
		// compute hash and assign node IDs
		var err error
		idAssigner := &NodeIDAssigner{version: c.stagedVersion()}
		hash, err = ComputeHashAndAssignIDs(c.root, idAssigner)
		if err != nil {
			return nil, err
		}
		c.walWriteChan <- walWriteBatch{
			commit: &diffWriteBatch{
				version:         c.stagedVersion(),
				root:            c.root,
				lastBranchIndex: idAssigner.branchNodeIdx,
			},
		}
	}
	c.version++

	return hash, nil
}

func (c *CommitTree) Close() error {
	close(c.walWriteChan)
	err := <-c.walDone
	if err != nil {
		return fmt.Errorf("WAL error: %w", err)
	}
	return <-c.diffDone
}
