package iavlx

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	corestore "cosmossdk.io/core/store"
)

type CommitTree struct {
	root                *Node
	store               NodeWriter
	zeroCopy            bool
	version             uint32
	leafSeq             uint32
	branchSeq           uint32
	writeMutex          sync.Mutex
	nodeKeyGen          NodeKeyGenerator
	batchProcessChan    chan *Batch
	batchDone           chan error
	leafWriteChan       chan *nodeUpdate
	leafWriteDone       chan error
	branchWriteChan     chan branchUpdate
	branchWriteDone     chan error
	branchCommitVersion atomic.Uint32
	// TODO settings for background saving, checkpointing, eviction, pruning, etc.
}

type branchUpdate struct {
	nodeUpdate *nodeUpdate
	commit     *struct {
		version uint32
		root    *Node
	}
}

func NewCommitTree(store NodeWriter) *CommitTree {
	tree := &CommitTree{
		store:     store,
		leafSeq:   1,
		branchSeq: 1,
		// TODO should we initialize version to 1?
	}
	tree.reinitBatchChannels()
	return tree
}

func (c *CommitTree) Set(key, value []byte) error {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	batch := c.NewBatch()
	err := batch.Set(key, value)
	if err != nil {
		return err
	}
	return c.ApplyBatch(batch)
}

func (c *CommitTree) Remove(key []byte) error {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	batch := c.NewBatch()
	err := batch.Remove(key)
	if err != nil {
		return err
	}
	return c.ApplyBatch(batch)
}

func (c *CommitTree) Iterator(start, end []byte, ascending bool) (corestore.Iterator, error) {
	return NewIterator(c.store, start, end, ascending, c.root, c.zeroCopy), nil
}

func (c *CommitTree) NewBatch() *BatchTree {
	batch := NewBatchTree(c.root, c.store, c.zeroCopy)
	return batch
}

func (c *CommitTree) ApplyBatch(batchTree *BatchTree) error {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	if batchTree.origRoot != c.root {
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
		return fmt.Errorf("batchTree original root does not match current root")
	}
	c.root = batchTree.root
	c.batchProcessChan <- batchTree.store
	return nil
}

func (c *CommitTree) reinitBatchChannels() {
	batchChan := make(chan *Batch, 256)
	batchDone := make(chan error, 1)
	c.batchProcessChan = batchChan
	c.batchDone = batchDone
	leafWriteChan := make(chan *nodeUpdate, 2048)
	leafWriteDone := make(chan error, 1)
	c.leafWriteChan = leafWriteChan
	c.leafWriteDone = leafWriteDone
	nodeKeyGen := c.nodeKeyGen
	store := c.store

	// process batches
	go func() {
		defer close(c.batchDone)
		// we also close the leafWriteChan here, after all batches have been processed
		defer close(c.leafWriteChan)
		for batch := range c.batchProcessChan {
			// First:
			// - assign each new leaf node a node key
			// - hash each leaf node
			// - push each leaf node to walWriteChan
			for _, update := range batch.leafNodes.updates {
				if update == nil {
					continue
				}
				if !update.deleted {
					nodeKeyGen.AssignNodeKey(update.Node)
					if _, err := update.Node.Hash(store); err != nil {
						c.batchDone <- err
						return
					}
				}
				c.leafWriteChan <- update
			}
			// Second:
			// - assign each new branch node a node key
			// - hash each branch node
			// - push each branch node to branchWriteChan
			for _, update := range batch.branchNodes.updates {
				if update == nil {
					continue
				}
				if !update.deleted {
					nodeKeyGen.AssignNodeKey(update.Node)
					if _, err := update.Node.Hash(store); err != nil {
						c.batchDone <- err
						return
					}
				}
				c.branchWriteChan <- branchUpdate{nodeUpdate: update}
			}
		}
	}()

	// write leave nodes
	go func() {
		defer close(leafWriteDone)
		for update := range leafWriteChan {
			var err error
			if update.deleted {
				err = store.DeleteNode(update.Node)
			} else {
				err = store.SaveNode(update.Node)
			}
			if err != nil {
				leafWriteDone <- err
				break
			}
		}
	}()

	if c.branchWriteChan == nil {
		branchWriteChan := make(chan branchUpdate, 2048)
		branchWriteDone := make(chan error, 1)
		c.branchWriteChan = branchWriteChan
		c.branchWriteDone = branchWriteDone
		go func() {
			defer close(branchWriteDone)
			for update := range branchWriteChan {
				nodeUpdate := update.nodeUpdate
				if nodeUpdate != nil {
					var err error
					if update.nodeUpdate.deleted {
						err = store.DeleteNode(update.nodeUpdate.Node)
					} else {
						err = store.SaveNode(update.nodeUpdate.Node)
					}
					if err != nil {
						branchWriteDone <- err
						return
					}
				} else if update.commit != nil {
					err := store.SaveRoot(int64(update.commit.version), update.commit.root)
					if err != nil {
						branchWriteDone <- err
						return
					}
				}
			}
		}()
	}
}

func (c *CommitTree) Commit() ([]byte, error) {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	close(c.batchProcessChan)

	c.version++
	c.leafSeq = 1
	c.branchSeq = 1

	if c.root == nil {
		return emptyHash, nil
	}
	return c.root.Hash(c.store)
}

func (c *CommitTree) Version() int64 {
	return int64(c.version)
}

func (c *CommitTree) Get(key []byte) ([]byte, error) {
	if c.root == nil {
		return nil, nil
	}
	_, value, err := c.root.get(c.store, key)
	return value, err
}

func (c *CommitTree) Close() error {
	// shutdown all write channels and wait for branch write completion
	//TODO implement me
	panic("implement me")
}

var _ io.Closer = (*CommitTree)(nil)
