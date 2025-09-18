package iavlx

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

type CommitTree struct {
	root                *NodePointer
	store               NodeWriter
	zeroCopy            bool
	version             uint32
	writeMutex          sync.Mutex
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
		store: store,
	}
	tree.reinitBatchChannels()
	return tree
}

func (c *CommitTree) Branch() *BatchTree {
	batch := NewBatchTree(c.root, c.store, c.zeroCopy)
	return batch
}

func (c *CommitTree) ApplyBatch(batchTree *BatchTree) error {
	select {
	case err := <-c.batchDone:
		if err != nil {
			return err
		}
	case err := <-c.leafWriteDone:
		if err != nil {
			return err
		}
	case err := <-c.branchWriteDone:
		if err != nil {
			return err
		}
	default:
	}

	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	if batchTree.origRoot != c.root {
		// TODO do a proper comparison between node keys
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
	store := c.store
	if c.branchWriteChan == nil {
		branchWriteChan := make(chan branchUpdate, 65536)
		branchWriteDone := make(chan error, 1)
		c.branchWriteChan = branchWriteChan
		c.branchWriteDone = branchWriteDone
		stagedVersion := c.version + 1
		go func() {
			defer close(branchWriteDone)
			for update := range branchWriteChan {
				nodeUpdate := update.nodeUpdate
				if nodeUpdate != nil {
					var err error
					if update.nodeUpdate.deleted {
						err = store.DeleteNode(int64(stagedVersion), EmptyNodeKey, update.nodeUpdate.Node)
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
					c.branchCommitVersion.Store(update.commit.version)
					stagedVersion = update.commit.version + 1
				}
			}
		}()
	}

	batchChan := make(chan *Batch, 256)
	batchDone := make(chan error, 1)
	c.batchProcessChan = batchChan
	c.batchDone = batchDone
	leafWriteChan := make(chan *nodeUpdate, 65536)
	leafWriteDone := make(chan error, 1)
	c.leafWriteChan = leafWriteChan
	c.leafWriteDone = leafWriteDone
	stagedVersion := c.version + 1

	// process batches
	go func() {
		defer close(batchDone)
		// we also close the leafWriteChan here, after all batches have been processed
		defer close(leafWriteChan)
		for batch := range batchChan {
			// First:
			// - assign each new leaf node a node key
			// - hash each leaf node
			// - push each leaf node to walWriteChan
			for _, update := range batch.leafNodes.updates {
				if update == nil {
					continue
				}
				if !update.deleted {
					store.AssignNodeKey(update.Node)
					if _, err := update.Node.Hash(store); err != nil {
						batchDone <- err
						return
					}
				} else {
					update.deleteKey = store.AssignDeleteLeafKey(update.Node)
				}
				leafWriteChan <- update
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
					store.AssignNodeKey(update.Node)
					if _, err := update.Node.Hash(store); err != nil {
						batchDone <- err
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
				err = store.DeleteNode(int64(stagedVersion), update.deleteKey, update.Node)
			} else {
				err = store.SaveNode(update.Node)
			}
			if err != nil {
				leafWriteDone <- err
				break
			}
		}
	}()

}

func (c *CommitTree) Commit() ([]byte, error) {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	close(c.batchProcessChan)
	err := <-c.batchDone
	if err != nil {
		return nil, err
	}

	// wait for all leaf writes to complete
	err = <-c.leafWriteDone
	if err != nil {
		return nil, err
	}

	var root *Node
	var hash []byte
	if c.root == nil {
		root = nil
		hash = emptyHash
	} else {
		var err error
		root, err = c.root.Get(c.store)
		if err != nil {
			return nil, err
		}
		hash, err = root.Hash(c.store)
		if err != nil {
			return nil, err
		}
	}
	c.branchWriteChan <- branchUpdate{commit: &struct {
		version uint32
		root    *Node
	}{
		version: c.version,
		root:    root,
	}}

	c.version++
	c.store.SetNodeKeyVersion(c.version + 1)

	c.reinitBatchChannels()

	return hash, nil
}

func (c *CommitTree) Version() int64 {
	return int64(c.version)
}
func (c *CommitTree) Close() error {
	// shutdown all write channels and wait for branch write completion
	//TODO implement me
	panic("implement me")
}

var _ io.Closer = (*CommitTree)(nil)
