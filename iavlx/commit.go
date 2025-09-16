package iavlx

import (
	"fmt"

	corestore "cosmossdk.io/core/store"
)

type CommitTree struct {
	root           *Node
	store          NodeWriter
	zeroCopy       bool
	version        uint32
	leafSeq        uint32
	branchSeq      uint32
	hashChan       chan *Node
	hashDone       chan error
	saveChan       chan *nodeUpdate
	saveDone       chan error
	backgroundSave bool
}

func NewCommitTree(store NodeWriter) *CommitTree {
	tree := &CommitTree{
		store:          store,
		leafSeq:        1,
		branchSeq:      1,
		backgroundSave: true,
		// TODO should we initialize version to 1?
	}
	tree.reinitHasher()
	err := tree.reinitSave()
	if err != nil {
		// this should never happen at initialization
		panic(err)
	}
	return tree
}

func (c *CommitTree) Set(key, value []byte) error {
	batch := c.NewBatch()
	err := batch.Set(key, value)
	if err != nil {
		return err
	}
	return c.ApplyBatch(batch)
}

func (c *CommitTree) Remove(key []byte) error {
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
	batch := NewBatchTree(c.root, NullStore{}, c.zeroCopy)
	return batch
}

func (c *CommitTree) ApplyBatch(batchTree *BatchTree) error {
	if batchTree.origRoot != c.root {
		return fmt.Errorf("batchTree original root does not match current root")
	}
	c.root = batchTree.root
	batch := batchTree.store
	for _, update := range batch.batchUpdates {
		if update != nil {
			if !update.deleted {
				if update.isLeaf() {
					update.nodeKey = NewLeafNodeKey(c.version+1, c.leafSeq)
					c.leafSeq++
				} else {
					update.nodeKey = NewBranchNodeKey(c.version+1, c.branchSeq)
					c.branchSeq++
				}
				c.hashChan <- update.Node
			}
			c.saveChan <- update
		}
	}
	return nil
}

func (c *CommitTree) reinitHasher() {
	hashChan := make(chan *Node, 1024)
	hashDone := make(chan error, 1)
	c.hashChan = hashChan
	c.hashDone = hashDone
	go func() {
		for node := range c.hashChan {
			_, err := node.Hash(NullStore{})
			if err != nil {
				hashDone <- err
				break
			}
		}
		close(hashDone)
	}()
}

func (c *CommitTree) reinitSave() error {
	if c.saveChan != nil {
		close(c.saveChan)
	}
	if c.saveDone != nil {
		err := <-c.saveDone
		if err != nil {
			return err
		}
	}
	saveChan := make(chan *nodeUpdate, 1024)
	saveDone := make(chan error, 1)
	c.saveChan = saveChan
	c.saveDone = saveDone
	store := c.store
	go func() {
		for update := range c.saveChan {
			var err error
			if update.deleted {
				err = store.DeleteNode(update.Node)
			} else {
				err = store.SaveNode(update.Node)
			}
			if err != nil {
				saveDone <- err
				break
			}
		}
		close(saveDone)
	}()
	return nil
}

func (c *CommitTree) Commit() ([]byte, error) {
	close(c.hashChan)
	err := <-c.hashDone
	if err != nil {
		return nil, err
	}
	c.reinitHasher()

	if !c.backgroundSave {
		err := c.reinitSave()
		if err != nil {
			return nil, err
		}
	}

	c.version++
	c.leafSeq = 1
	c.branchSeq = 1

	if c.root == nil {
		return emptyHash, nil
	}
	return c.root.Hash(NullStore{})
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
