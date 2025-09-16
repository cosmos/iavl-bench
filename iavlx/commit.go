package iavlx

import "fmt"

type CommitTree struct {
	root      *Node
	store     NodeWriter
	version   uint32
	leafSeq   uint32
	branchSeq uint32
	hashChan  chan *Node
	hashDone  chan error
	zeroCopy  bool
}

func NewCommitTree(store NodeWriter) *CommitTree {
	tree := &CommitTree{
		store:     store,
		leafSeq:   1,
		branchSeq: 1,
		// TODO should we initialize version to 1?
	}
	tree.reinitHasher()
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

func (c *CommitTree) NewBatch() *BatchTree {
	batch := NewBatchTree(c.root, MemStore{}, c.zeroCopy)
	return batch
}

func (c *CommitTree) ApplyBatch(batchTree *BatchTree) error {
	if batchTree.origRoot != c.root {
		return fmt.Errorf("batchTree original root does not match current root")
	}
	c.root = batchTree.root
	batch := batchTree.store
	for _, node := range batch.batchNodes {
		if node != nil {
			if node.isLeaf() {
				node.nodeKey = NewLeafNodeKey(c.version, c.leafSeq)
				c.leafSeq++
			} else {
				node.nodeKey = NewBranchNodeKey(c.version, c.branchSeq)
				c.branchSeq++
			}
			c.hashChan <- node
		}
	}
	for _, _ = range batch.batchOrphans {
		// TODO delete orphan nodes from storage
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
			_, err := node.Hash(MemStore{})
			if err != nil {
				hashDone <- err
				break
			}
		}
		close(hashDone)
	}()
}

func (c *CommitTree) Commit() ([]byte, error) {
	close(c.hashChan)
	err := <-c.hashDone
	if err != nil {
		return nil, err
	}

	c.version++
	c.leafSeq = 1
	c.branchSeq = 1
	c.reinitHasher()

	if c.root == nil {
		return emptyHash, nil
	}
	return c.root.Hash(MemStore{})
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
