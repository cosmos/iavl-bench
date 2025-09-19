package x1

import (
	"fmt"
	"io"
	"sync"

	"github.com/alitto/pond/v2"
)

type CommitTree struct {
	root           *NodePointer
	store          NodeWriter
	zeroCopy       bool
	version        uint64
	writeMutex     sync.Mutex
	walProcessChan chan *ChangeBatch
	walDone        chan error
	walWriter      *WALWriter
	pond           pond.Pool
	//hashGroup      pond.TaskGroup
	//branchWriteChan     chan branchUpdate
	//branchWriteDone     chan error
	//branchCommitVersion atomic.Uint32
	// TODO settings for background saving, checkpointing, eviction, pruning, etc.
}

type branchUpdate struct {
	nodeUpdate *leafUpdate
	commit     *struct {
		version uint32
		root    *Node
	}
}

func NewCommitTree(dir string) (*CommitTree, error) {
	walWriter, err := OpenWALWriter(fmt.Sprintf("%s/wal", dir))
	if err != nil {
		return nil, err
	}
	tree := &CommitTree{
		store:     NewNullStore(NewVersionSeqNodeKeyGen()),
		walWriter: walWriter,
		pond:      pond.NewPool(4),
	}
	tree.reinitBatchChannels()
	return tree, nil
}

func (c *CommitTree) Branch() *Tree {
	return NewTree(c.root, NewChangeBatch(uint32(c.version+1), c.store), c.zeroCopy)
}

func (c *CommitTree) ApplyBatch(tree *Tree) error {
	batch, ok := tree.store.(*ChangeBatch)
	if !ok {
		return fmt.Errorf("tree store is not a ChangeBatch")
	}
	select {
	case err := <-c.walDone:
		if err != nil {
			return err
		}
	//case err := <-c.branchWriteDone:
	//	if err != nil {
	//		return err
	//	}
	default:
	}

	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	if tree.origRoot != c.root {
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
		return fmt.Errorf("batch tree original root does not match current root")
	}
	c.root = tree.root
	c.walProcessChan <- batch
	return nil
}

func (c *CommitTree) reinitBatchChannels() {
	//store := c.store
	//if c.branchWriteChan == nil {
	//	branchWriteChan := make(chan branchUpdate, 65536)
	//	branchWriteDone := make(chan error, 1)
	//	c.branchWriteChan = branchWriteChan
	//	c.branchWriteDone = branchWriteDone
	//	stagedVersion := c.version + 1
	//	go func() {
	//		defer close(branchWriteDone)
	//		for update := range branchWriteChan {
	//			nodeUpdate := update.nodeUpdate
	//			if nodeUpdate != nil {
	//				var err error
	//				if update.nodeUpdate.deleted {
	//					err = store.DeleteNode(int64(stagedVersion), EmptyNodeKey, update.nodeUpdate.Node)
	//				} else {
	//					err = store.SaveNode(update.nodeUpdate.Node)
	//				}
	//				if err != nil {
	//					branchWriteDone <- err
	//					return
	//				}
	//			} else if update.commit != nil {
	//				err := store.SaveRoot(int64(update.commit.version), update.commit.root)
	//				if err != nil {
	//					branchWriteDone <- err
	//					return
	//				}
	//				c.branchCommitVersion.Store(update.commit.version)
	//				stagedVersion = update.commit.version + 1
	//			}
	//		}
	//	}()
	//}

	batchChan := make(chan *ChangeBatch, 256)
	batchDone := make(chan error, 1)
	c.walProcessChan = batchChan
	c.walDone = batchDone
	//c.hashGroup = c.pond.NewGroup()
	//stagedVersion := c.version + 1

	// process batches
	go func() {
		defer close(batchDone)
		for batch := range batchChan {
			//c.hashGroup.SubmitErr(func() error {
			//	for _, update := range batch.leafUpdates {
			//		if !update.deleted {
			//			_, err := update.Node.Hash(c.store)
			//			if err != nil {
			//				return err
			//			}
			//		}
			//	}
			//	return nil
			//})
			// First:
			// - assign each new leaf node a node key
			// - hash each leaf node
			// - push each leaf node to walWriteChan
			if err := c.walWriter.WriteUpdates(batch.leafUpdates); err != nil {
				batchDone <- err
				return
			}

			//for _, update := range batch.leafUpdates {
			//	if update == nil {
			//		continue
			//	}
			//	if !update.deleted {
			//		//store.AssignNodeKey(update.Node)
			//		if _, err := update.Node.Hash(store); err != nil {
			//			batchDone <- err
			//			return
			//		}
			//	} else {
			//		//update.deleteKey = store.AssignDeleteLeafKey(update.Node)
			//	}
		}
	}()
}

func (c *CommitTree) Commit() ([]byte, error) {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	close(c.walProcessChan)
	err := <-c.walDone
	if err != nil {
		return nil, err
	}

	//err = c.hashGroup.Wait()
	//if err != nil {
	//	return nil, err
	//}

	c.version, err = c.walWriter.CommitVersion()
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
	//c.branchWriteChan <- branchUpdate{commit: &struct {
	//	version uint32
	//	root    *Node
	//}{
	//	version: c.version,
	//	root:    root,
	//}}

	c.version++
	//c.store.SetNodeKeyVersion(c.version + 1)

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
