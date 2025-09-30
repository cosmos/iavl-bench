package x3

import (
	"fmt"
	"log/slog"
	"sync"
)

type CommitTree struct {
	latest           *NodePointer
	root             *NodePointer
	zeroCopy         bool
	version          uint32
	writeMutex       sync.Mutex
	evictionDepth    uint8
	logger           *slog.Logger
	store            *TreeStore
	commitChan       chan<- commitRequest
	commitDone       <-chan error
	evictorRunning   bool
	lastEvictVersion uint32
}

type commitRequest struct {
	root            *NodePointer
	version         uint32
	branchNodeCount uint32
	leafNodeCount   uint32
}

func NewCommitTree(dir string, opts Options, logger *slog.Logger) (*CommitTree, error) {
	ts, err := NewTreeStore(dir, TreeStoreOptions{}, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create tree store: %w", err)
	}

	commitChan := make(chan commitRequest, 1024)
	commitDone := make(chan error, 1)

	tree := &CommitTree{
		root:       nil,
		zeroCopy:   opts.ZeroCopy,
		version:    0,
		logger:     logger,
		store:      ts,
		commitChan: commitChan,
		commitDone: commitDone,
	}

	// background commit processor
	go func() {
		defer close(commitDone)
		for req := range commitChan {
			err := ts.SaveRoot(req.root, req.version, req.leafNodeCount, req.branchNodeCount)
			if err != nil {
				commitDone <- err
				return
			}
			//// start eviction if needed
			//tree.startEvict(req.version)
		}
	}()

	return tree, nil
}

func (c *CommitTree) stagedVersion() uint32 {
	return c.version + 1
}

func (c *CommitTree) Branch() *Tree {
	return NewTree(c.root, NewKVUpdateBatch(c.stagedVersion()), c.zeroCopy)
}

func (c *CommitTree) Apply(tree *Tree) error {
	// TODO check channel errors
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	if tree.origRoot != c.root {
		// TODO find a way to apply the changes incrementally when roots don't match
		return fmt.Errorf("tree original root does not match current root")
	}
	c.root = tree.root
	// TODO prevent further writes to the branch tree
	// process WAL batch
	return nil
}

func (c *CommitTree) startEvict(evictVersion uint32) {
	if c.evictorRunning {
		// eviction in progress
		return
	}

	if evictVersion <= c.lastEvictVersion {
		// no new version to evict
		return
	}

	latest := c.latest
	if latest == nil {
		// nothing to evict
		return
	}

	go func() {
		evictTraverse(latest, 0, c.evictionDepth, evictVersion)
		c.lastEvictVersion = evictVersion
		c.evictorRunning = false
	}()
}

func (c *CommitTree) Commit() ([]byte, error) {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	// check WAL errors
	select {
	default:
	}

	var hash []byte
	commitCtx := &commitContext{
		version:      c.stagedVersion(),
		savedVersion: c.store.SavedVersion(),
	}
	if c.root == nil {
		hash = emptyHash
	} else {
		// compute hash and assign node IDs
		var err error
		hash, err = commitTraverse(commitCtx, c.root, 0)
		if err != nil {
			return nil, err
		}
	}

	// cache the committed tree as the latest version
	c.latest = c.root
	c.version++

	// send commit request to background processor
	c.commitChan <- commitRequest{
		root:            c.root,
		version:         c.version,
		branchNodeCount: commitCtx.branchNodeIdx,
		leafNodeCount:   commitCtx.leafNodeIdx,
	}

	return hash, nil
}

func (c *CommitTree) Close() error {
	close(c.commitChan)
	return <-c.commitDone
}

type commitContext struct {
	version       uint32
	savedVersion  uint32
	branchNodeIdx uint32
	leafNodeIdx   uint32
}

func commitTraverse(ctx *commitContext, np *NodePointer, depth uint8) (hash []byte, err error) {
	memNode := np.mem.Load()
	if memNode == nil {
		node, err := np.Resolve()
		if err != nil {
			return nil, err
		}
		return node.Hash(), nil
	}

	if memNode.version != ctx.version {
		if memNode.version <= ctx.savedVersion {
			// node is already persisted, evict
			np.mem.Store(nil)
		}
		return memNode.hash, nil
	}

	var leftHash, rightHash []byte
	if memNode.IsLeaf() {
		ctx.leafNodeIdx++
		np.id = NewNodeID(true, uint64(ctx.version), ctx.leafNodeIdx)
	} else {
		// post-order traversal
		leftHash, err = commitTraverse(ctx, memNode.left, depth+1)
		if err != nil {
			return nil, err
		}
		rightHash, err = commitTraverse(ctx, memNode.right, depth+1)
		if err != nil {
			return nil, err
		}

		ctx.branchNodeIdx++
		np.id = NewNodeID(false, uint64(ctx.version), ctx.branchNodeIdx)

	}

	if memNode.hash != nil {
		// not sure when we would encounter this but if the hash is already computed, just return it
		return memNode.hash, nil
	}

	return computeAndSetHash(memNode, leftHash, rightHash)
}

func evictTraverse(np *NodePointer, depth, evictionDepth uint8, evictVersion uint32) {
	memNode := np.mem.Load()
	if memNode == nil {
		return
	}

	if memNode.version > evictVersion {
		return
	}

	// Evict nodes at or below the eviction depth
	if depth >= evictionDepth {
		np.mem.Store(nil)
	}

	if memNode.IsLeaf() {
		return
	}

	// Continue traversing to find nodes to evict
	evictTraverse(memNode.left, depth+1, evictionDepth, evictVersion)
	evictTraverse(memNode.right, depth+1, evictionDepth, evictVersion)
}
