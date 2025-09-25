package internal

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

type CommitTree struct {
	latest        *NodePointer
	root          *NodePointer
	zeroCopy      bool
	version       uint64
	writeMutex    sync.Mutex
	wal           *WAL
	walWriteChan  chan<- walWriteBatch
	walDone       <-chan error
	rollingStore  RollingStore
	diffWriteChan chan<- *diffWriteBatch
	diffDone      <-chan error
	evictorDone   chan<- struct{}
	evictionDepth uint8
	logger        *slog.Logger
}

type diffWriteBatch struct {
	version            uint64
	root               *NodePointer
	branchNodesCreated uint32
	leafNodesCreated   uint32
}

func NewCommitTree(dir string, opts Options, logger *slog.Logger) (*CommitTree, error) {
	useInline := opts.Inline
	zeroCopy := opts.ZeroCopy
	wal, err := OpenWAL(dir, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	var rollingStore RollingStore
	if useInline {
		rollingStore, err = NewRollingDiffInline(dir, 0, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to open rolling diff inline: %w", err)
		}
	} else {
		rollingStore, err = NewRollingDiff(wal, dir, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to open rolling diff: %w", err)
		}
	}

	walWriteChan := make(chan walWriteBatch, 262_144)
	walDone := make(chan error, 1)
	diffWriteChan := make(chan *diffWriteBatch, 1024)
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
			err := rollingStore.writeRoot(commit.version, commit.root, 0)
			if err != nil {
				diffDone <- err
				return
			}
		}
	}()

	evictorDone := make(chan struct{})
	tree := &CommitTree{
		root:          nil,
		zeroCopy:      zeroCopy,
		version:       0,
		wal:           wal,
		walWriteChan:  walWriteChan,
		walDone:       walDone,
		rollingStore:  rollingStore,
		diffWriteChan: diffWriteChan,
		diffDone:      diffDone,
		evictorDone:   evictorDone,
		evictionDepth: opts.EvictDepth,
		logger:        logger,
	}

	go func() {
		evictDepth := tree.evictionDepth
		lastEvictVersion := uint64(0)
		for {
			select {
			case <-evictorDone:
				return
			default:
			}
			evictVersion := tree.rollingStore.SavedVersion()
			if evictVersion > lastEvictVersion {
				latest := tree.latest
				if latest != nil {
					// TODO check tree height
					logger.Info("Starting eviction traversal", "evict_version", evictVersion, "evict_depth", evictDepth, "latest_version", tree.version)
					evictTraverse(latest, 0, evictDepth, evictVersion)
				}
			} else {
				select {
				case <-evictorDone:
					return
				// wait a bit before next eviction if no new version to evict
				case <-time.After(time.Second):
				}
			}
			lastEvictVersion = evictVersion
		}
	}()

	return tree, nil
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
	commitCtx := &commitContext{
		version: c.stagedVersion(),
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
	c.walWriteChan <- walWriteBatch{
		commit: &diffWriteBatch{
			version:            c.stagedVersion(),
			root:               c.root,
			branchNodesCreated: commitCtx.branchNodeIdx,
			leafNodesCreated:   commitCtx.leafNodeIdx,
		},
	}
	// cache the committed tree as the latest version
	c.latest = c.root
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

type commitContext struct {
	version       uint64
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
		return memNode.hash, nil
	}

	var leftHash, rightHash []byte
	if memNode.IsLeaf() {
		ctx.leafNodeIdx++
		np.id = NewNodeID(true, ctx.version, ctx.leafNodeIdx)
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
		np.id = NewNodeID(false, ctx.version, ctx.branchNodeIdx)

	}

	if memNode.hash != nil {
		// not sure when we would encounter this but if the hash is already computed, just return it
		return memNode.hash, nil
	}

	return computeAndSetHash(memNode, leftHash, rightHash)
}

func evictTraverse(np *NodePointer, depth, evictionDepth uint8, evictVersion uint64) {
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
