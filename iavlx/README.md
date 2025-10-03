# iavlx (IAVL Experiments)

## Code Organization

### Node Types & On-Disk Layouts

* node.go: the `Node` interface which all 3 node types implement (`MemNode`, `BranchPersisted`, `LeafPersisted`)
* mem_node.go: in-memory node structure, new nodes always use the `MemNode` type
* node_pointer.go: all child references are wrapped in `NodePointer` which can point to either an in-memory node or an
  on-disk node, or both (if the node has been written and node evicted)
* node_id.go: defines `NodeID` (version + index + leaf) and `NodeRef` (either a `NodeID` or a node offset in the
  changeset file)
* branch_layout.go: defines the on-disk layout for branch nodes
* leaf_layout.go: defines the on-disk layout for leaf nodes
* branch_persisted.go: a wrapper around `BranchLayout` which implements the `Node` interface and also tracks a store
  reference
* leaf_persisted.go: a wrapper around `LeafLayout` which implements the `Node` interface and also tracks a store
  reference

### Tree Management

* tree.go: a `Tree` struct which implements the Cosmos SDK `KVStore` interface and implements the key methods (get, set,
  delete, commit, etc). `Tree`s can be mutated, and changes can either be committed or discarded. This is essentially an
  in-memory reference to a tree at a specific version that could be used read-only or mutated ad hoc without affecting
  the underlying persistent tree (say for instance in `CheckTx`).
* commit_tree.go: defines the `CommitTree` structure which manages the persistent tree state. Using `CommitTree` you can
  create new mutable `Tree` instance using `Branch` and decide to `Apply` its changes to the persistent tree or discard
  them. Calling `Commit` flushes changes to the underlying `TreeStore` which does all of the on disk state management
  and cleanup.
* `node_update.go` and `update.go`: the code for setting and deleting nodes and doing tree rebalancing, and managing pending
  changesets (which can be applied to `CommitTree`s or discarded)
* node_hash.go: code for computing node hashes
* iterator.go: implements the Cosmos SDK `Iterator` interface

### Disk State Management

* tree_store.go: the `TreeStore` struct which manages all of the on-disk state, including writing changesets,
  maintaining version info, tracking orphans and doing compaction and cleanup
* changeset.go
* changeset_writer.go
* version_info.go: defines the on-disk layout for version info records, which track the root node and other metadata for
  each version
* changeset_info.go
* kvlog.go
* kvlog_writer.go

### Utilities

* dot_graph.go: code for exporting trees to Graphviz dot graph format for visualization
* verify.go: code for verifying tree integrity