## Files

### Leaf/WAL File

The leaf/WAL file(s) records all leaf node insertions and deletions in insertion/deletion order.
The WAL can get truncated when snapshots are created to remove all pre-snapshot versions.

### Rolling Diffs

```
BranchNode {
    subtree_bz_len: u64,
    size: u32,
    self_bz_len: u32,
    left_ptr: u64,
    right_ptr: u64
    version: varint,
    height: u8,
    hash: byte[32],
    key: varint prefixed bytes,
}

LeafNode {
    subtree_bz_len: u64,
    size: 1u32
    version: varint
    hash: byte[32]
    key: varint prefixed bytes,
    value: varint prefixed bytes
}
```


### Checkpoints

### Snapshots