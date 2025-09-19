## Files

### Leaf/WAL File

The leaf/WAL file(s) records all leaf node insertions and deletions in insertion/deletion order.
The WAL can get truncated when snapshots are created to remove all pre-snapshot versions.

### Node Format

```
BranchNode {
    height: u8,
    subtree_bz_len: u64,
    left_ptr: u64,
    right_ptr: u64,
    version: varint,
    key: varint prefixed bytes,
    size: varint,
    hash: byte[32],
}

LeafNode {
    height: 0u8,
    version: varint
    key: varint prefixed bytes,
    value: varint prefixed bytes
    hash: byte[32]
}
```


For a compact version with fixed length nodes like memiavl,
we want to keep the fixed size to 64 bytes if possible
(probably can't do 48 bytes like memiavl because 32 bit pointers
are too small and we can't make the same assumptions about
location of the left and right child), but we can
maybe make things compact if we always deserialize
and don't worry about unaligned reads.
```
# 64 bytes
BranchNode {
    height: 1
    version: 6
    size: 6
    key index: 6
    left index: 6
    right index: 6
    _padding/extra: 1
    hash: 32
}

# 48 bytes
LeafNode {
    height: 1
    version: 6
    key index: 6
    _padding/extra: 3
    hash: 32
}
```

- `kvs`, sequence of leaf node key-value pairs, the keys are ordered and no duplication.

  ```
  keyLen: varint-uint64
  key
  valueLen: varint-uint64
  value
  *repeat*
  ```

#### Node pointers

Pointers are either local or remote referencing either the current file or the checkpoint or snapshot
file corresponding to that version.
In snapshot files, all nodes are local.
In checkpoint and rolling diff files, pointers can reference

### Rolling Diffs




### Checkpoints

### Snapshots