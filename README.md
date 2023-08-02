# IAVL Bench
iavl-bench contains a set of benchmarks for the IAVL tree implementation in [cosmos/iavl](https://github..com/cosmos/iavl).  This was originally compiled for the CosmosSDK storage v2 working group on Aug 2, 2023.

## About the data set
It has been said that "a benchmark is only as good as the input data" and great care has been taken to 
create quality input. The input data set is **live chain** data harvested from `osmosisd`.  The leveldb 
instance in `application.db` was copied from a full archive node synced to block 10,251,845, about 5.2TB.  
Since every change to state is recorded in IAVL (here in leveldb), it is possible extract a set of all 
key-value state changes into structs.
```golang
type KeyValueChange struct {
  Key      []byte
  Value    []byte
  Delete   bool
  Block    int64
}
```
Every key-value change in the `bank` module subtree of this db was extracted, ordered by block height, 
then stored in compressed binary files.  The resulting data set is 11.1GB and contains ~368,700,000 key-value
changes.
```bash
mattk@quartz ~/d/o/b/ordered> ls -lh
total 11G
-rw-r--r-- 1 mattk mattk 51M Jul 28 11:48 00000001-00347691.pb.gz
-rw-r--r-- 1 mattk mattk 51M Jul 28 11:48 00347691-00695449.pb.gz
...
-rw-r--r-- 1 mattk mattk 51M Jul 28 19:55 10178504-10251845.pb.gz
```

## Methodology
To benchmark IAVL tree performance, changesets are read from disk, applied to the tree by calling either 
`Set` or `Remove`, then `Commit` is called to advance the version. The following metrics are recorded and scrutinized, though others could be added:

- leaves (changesets) processed per second
- nodes processed per second (including inner nodes)
- memory usage
- total disk usage

Since the current IAVL implementation pushes all nodes out of memory on `Commit`, each update requires 
*tree height* reads from storage (memory or disk), and leaves/s is a good proxy for overall performance of 
both reads and writes. To gain further insight into tree performance these are also correlated with:

- tree size/height
- cache hits/misses
- db fetch rate

Metrics were collected using [prometheus](https://prometheus.io/) and console logging.  The full Prometheus 
tsdb is available upon request.

## Implementations
The following implementations are benchmarked:
- iavl v0.21.1 (legacy node hash key format) backed by leveldb
- iavl v1, current master (`version|sequence` key format) backed by leveldb
- [crypto-org-chain/cronos/memiavl](https://github.com/crypto-org-chain/cronos/tree/main/memiavl)
- iavl v1 backed by an in-memory hash map "mapDb" (for comparison)
- experimental iavl "version 2", `avlite` branch of cosmos-sdk/iavl.

## Results
Results were collected on a 16-core AMD Ryzen 9 5950x with 128GB of RAM and a 2TB M.2 NVMe SSD.  Process 
rates are calculated as a moving average over 1 hour unless otherwise noted.  The last hour is used since 
this indicates a more realistic live chain tree size.

### iavl v0.21.1
Legacy IAVL was not fully instrumented for metrics, so only leaves/s and memory are available. More could 
be added if needed.

After 3 hours runtime the tree size is 410,000 with a throughput of 1,800 leaves/sec. 26,600,000 leaves 
were processed. Total leveldb size on disk is 59G.  Memory usage is 800MB.  The tree was 
constructed with a cache size 1,000,000 [like so.](https://github.com/kocubinski/iavl-bench/blob/0c657c91796a2c1adfc4ec8882c9bc408aa77d8a/iavl-v0/main.go#L55)

Will continue monitoring progress but the expectation is that performance will degrade as the tree and db 
grow in size.

### iavl v1
IAVL v1 is the current master branch of cosmos/iavl.  It uses the new `version|sequence` key format and 
performs about 10x better than IAVL v0.21.1.  

After 6 hours runtime the tree size is 1,889,000 with 12,225 leaves/sec.  Leveldb size on disk 211G. 
Memory usage is roughly 1GB. The tree was constructed with a cache size 1,000,000 [like so.](https://github.com/kocubinski/iavl-bench/blob/7e02c02d968505b307a8782b3088a72a622a7e8f/iavl-v1/main.go#L57)

### memiavl
MemIAVL is a fork of IAVL v1 that uses custom AVL disk storage backend and mmap for disk access. 
Unfortunately MemIAVL hangs on commit or fails with a `wal: out of order` error at first commit. The 
kick-off code is [here](https://github.com/kocubinski/iavl-bench/blob/a13b4acdfc81ec5cac877a0601571bb0c4fde775/memiavl/memiavl.go#L41).  It is not clear if this is a bug or a configuration issue, and request has been put to the authors for help. 

### iavl v1 in-memory
Processing completed in 2 hours with a throughput of 61,000 leaves/sec at a final tree size of 2,215,217
Memory usage is 1.5GB. Memory usage is ~3GB.

This implementation uses the same code as IAVL v1 but backed by an in-memory hash map instead of leveldb.  
We consider this the maximum possible throughput which could be obtained by *current* IAVL without 
undergoing major structural refactors. Such refactors could optimize tree traversal or formulate a 
copy-on-write tree which never invalidates nodes referenced by the current version, thereby avoiding a 
second fetch.

In this implementation no data is persisted to disk, the hashmap is a proxy for a disk-backed database, 
but root hashes are still calculated and stored in memory.  This is a good baseline for comparison.

### iavl v2
Processing completed in 2 hours with a throughput of 42,686 leaves/sec.  Memory usage was 3.5-4GB. Final 
db size is 350M.

IAVL v2 is an experimental branch of cosmos/iavl which tries to make efficient use of memory for caching, 
and a WAL to optimize disk writes.  It is not yet merged into master and is still under active development.
Key features include:

- only the latest n tree versions (here n=1) are kept in leveldb, all previous versions are written into 
  storage in a background thread.
- `Commit` writes nodes into a WAL which is merged into leveldb at configurable a checkpoint interval. 
- The in-memory WAL cache is double-buffered so that checkpoints can be written asynchronously while the tree 
  continues processing.  This cache averaged 600k hits/s in this benchmark.  After checkpointing the cache is
  merged back into the main LRU cache.
- Significant put/delete thrashing (orphaned nodes) are handled in memory in the WAL cache and never 
  written to leveldb. 
- Tuning cache size/checkpoint interval allows the user to trade memory space for time as need demands.