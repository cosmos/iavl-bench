## Generation algorithm

The changeset generator in this module is parameterized by the parameters in the `ChangesetGenerator` struct.
Refer to the struct documentation for details on each parameter.

For each version:

1. calculate number of create, update, and deletion operations based on `ChangePerVersion`, `DeleteFraction`,
   `InitialSize`, `FinalSize` and `Versions`. Essentially we want to do `ChangePerVersion` operations per version with
   `DeleteFraction` of them being deletions and the remainder being updates. We also want to make sure there are enough create operations to progress linearly in size from
   `InitialSize` to `FinalSize` over the course of the number of specified `Versions`.
2. populate an empty list of the desired number of create, update, and delete operations in random order
3. for delete operations, select a random existing key from the current set of keys in the tree
4. for create operations, generate new keys and values randomly where the length of the key and value are generated as
   to match `KeyMean`, `KeyStdDev`, `ValueMean`, and `ValueStdDev` using a normal distribution
5. for update operations, select a random existing key and generate a new value using `ValueMean` and `ValueStdDev`

Note: Version 1 is special and only contains create operations to establish the initial set of keys.