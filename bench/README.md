## Generation algorithm

The changeset generator in this module is parameterized by the parameters in the `ChangesetGenerator` struct.
Refer to the struct documentation for details on each parameter.

For each version:

1. calculate number of create, update, and deletion operations based on `ChangePerVersion`, `DeleteFraction`,
   `InitialSize`, `FinalSize` and `Versions` - essentially we want to do `ChangePerVersion` operations per version with
   `DeleteFraction` of them being deletions, but we also want to make sure we progress linearly in size from
   `InitialSize` to `FinalSize` over the course of `Versions` versions
2. for delete operations, select a random existing key from the current set of keys in the tree
3. for create operations, generate new keys and values randomly where the length of the key and value are generated as
   to match `KeyMean`, `KeyStdDev`, `ValueMean`, and `ValueStdDev` using a normal distribution
4. for update operations, select a random existing key and generate a new value using `ValueMean` and `ValueStdDev`