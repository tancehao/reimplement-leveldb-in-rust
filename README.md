# reimplement-leveldb-in-rust
reimplement leveldb in rust, with some new features.

## Features
* Search tired levels(L0 currently) in parellel;
* Gather writes from multiple threads at the same time into a single wal;
* User-defined hooks for various events: compaction, data insertion, version changes and etc.

## Plans
* Support the async feature: provide the async versions of each interface;
* Support the Skiplist memtable;
* Support the dump and scan interfaces, and support manual compactions;
* More detailed metrics;
* Support the compare-and-set primitives;
* Support other features that are interesting in rocksdb.
