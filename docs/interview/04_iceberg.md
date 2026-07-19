# Apache Iceberg

**Q: Why Iceberg over Delta/Hudi/raw Parquet?**
A: ACID snapshot commits (basis of exactly-once), hidden + evolvable partitioning,
native schema evolution, time travel, and engine-agnostic reads (Spark writes,
Trino reads the same table). Raw Parquet lacks atomic commits and evolution.

**Q: What is a snapshot and how does time travel work?**
A: Each commit creates an immutable snapshot pointing to a manifest list of data
files. You can query `AS OF` a snapshot id/timestamp to read historical state —
invaluable for debugging and reproducible backfills.

**Q: merge-on-read vs copy-on-write?**
A: CoW rewrites whole data files on update/delete (fast reads, slow writes). MoR
writes delete/position files and merges at read time (fast writes, slower reads);
we use MoR for Silver because CDC is write-heavy, then compact periodically.

**Q: How does Iceberg handle schema evolution safely?**
A: Columns have stable IDs, so add/drop/rename/reorder are metadata-only — no
data rewrite and no risk of reading the wrong column by position.

**Q: What's the small-files problem and the fix?**
A: Frequent streaming commits produce many tiny files, bloating metadata and
slowing scans. Fix: scheduled `rewrite_data_files` (compaction),
`expire_snapshots`, and `remove_orphan_files` — our nightly maintenance DAG.
