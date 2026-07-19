# CDC & Debezium

**Q: What is CDC and why log-based over query-based?**
A: Change Data Capture streams row-level changes out of a database. Log-based CDC
reads the WAL/redo log, so it captures every change (including deletes and
intermediate updates), imposes near-zero load on the source, and preserves exact
commit order via the LSN. Query-based polling (`WHERE updated_at > x`) misses
deletes and states between polls and adds scan load.

**Q: What does `REPLICA IDENTITY FULL` do and why set it?**
A: It makes Postgres emit the full previous row image in the WAL for UPDATE and
DELETE. Without it you only get the primary key for the `before` image, which is
insufficient for downstream diffing and for tombstoning with full context.

**Q: Walk through a Debezium change event.**
A: An envelope with `op` (c/u/d/r), `before`, `after`, `ts_ms`, and a `source`
block containing `lsn`, `ts_ms`, `table`, etc. Creates/reads populate `after`;
updates populate both; deletes populate `before` and emit a tombstone (null
value) for log compaction.

**Q: What happens if the CDC consumer is down for hours?**
A: The replication slot holds the WAL position, so no data is lost — but WAL
accumulates on the source and can fill disk. You monitor slot lag / WAL size and
alert. This is the key operational risk of log-based CDC.

**Q: How do you handle a schema change on the source table?**
A: Debezium detects DDL and publishes a new schema version to the registry (must
pass BACKWARD compatibility). Bronze reads with the latest schema; Iceberg adds
the new column. No downstream breakage; old data reads back null for new columns.

**Q: How do you scale Debezium?**
A: Run Kafka Connect in distributed mode with multiple workers for HA (a failed
worker's tasks rebalance). A single Postgres slot is single-threaded, so extreme
scale means sharding into multiple publications/slots by table group.
