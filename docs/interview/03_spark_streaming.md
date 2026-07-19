# Spark Structured Streaming

**Q: How does checkpointing give fault tolerance?**
A: The checkpoint stores the Kafka offsets and stream state. On restart Spark
resumes from the last committed offsets. We commit the Iceberg snapshot *before*
committing offsets, so a crash in between just replays the batch.

**Q: How exactly do you achieve exactly-once here?**
A: Bounded batch read → atomic Iceberg commit → offset commit. The Silver apply
is an idempotent, LSN-guarded MERGE, so replaying a batch yields identical state.
Delivery is at-least-once; the apply is idempotent ⇒ effectively exactly-once.

**Q: How do you deduplicate within a micro-batch?**
A: `row_number()` over a window partitioned by primary key, ordered by
`(source_lsn, source_ts_ms)` desc; keep rank 1. This selects the latest version
per key regardless of arrival order.

**Q: How do you handle out-of-order / late data?**
A: Order by LSN (source commit order), not wall-clock. The MERGE guard
`incoming.lsn > stored.lsn` ensures a late/older event never overwrites a newer
one, even across batches. Watermarks bound any stateful operation's memory.

**Q: `foreachBatch` vs a plain streaming sink?**
A: `foreachBatch` gives us a normal DataFrame per micro-batch so we can run
arbitrary logic — crucially an Iceberg `MERGE INTO`, which streaming append sinks
can't express. It also lets us instrument metrics and make the write idempotent.

**Q: How do you apply back-pressure?**
A: `maxOffsetsPerTrigger` bounds batch size; the trigger interval bounds
frequency; adaptive query execution + shuffle-partition tuning keep batches
balanced. Bounded batches keep latency and memory predictable under load spikes.
