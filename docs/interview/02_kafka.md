# Kafka & delivery semantics

**Q: Why put Kafka between Debezium and Spark?**
A: Decoupling. Kafka is a durable, replayable, partitioned buffer that absorbs
source bursts, lets multiple consumers read the same stream, and enables
reprocessing by resetting offsets. Without it, ingestion is coupled to transform
availability and you lose replay.

**Q: at-least-once vs at-most-once vs exactly-once?**
A: At-least-once = no loss, possible duplicates (retries). At-most-once = no
duplicates, possible loss. Exactly-once = each record affects state once. We use
at-least-once delivery + idempotent apply to get *effectively* exactly-once.

**Q: How do you preserve per-row ordering across partitions?**
A: Key the topic by primary key so all changes to one row hash to one partition
(Kafka guarantees order within a partition). Cross-partition order is not
guaranteed, which is fine because we only need per-key order.

**Q: What is a tombstone and why does it matter?**
A: A record with a non-null key and null value. For deletes it signals "this key
is gone" and enables log compaction to physically drop the key. We translate it
into a MERGE DELETE in Silver.

**Q: What is the DLQ strategy?**
A: `errors.tolerance=all` with a dead-letter topic. A poison message is routed to
`dlq.commerce` with error headers instead of blocking the stream; we alert on DLQ
growth and reprocess after fixing the root cause.
