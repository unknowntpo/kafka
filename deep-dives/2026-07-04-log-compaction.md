# Deep Dive: Log Compaction

## The pattern
Log compaction is Kafka's alternative retention policy: instead of deleting
records after a time/size limit, the log cleaner retains only the *latest*
value per key, discarding older records with the same key (`cleanup.policy=compact`).
Deletion is expressed as a tombstone — a record with the key and a `null`
value — which is itself eventually removed after `delete.retention.ms`.

## Why it exists
Kafka topics are often used as a changelog for keyed state (e.g. Kafka
Streams' `KTable`, `__consumer_offsets`, Connect's config/status/offset
topics). For that use case, full retention is wasteful — a consumer
rebuilding state only needs the last value per key. Compaction turns a log
into a durable, replayable snapshot mechanism without needing a separate
snapshot format: any consumer reading from offset 0 rebuilds the current
key→value map by the time it reaches the log end.

## Design choices and trade-offs
- **Segment-based, offline compaction** (`LogCleaner` background threads
  dedup closed segments, never the active segment): keeps the write path
  untouched — compaction is O(segments), not O(writes), at the cost of
  latency before a duplicate is actually removed.
- **Strength**: bounded storage growth independent of write volume; enables
  `__consumer_offsets` and Streams state stores to scale with key
  cardinality, not event count.
- **Limitation**: readers can still observe stale duplicates until a
  segment is cleaned; ordering between compacted and non-compacted
  (`compact,delete`) semantics is subtle and a frequent source of bugs in
  changelog-topic consumers.
- **Limitation**: tombstones need a grace period (`delete.retention.ms`)
  before removal, otherwise slow consumers can miss deletes — a classic
  distributed-GC problem.

## Where a contributor could push further
The cleaner's dirty-ratio scheduling (`min.cleanable.dirty.ratio`) is a
coarse global heuristic; per-key TTL within a compacted topic, or exposing
cleaner lag as a first-class metric per partition (rather than only via
log4j), remain open areas. Tiered-storage interaction with compaction
(compacting segments already offloaded to remote storage) is also still
maturing.

Reference: https://kafka.apache.org/documentation/#compaction
