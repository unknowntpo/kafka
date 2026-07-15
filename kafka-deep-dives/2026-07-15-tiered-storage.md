# Deep Dive: Tiered Storage (KIP-405)

## The problem

Kafka couples storage and compute: every broker keeps a full copy of every
partition it hosts on local disk, for as long as retention requires. Long
retention (days to years, common in event-sourcing and compliance use
cases) forces brokers to carry huge local disks, which makes scaling
capacity and scaling throughput the same operation — adding disk space
means adding a broker, and rebalancing that broker's partitions means
moving terabytes of data over the replication protocol. Storage cost also
tracks the most expensive tier (local SSD/EBS) even for cold, rarely-read
historical segments.

## The design

KIP-405 splits a partition's log into two tiers. "Local" segments — the
active segment plus a configurable trailing window — stay on broker disk
exactly as before. Once a segment rolls and falls outside that local
window, a pluggable `RemoteStorageManager` uploads it to an external
store (S3, GCS, HDFS, etc.), and a `RemoteLogMetadataManager` (by default,
an internal Kafka topic) tracks which segment lives where. Reads for
offsets still in the local window hit disk as before; reads that fall
into the remote range are fetched from the remote tier and streamed back
to the consumer, transparently. Leadership, replication, and the
produce/consume APIs are unchanged — tiering is purely a storage
back-end swapped in at the segment level. This is why the design took
the pluggable-interface route rather than baking in a specific object
store: it keeps Kafka's core replication/consistency model untouched
and lets operators choose their own remote store and even their own
metadata implementation.

## Trade-offs

**Strengths:** decouples retention window from local disk size (broker
count now scales with throughput, not history); dramatically cuts
storage cost for cold data; shrinks broker failure/rebalance blast
radius since less data lives on local disk; new consumers replaying
history no longer strain broker page cache.

**Limitations:** remote reads have materially higher and less
predictable latency than local disk, so tiering trades cost for tail
latency on old data; adds an operational dependency on the remote
store's availability and IAM setup; the RLMM's internal topic adds
another piece of critical metadata infrastructure to reason about;
early releases had rough edges around unclean leader election,
retention-deletion races, and quota interaction with remote fetches.

## Where a contributor could dig in

- Caching layer for recently-tiered segments (a hot/warm cache in front
  of the remote store) to close the read-latency gap.
- Better observability: per-partition metrics on remote-fetch latency
  and cache hit rate are still coarse in places.
- Additional `RemoteStorageManager` implementations and conformance
  tests, since correctness bugs here are store-specific and easy to miss.
- Tightening the interaction between tiered storage and log compaction,
  which is still evolving (KIP-950 and follow-ups).

## Reference

- [KIP-405: Kafka Tiered Storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)
