# Deep Dive: Exactly-Once Semantics (Idempotent Producer + Transactions)

## The pattern
KIP-98 (and the surrounding KIP-129/KIP-447 work) gives Kafka
producer/consumer/streams pipelines exactly-once processing without an
external transaction manager. It layers two mechanisms: an **idempotent
producer**, which tags each producer with a PID and per-partition sequence
numbers so the broker can dedup retried writes, and **transactions**, which
let a producer atomically write to multiple partitions (including consumer
offsets) via a transaction coordinator, a `__transaction_state` log, and
commit/abort control markers. Consumers set `isolation.level=read_committed`
to filter out records from aborted or in-flight transactions.

## Why it exists
Kafka's original at-least-once contract meant retries (broker failover,
producer timeouts) could produce duplicates, and consume-transform-produce
loops (the core of Kafka Streams) had no way to make "read input, write
output, commit offset" atomic. Rather than bolt on two-phase commit against
external systems, Kafka reused primitives it already had — logs and offsets
— treating the transaction log itself as the durable coordinator state, and
control records as an in-band commit protocol.

## Design choices and trade-offs
- **Sequence numbers per partition, not globally**: dedup is local and
  O(1), but the PID/epoch space is finite and producer restarts require
  careful epoch fencing to avoid zombie writers.
- **Strength**: EOS composes with Streams' consume-process-produce loop
  almost for free, since offsets are just another partition written inside
  the same transaction.
- **Limitation**: read_committed consumers must buffer until a transaction
  resolves, adding latency and memory pressure under long-lived or stuck
  transactions (a slow/hung producer stalls downstream reads).
- **Limitation**: cross-cluster or cross-system exactly-once (e.g. into a
  database via Connect) still isn't covered — the guarantee stops at
  Kafka's own log boundary.

## Where a contributor could push further
Transaction timeout tuning and coordinator failover still surface as
operational pain points (visible in recurring KAFKA-* tickets about hanging
transactions blocking log cleanup, since compaction can't remove records a
pending transaction might still reference). Better observability into
in-flight transaction state per partition, and reducing the worst-case
buffering cost for read_committed consumers, remain open areas.

Reference: https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging
