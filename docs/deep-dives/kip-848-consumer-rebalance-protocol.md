# Deep Dive: KIP-848 — The Next Generation Consumer Rebalance Protocol

*Part of an ongoing series digging into Kafka's internal design decisions.*

## The problem

Kafka's original consumer group protocol (`JoinGroup` / `SyncGroup`, implemented
in the "classic" `GroupCoordinator` and still visible today via
`ClassicGroup` in `group-coordinator/`) assigns partitions *client-side*: one
elected group leader downloads every member's subscription metadata, runs the
assignor (range, round-robin, sticky, cooperative-sticky) locally, and pushes
the result back through the coordinator. Membership changes are tracked with
a single monotonically increasing **generation** shared by the whole group.

That design has two structural costs:

1. **Stop-the-world coordination.** Under the eager protocol every member
   revokes all partitions and rejoins on any membership change; even the
   "incremental cooperative" assignor still needs a barrier — the group can't
   settle in one round because members only learn the *next* target
   assignment after everyone rejoins with the *current* one. Large groups
   (thousands of members, e.g. Kafka Streams apps) can take many rebalance
   rounds to converge, during which consumption stalls.
2. **Duplicated, fragile client logic.** Every client SDK (Java, librdkafka,
   sarama, ...) has to re-implement the assignor, the leader-election dance,
   and generation bookkeeping identically, or groups behave inconsistently
   across languages.

## The design

KIP-848 moves assignment computation to the **group coordinator on the
broker**, and replaces `JoinGroup`/`SyncGroup`/`Heartbeat` with a single RPC,
`ConsumerGroupHeartbeat` (`clients/.../ConsumerGroupHeartbeatRequest.java`).
Each member periodically heartbeats its current epoch and owned partitions;
the response carries any *new* target assignment. The coordinator's
`ConsumerGroup`/`TargetAssignmentBuilder`
(`group-coordinator/src/main/java/.../modern/`) computes a **target
assignment** for the whole group once, keyed by a **group epoch**, but each
member reconciles toward that target **independently**, at its own pace,
tracked by a per-member epoch. There is no global barrier: member A can
finish adopting its slice of the new assignment while member B is still
revoking partitions, and neither blocks the other. Partition ownership
transfers are still safe because a partition is only handed to its new owner
after the old owner acknowledges revocation — cooperative rebalancing is now
the built-in behavior, not an opt-in assignor.

This is the same philosophy KRaft applied to metadata (single authoritative
computation, pushed out and reconciled incrementally) applied to group
membership.

## Trade-offs

**Strengths**
- No stop-the-world pause; convergence is proportional to the size of the
  *change*, not the size of the group.
- One assignment algorithm, implemented once, on the broker — client SDKs
  become "dumb" reconcilers, eliminating cross-language assignor drift.
- Heartbeat and rebalance are unified, simplifying failure detection and
  session timeout handling.

**Limitations**
- Centralizes CPU/memory cost of assignment on the coordinator shard; a
  single very large group (millions of partition-assignments) can become a
  hot spot in a way client-side computation never was.
- Requires KRaft — no ZooKeeper support — and a new on-disk record schema,
  so it's a hard fork of coordinator state machines (`ClassicGroup` vs
  `ConsumerGroup` coexist and must be migrated between).
- Non-Java clients need to implement the new heartbeat/reconciliation state
  machine from scratch to get the benefits; adoption lags the protocol.

## Where a contributor could dig in

- **Migration tooling**: smoothing the classic→consumer group upgrade path
  (mixed-member groups during rollout) still has rough edges worth
  instrumenting/testing.
- **Coordinator scalability**: profiling `TargetAssignmentBuilder` for
  groups with very high member×partition cardinality and optimizing the
  incremental diff computation.
- **Observability**: metrics/logging around reconciliation latency per
  member are still coarse compared to the rich rebalance listeners the
  classic protocol had.
- **Share groups (KIP-932)** reuse this heartbeat/epoch machinery for queue
  semantics — there's overlap to exploit and edge cases to shore up between
  the two coordinator paths.

## Reference

KIP-848: The Next Generation of the Consumer Rebalance Protocol —
https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
