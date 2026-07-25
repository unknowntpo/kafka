# Deep Dive: KRaft — Kafka's Self-Managed Metadata Quorum (KIP-500)

**Topic:** Removal of the ZooKeeper dependency in favor of a Raft-based metadata
quorum built into Kafka itself.

## The problem

Before KRaft, cluster metadata (topics, partitions, ACLs, configs) lived in
ZooKeeper, while the *controller* — a single broker elected via ZK — pushed
that state to every other broker over separate RPCs. This created two sources
of truth that could drift, a controller failover path that had to reload the
entire metadata set from ZK on every election (slow at scale), and a hard
ceiling on partition counts because ZK watches and full-state propagation
didn't scale past roughly 200k partitions. Operators also had to run, secure,
and monitor an entirely separate distributed system just to run Kafka.

## The design

KRaft (`raft/` and `metadata/` modules) treats metadata as an event-sourced
log, replicated via a Raft variant implemented directly in `KafkaRaftClient`.
A small set of controller nodes forms the quorum; the active controller is
the Raft leader, and every state change (topic creation, ISR shrink, config
update) is appended as a record to `__cluster_metadata` before being applied.
Brokers no longer receive imperative RPCs — they are Raft observers that
fetch and replay the log locally, so their view of metadata is just "how far
have I replayed," making catch-up and restart deterministic and cheap. Log
segments are periodically compacted into snapshots so a new controller or
broker doesn't need to replay history from epoch zero.

## Trade-offs

- **Wins:** one system to operate instead of two; sub-second controller
  failover since the new leader already has the log; metadata scales to
  millions of partitions; a cleaner security model (no separate ZK ACLs).
- **Costs:** the correctness burden that ZooKeeper's maturity used to absorb
  now lives in Kafka's own Raft implementation; very large metadata logs
  still cost real time to replay on cold broker start; the migration path
  from ZK (bridge release, dual-write) was itself a multi-year engineering
  effort with real operational risk.

## Where a contributor could help

The raft layer is still evolving in the open. Two recent examples visible in
the code: `ProspectiveState` implements a **pre-vote** phase (KIP-996) to
stop a partitioned node from forcing needless elections, and
`DynamicVoters`/`ReplicaKey` support **dynamic quorum reconfiguration**
(KIP-853) so voters can be added/removed without a rolling restart. Open
areas: faster snapshot loading for clusters with very large metadata logs,
better observability into quorum health (replication lag per voter, election
churn), and witness/observer roles for stretched multi-region quorums.

**Reference:** [KIP-500: Replace ZooKeeper with a Self-Managed Metadata
Quorum](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)
