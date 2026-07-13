# Deep Dive: KRaft — Kafka's Self-Managed Metadata Quorum (KIP-500 / KIP-631)

## The problem

Before KRaft, every Kafka cluster depended on an external ZooKeeper ensemble to
store metadata (topics, partitions, ACLs, configs) and to elect a single
controller broker. This split-brain architecture had real costs: two systems
to operate and upgrade in lockstep, a hard ceiling on partition counts because
controller failover meant re-reading the *entire* metadata tree from
ZooKeeper (tens of seconds to minutes on large clusters), and a class of bugs
caused by metadata drifting between ZK's tree and the brokers' in-memory
state.

## The design

KRaft removes ZooKeeper and makes Kafka self-managed: a small quorum of
controller nodes runs a Raft variant to replicate an internal
`__cluster_metadata` topic. Metadata changes become **events appended to a
log**, not synchronous RPCs to an external store — the same log-as-source-of-
truth philosophy Kafka already applies to user topics, turned inward on
itself. The active controller is simply the Raft leader; brokers become
followers of this log and materialize an in-memory `MetadataCache` by
replaying it, with periodic **snapshots** so a restarting broker doesn't
replay history from offset 0. This gets failover down to a leader election
(sub-second) instead of a metadata reload, and lets one binary/process model
serve both roles.

## Trade-offs

Strengths: unified operational surface, much faster controller failover,
higher partition-count ceilings, simpler mental model (log-centric
throughout the stack). Limitations: the ZK→KRaft migration path was itself a
multi-release engineering effort (dual-write bridge mode); some third-party
tooling built directly against ZooKeeper's tree had to be rewritten; and
KRaft concentrates quorum health onto a small set of controller nodes, so
their disks/network now sit squarely on the metadata critical path.

## Where a contributor could dig in

- Metadata log compaction/snapshot tuning for very large topic counts (record
  batching in `KRaftMetadataLog`).
- Observability: metrics distinguishing "controller is slow" from "quorum
  replication is slow" are still coarse in places.
- Broker-side `MetadataCache` replay performance under bursty topic
  create/delete workloads (relevant to the share-group and tiered-storage
  work landing now, both of which grow metadata event volume).

## Reference

- KIP-500: https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum
- KIP-631 (quorum-based Kafka Controller): https://cwiki.apache.org/confluence/display/KAFKA/KIP-631%3A+The+Quorum-based+Kafka+Controller
