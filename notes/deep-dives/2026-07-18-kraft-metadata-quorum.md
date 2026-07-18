# Deep Dive: KRaft — Kafka's Self-Managed Metadata Quorum (KIP-500)

## The problem

Before KRaft, every Kafka cluster depended on an external ZooKeeper ensemble to
store topic configs, partition assignments, ACLs, and to elect the controller.
This split the system in two: Kafka's own scalability was capped by ZooKeeper's
(practically, clusters topped out around a couple hundred thousand partitions),
operators had to run and tune two distributed systems instead of one, and
controller failover was slow — a newly elected controller had to re-read the
*entire* metadata state from ZooKeeper and push full snapshots to every broker
before the cluster was usable again.

## The design

KIP-500 replaces ZooKeeper with KRaft: a Raft-based consensus protocol
(specified in KIP-595, `raft/` in this repo) that treats cluster metadata as
just another replicated log — the internal `__cluster_metadata` topic. A small
quorum of controller nodes runs Raft to agree on an ordered sequence of
metadata records; brokers become simple consumers of that log and build their
in-memory metadata cache by applying incremental deltas rather than pulling
full snapshots (KIP-631). This reuses Kafka's own replication machinery for
its own control plane — the metadata store is now "just Kafka."

## Trade-offs

**Strengths:** one system to deploy and secure instead of two; controller
failover is fast because the new leader already has the replicated log, not a
cold re-read from ZK; partition-count scalability jumps by roughly an order of
magnitude; a single, consistent ACL/authorizer path.

**Limitations:** migrating a live ZK cluster requires a fragile dual-write
bridge phase (KIP-866); early KRaft lost some ZK-era tooling and had rougher
edges around dynamic reconfiguration of the controller quorum; the controller
quorum is still a leader-based system, so it inherits Raft's sensitivity to
WAN latency in stretched/multi-region deployments — the SPOF just moved.

## Where a contributor could dig in

- Snapshot/compaction tuning of the metadata log for very large partition counts.
- Better observability into Raft internals (leader lease duration, replica
  catch-up lag) exposed as metrics.
- Hardening `kafka-metadata-shell` and the ZK migration tooling.
- Raft quorum behavior under high inter-node latency (stretch clusters).

## Reference

KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum —
https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum
