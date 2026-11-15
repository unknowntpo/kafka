# KIP-XXXX: Let the operating system size socket buffers by default

## Status

- Current state: Under Discussion
- Discussion thread: TBD
- JIRA: TBD (new). Related: [KAFKA-16496](https://issues.apache.org/jira/browse/KAFKA-16496)

## Motivation

Each TCP connection has a send buffer and a receive buffer in the kernel.
The socket options SO_SNDBUF and SO_RCVBUF set the size of these buffers.
Kafka sets them from configs such as `socket.send.buffer.bytes` and `receive.buffer.bytes`.
Today these configs have small fixed defaults, from 32 KB to 128 KB.

On Linux, the kernel normally sizes these buffers itself. This is called kernel auto-tuning.
With auto-tuning, a buffer starts small and grows on demand.
The upper limit comes from the sysctls `net.ipv4.tcp_wmem` (send) and `net.ipv4.tcp_rmem` (receive).
When an application sets SO_SNDBUF or SO_RCVBUF to a fixed value, Linux turns off auto-tuning for that socket.
The kernel then doubles the requested value for bookkeeping and never grows it.
For example, a 100 KB request gives a 204800-byte buffer, and a 64 KB request gives 131072 bytes.

The buffer size limits how many bytes can be in flight on one connection.
The TCP window is the amount of data a sender may have in flight before it gets an acknowledgement.
A small fixed buffer keeps the TCP window small, so it limits bytes per second per connection.
On a fast network this limit is far below the link speed.

All these configs already accept `-1`, which means "the OS default will be used" (KAFKA-724 for the broker, KAFKA-3840 for clients).
With `-1`, Kafka does not set SO_SNDBUF or SO_RCVBUF, and auto-tuning stays on.
Kafka's own broker-internal clients already use `-1`.
The follower fetch sender uses it for the send buffer.
The node-to-controller channel, the KRaft client, and the transaction marker channel use it for both buffers.
This KIP makes `-1` the default for the user-facing configs as well.

### Measurement

Setup: Google Cloud, one zone, Ubuntu 24.04, JDK 17, Kafka trunk at commit `e90d6f42c2`.

| Item | Value |
|---|---|
| Broker | one broker on one `n2-standard-16` VM (16 vCPU) |
| Client | one `c4-standard-8` VM (8 vCPU) |
| Round-trip time | about 0.1 ms |
| `iperf3`, one TCP stream | about 30 Gbit/s; 11.3 Gbit/s with the window limited to 200 KB (`-w 200K`) |
| Kernel limits on the VMs | `tcp_wmem` max 4 MB, start 16 KB; `tcp_rmem` max 32 MB, start 128 KB |
| Topic | 12 partitions, replication factor 1, 20 GB of 1000-byte records, no compression |
| Consumer | one `kafka-consumer-perf-test`, `group.protocol=consumer`, 2 GB heap |

`kafka-consumer-perf-test` sets `receive.buffer.bytes` to 2 MB unless `--socket-buffer-size` is given.
To measure the consumer default, the runs used `--socket-buffer-size 65536`.
Each value is the median of 3 runs.

| Broker `socket.send.buffer.bytes` | Consumer `receive.buffer.bytes` | MB/s |
|---|---|---|
| 102400 (default) | 65536 (default) | 457 |
| 102400 (default) | -1 | 765 |
| -1 | 65536 (default) | 449 |
| -1 | -1 | 1163 |

Results:

- Changing both sides from the defaults to `-1` gives 2.5 times the throughput.
- Changing only one side gives little or no gain. Both sides must change together.
- With the 64 KB consumer buffer, the consumer threads were not busy. The network thread used about 20% of one core, and the application thread about 14%.
- With the 64 KB consumer buffer, average fetch latency was about 22 ms for a 12.3 MB fetch response. With a 2 MB buffer it was about 5 ms.
- `ss -ti` on the broker showed the cause. These two runs used a 2 MB consumer buffer. With the broker default, the send buffer was 204800 bytes and the delivery rate was 10.2 Gbit/s.
  The socket was flagged `sndbuf_limited`. This flag means the connection waited for space in the send buffer.
  With `-1`, the send buffer grew to 4194304 bytes and the delivery rate was 26.7 Gbit/s.

Limits of this measurement:

- One broker, one consumer, one record size, one cloud, a low-latency network, and 3 runs per case.
- Only the fetch direction was measured (broker send buffer and consumer receive buffer).
- The produce direction and follower fetch were not measured. This KIP includes them because the same mechanism applies.
- On a slower network (for example 1 Gbit/s) with low latency, the fixed buffers are probably not the limit. We do not claim a gain there.
- Throughput per connection is at most about buffer size divided by round-trip time.
  So a higher round-trip time should make a small fixed buffer more limiting. This is reasoning, not a measurement.

## Public Interfaces

No new config, API, or protocol change. Only default values change.

| Config | Component | Current default | New default |
|---|---|---|---|
| `socket.send.buffer.bytes` | broker | 102400 | -1 |
| `socket.receive.buffer.bytes` | broker | 102400 | -1 |
| `replica.socket.receive.buffer.bytes` | broker (follower fetch) | 65536 | -1 |
| `send.buffer.bytes` | producer, consumer, admin client, Connect worker, Streams | 131072 | -1 |
| `receive.buffer.bytes` | consumer, admin client | 65536 | -1 |
| `receive.buffer.bytes` | producer, Connect worker, Streams | 32768 | -1 |

## Proposed Changes

1. Change the defaults in the table above to `-1`.
2. Remove `socket.send.buffer.bytes=102400` and `socket.receive.buffer.bytes=102400` from the sample config files:
   `config/server.properties`, `config/broker.properties`, `config/controller.properties`, and `docker/server.properties`.
3. Add an entry to the upgrade notes in `docs/getting-started/upgrade.md`.

No new code path is needed.
With `-1`, Kafka already skips `setSendBufferSize` and `setReceiveBufferSize`.
See `Selector.java`, `SocketServer.scala`, and `ServerSocketFactory.java`.

Target release: 4.5.0.

## Compatibility, Deprecation, and Migration Plan

- Users who set these configs explicitly see no change.
- For users on the defaults, the operating system now sizes the socket buffers.
- On Linux, memory per busy connection can grow up to the `tcp_wmem` or `tcp_rmem` maximum.
  Idle connections stay small, because auto-tuning grows a buffer only on demand.
  The sysctl `net.ipv4.tcp_mem` limits the total TCP memory of the host.
- A broker with many busy connections can use more kernel memory than before.
- To restore the old behaviour, set the old values explicitly.
- On other operating systems, behaviour follows their defaults. This was not measured.
- No protocol change, no API change, and no deprecation.
- A rolling upgrade is safe. Brokers and clients can change independently, but the gain needs both sides.

## Test Plan

- Existing unit and integration tests already cover the value `-1`.
- Before the vote, we will repeat the benchmark above and add these cases:
  - Produce direction (producer `send.buffer.bytes`, broker `socket.receive.buffer.bytes`).
  - Follower fetch (`replica.socket.receive.buffer.bytes`).
  - Broker kernel TCP memory with several thousand connections.
  - A network with higher round-trip time.
- We will publish the numbers in the discussion thread.

## Rejected Alternatives

### Change only the client `receive.buffer.bytes` (KAFKA-16496)

KAFKA-16496 proposes `-1` only for the client `receive.buffer.bytes`.
In our setup this raised throughput from 457 MB/s to 765 MB/s.
Changing only the broker side gave no gain.
Both sides together gave 1163 MB/s, so the pair is needed.

### Raise the fixed defaults (for example to 1 MB or 2 MB)

A fixed value still turns off auto-tuning.
The kernel caps a fixed request at `net.core.rmem_max` or `net.core.wmem_max`.
A large fixed value gives no benefit to slow links.
Any fixed number becomes wrong as networks get faster.

### Documentation only

Most deployments keep the defaults, so a documentation change alone would help few users.

### A new config for an adaptive buffer inside Kafka

The kernel already sizes buffers on demand. A Kafka-side mechanism would duplicate it.
