# Issue draft: reuse the receive buffer of fetch responses in the consumer

Status: DRAFT. Not posted anywhere. Branch `consumer-receive-buffer-reuse` (local only), based on trunk `5b648ca9cb`.

---

**Title:** Reuse the receive buffer of fetch responses in the Java consumer

**Component:** clients, consumer

## Problem

The consumer reads every response into a new heap `ByteBuffer`. `NetworkReceive` allocates it from `MemoryPool.NONE`, and the garbage collector frees it later.

For fetch responses this buffer is large. A response can be as large as `fetch.max.bytes` (50 MiB by default). The JVM must fill a new array with zeros, so the cost of each allocation grows with the size of the response. With G1, an array of at least half a region is a humongous object, which is allocated directly in the old generation.

In a local test with 1 KB records, this allocation site (`HeapByteBuffer.<init>`) is 43% of the allocation pressure that JFR samples in the consumer process.

## Proposal

Keep the receive buffers of fetch responses and use them again.

1. Add `CachingMemoryPool`, an implementation of the existing `MemoryPool` interface.
   - It keeps released buffers of at least 1 MiB. Smaller buffers are plain heap buffers, as today.
   - It keeps at most `2 * fetch.max.bytes` in total for one consumer.
   - It never blocks and never refuses an allocation. If no cached buffer fits, it allocates a new one.
   - A buffer that is never released is collected by the GC. A missed release costs one reuse, not correctness.
2. The consumer passes this pool to its `Selector`. The producer, the admin client and the share consumer keep `MemoryPool.NONE`.
3. `ClientResponse.releaseBuffer()` returns the buffer of a response to the pool. `NetworkReceive.close()` does the work.
4. The fetcher releases the buffer when the last partition of a fetch response is drained. The hook is in `FetchMetricsAggregator`, which already knows when every partition of a response is done.

There is no new configuration.

## Why it is safe

A buffer can be used again only if nothing still points into it.

- The pool is used only when both deserializers are known to copy their input: `ByteArrayDeserializer`, `BytesDeserializer`, `StringDeserializer`, `UUIDDeserializer`, `VoidDeserializer` and the number deserializers. The check uses the exact class. A subclass, a wrapper, `ByteBufferDeserializer` or any custom deserializer gets `MemoryPool.NONE`, which is the behaviour of today.
- Record headers are decoded lazily from the buffer. `CompletedFetch.parseRecord` now reads the header key and value before it returns the record.
- `RecordDeserializationException` gets a copy of the key and value buffers.
- Compressed batches are decompressed into other buffers, so their records never point into the receive buffer.

## A related fix in `FetchCollector`

`FetchCollector.initialize()` reports a partition to `FetchMetricsAggregator` in a `finally` block, also when it throws. But a fetch that throws and still has data stays in the fetch buffer and is read again by the next `poll()`. One example is the error "Failed to make progress reading messages ... no complete records were found".

With buffer reuse this would release the buffer while a fetch in the queue still reads from it. The change reports such a fetch only when it is removed from the queue.

This also removes a problem that exists on trunk today. It is found by reading the code, not by a test: the fetch is reported again on every `poll()` that fails. If every other partition of the response was reported before, `FetchMetricsAggregator` records the `bytes-fetched` and `records-fetched` values of the whole response again each time.

One visible change: the metrics of the other partitions in that response are now recorded when the failed fetch leaves the queue, not at the first failure.

## Measurement

Local machine only. The numbers show the direction, not the size of the effect on a real cluster.

- Tool: `kafka-consumer-perf-test` (it does not read the record values). Same build for both sides; only the `kafka-clients` jar differs.
- Baseline: trunk `5b648ca9cb`. Change: branch `consumer-receive-buffer-reuse`.
- Machine: Apple M1 Pro, 8 cores, macOS 26.3, JDK 21.0.1, G1, heap fixed at 2 GB. One broker and the client on the same machine, over loopback.
- Data: 12 partitions, 6,000,000 records of 1 KB (5859 MB), no compression, no key. Default consumer configuration.
- Units: MB is 1024 * 1024 bytes, in the tool output and in the GC log.
- Each run reads the whole topic (about 20 to 27 seconds). Runs alternate between baseline and change. The value is the median, with the range in brackets.

| | Runs | MB/s | CPU seconds per GB (user + sys) | MB allocated per MB consumed |
|---|---|---|---|---|
| `group.protocol=consumer`, trunk | 5 | 272.5 (249.5 – 291.6) | 1.97 (1.91 – 2.15) | 2.38 (2.25 – 2.46) |
| `group.protocol=consumer`, change | 5 | 298.8 (287.7 – 315.7) | 1.86 (1.80 – 1.88) | 1.20 (1.16 – 1.31) |
| `group.protocol=classic`, trunk | 3 | 224.0 (216.0 – 238.5) | 2.09 (1.98 – 2.11) | 2.37 (2.35 – 2.49) |
| `group.protocol=classic`, change | 3 | 262.7 (249.8 – 279.5) | 1.92 (1.86 – 1.94) | 1.34 (1.33 – 1.34) |

- Allocation is about half. This is measured: it is the sum of the heap growth between young collections in the GC log, with a resolution of 1 MB.
- CPU per GB is 6% to 8% lower. It includes the start of the JVM.
- Throughput is 10% (consumer protocol) and 17% (classic protocol) higher. The ranges of the consumer protocol runs overlap a little, so this number is less certain than the other two.
- JFR (`settings=profile`, one run each): on trunk `HeapByteBuffer.<init>` is 42.9% of the allocation pressure. With the change it is not in the first seven entries.

Small records (30,000,000 records of 100 bytes, 2861 MB, `group.protocol=consumer`, 3 runs each): allocation per MB consumed goes from 5.20 (5.06 – 5.22) to 3.76 (3.59 – 3.84), and CPU seconds per GB from 5.44 (4.56 – 5.47) to 4.29 (4.02 – 4.30). The throughput of these runs changed too much between runs (48 to 161 MB/s) to compare the two sides, so the CPU number of these runs is also uncertain. Only the allocation number is reliable here.

Not measured: more than one broker, a client on a separate machine, compression, a larger heap, memory use over time, and a workload where the size of the responses changes a lot.

## Open questions

1. **The cache does not shrink.** A consumer that received large responses keeps up to `2 * fetch.max.bytes` (100 MiB by default) until it is closed, also when it is idle. Today the GC frees this memory. Kafka Streams, Connect and MirrorMaker use `ByteArrayDeserializer`, so every consumer in them gets the pool. Should the cache release buffers that were not used for some time, or should the limit follow the size of recent responses?
2. **Buffers of different sizes.** A new buffer has exactly the size of the response. When a cached buffer is too small for the next response, the pool drops it. If the size of the responses changes a lot, many allocations miss the cache. Rounding the size up would raise the hit rate and use more memory. This is not measured yet.
3. **Deserializers.** The list of copying deserializers is fixed in the code. A custom deserializer that copies its input does not get the pool. Is a marker on `Deserializer` worth a KIP later?
4. **Scope.** The share consumer does not use the pool in this change.

## Not in this change

- A change of when the consumer sends fetch requests.
- A change of how records are decoded.
- Records that point into the receive buffer without a copy.
