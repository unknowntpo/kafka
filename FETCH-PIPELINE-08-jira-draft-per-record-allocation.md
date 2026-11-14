# Jira draft: per-record allocation in the consumer

- Project: KAFKA
- Issue Type: Improvement
- Component: clients, consumer
- Summary: Reduce per-record object allocation when the consumer decodes fetched records

## Description

When the consumer reads small records, most of its work is object allocation, not decoding.

I profiled `kafka-consumer-perf-test` with async-profiler (`event=alloc`) on a topic with 100-byte records, no key, no headers, no compression, and `ByteArrayDeserializer`. To read 11.4 GB of record data the JVM allocated 47.1 GB, which is 4.1 times the data.

Two kinds of objects are required: the `byte[]` value (31%) and the `ConsumerRecord` (18%). About 40% of the rest are short-lived objects that are created for every record:

| Object | Share | Where |
|---|---|---|
| `HeapByteBuffer` | 14% | `DefaultRecord.readFrom` calls `Utils.readBytes` for the key and the value. This creates a slice. `CompletedFetch.parseRecord` then copies the slice into a `byte[]` and drops it. |
| `DefaultRecord` | 14% | One per record. It only lives until `parseRecord` builds the `ConsumerRecord`. |
| `RecordHeaders` and its `ArrayList` | 12% | `parseRecord` calls `new RecordHeaders(record.headers())` even when the record has no headers. |
| `Optional` | 4% | `CompletedFetch.fetchRecords` builds the leader epoch `Optional` for every record. The value is the same for the whole batch. |
| `Object[]` | 3.5% | `fetchRecords` starts with `new ArrayList<>()` and grows it up to `max.poll.records`. |

The test machine has 4 cores and is limited by memory bandwidth. On this machine, moving the decode work to other threads did not help, because the same number of bytes is still written to memory. Reducing allocation did help.

## Proposal

Remove the per-record objects that are not part of the result. None of these changes the public API.

1. Do not create a `ByteBuffer` slice when the key or value is only copied into a `byte[]`.
2. Build the leader epoch `Optional` once per batch.
3. Create the result list with a capacity based on `max.poll.records`.
4. Do not allocate the header list for a record that has no headers until the application adds a header.
5. Decode from the batch buffer directly into `ConsumerRecord`, without a `DefaultRecord` for each record.

Items 1 to 3 are small. A prototype with only these three reduced allocation by 17% and raised throughput from 855 MB/s to 985 MB/s (+15%) in the test above. Items 4 and 5 are larger and can be separate sub-tasks.

## Scope

- Only the consumer decode path (`CompletedFetch`, `DefaultRecord`, `RecordHeaders`).
- No new configuration.
- No change to `ConsumerRecord`, `Headers` or `Deserializer`.

## How to reproduce

1. Create a topic with 6 partitions and produce 10 GB of 100-byte records with `kafka-producer-perf-test`.
2. Run `kafka-consumer-perf-test` with async-profiler attached: `-agentpath:libasyncProfiler.so=start,event=alloc,file=alloc.html`.
3. Read the allocation flame graph for the application thread.
