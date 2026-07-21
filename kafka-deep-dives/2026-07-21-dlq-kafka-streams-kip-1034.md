# Deep Dive: Dead Letter Queue in Kafka Streams (KIP-1034)

## The problem

Kafka Streams already had exception handlers for the three places a
record can blow up mid-processing — `DeserializationExceptionHandler`,
`ProductionExceptionHandler`, and (since KIP-1033)
`ProcessingExceptionHandler`. But every handler's remedies were binary:
log-and-skip the record, or fail and kill the whole application. There
was no built-in way to *keep* the offending record anywhere. Operators
who wanted a dead-letter queue had to hand-roll one — wrap their
topology in a try/catch, manually re-serialize the raw bytes, and
produce to a side topic themselves — which every team did slightly
differently and none of it composed with the existing handler
interfaces. KIP-1034 (KAFKA-16505, shipped in Kafka 4.2.0) closes that
gap natively.

## The design

Each exception handler's response type gained an optional
`deadLetterQueueRecords()` list. When a handler decides to continue
processing rather than fail, it can now also return one or more
producer-ready `ProducerRecord`s — raw key/value bytes plus headers,
already reconstructed from whatever context the handler had (original
topic/partition/offset, the exception, deserialization vs. processing
vs. production stage). Streams takes that list and produces it to the
topic named by the new `errors.deadletterqueue.topic.name` config.
Kafka Streams does not auto-create this topic — operators provision it
like any other topic, which keeps partitioning/replication/ACL
decisions where they already belong. Keeping this inside the handler
interface (rather than a separate global DLQ subsystem) was deliberate:
it reuses a mechanism developers already understood, and per-handler
control means a team can send deserialization failures to one DLQ and
production failures to another, or skip DLQ routing per exception type
entirely.

## Trade-offs

**Strengths:** first-class, replayable record of what failed and why,
without hand-rolled plumbing; composes with existing handler logic
instead of replacing it; no forced schema — headers/bytes are passed
through as-is so any downstream tooling can reprocess.

**Limitations:** one DLQ topic per handler config, not per
error-type, so downstream consumers must inspect headers to
disambiguate causes; no built-in retry-from-DLQ tooling — replay is
still a manual operational step; extra produce calls on the failure
path add latency/throughput cost under sustained error bursts; because
the topic isn't auto-created, misconfiguration silently drops the DLQ
path (handler still "succeeds" from Streams' perspective).

## Where a contributor could dig in

- Built-in reprocessing tooling (a connector or CLI) to replay DLQ
  records back into the source topic after a fix ships.
- Standardized DLQ record headers across Streams, Connect, and any
  future consumer-side DLQ work, so tooling isn't handler-specific.
- Metrics/alerting hooks fired on DLQ writes, so bursts show up in
  monitoring without scraping the DLQ topic itself.
- Backpressure or rate-limiting when the DLQ topic itself becomes the
  bottleneck during correlated failure storms.

## Reference

- [KIP-1034: Dead letter queue in Kafka Streams](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1034:+Dead+letter+queue+in+Kafka+Streams)
- [KAFKA-16505 PR #17942](https://github.com/apache/kafka/pull/17942)
