# Fix: Preserve Original Headers in DLQ Records

## Context

This fix is for Apache Kafka Streams. The DLQ (Dead Letter Queue) feature should preserve original record headers when forwarding failed records to DLQ topic.

## Specification

**KIP-1034:** https://cwiki.apache.org/confluence/display/KAFKA/KIP-1034%3A+Dead+letter+queue+in+Kafka+Streams

Quote from KIP-1034:
> **Headers** - Existing context headers are automatically forwarded into the new DLQ record

## Bug Description

- **What's wrong:** When a record fails processing and is sent to DLQ, the original headers from the source record are NOT copied to the DLQ record
- **Impact:** Downstream consumers of DLQ lose important context (e.g., tracing IDs, correlation IDs)
- **Root cause:** `ExceptionHandlerUtils.buildDeadLetterQueueRecord()` creates a new `ProducerRecord` without copying `context.headers()`

## References

- PR Discussion: https://github.com/apache/kafka/pull/17942#discussion_r2738033268

---

## Implementation Steps

### Step 1: Add Test (to verify bug exists)

**File:** `streams/src/test/java/org/apache/kafka/streams/errors/ExceptionHandlerUtilsTest.java`

**Action:** In method `checkDeadLetterQueueRecords()`, add this assertion at the end (after line 89):

```java
// Verify original source headers are preserved
assertEquals("hello world",
    stringDeserializer.deserialize(null,
        headers.lastHeader("sourceHeader").value()));
```

**Note:** The test setup already creates a source record with header `"sourceHeader"` = `"hello world"` (see lines 64-65). This assertion verifies the header is preserved in the DLQ record.

### Step 2: Run Test (should FAIL)

```bash
./gradlew :streams:test --tests "org.apache.kafka.streams.errors.ExceptionHandlerUtilsTest.checkDeadLetterQueueRecords"
```

**Expected result:** `NullPointerException` because `headers.lastHeader("sourceHeader")` returns `null`

### Step 3: Fix Implementation

**File:** `streams/src/main/java/org/apache/kafka/streams/errors/internals/ExceptionHandlerUtils.java`

**Action 1:** Add import at top of file (after line 20):

```java
import org.apache.kafka.common.header.Header;
```

**Action 2:** In method `buildDeadLetterQueueRecord()`, add header copying logic after line 85 (after `ProducerRecord` creation, before `StringWriter` creation):

```java
// Copy original headers from source record
if (context.headers() != null) {
    for (Header header : context.headers()) {
        producerRecord.headers().add(header);
    }
}
```

**Full context of the change:**

```java
// BEFORE (line 85-86):
final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(deadLetterQueueTopicName, null, context.timestamp(), key, value);
final StringWriter stackTraceStringWriter = new StringWriter();

// AFTER:
final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(deadLetterQueueTopicName, null, context.timestamp(), key, value);

// Copy original headers from source record
if (context.headers() != null) {
    for (Header header : context.headers()) {
        producerRecord.headers().add(header);
    }
}

final StringWriter stackTraceStringWriter = new StringWriter();
```

### Step 4: Run Test (should PASS)

```bash
./gradlew :streams:test --tests "org.apache.kafka.streams.errors.ExceptionHandlerUtilsTest.checkDeadLetterQueueRecords"
```

**Expected result:** `BUILD SUCCESSFUL`, test passes

---

## Summary

| Item | Value |
|------|-------|
| Test file | `streams/src/test/java/org/apache/kafka/streams/errors/ExceptionHandlerUtilsTest.java` |
| Implementation file | `streams/src/main/java/org/apache/kafka/streams/errors/internals/ExceptionHandlerUtils.java` |
| New import needed | `org.apache.kafka.common.header.Header` |
| Test command | `./gradlew :streams:test --tests "org.apache.kafka.streams.errors.ExceptionHandlerUtilsTest.checkDeadLetterQueueRecords"` |
