# KIP-1371 contracts-first POC: implementation and evidence

## Provenance and limits

- Code branch: `codex/kip-1371-contracts-poc`.
- Baseline: `820533b870106cc0e0ac60e2076b8644d68bd85f` (trunk).
- Design: `codex/kip-1371-contracts-docs` at `413a0cee21`,
  `docs/design/kip-1371-contracts-first-draft.md`.
- The previous POC, DOC branch, published KIP, and historical benchmark evidence are unchanged.
- This is a selective implementation experiment, not completion of every migration/variant gate.
  A passing current-baseline regression is not a failing historical reproduction and is not a new fix.

## Reviewer order

1. **Activation:** typed `PollResult` compatibility boundary, concrete coordinator activation,
   coordinator-blocked commit wait, and its enabling scheduling notification.
2. **Ownership/admission:** captured coordinator version, owner-side invalidation checks,
   and no new work admission inside the network-completion batch.
3. **Publication/effects:** selected error/completion paths, preserving existing buffers and positions.
4. **Extraction decision:** assess repetition and open gates; do not add generic machinery by default.

Each implementation commit follows this order. Scheduling notification is included in step 1 because
removing an application deadline without an enabling wake would introduce a liveness regression.
It is not a migration of all application effects.

## Reproducible local validation

Use JDK 17 and the existing offline Gradle cache; all tests use isolated fixtures, not a running broker:

```sh
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home ./gradlew :clients:test \
  --tests '*CoordinatorRequestManagerTest' --tests '*CommitRequestManagerTest' \
  --tests '*NetworkClientDelegateTest' --tests '*ConsumerNetworkThreadTest' \
  --tests '*ApplicationEventHandlerTest' --offline --max-workers=2 -PmaxParallelForks=1
```

The initial unchanged-baseline run passed 42 tests: ConsumerNetworkThread (18), CoordinatorRequestManager (11),
FetchBuffer (5), and ShareFetchBuffer (8), including Checkstyle and SpotBugs.

### Step 1: activation

Validation: 196 tests passed (144 commit, 11 coordinator, 16 delegate, 19 network-thread, 6 application-handler),
zero failed/skipped/errors. Checkstyle passed; SpotBugs was skipped on this development run and remains a final gate.

- **Observed red test:** `CommitRequestManagerTest.testExpiredAutoCommitAwaitsCoordinatorInsteadOfZeroApplicationWait`
  failed on unchanged production baseline: expected `Long.MAX_VALUE`, actual `0`, after an expired auto-commit
  with no coordinator. This is a component reproduction of the coordinator-blocked auto-commit shape
  (related to KAFKA-20970), not a historical release CPU benchmark.
- Coordinator request admission and next condition use the same local state calculation. A sent request
  waits for its completion; successful discovery changes the next condition without advancing time.
- The application wait uses the same coordinator requirement as commit admission. An earlier aggregate
  deadline is published before a latched buffer wake. Repeating the same expired obligation does not wake again.
- Outputs and next conditions remain independent. An empty immediate result is rejected. Legacy zero
  remains a zero retry adapter, not evidence of progress. Maximum finite retries remain typed retries.
- No arbitrary manager output lists are retained by reference: `PollResult` copies the request list.

Remaining activation migration: regular/share/Streams heartbeat, fetch, metadata, share acknowledgement,
and topology producers still have raw-delay adapters. Existing local guards remain necessary and are not
credited as new typed-activation fixes. Public-consumer recovery and all enabling-input interleavings need
coverage before declaring the complete activation slice ready for production.

## Historical issues and acceptance scope

| Evidence | What this experiment must establish | Status |
| --- | --- | --- |
| KAFKA-20253 / PR 22836 | In-flight work must not create a no-progress poll loop. | Existing coordinator regression retained and strengthened with typed cause; exact historical CPU reproduction not performed. |
| KAFKA-17674 / PR 17342 | An older offset operation must retain its admitted partition scope. | Baseline contains the historical fix; existing production-path regression must remain passing. |
| PR 21476 | Consumed position must precede the matching consumed marker. | Baseline fix and synchronized handoff retained; consumed-position ownership is not moved. |
| KAFKA-20854 / PR 23014 | Empty/no-progress fetch preparation must not select a wake. | Baseline classification retained; no credit claimed for re-fixing it. |
| KAFKA-20397 / proposed PR 21991 | Metadata error must be observable before a latched notification, including error-before-wait. | Selected effect-path experiment pending. |

No new throughput, allocation, GC, CPU, real-broker recovery, or tail-latency claim is made.
The old POC's benchmark does not validate this branch. No Jenkins job is submitted by this work.
