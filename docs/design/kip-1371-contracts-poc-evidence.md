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

### Step 2: ownership and admission

Validation: 568 tests passed, zero failed/skipped/errors: commit 147, coordinator 12, admission contract 1,
regular heartbeat 67, share heartbeat 35, Streams heartbeat 256, Streams topology 13, offsets 37.
Checkstyle passed. The unchanged captured-partition regression
`testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition` passed.

- Coordinator target/version is network-thread-owned. Request construction captures its version on the same thread.
  Only `CoordinatorRequestManager` compares it before applying invalidation. Node rediscovery, including the same
  node after an unknown interval, creates a new identity. This is not a member epoch or transport correlation ID.
- Regular/share/Streams heartbeat, offset commit/fetch, and Streams topology callbacks pass the captured version.
  A stale invalidation does not mutate the current coordinator; the originating operation still completes or retries
  by its existing policy. Successful older offset-fetch results remain usable within their captured scope.
- The selected admission boundary is the existing **next full pass**, not a new post-I/O owner poll.
  `ConsumerAdmissionContractTest.testCompletionBatchFinishesBeforeReentrantCommitAdmission` runs the real
  network thread, delegate, coordinator and commit manager with `MockClient`. Offset-commit success queues a
  distinct follow-up commit; an offset-fetch error invalidates the coordinator in the same I/O poll. No speculative
  request is staged/sent. Discovery restores the owner, and the following full pass sends the ready commit without
  another timer tick. The legitimate discovery backoff is elapsed before this race starts, not removed.
- This is a commit/offset-fetch completion batch, not the exact heartbeat-plus-commit integration variant.
  That variant and real-broker recovery remain explicit gates. Existing pre-batch admitted requests are retained;
  no newly introduced command-discard or in-flight rollback protocol is claimed.

### Step 3: publication and selected effects

Validation: 630 tests passed, zero failed/skipped/errors, with Checkstyle and SpotBugs passing:
publication contract 4, fetch manager 111, async consumer 128, application processor 46,
fetch collector 155, share collector 132, fetch buffer 5, share buffer 8, share consumer 41.

Two new tests first failed against unchanged step-2 production code:

- `ConsumerPublicationContractTest.testErrorStatePrecedesReconciliationFutureRelease`: a future dependent ran
  inline and found no published error (`NoSuchElementException`). Publish the error and completion state first.
- `FetchRequestManagerTest.testReentrantPreparationRemainsANewOperation`: a request created by a completion
  callback was immediately completed as part of the old preparation. Detach the selected operation before
  completion; reentrant preparation remains pending until its own subsequent manager poll.

`AsyncPollEvent` now accepts a per-event error notification. `AsyncKafkaConsumer` connects it to the existing
latched fetch-buffer wake; successful empty preparation does not use this error notification. Tests cover
error-before-wait, an actual thread already blocked on the buffer, independent error identities, and public
`poll()` surfacing an error injected at wait entry. The public-poll wiring test controls error arrival; it is not
a real-broker metadata-error reproduction. No generic effect queue, retry, or coalescing registry was introduced.

The baseline position-before-drain and no-fetchable-partitions/no-wake tests pass unchanged. FetchBuffer,
ShareFetchBuffer, consumed-position ownership, user wakeup, and existing timeout/cancel/close policies remain.

**Open metadata-delivery gate:** `ConsumerNetworkThread` checks metadata errors at application admission and
against its `CompletableEventReaper` list after I/O. `AsyncPollEvent` is not a `CompletableEvent`. The notification
tests do not prove that every transport metadata error arriving after admission reaches that async operation.
An admission-to-transport-to-wait reproduction is required before claiming KAFKA-20397 fixed end to end.
Do not silently solve this by retaining only the latest operation or by broadcasting errors to unrelated calls.

## Historical issues and acceptance scope

| Evidence | What this experiment must establish | Status |
| --- | --- | --- |
| KAFKA-20253 / PR 22836 | In-flight work must not create a no-progress poll loop. | Existing coordinator regression retained and strengthened with typed cause; exact historical CPU reproduction not performed. |
| KAFKA-17674 / PR 17342 | An older offset operation must retain its admitted partition scope. | Baseline contains the historical fix; captured-scope regression passed in step 2. |
| PR 21476 | Consumed position must precede the matching consumed marker. | Baseline fix and synchronized handoff retained; position-before-drain regression passed in step 3. |
| KAFKA-20854 / PR 23014 | Empty/no-progress fetch preparation must not select a wake. | Baseline classification and no-wakeup regression passed; no credit claimed for re-fixing it. |
| KAFKA-20397 / proposed PR 21991 | Metadata error must be observable before a latched notification, including error-before-wait. | Selected notification/publication path tested; complete transport-to-operation delivery remains open as described above. |

No new throughput, allocation, GC, CPU, real-broker recovery, or tail-latency claim is made.
The old POC's benchmark does not validate this branch. No Jenkins job is submitted by this work.

## Step 4: extraction decision and final verification

The smaller mechanisms are sufficient for the tested paths; these tests do not establish that a generic
event/command/action framework is necessary. No additional framework is extracted in this POC.

The four concerns remain useful review obligations, not four mandatory classes: typed activation does not
supply its own enabling input; single-threaded ownership does not imply captured-scope/version safety;
an immutable schedule does not prove record-delivery acceptance; publication order alone does not latch a wake.
Each obligation needs its own named production path and counterexample.

Final combined validation: **1,253 tests in 23 suites, zero failures/errors/skips**, run twice on the same
code/test tree. The second invocation used `:clients:test --rerun`, not cached test results. Checkstyle and
SpotBugs passed. Test retries were disabled (`maxTestRetries=0`). This is not all Kafka tests or an E2E run.

The final tests additionally cover in-flight A / ready B / backing-off C in the real commit manager:
B is sent without duplicating A or sending C early, and producing B can coexist with a finite retry for C.
A throwing completion dependent also cannot erase a separate reentrant fetch preparation.

Exact combined command (add `--rerun` immediately after `:clients:test` to repeat):

```sh
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home ./gradlew :clients:test \
  --tests '*CommitRequestManagerTest' \
  --tests '*CoordinatorRequestManagerTest' \
  --tests '*ConsumerAdmissionContractTest' \
  --tests '*ConsumerHeartbeatRequestManagerTest' \
  --tests '*ShareHeartbeatRequestManagerTest' \
  --tests '*StreamsGroupHeartbeatRequestManagerTest' \
  --tests '*StreamsGroupTopologyDescriptionRequestManagerTest' \
  --tests '*OffsetsRequestManagerTest' \
  --tests '*ConsumerPublicationContractTest' \
  --tests '*FetchRequestManagerTest' \
  --tests '*AsyncKafkaConsumerTest' \
  --tests '*ApplicationEventProcessorTest' \
  --tests '*FetchCollectorTest' \
  --tests '*FetchBufferTest' \
  --tests '*ShareConsumerImplTest' \
  --tests '*NetworkClientDelegateTest' \
  --tests '*ConsumerNetworkThreadTest' \
  --tests '*ApplicationEventHandlerTest' \
  --tests '*RequestStateTest' \
  --offline --max-workers=2 -PmaxParallelForks=1
```

### Required follow-up before a production-ready claim

1. Complete metadata-error delivery after async-poll admission, not just notification once the error arrives.
2. Add the exact heartbeat-plus-commit completion-batch and real-broker recovery variants. The tested
   commit/offset-fetch batch demonstrates the boundary, not every protocol combination.
3. Migrate and prove enabling inputs for remaining adapters: AbstractHeartbeatRequestManager (regular/share),
   StreamsGroupHeartbeatRequestManager, StreamsGroupTopologyDescriptionRequestManager, CommitRequestManager
   (other/close paths), FetchRequestManager, OffsetsRequestManager, TopicMetadataRequestManager and
   ShareConsumeRequestManager. Retain their existing guards until their replacements are proven.
4. Cover full affected lifecycle and terminal-result paths through the production run loop. Existing close tests
   passing does not prove a new fatal-error or cancellation protocol; none is proposed by this POC.
5. Benchmark this branch independently: throughput, CPU, allocation/GC, tail latency, reconnect and rebalance,
   with a pinned baseline/candidate and predeclared thresholds. No current performance conclusion is available.

Three red-to-green component regressions were observed, but none is labelled a pinned historical-release
reproduction. Previously fixed historical issues remain inherited regression evidence. The implementation
therefore supports a **partial feasibility result**, not a claim that every listed issue is fixed end to end.
