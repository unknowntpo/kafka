# Contract-Guided Coordination: completion work log

## Authority and preservation

The user authorized autonomous implementation, testing, and design decisions on 2026-09-06 while unavailable.
Complete a local KIP draft with explicit evidence and limitations. Do not publish to upstream, Jira, or Confluence.
This work also keeps new commits local: no remote pushes or new CI submissions without a subsequent request.
Preserve the original KIP and independent worktrees. Starting revision: `45e9cc7275`.
Rollback checkpoint: `checkpoint/kip-1371-design-v0-2026-09-06`.

## Acceptance method

1. Read the entire original KIP and trace every named issue to its actual failure shape and regression evidence.
2. Inventory existing consumer behaviors, including lifecycle, callback acknowledgement, regular/share/Streams
   compositions, and distinguish inherited repairs from new POC mechanisms.
3. Resolve demonstrated correctness gaps with the smallest justified mechanism. Record behavioral tradeoffs;
   neither callback centralization nor an extra manager pass is a correctness proof by itself.
4. Run deterministic regressions without automatic retries, then real isolated broker integration tests.
5. Finish the local KIP Markdown and matching HTML preview, linking pinned receipts and explicit open gates.

Passing the existing suite is regression evidence, not proof of every possible interleaving. Historical benchmark
results for another design/revision are not acceptance evidence for this implementation.

## Receipts

### Full consumer unit namespace at `45e9cc7275`

- JDK: Temurin 17. Gradle: cached 9.7.1. Offline, two workers, one test fork, retries disabled.
- Command: `:clients:test --rerun --tests 'org.apache.kafka.clients.consumer.*' :clients:spotlessJavaCheck`
  with `-PmaxParallelForks=1 -PmaxTestRetries=0 -PtestLoggingEvents=failed`.
- Result: **97 suites, 3,392 tests, zero failures/errors/skips**; build successful in 3m 12s.
- XML source: `clients/build/test-results/test/TEST-*.xml` (ephemeral; subsequent test runs can overwrite it).
- Scope: existing consumer unit/component namespace, including the POC tests. Not a real-broker suite.
- Raw XML archived at `/tmp/kip1371-overnight-evidence.TC1I8d/unit-baseline-45e9/` before later reruns.

### Broad plaintext integration invocation at `45e9cc7275`

Selection: `:clients:clients-integration-tests:test --rerun --tests 'org.apache.kafka.clients.consumer.PlaintextConsumer*'`;
same JDK, offline cache, one fork and zero retries. Production classes were compiled before the snapshot-policy edit.

Result: **236 tests across eight suites: 235 passed, one failed, zero errors/skips**, 16m 51s.
Raw XML archived at `/tmp/kip1371-overnight-evidence.TC1I8d/plaintext-baseline-45e9/`.

Failure: `PlaintextConsumerSubscriptionTest.testAsyncConsumerPatternSubscription()[2]`
timed out in `setup()` while awaiting topic metadata. Its test log contains `OutOfMemoryError: Java heap space`
in `kafka-admin-client-thread | adminclient-162`, allocating a network receive buffer. The build config uses a
3 GiB test heap. This is before that case's consumer assertions, but its deeper cause is **not established**:
do not conclude either a consumer regression or harmless infrastructure flakiness from the stack alone.
Preserve this failure even if isolated reruns pass. Further isolation should use fresh test JVMs per class and
the same heap rather than quietly raising memory or enabling retry.
Raw failure log: `/tmp/kip1371-overnight-evidence.TC1I8d/plaintext-subscription-heap-failure.log`.

## Current work

- Original 1,192-line KIP fully read, including migration targets, lifecycle issues and historical evidence caveats.
- Next: inherited lifecycle regression provenance and real integration coverage; safe rebalance snapshot retry
  decision; complete candidate KIP and evidence index.

## Decision D1: retain admitted pre-rebalance commit offsets

The candidate removes callback-time recapture and the default-off experiment toggle.
`OffsetCommitRequestState.offsets` is now a final reference. Initial safe capture remains
the application-poll checkpoint; identity, coordinator and retry deadlines retain existing rules.
This sacrifices retry freshness to avoid committing not-yet-returned records without adding
another queue or delaying retries until the next application poll. See the updated snapshot audit.
The legacy two-mode controls remain at starting revision `45e9cc7275`; current tests must prove
the selected behavior with normal construction. Verification is pending; no passing receipt is
claimed yet for this change.

D1 completed locally in commit `0829a57bfa`: 415 affected unit/component tests passed twice.
No push. See the snapshot audit for counts and raw receipt paths.

## Decision D2: do not interpret the initial heartbeat interval as useful urgency

New deterministic regular and Streams tests both failed at the unchanged heartbeat implementation:
unknown coordinator + initial heartbeat interval zero still returned application wait zero.
The earlier KAFKA-20253 test used a negotiated positive interval and did not cover startup.
Raw red receipt: `/tmp/kip1371-overnight-evidence.TC1I8d/zero-heartbeat-red/` (417 tests, exactly these two failed).

Candidate correction: mirror heartbeat admission guards in Streams application waiting and, when blocked
with interval zero, retain a positive half-remaining poll-timer refresh bound (minimum 1 ms while live).
Preserve existing negotiated-interval and actual poll-timer-expiry behavior. This adds no timer setting
or global scheduler policy. Shared regular/share tests cover the rounding boundary; discovery recovery
must still produce a real heartbeat request. Validation pending.

## Callback integration fixture correction

The first fresh-JVM follow-up ran six integration cases: two wakeup-callback cases and both previously
failed pattern-subscription configurations passed. Two new interruption cases failed in setup because
constructing `InterruptException` itself sets the calling thread's interrupt flag. This was a test
construction error before topic creation, not consumer behavior. The fixture now constructs the exception
inside the intended callback and explicitly handles/clears interruption before choosing another poll.
Raw receipt: `/tmp/kip1371-overnight-evidence.TC1I8d/callback-fixture-construction-error/`.
The temporary Gradle isolation script also needed `findProject` rather than `project` to tolerate included
builds; the initial script attempt ran no tests. Neither failed invocation is counted as a passing run.

## Local document preview

Complete narrative draft created separately at `kip-1371-contracts-docs/docs/design/kip-1371-contract-guided-coordination.md`.
Pandoc generated a self-contained UTF-8 HTML preview. Plain-text round trips of Markdown and HTML are identical;
there are no embedded old diagrams or external assets. Browser visual inspection is pending: browser-harness
requires the user's remote-debugging approval. The waiting CLI was cancelled, without repeated prompts or
changing browser settings. This does not block code/tests or static document checks.

## Checkpoints and full regression

- `0829a57bfa`: selected safe snapshot policy; 415 affected tests passed twice.
- `6588fa8c4a`: zero-initial-interval heartbeat wait repair; 362 tests passed twice.
- `074cb9dc07`: real callback acknowledgement/recovery proof; four integration cases passed twice.
- DOC branch `3498de3758`: complete replacement narrative and identical generated local HTML.

Full consumer unit and integration namespaces are now running at `074cb9dc07`, offline,
retries disabled, one test fork, with a fresh JVM per integration class. This replaces neither
the earlier failed plaintext receipt nor the need to inspect each failure independently.
Do not edit compiled inputs or run another Gradle invocation against this worktree while that
build is compiling. Preserve reports before any later run overwrites them.

Next independently scoped check: an already in-flight heartbeat with an expired interval.
Existing tests assert no duplicate request but do not necessarily assert a nonzero wait.
Inspect and reproduce that branch before declaring activation coverage complete.

## Full-run finding and decision D3

The full unit run at `074cb9dc07` ran 3,375 cases, with one failure and no automatic retry.
It stopped before integration. The failing BEFORE_REBALANCE / COORDINATOR_LOAD_IN_PROGRESS
case still asserted legacy callback recapture: assignment loss made the recaptured map empty,
so the operation completed without sending anything. D1 intentionally removes that behavior.
The assertion now proves the stronger selected outcome: retry captured offset 10 with current
epoch -1, receive success, then complete. It does not cancel an admitted commit on assignment loss
or pretend that broker rejection cannot occur. Other fatal errors still fail the operation.
Raw reports: `/tmp/kip1371-overnight-evidence.TC1I8d/full-unit-074cb9-failure/`.

Five new regular/share/Streams in-flight heartbeat cases failed before D3: request admission
correctly refused duplicates, but the expired interval still returned zero network wait.
D3 returns `AwaitInput(NETWORK_COMPLETION)` for that blocked result. Application waiting retains
a positive negotiated-interval/poll-timer refresh bound; actual poll timeout and urgent leave
remain actionable. No global scheduler, new queue, or public configuration is introduced.
Raw red reports: `/tmp/kip1371-overnight-evidence.TC1I8d/inflight-heartbeat-red/`.
Two old Streams assertions expected a finite timer while in flight; these now check the typed
completion condition. Their intervening failed receipt is retained separately.

The selected suite now passes, including three new configured-loop cases: in-flight wait and
recovery with/without the extra pass, plus manual-assignment no-spin and membership recovery.
Those use real managers and a real delegate with mocked membership/transport, not real brokers.
Raw first green: `/tmp/kip1371-overnight-evidence.TC1I8d/inflight-first-green/`.
