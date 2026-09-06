# Original KIP regression provenance

Baseline for inheritance: `820533b870106cc0e0ac60e2076b8644d68bd85f`.
Sources below are local Apache Kafka commit objects and current production/test code.
An inherited commit is established with `git merge-base --is-ancestor`, not by a matching title.
This is a source audit, not a claim that a pre-fix binary was executed in this run.

| Evidence | Baseline ancestor containing the repair | Interpretation |
| --- | --- | --- |
| KAFKA-17066 / PR 16885 | `6744a718c2` | Background position initialization is inherited, not introduced by renaming the loop. |
| KAFKA-17674 / PR 17342 | `1962917436` | Restrict completion to the operation's captured partition scope. |
| KAFKA-18641 / PR 18737 | `709bfc506a` | Initial reconciliation/commit capture must precede collection; retry recapture needs its own proof. |
| PR 21476 (KAFKA-15529) | `5d03ccff57` | Drain exhausted fetch only after position update, so the background observer cannot fetch using the older position after seeing consumed=true. |
| KAFKA-20426 / PR 22018 | `44bafc60e7` | An unsubscribed/manual-assignment heartbeat must not force immediate application polls. |
| KAFKA-20253 / PR 22836 | `28de22de34` | Mirror heartbeat feasibility guards in application waiting, including unavailable coordinator and skipped heartbeat states. |
| KAFKA-20854 / PR 23014 | `7ff5d7b71c` | Repair no-progress fetch wake classification while preserving retry opportunities; not every spin originates in a numeric deadline. |
| KAFKA-18160 / PR 18089 | `0815d70592` | Enqueue the callback acknowledgement even when user callback throws WakeupException or InterruptException. |
| KAFKA-19357 / PR 19914 | `92169b8f08` | When closing with unknown coordinator, fail pending unsent commits instead of leaving their callbacks uncompleted. |
| KAFKA-18569 / PR 18590 | `9dd73d43b0` | Signal coordinator shutdown after dependent close work; do not keep generating unnecessary FindCoordinator requests. |

KAFKA-20970 fixes `cd44c5de0f` / `ec65e09a04` are **not** ancestors of this baseline.
The Streams branch must be checked independently of the POC's regular-commit repair.
The local KAFKA-20397 proposal `f171b1c00e` is also not an ancestor. The POC's current
metadata-delivery tests must establish its own mechanism, not borrow inheritance credit.

## Lifecycle is a pair of obligations, not a universal close barrier

KAFKA-19357's exact historical integration test survives as
`PlaintextConsumerCommitTest.testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose`.
It stops the isolated brokers, queues an asynchronous commit, closes with a 500 ms budget,
and requires exactly one callback containing CommitFailedException (unknown coordinator),
without an excessively long close. The correct outcome here is **failure**, not unlimited
discovery until commit succeeds. The earlier issue inventory's "required work can still use
discovery" was too broad and must not be used to redefine close semantics.

Current `CommitRequestManagerTest.testPollWithClosingAndPendingRequests` directly checks
that failure. `testPollWithFatalErrorDuringCoordinatorIsEmptyAndClosing` separately covers
a stored coordinator fatal error. Both passed in the 3,392-test consumer run at `45e9cc7275`.

For KAFKA-18569, the original source test `testSignalOnClose` illustrates the stop rule:
even after retry backoff expires, no new FindCoordinator request is produced after the
close signal. That exact named test is no longer in the current class; the POC's
`ConsumerBatchedDecisionTest.testPublicCloseOrdersCommitDiscoveryAndRealMembership`
instead verifies public-close event order and no subsequent discovery after stopping,
crossed with coordinator-known/discovery-needed and success/timeout schedules.

## Callback failure must still notify the background owner

The KAFKA-18160 repair catches WakeupException/InterruptException around callback invocation,
builds `ConsumerRebalanceListenerCallbackCompletedEvent`, enqueues it, and only then propagates
the callback error to `poll()`. It does not suppress the error or move the user callback
off the application thread.

Current `AsyncKafkaConsumer.invokeRebalanceCallbacks` preserves the caught exception;
`invokeRebalanceCallbackAndNotifyBackgroundThread` enqueues the acknowledgement before throwing.
The existing `ConsumerMembershipManagerTest.testListenerCallbacksThrowsErrorOnPartitionsRevoked`
and corresponding assigned/lost cases include wakeup, interruption and ordinary errors and
exercise owner-side completion. These are component evidence, not by themselves proof that
the public producer actually enqueues the acknowledgement.

New real-cluster public-poll tests cover both callback exception types. They
require the first exception to be surfaced, reconciliation to retry successfully, and a real
record to be delivered subsequently. This tests progress through the actual background owner.
Both tests passed twice in both isolated-controller and combined-controller KRaft configurations:
**four tests per invocation, zero failures/errors/skips**, retries disabled, a fresh JVM per
test class and fresh cluster per invocation. The fixture constructs InterruptException inside
the callback (its constructor sets the interrupt flag); after observing that exception the
application explicitly clears the interruption before choosing to poll again. An earlier fixture
constructed it before topic setup and failed there; that receipt is retained, not counted as a pass.
This is not a claim about arbitrary OS-thread interruption timing.

Raw passing receipts: `/tmp/kip1371-overnight-evidence.TC1I8d/callback-first-green/` and
`callback-second-green/`. The historical KAFKA-19357 real-broker close regression also passed
in the broader 236-case plaintext invocation at `45e9cc7275`. That overall invocation had one
separate topic-setup heap failure; it is not described as an entirely passing run.
