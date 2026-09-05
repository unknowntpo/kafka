# KIP-1371 approach 2: Fetch capability boundary experiment

Status: local follow-up to `36924fc94f` on
`codex/kip-1371-batched-decisions-poc`. No remote, other worktree, public KIP,
or benchmark result is changed by this experiment.

Review order: `eb98da2572` restricts Fetch buffer capabilities; `01b6bacdf7`
isolates preparation observers; the following scheduling-contract test commit
contains this evidence record. No commit in this series has been pushed.

## Question

Can existing Fetch behavior be extended without giving the request manager an
unconditional application-wakeup capability? A direct call to `FetchBuffer.wakeup()`
in the earlier POC demonstrates an open dependency boundary, not an inherent
limitation of approach 2. Narrowing that boundary is a competing implementation
of the shared guarantee, not a replacement for all KIP-1371 contracts.

The forces are developer-local reasoning, preservation of enabling-input
notifications, data-before-notification visibility, compatibility with the shared
classic Fetcher implementation, and avoiding a generic event/action framework
when a smaller boundary suffices.

## Mechanism

- `FetchRequestManager` and `AbstractFetch` no longer have an executable dependency
  on or retain `FetchBuffer` (existing explanatory Javadoc may still refer to it).
  `AbstractFetch` privately retains a final `FetchBufferProducer`; the subclass
  cannot access that field. Construction adapts the buffer outside the manager.
- The producer offers buffer queries, completed-fetch delivery, conditional
  buffered-data notification, and request start/completion bookkeeping. It offers
  no raw buffer getter, consumption/wait API, or unconditional `wakeup()`.
- The existing pending-node set moves into the producer; it is not duplicated.
  A completion removes a registered node before notification. Unknown or already
  removed nodes do not cause an additional completion notification. An empty,
  failed, or session-error response still notifies when it releases an in-flight
  request. This is a genuine state transition, not a renamed unconditional wake.
- `finishFetchPreparation` rechecks actual buffered partitions and fetchability
  before notification. This includes partially consumed `nextInLineFetch`, not
  just queue entries. An empty close/preparation result alone is no longer
  sufficient to notify when there is no available buffered fetch.
- Completed-fetch delivery preserves the existing buffer lock and store-before-
  signal behavior. These notifications are not deferred to the reactor schedule.
- Classic `Fetcher` retains its own private raw buffer for collection and
  partition retention. Both paths retain the existing close chain through
  `AbstractFetch`; the producer delegates buffer closure, and the superclass
  still closes the decompression supplier. Request bookkeeping retains its
  existing thread/monitor confinement.

## Enforced boundary and remaining obligations

The dependency test scans class-file constants for the actual manager and its
superclasses, covering raw-buffer types in signatures and bytecode references,
not merely field declarations. A deliberately invalid fixture proves that the
guard detects reintroduced raw access. Java visibility prevents access through
the former inherited field; the dependency test prevents adding that dependency
back unnoticed during CI.

This is an ordinary extension guard, not a security sandbox or a complete
transitive dependency analysis. A new helper exposing an unconditional callback
would require extending the guard. Domain eligibility, correct response routing,
and safe use of transferred `CompletedFetch` objects still require contract tests.

The pending-node set preserves the existing one-in-flight-request-per-node model.
It is not a request-generation token: a misrouted old completion after a newer
request starts would require attempt identity/fencing. No such new guarantee is
claimed. Nor does a buffered-data recheck freeze application consumption or
assignment; another thread can consume the data before observing the notification.

The experiment does not move share-fetch onto this adapter, stage all futures,
guarantee aggregate-schedule-before-every-effect ordering, or prove throughput.
The inherited admission/ordering POC's evidence remains separate.

## Validation scope

The new unit/component tests cover:

- registered-node removal before notification and duplicate/unknown completion;
- available versus unavailable buffered data, including a partial next-in-line fetch;
- data visible after wait release;
- completion notification before and during wait;
- absence of the manager hierarchy's raw-buffer dependency, plus a negative fixture.

Existing regression suites cover real Fetch manager empty responses, disconnect
failures, session errors, no-fetchable partitions, backoff, paused partitions,
close, reentrant preparation, classic fetching, and application metadata errors.
Tests use in-memory clients/buffers, no broker or persistent external state, and
disable test retries.

Initial buffer-boundary validation on 2026-09-05: **753 tests in 13 suites passed twice**, with zero
failures, errors, or skips and retries disabled. The second run forced the test
task with `--rerun`; it was not an up-to-date test result. Both runs used the same
production/test sources. Eight tests are in the new `FetchBufferProducerTest`.
Checkstyle main/test, Spotless Java, and SpotBugs main passed. This selection is
not the earlier 1,262-test admission POC selection or an all-clients test run.

Using JDK 17 and cached Gradle 9.7.1 (or the installed wrapper), the selection is:

```sh
./gradlew :clients:test --rerun \
  --tests '*FetchBufferProducerTest' \
  --tests '*FetchRequestManagerTest' \
  --tests '*FetcherTest' \
  --tests '*ConsumerPublicationContractTest' \
  --tests '*FetchBufferTest' \
  --tests '*FetchCollectorTest' \
  --tests '*AsyncKafkaConsumerTest' \
  --tests '*ApplicationEventProcessorTest' \
  --tests '*ConsumerBatchedDecisionTest' \
  --offline --max-workers=2 -PmaxParallelForks=1 -PmaxTestRetries=0
```

The suffix selectors also include offset/topic-metadata fetcher and share
buffer/collector suites. Local reports are in `clients/build/reports/tests/test/`
and `clients/build/test-results/test/`; later runs can overwrite them. The bounded
conclusion is that a small capability boundary can remove the current direct
raw-buffer bypass while preserving the selected Fetch behaviors. It does not
establish that a complete coordination framework is unnecessary.

## Follow-up: separate observation from completion authority

A normal extension is to give an individual preparation waiter a timeout or
cancellation policy. The buffer boundary does not protect that operation:
`createFetchRequests()` previously returned the manager's actual pending future
to the first caller. Later callers were chained to that same future. Cancelling,
timing out, or prematurely completing the first returned future therefore settled
the other callers before the manager prepared any work.

Three deterministic tests failed against the initial buffer-capability POC,
before changing future ownership. Each failed because a second caller was
already complete immediately after the first caller changed its own wait.
The timeout test applies `completeExceptionally(TimeoutException)` directly,
modelling the terminal mutation performed by `orTimeout` without scheduler noise.
This is evidence of an extension hazard in the exposed capability, **not evidence
that an existing production caller currently invokes cancellation or timeout on
this future**, and not a claim of reproducing a named historical Kafka issue.

The correction retains one private owner future and relays its result to a new
observer future for every caller, including the first. One observer can end its
own wait without changing the shared preparation or another observer. Pending
work is still detached before owner completion, preserving the earlier reentrant
preparation contract. The relay passes the original exception unchanged because
the application-event path classifies timeout types before unwrapping a
`CompletionException`. A fourth test checks exception identity for both callers.

This is not cancellation of underlying work: even if all observers stop waiting,
the admitted preparation still runs. It adds one owner future and relay for the
first caller, retains observer relays until owner completion, and does not add
queue bounds or make `createFetchRequests()` a thread-safe admission API. No
performance claim is made. Existing call sites and the return type are unchanged.

The architectural conclusion is narrower than “remove every CompletableFuture”:
give callers the ability to observe a result without giving them the ability to
settle shared owner state. This complements removal of unconditional wakeup
authority, but still does not enforce a global order across all manager effects.

Follow-up validation: **757 tests in 13 suites passed twice** on the same final
production/test sources, with zero failures/errors/skips and retries disabled.
The second run forced test execution with `--rerun`. Checkstyle main/test,
Spotless Java, and SpotBugs main passed. The selection above is unchanged; the
increase is the four preparation-observer tests. During development, the new
tests were corrected to use the fixture's existing position-validation path
before fetch preparation; the final tests verify one actual request is prepared,
not merely that two futures eventually finish.

## Follow-up: a cross-manager scheduling obligation

Intent: an application that consumes a valid local notification must not remain
in an obsolete wait when the rest of the I/O batch makes the aggregate deadline
earlier. The forces are timely progress, developer-local reasoning, preservation
of existing data handoffs, and avoiding unchanged-deadline wakeup ping-pong.

The interleaving in `ConsumerNetworkThreadTest.testEarlyFetchWakeAndAggregateSchedule`
is explicit:

1. Two manager contributions publish an aggregate application wait of 30 seconds.
2. A registered fetch request completes through `FetchBufferProducer`, removes its
   in-flight state and notifies the real buffer.
3. An application test thread consumes that notification and reads the old wait
   while the delegate's completion batch has not returned.
4. A later modelled completion changes another manager's contribution to zero.
5. The real loop publishes the new aggregate and issues its scheduling notification.

Four scenarios vary whether the second wait starts before or after publication
and whether the scheduling notification is delivered. With delivery, both waits
return and observe the published zero. With delivery deliberately suppressed,
the thread remains in its old wait despite the published zero, until test cleanup
explicitly wakes it. The negative controls are expected to pass by detecting
this blocked state; they do not disable anything in production. A later deadline
does not generate another scheduling wake. An existing separate test covers
unchanged expired deadlines without repeated scheduling wakes.

This is a component-level ordering experiment: the loop, buffer and producer are
real; manager contributions and network completion dispatch are controlled test
seams. It is not a broker reproduction or the complete `AsyncKafkaConsumer.poll`
path. That path still has retry-backoff clamps and subscription/position checks,
so this experiment does not establish a literal 30-second production hang or
justify removing those checks. Synchronization gates force the selected legal
interleaving; they are not added to production callbacks.

### Alternatives and boundary

| Alternative | What the experiment establishes | Cost or open obligation |
| --- | --- | --- |
| Local completion notification plus silently replacing the aggregate | Insufficient for the stale-wait interleaving in both negative controls. | A reader can already be waiting or can enter a wait using its earlier read. |
| Local handoff plus aggregate publication followed by a latched scheduling notification when the deadline moves earlier | Existing approach 2 mechanism covers both tested wait-entry orders. | One batch can produce both a local wake and a scheduling wake; this is not global wake coalescing. |
| Stage every effect until the batch's aggregate is published | Could offer a stronger uniform observation boundary, but is not implemented or evaluated by this test. | Must preserve errors, timeouts, reentrant operations, data handoff and close semantics; effects are delayed. |

The resulting contract has two distinct obligations. The local owner publishes
the state supporting its local notification. The loop combines manager timing
contributions, publishes the aggregate, and invalidates an obsolete longer wait
with a latched notification. No individual manager must reconstruct other
managers' deadlines. A generic callback notification is **not** a promise that
the entire consumer's aggregate has already been refreshed.

No production change was needed for this follow-up. This supports a narrower
common scheduling contract, not the conclusion that every global effect-ordering
requirement can be removed. Next-poll typing, owner scope/version validation,
cross-owner changes first created during polling, and complete lifecycle behavior
remain separate obligations.

Validation: **807 tests across 17 suites passed twice** on identical final
production/test sources, zero failures/errors/skips, retries disabled, with a
forced `--rerun` on the second execution. The selection is the 757-test selection
above plus `*ConsumerNetworkThreadTest`, `*ConsumerAdmissionContractTest`,
`*NetworkClientDelegateTest`, and `*ApplicationEventHandlerTest`. Four cases are
new; the other additional cases are expanded regression coverage. Checkstyle
main/test and Spotless Java passed; unchanged production sources retain their
previous passing SpotBugs main result (up-to-date in these runs).

## Follow-up: owner changes during manager polling

The next boundary is not a hypothetical manager arbitrarily corrupting its peer.
The regular `RequestManagers` order is coordinator, commit, heartbeat, then
membership and the remaining managers. `AbstractHeartbeatRequestManager.poll`
can detect `max.poll.interval.ms` expiry and call
`membership.transitionToSendingLeaveGroup(true)`. The membership owner updates
its epoch and notifies the registered Commit RM listener. This is a real owner
transition occurring during polling, after Commit RM may already have built an
attempt in the same pass.

`ConsumerBatchedDecisionTest.testLeaveTransitionDoesNotRewriteAnAdmittedCommit`
uses real coordinator, commit, heartbeat, membership and network-delegate code,
with MockClient, controlled metadata, and MockTime. Membership starts in a
controlled joined-epoch setup; advancing time triggers the real heartbeat poll
timeout path. The test evaluates two orders without changing production order:

| Order | Commit request field after both polls | Leave heartbeat field | Observed MockClient submission order |
| --- | --- | --- | --- |
| Existing commit poll, then heartbeat poll | Captured member epoch 7 remains 7. | Member epoch -1. | Commit, leave heartbeat. |
| Comparison: heartbeat poll, then commit poll | Commit uses its now-empty member epoch, encoded as default -1. | Member epoch -1. | Leave heartbeat, commit. |

Both cases reach membership STALE. A supplied successful commit response still
completes the admitted commit operation. This does **not** prove a broker would
accept either sequence, establish processing order at a real broker, endorse a
commit after leave, or prove stale-epoch retry handling. It characterizes request
construction, local transport submission, and preservation of an observed success.
The test executes the production-order manager slice and delegate directly, not
a complete public-consumer or `runOnce` lifecycle.

The important distinction is between an operation already queued and an attempt
already admitted. Changing the poll order changes the request's membership
context even though the operation was queued at the same point in both tests.
Reordering managers or rebuilding staged requests is therefore not a harmless
implementation cleanup.

### Resulting design question

The current approach can preserve sequential admission: owner transitions affect
later admissions, while existing attempts retain their captured context. It does
not provide the stronger rule that **all timer-driven owner transitions in this
pass are applied before any request is admitted**. Completing the I/O callback
batch first does not itself establish that stronger rule, because polling can
create another owner transition.

If the stronger rule is required, the next design step is to specify whether
poll expiry must preempt a same-pass commit and how pending operations retain
their outcomes, then evaluate a separate transition/admission boundary. Do not
silently move heartbeat before commit, discard reserved attempts, or run manager
polls repeatedly to a fixed point. No production defect or need for a new generic
event framework is claimed by this characterization alone.

Validation: **809 tests across 17 suites passed twice**, zero failures/errors/skips,
with retries disabled and a forced `--rerun` for the second execution. This is
the preceding 807-test selection plus the two admission-order cases. Checkstyle
main/test and Spotless Java passed; production sources are unchanged, and their
previous passing SpotBugs main result remains up-to-date. This follow-up changes
only the characterization test and this evidence record.
