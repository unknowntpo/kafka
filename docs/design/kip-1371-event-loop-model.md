<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# KIP-1371: Explicit Progress Conditions for the Consumer Event Loop

Status: locally implemented and validated design, not submitted. Baseline: `74fbd500616db4d5ddc490d190e4e642c72cce21`.
The implementation and evidence remain local; this document does not claim an upstream decision or release commitment.

## Intent and forces

A consumer programmer must be able to identify the next enabling input or deadline for every unfinished operation, the owner of its continuation, and the prerequisites for exposing its result. The model must preserve regular consumer, share and Streams behavior without introducing a general runtime where the existing loop suffices. Performance is a secondary goal, evaluated and improved using JMH and profiling after correctness is established.

The tension is between complete progress guarantees and a small model. An explicit condition alone cannot preserve a discarded operation. A retained operation alone cannot detect an unregistered dependency. A completed future alone cannot prove that a dependent observer sees the required state.

## Problem space

| Problem | Required observable outcome | Conflicting forces |
| --- | --- | --- |
| P1: a numeric deadline hides why work is blocked | No self-sustaining execution without a new enabling event, an actual deadline, or local progress | Prompt progress versus bounded idle work |
| P2: dependencies and operation scope are implicit | A producer change makes the dependent work eligible; late results apply only within their still-valid scope | Local manager policy versus cross-manager reasoning |
| P3: completion and observation milestones differ | Each observer sees the state required for the particular effect it consumes | Small handoff mechanisms versus explicit publication guarantees |
| P4: notification races with waiting | Relevant change before or after wait preparation cannot be lost; notification does not imply success | Low latency versus spurious activity |
| P5: shutdown changes the progress obligations | Accepted work completes or fails under its existing close contract, with necessary cleanup even after observer timeout | Bounded close versus retained requests and callbacks |

The issue and use-case inventory is an acceptance input. Already-fixed issues establish required behavior, not novel fixes to claim. Broader lifecycle changes cannot be justified solely by the existence of a scheduling condition.

## Solution space

| Option | What it provides | Cost or unresolved problem |
| --- | --- | --- |
| Keep numeric waits and document conventions | Smallest code change; preserves current behavior | The wait value still loses the enabling reason; new managers can silently depend on periodic calls |
| Explicit conditions inspected by the existing ordered loop | Makes readiness and deadlines inspectable without a new dispatcher; existing queues and retained owners preserve inputs | Scans a small manager list; all enabling transitions must be identified; does not itself own operations |
| Explicit conditions plus subscriptions and a ready queue | Directly enqueues dependent managers | Adds registration replacement, cancellation, queue fairness and lifetime obligations; adopt only if measured costs justify it |
| Continuation task runtime or actor per manager | Can make operation stages separately executable | Adds scheduling and ownership boundaries; existing futures and managers still need domain validity checks |

Initial implementation candidate: explicit conditions in the existing loop, plus narrowly scoped continuation/observation contracts in existing owners. This is a candidate to test, not a declaration that all managers are migrated. No heap, subscription registry, new thread, actor runtime or global effect barrier is selected.

## Definitions

- **Operation:** a domain obligation admitted by an owner. It may require multiple request attempts and manager invocations.
- **Attempt:** one concrete request preparation/send/response lifecycle. Completion of an attempt need not complete its operation.
- **Execution eligibility:** permission to invoke the owner to recheck state. It is not permission to send a request or apply a response.
- **Input:** a relevant owner-state change or completion fact. Re-evaluating an unchanged predicate is not an input.
- **Deadline:** an absolute point on the consumer's time source after which the owner must recheck a named responsibility. A retry deadline, request timeout, application budget and close budget are distinct responsibilities.
- **Continuation:** the next owner step of an unfinished operation. It can be an existing method or future callback; the definition does not require a new task object.
- **Scope:** the partitions, assignment/position identity and attempt context within which a result remains applicable. A scheduling condition is not an operation-scope token.
- **Observation milestone:** the state that must be visible before a particular future, record, error or callback may be observed.
- **Terminal outcome:** the selected success/failure of an operation. Ending an observer's wait does not imply retracting an already-sent request.

## Minimal condition algebra

`NextPollCondition` expresses immediate eligibility, an absolute deadline, quiescence, or either of two conditions. A quiescent condition has no autonomous trigger; it is appropriate only if no work remains or a documented queue, completion, peer-state change or close path creates work.

A condition describes the reason for the next invocation. The loop queries current owner state before invocation and again after the ordered manager pass; it does not retain a subscription for every manager. The owner rechecks protocol eligibility on every invocation. The loop must not infer permission from a particular coordinator value, epoch, or request count.

Existing queues, callbacks and domain state retain inputs; conditions query that authoritative state after application-event processing and network I/O. A deadline OR a retained input supports waiting for a completion while still respecting the operation's budget. An expired condition yields immediate eligibility. The absence of output does not forbid immediate eligibility if a finite local transition made progress. Conversely, an already-completed future must not manufacture a new input just because it was observed again.

The two wait responsibilities remain distinct: when the network thread must act and when the application must recheck or execute its work. Both use the same condition algebra, while separate manager methods preserve their different responsibilities.

## Three required end-to-end cases

1. **Coordinator to heartbeat:** inspect coordinator state, retain the heartbeat's relevant time responsibilities, handle discovery success/failure and membership changes, then recheck normal heartbeat eligibility. A return to the same node address still represents a new discovery outcome where relevant; a condition alone does not fence old responses.
2. **Position to fetch:** retain the valid poll/preparation obligation; distinguish initiation, validation, request creation and record delivery. A synchronous no-op cannot create a self-trigger; a slow partition cannot impose an unrelated all-partition barrier. Late initialization must not start invalidated work.
3. **Close to pending commit:** preserve normal/unknown coordinator and manual/group distinctions. Stop unneeded discovery, but explicitly terminate affected pending commits instead of stranding them. Preserve callback thread and existing success/failure semantics, even when the observer stops waiting first.

## Acceptance evidence and scope

The accompanying acceptance map covers every public Consumer and ShareConsumer entry, all application-event types and
every manager composition. It records the operation owner, enabling inputs, deadlines, continuation, observation,
termination and current-revision evidence. Adversarial interleavings are paired with complete clients-module tests and
local regular, share and Streams broker integration.

Performance evidence compares the unoptimized condition composition with the final algebra under the same two-fork JMH
and GC-profiler configuration. The measured allocation chain justified the deadline specialization; correctness suites
were rerun afterward. CPU reductions from isolated heartbeat wait fixes are not used as proof of the programming model.

The design remains intentionally scoped to the background event-loop model. It does not change public consumer APIs,
wire protocols, configuration names, callback threads or classic-consumer scheduling. A future proposal may split out
an implementation phase only at a boundary whose correctness stands independently.

## Deadline accounting contract

**Problem:** relative wait values can be correct when calculated and stale when used. The old loop passed its turn-start timestamp to every manager, the network poll and the post-network application-wait calculation. Work and network I/O in between could extend a named budget.

**Forces:** preserve the meaning of each existing manager timer; permit non-blocking manager work to consume time; keep network and application responsibilities separate; avoid treating the numeric no-deadline sentinel as a real deadline at `Long.MAX_VALUE`.

**Solution:** sample time at each owner's invocation and have it return an immutable condition anchored to that sample. Calculate the remaining budget immediately before the wait. Post-I/O reaping uses post-I/O time. Quiescence is represented directly rather than overloading `Long.MAX_VALUE`; an explicit `at(Long.MAX_VALUE)` remains an actual deadline. This distinction is required at the representable time boundary.

**Alternatives:** resampling only before network poll still leaves earlier relative budgets unaccounted; subtracting the full turn duration from all managers incorrectly charges later managers for time before their own calculation. A timer thread would add a scheduling mechanism without removing the accounting obligation.

The final implementation queries each manager before invocation and again before the network wait. Input coverage and
operation contracts are recorded in the manager catalog and use-case map. Condition composition was measured and then
specialized only for the allocation cost shown by JMH; deadline accounting itself is a correctness change, not a
performance claim.

Application deadline publication uses a volatile reference to an immutable, time-only condition. The application calculates remaining time when it reads that reference, so a delayed read cannot renew the network thread's old budget. An expired application deadline is a request to recheck; application poll processing discharges it and the network thread publishes the next composed condition. Deadline accounting by itself does not prove that repeated zero application waits are productive, so each ready application condition must name the action it enables.

## Failure publication contract

**Problem:** a processing exception can escape before an `AsyncPollEvent` publishes its reconciliation milestone. The loop's general catch previously completed only `CompletableEvent` instances, while `AsyncPollEvent` is a separate event type. Also, its exceptional completion released the reconciliation future before writing its error, permitting a released observer to see no error.

**Forces:** preserve non-blocking poll stages and the existing application-side error path; release waiters on failure; do not turn reconciliation into a barrier for all position initialization or record delivery.

**Solution:** the event publishes error and terminal state before completing its fallback reconciliation future. The loop routes processing failures to this event's exceptional completion. A deterministic waiter attached before dispatch asserts the error at release, exercising the actual loop catch path. The normal reconciliation milestone still completes immediately after the required revocation state is installed; it does not wait for the whole poll preparation operation.

**Alternatives:** logging alone strands the observer; completing the future exceptionally would change the existing error delivery path; waiting for all poll stages would over-constrain record availability. Late-continuation validity and first-terminal-result arbitration are handled by the terminal poll-event contract below.

## Readiness query versus retained notification state

The manager inventory exposes a simpler candidate than adding signals to every existing producer.

**Problem:** eligibility depends on state that already lives in managers, request states, subscription state and pending queues. Adding a second generation to every producer duplicates the responsibility to record each transition. An omitted publication can strand work even though its authoritative state is ready.

**Forces:** conditions must see changes made by a later manager in the same turn; future callbacks and application events already enter the network thread; repeated inspection must not perform domain work; a small manager list permits a bounded scan; expensive domain preparation should run only when eligible.

**Solution:** query each manager's current `NextPollCondition` immediately before deciding whether to invoke it, and query again after the ordered manager pass to choose the network wait. The query may read existing owner and peer state and time, but must not consume an error, complete an operation, publish a callback, create a request, or advance a membership transition. `poll` remains the owner of those actions. Application events are processed before this pass; transport response callbacks finish before the network delegate returns, so their new state participates in the next pass. Existing queue/wakeup behavior remains responsible for cross-thread events arriving while the network waits.

The second scan is necessary: a later membership manager may enable an earlier heartbeat manager. The loop then uses a zero network wait and re-enters the ordered pass. It does not synchronously recurse into a peer. An eligibility predicate is not a send predicate: for example, an unknown-coordinator fatal error may make heartbeat processing eligible without allowing a heartbeat request.

**Alternatives:** owner-published generations avoid some recomputation, but require complete publication coverage and condition replacement rules. A global change generation still requires every meaningful mutation to publish and invalidates unrelated work. Direct readiness queries reuse authoritative state; the completed implementation therefore omits a generation/signal primitive. Retained notification state remains appropriate only inside a domain owner, such as ShareConsume's pending-input bit, when a concrete transition cannot be reconstructed safely.

Each query covers the local transitions, fatal-error propagation, retries, deadlines and close actions listed in the manager catalog. Fetch demand retention and position continuation scope remain separate operation contracts; recomputing a condition cannot recreate a discarded operation. PollResult now contains requests only, and application-side rechecks also use explicit conditions.

## Coordinator-dependent processing

**Problem:** a send-only predicate would suppress the code that reports a coordinator error or completes an unsendable leave. Conversely, an expired heartbeat interval is not permission to send a second in-flight heartbeat.

**Forces:** preserve existing leave exceptions and fatal-error ownership; retain the max-poll responsibility while awaiting a heartbeat response; inspect eligibility without consuming the error that processing must handle.

**Solution:** the conditions use the coordinator’s existing non-consuming fatal-error accessor. Heartbeat conditions permit processing an unresolved fatal error or a LEAVING transition even when no coordinator exists. Otherwise, blocked group states wait for changes to the authoritative state. With a known coordinator and an active member, a pending heartbeat suppresses the heartbeat-interval deadline while retaining the existing max-poll deadline. The existing explicit leave override remains eligible. Streams keeps its separate skipped-leave transition and static/dynamic membership policy.

Commit conditions similarly distinguish error handling and close from sending. Normal pending requests retain their backoff. Retried commits retain the expiration that `poll` already processes; initial admitted attempts retain the existing zero-budget allowance. Auto-commit initiation still belongs to the application poll stage and must receive its own application-side condition; the network condition does not invent that action.

Topology push requires both retry backoff and broker throttle to have elapsed, so the next deadline is the later of those two. Missing member, coordinator or description, and an in-flight push, are input waits rather than permanently expired timer waits.

**Alternatives:** consuming a fatal error while querying loses it before processing; treating every LEAVING state as idle strands its completion; taking the earlier throttle/backoff deadline repeatedly invokes an owner that still cannot act. Returning the full heartbeat interval while a request is in flight does not express the independent max-poll obligation.

The final source verifies these queries together with membership reconciliation, share acknowledgements,
application-side conditions and terminal continuation behavior in the complete clients and broker suites.

## A failed reconciliation has an owner and a scope

**Problem:** after an assignment callback fails, the network thread must not recreate the same callback event on every turn. The application contract nevertheless retries that reconciliation from the next application `poll`. Treating the failed assignment as permanently ineligible prevents the consumer from ever fetching its assignment after a transient listener failure.

**Forces:** preserve callback error delivery and the broker-driven membership state; retain the next-application-poll retry contract; prevent retries generated solely by network turns; allow a genuinely new target to proceed; do not let a late callback from an old session disable a new session; avoid a generic continuation runtime.

**Solution:** the membership owner retains the failed target assignment. A network-thread reconciliation pass treats that same target as idle, while the next application-poll reconciliation may retry it. A different target remains eligible after the previous attempt completes. A successful retry and existing session-reset paths clear the failed target, and the existing rejoined-during-reconciliation guard prevents an old callback from recording a failure against the new session. The callback records the target captured when the operation began, not whatever target happens to be current when the callback finishes.

**Alternatives:** an unscoped failed boolean would block a new target; suppressing both network and application attempts breaks the public retry behavior; recording the current target at callback completion could blame a newer target for an older failure. A new generation counter is unnecessary where the existing rejoin guard already distinguishes the relevant lifecycle boundary.

The corrected tests prove that repeated network turns do not enqueue the same failed callback, the next application
poll starts one retry, a new target can proceed, and the same assignment works after rejoin. Regular/share
reconciliation tests additionally resolve partial metadata and associate a failure with the full captured target
rather than its resolved projection. Final-source Streams verification includes
383 consumer-boundary unit tests and 18 broker integration tests: topology push/dedup/permanent failure/expiration,
plugin absence, group deletion, classic-to-Streams migration, close options and static-member leave behavior. All pass
with zero failures, errors or skips.


## Share session input retention

**Problem:** ShareFetch preparation mutates session membership, acknowledgement buffers and the record-limit node selection. Reusing preparation as a readiness query would perform domain work while inspecting eligibility. Duplicating its entire planning algorithm would create another large definition to maintain.

**Forces:** preserve async-before-sync-before-close acknowledgement ordering; allow a free node to retry while another node has a request in flight; retain input arriving during processing; avoid a second scheduler or a parallel session model.

**Solution:** ShareConsume retains one network-thread-owned pending-input bit. Commands, member-ID/assignment changes, metadata updates and request completion handlers set it. `poll` consumes it at entry, so an input arriving during the pass remains pending. Its query also computes independent retry/expiration conditions for eligible acknowledgement work and permits the local cleanup that exposes a later close step. Pending transport work continues to belong to the network delegate. A response or command is a new input; repeatedly inspecting an unchanged session is not.

**Alternatives:** an always-ready condition would spin on blocked or empty session work; a global pending-request gate would hide another node's retry; resetting the bit on return would discard reentrant input; a duplicate pure session planner adds policy duplication without evidence of need. This local input memory is deliberately limited to the owner that needs it, while simpler owners directly query their existing state.

The ShareConsume tests use the same real object for metadata registration and polling. A copying Mockito spy otherwise registers a listener against the original instance while polling the copy, invalidating a callback-routing test. Only the separate close-idempotence test installs a spy to count close calls.

## Selective loop integration

All production manager implementations now provide `nextPollCondition`; the default unconditional implementation has been removed. The loop invokes only eligible managers, preserving their existing order, then re-queries every condition before network polling. A later manager can therefore make an earlier manager eligible for the next turn without waiting for the fallback timeout. Tests exercise this with local progress that produces no requests and with an application event that enables a previously quiescent owner.

The obsolete numeric fields and constructors in `PollResult` are removed; it contains outgoing requests only.
Application waits use the same immutable condition language. The continuation, scope, close, public-consumer and JMH
evidence is recorded in the contracts below and in the verification index.

## Terminal poll events stop later stages

**Problem:** metadata errors may finish an AsyncPollEvent while its position lookup or fetch preparation is still pending. The old callback chain could subsequently start fetch preparation or replace the original error, although the logical poll had already ended.

**Forces:** preserve the first terminal outcome and publish it before waking reconciliation waiters; prevent late callbacks from starting later stages; preserve the existing timeout policy until separately validated; avoid a generic operation framework.

**Solution:** terminal completion methods arbitrate once, publishing error and completion before notifying waiters. The event processor skips an already completed event and stops each asynchronous continuation if the event has ended. This does not cancel shared underlying offset work, which retains its own subscription validity checks. Tests cover a late successful position result, late fetch failure, and metadata failure after success.

**Alternatives:** checking only callback errors still allows a successful late callback to start fetch. Overwriting terminal error makes the public result depend on unrelated callback arrival order. Cancelling shared offset work would conflate poll-event ownership with the lifetime of a reusable request.

## Completion cannot consume the next command

**Problem:** a fetch-preparation completion callback can submit another preparation command. The previous owner left its completed future in the pending slot until a `finally` block ran. Reentrant demand was chained to that completed future and reported complete without another preparation pass. The local probe reproduced this: the second future was already complete where it had to remain pending.

**Forces:** coalesce commands that arrive before an attempt starts; distinguish commands arriving while completion is delivered; preserve the existing broker-fetch lifecycle; avoid an extra queue when a single pending preparation slot suffices.

**Solution:** detach the current preparation future before work and completion delivery. Complete that captured future. A new command fills the now-empty pending slot and makes the next manager pass eligible. Do not clear the slot on return, as it may now belong to a newer command.

**Alternatives:** clearing the slot in `finally` loses reentrant input. Completing a newly queued command with the old result claims work that was never performed. Allocating a general command queue is unnecessary because concurrent preparation demand can still coalesce within each pending attempt.

## Pending request slots belong to a particular attempt

**Problem:** a committed-offset lookup for partition set A may still be running when set B starts another lookup. Completing A unconditionally cleared the owner's pending slot. The next update for B then submitted a duplicate lookup instead of reusing B. A local regression probe observed two B submissions where one was expected. Attaching a callback before publishing its pending state also left an already-completed source stored as pending.

**Forces:** keep B reusable while A completes; support immediately completed source futures; ensure shared waiters resume only after offsets are applied; retain the existing result validity filters without claiming they prove reassignment/seek scope correctness.

**Solution:** publish the request object before attaching the completion callback. Clear the pending slot only when it still contains that same object. Store the completion of offset application as the shared result, preserving ordering for reused callers. Snapshot the requested partition set. This uses object identity for ownership of one pending slot; it does not introduce a global generation system.

**Alternatives:** an unconditional clear loses another attempt's state. Comparing only the requested set confuses two attempts for the same set. Publishing after attaching a callback resurrects an already-completed operation. Sharing the raw broker response would let waiters resume before the subscription state was refreshed.

## Initialization validity follows each partition lifetime

**Problem:** a real SubscriptionState probe assigned A and B, started a committed-offset lookup, removed A, then re-added A while preserving B. The old response installed offset 10 on the new A. Matching only topic-partition names and the current initializing flag did not distinguish the new assignment lifetime.

**Forces:** reject work for removed and re-added partitions; keep the response useful for unaffected B; preserve explicit seek/reset decisions; prevent the old continuation from resetting a new partition that has not fetched its own committed offset; avoid invalidating an entire assignment because one partition changed.

**Solution:** SubscriptionState captures a predicate over the existing state-object identity of each initializing partition. The predicate requires that exact object to remain assigned. Each operation still checks its own state transition: committed-offset application requires the partition to remain initializing, while fallback reset may carry the same lifetime after initialization changes to awaiting reset. Offset initialization carries this predicate through committed-offset application, the fallback reset step, and pending-request reuse. Removing a partition destroys that identity; keeping it in an updated assignment retains it. No new global counter or generic operation runtime is needed for this initialization stage.

**Alternatives:** the partition-name set alone admits the reproduced stale response. A global assignment generation would discard B's valid progress along with A. A wakeup generation identifies an input, not the lifetime of the partition to which an offset may be applied. Position validation and other continuations therefore use their own validity proofs below.

The initialization regression additionally exercises issuing a new lookup for the same partition names before the old response arrives. The new A must remain pending, the old response may initialize unchanged B, and the new response must initialize A without overwriting B. A separate missing-offset case checks that the old fallback resets B but leaves the new A initializing. These checks passed in the recorded broad regression run: 2,572 consumer-internals tests, with zero failures, errors or skips; Checkstyle, Spotless and SpotBugsMain also passed. Assignment, seek, predicate evaluation and response application are serialized through the AsyncKafkaConsumer network thread, so this design does not add a second cross-thread transaction protocol.

## Background input wakes the application wait

**Problem:** BackgroundEventHandler published callbacks and errors to its queue without notifying the application thread. A consumer already waiting for fetch data could therefore delay handling the event until a numeric wait bound expired, even though application work was available.

**Forces:** publish the payload before notification; retain an input arriving between the application queue check and parking; preserve all queued callbacks and errors even when notifications coalesce; use the existing synchronization mechanism in regular and share fetch buffers; avoid a second signal or waiter framework.

**Solution:** require the handler to receive an application wakeup action. Both consumer implementations supply their existing fetch-buffer wakeup in every constructor. After successful enqueue, the handler invokes that action. The buffer retains a wakeup that arrives before waiting, and signals a thread already parked. A wakeup conveys eligibility to inspect the queue; the queue remains the owner of event payloads.

**Alternatives:** notifying before enqueue allows the application to wake and observe no input. Queue publication alone cannot wake a thread parked on the fetch buffer. An optional no-op production constructor would silently preserve the missing-notification path, so callers supply the action explicitly. This change covers BackgroundEvent delivery; acknowledgement queues and completion-only paths still require separate coverage.

Tests exercise real regular and share buffers, both an event published before waiting and an event published after observing the waiting thread parked. They assert the event is visible when notification runs, the wait returns without any fetch data, and the original event remains available for delivery. The broad regression passed 2,576 tests with zero failures, errors or skips, including the four wakeup scenarios; Checkstyle, Spotless and SpotBugsMain also passed. Completion-only notifications and acknowledgement delivery are covered by the following contracts.

## Completion queues preserve their observation boundary

**Problem:** regular-consumer commit callbacks and share acknowledgement events have queues separate from BackgroundEventHandler, but they do not share one observation contract. Regular commit completion is application work that should interrupt a fetch wait. Share acknowledgement callbacks are observed at the established share-poll boundaries; interrupting the current poll changes whether that poll may return newly fetched records and when a callback-triggered `wakeup()` is raised.

**Forces:** preserve callback payloads, errors and FIFO ordering; keep user callbacks on the application thread; retain existing share acknowledgement, renewal and callback-wakeup behavior; notify only when the relevant contract requires observation during the current wait; avoid a new event bus or synchronous execution on the producer thread.

**Solution:** the regular commit callback owner receives the existing application-buffer wakeup action. Successful publication precedes notification, and commit interceptors notify only if a non-empty interceptor set caused a task to be queued. Notification does not drain the queue; the application retains its existing callback execution path. Share acknowledgement completion remains retained in its queue and is drained at the existing application-poll boundaries without waking the active fetch wait.

**Alternatives:** notifying for empty interceptor sets manufactures application work. Executing callbacks while notifying changes their thread and exception contract. Waking for share acknowledgement completion can make the same poll continue into new fetches after sending acknowledgements and can surface a callback-triggered wakeup one poll too early. Waking on every successful fetch-preparation completion is also invalid: an empty preparation can cause the application to submit another empty preparation, producing a self-sustaining loop. Terminal poll errors use the distinct notification path below.

Tests retain existing callback ordering checks, verify that empty interceptors do not notify, and exercise regular commit publication before and after a real buffer wait. The commit test verifies both the error payload and execution on the waiting application thread. Share integration tests verify callback-triggered wakeup ordering and the renewal poll that sends acknowledgements without returning the next batch early. Broad verification is recorded separately.

## Terminal poll errors wake their observer

**Problem:** AsyncPollEvent publishes errors through its own fields, rather than a background-event queue. An error arriving while the application waits for fetch data did not wake that wait. A public-poll regression observed no buffer wakeup when the captured poll event failed during the wait.

**Forces:** expose the first terminal error promptly; publish error/completion state before notification; preserve reconciliation waiter release; retain an input arriving before parking; avoid waking for successful no-op preparation, which can self-trigger another preparation; avoid a generic completion bus.

**Solution:** the poll event receives the existing application-buffer wakeup action. Its first exceptional completion publishes the error and terminal state, releases reconciliation waiters, then wakes the application. The existing terminal guard prevents duplicate notification or error replacement. Successful preparation does not notify this path. Metadata-error completion uses the same exceptional path.

**Alternatives:** waking every completion allows empty preparation to trigger the next empty preparation. Notifying before publishing state allows the observer to see no error. Routing a second error payload through another queue introduces duplicate ownership when the poll event already owns the terminal outcome.

The tests cover first-terminal behavior, absence of success/late-error notifications, direct and metadata errors before/after a real buffer wait, and public poll reporting the same error. This completes the error notification path, subject to the recorded regression result; it does not prove all poll expiration or shared-operation validity rules.

Terminal-error verification passed 2,588 tests across 75 consumer-internals suites, with zero failures/errors/skips; Checkstyle, Spotless and SpotBugsMain passed. This covers direct/metadata errors before or after a buffer wait and public-poll error propagation, while excluding successful no-op completion from notification.

## One scheduling contract per network owner

**Problem:** after the loop adopted NextPollCondition, PollResult still carried a second numeric wait value. It was ignored in both normal and close request staging, yet managers continued computing it and tests continued asserting it. This left two apparent scheduling contracts for future changes to maintain.

**Forces:** expose the actual condition used by the loop; retain heartbeat, retry and poll-expiration responsibilities; preserve outgoing request batches and close staging; avoid keeping a compatibility adapter that no caller consumes; do not infer performance improvement from removing unused work.

**Solution:** PollResult contains only its request batch. The numeric field, sentinel and numeric constructors are removed, and delegate staging returns void. Managers stop computing the obsolete report, including CommitRequestManager's scan used only for that field. Eligibility and network waiting continue to use NextPollCondition. Application waiting remains a separate migration obligation.

Tests now assert coordinator quiescence and commit retry boundaries through NextPollCondition. Regular/share heartbeat tests retain request-in-flight and response/timer checks. Streams request-generation tests assert send-attempt state transitions rather than echoing a mocked numeric report, while non-send tests query the relevant conditions. A real Streams heartbeat-state test verifies that an in-flight request does not spin on the interval and that the poll deadline continues to decrease and expire. Network staging tests verify actual queued requests; close tests retain outgoing request assertions.

Request-batch migration verification passed 2,589 consumer-internals tests across 75 suites, zero failures/errors/skips; Checkstyle, Spotless and SpotBugsMain passed. The numeric PollResult interface has no remaining source references. Application waiting and operation continuations use the contracts below.

## Offset reset retains one scoped continuation

**Context:** resetting a position may need metadata before a request can be built, and a broker response may require both refreshed metadata and retry backoff. Application polls can revisit position initialization while that work remains unfinished.

**Problem:** the previous reset helper completed when no request could be built and delegated retriable responses to a later application poll. Retaining the operation without also coalescing callers would instead create duplicate owners whenever the same partition remained visible. A response addressed only by topic-partition name could also update a removed and re-added partition.

**Forces:** keep an unfinished operation alive across metadata and backoff waits; allow unchanged partitions to make progress when a peer is reassigned; preserve reset-strategy checks and cached terminal-error behavior; retain a metadata update that races with an in-flight response; expose the actual retry deadline through `NextPollCondition`; avoid a global operation scheduler or assignment generation.

**Solution:** each reset operation owns its strategy snapshot, remaining partitions, assignment-lifetime predicate, result, in-flight count, metadata wait and retry deadline. Unknown-leader and retriable partitions remain with that owner. A metadata update either releases a waiting owner or is recorded while requests are in flight, so the final response cannot erase it. The condition is ready for staged requests and otherwise exposes a due retry only after metadata is available. Repeated callers join an existing owner when partition lifetime and strategy still match; unowned partitions receive a new owner. Response application filters each partition through the captured state-object identity, so removing and re-adding A rejects A's old response while unchanged B can still advance.

**Alternatives:** completing an empty request batch loses the continuation. Depending on another application poll makes progress accidental. Keeping only a pending boolean cannot express metadata-versus-backoff ordering or partition scope. A global assignment generation invalidates unaffected partitions. Starting a fresh aggregate for every caller duplicates requests while metadata is unavailable.

The local regressions cover missing leader with a repeated caller, retry after metadata-before-response and response-before-metadata orderings, the exact backoff boundary, and A/B reassignment with a replacement request for new A. The full consumer-internals run passed 2,592 tests across 75 suites with zero failures, errors or skips; Checkstyle, Spotless and SpotBugsMain passed. This establishes reset continuation and assignment scope. Position validation, remaining operation families, application-wait migration, close behavior, broker integration and performance acceptance remain separate requirements.

## Position validation retains dependency and response scope

**Context:** leader-epoch validation begins with a position snapshot, then may wait for a leader, API-version discovery, retry backoff or a broker response. Metadata can change before or during the request, and a partition can be removed and re-added with an equal-valued position.

**Problem:** the former fire-and-forget pass discarded missing-leader and missing-API-version work after requesting metadata or connection progress. Retriable responses only marked subscription backoff and depended on a later application poll to recreate the operation. Its response-side position equality check rejected many stale responses, but equal values on a new assignment lifetime remained indistinguishable, and a stale fatal error could still poison the new lifetime.

**Forces:** retain actual dependency state without polling an unchanged owner; keep metadata-before-response and response-before-metadata equivalent; preserve the exact position snapshot and unaffected-partition progress; prevent stale success and stale failure effects; allow explicit application processing to recheck newly available API versions; avoid one global generation or scheduler.

**Solution:** a validation owner retains the original position map, per-partition assignment-lifetime predicate, remaining work, response count, metadata wait and retry deadline. Metadata processing refreshes current validation positions, retires invalid waiting snapshots, creates owners for new snapshots and records updates that arrive while a response is in flight. Missing API versions initiate connection progress and use a finite recheck deadline. Retriable partitions stay with the same owner until both metadata and backoff permit another request. Response payloads and errors are filtered by both state-object lifetime and exact position before they reach validation effects; an entirely stale fatal response has no current recipient and is discarded. Repeated processing joins a matching owner rather than duplicating it.

**Alternatives:** a fire-and-forget pass makes continuation depend on another public poll. Always-ready rechecks spin while metadata or connection state is unchanged. Position value equality alone admits remove/re-add ABA. A global assignment generation discards a valid response for unchanged B when only A changes. Treating any metadata callback as immediate retry violates broker backoff; ignoring metadata received in flight can strand a ready retry.

Tests cover coalesced missing-leader work, automatic replacement after metadata supplies a leader, API-version recovery, both metadata/response retry orderings at the exact deadline, real A/B reassignment with equal-valued new A, and suppression of stale fatal errors for both reset and validation. The broad run passed 2,598 tests across 75 suites with zero failures, errors or skips; Checkstyle, Spotless and SpotBugsMain passed. Application waiting, fetch transport continuation, close, broker integration and performance are verified by the later contracts and final evidence index.

## Application waits use the same condition language

**Problem:** after network scheduling moved to `NextPollCondition`, request managers still returned a numeric application delay. The network thread converted and cached that value, and consumers aged the cached relative delay later. This left two scheduling languages and made quiescence indistinguishable from a deadline at the largest representable time.

**Forces:** application-visible conditions cross a thread boundary and therefore must be immutable and time-only; a delayed read must not renew an old budget; in-flight fetch responses already provide their own wakeup; existing public poll timeout behavior must remain compatible; no new cross-thread signal should be introduced without a retained payload.

**Solution:** each manager returns an immutable application `NextPollCondition` anchored to the network-thread sample. The network thread OR-composes and publishes the condition; regular and share consumers compute its remaining time when they read it. Heartbeat poll deadlines, commit timers, fetch rechecks and Streams topology deadlines retain their existing owners. Fetch is quiescent while a broker request is in flight because its completion wakes the existing fetch buffer; otherwise its retry backoff bounds the application wait.

**Alternatives:** retaining the numeric adapter preserves duplicate semantics and sentinel ambiguity. Publishing a relative duration lets delayed readers extend the original deadline. Notifying every successful preparation creates a self-sustaining empty-fetch loop. A live condition that reads request-manager state from the application thread violates manager confinement.

The obsolete application numeric endpoints and the unused signal prototype were removed. The subsequent full consumer-internals run, including the position-intent changes below, passed 2,596 tests across 75 suites with zero failures, errors or skips; Checkstyle, Spotless and SpotBugsMain passed.

## Position operations require an intent boundary

**Problem:** assignment-object identity rejects remove-and-readd races but does not distinguish two position operations in the same partition lifetime. A deterministic probe started an EARLIEST reset, sought to 99, requested EARLIEST again, and observed the new call join the old in-flight owner. The old response could therefore satisfy a newer, equal-valued reset. A second probe returned validation to the exact same structural position after an intervening seek and exposed the same ABA shape. Separately, a reset owner waiting for metadata stayed permanently idle after its only partition was revoked because no later metadata event was guaranteed to arrive.

**Forces:** fence only the changed partition; keep unaffected partitions from a multi-partition response useful; distinguish a new seek/reset intent from internal stages of the same initialization; keep eligibility queries read-only; allow an invalid metadata-waiting owner to terminate without an unrelated input; avoid a global assignment or wakeup generation.

**Solution:** each assigned partition state carries a monotonic position-intent version. Explicit seek and reset entry points advance it. Initialization's internal committed-offset-to-reset fallback and response application do not, so one admitted operation keeps its scope across its own stages. Reset and validation owners capture both state-object identity and intent version, and every response is filtered per partition before applying effects. A newer intent therefore receives a new owner even when strategy and final position equal the older operation. For metadata-waiting owners, the pure condition query detects an invalid remaining scope and returns ready; `poll` performs the pruning and completes an empty owner.

**Alternatives:** strategy or position equality admits the reproduced ABA. State-object identity alone covers reassignment but not same-lifetime intent changes. Incrementing on every internal transition would invalidate the initialization continuation that the version is intended to protect. A global counter would discard valid progress for unaffected partitions. Mutating the owner from `nextPollCondition` would make observation consume lifecycle state.

The two reset probes failed before this mechanism and all three reset/validation probes pass afterward. Full targeted offset/subscription coverage passed 122 tests. The broad run passed 2,596 tests across 75 consumer-internals suites with zero failures, errors or skips; Checkstyle, Spotless and SpotBugsMain passed. OffsetFetch initialization cannot return to `INITIALIZING` through seek or reset on the same state object: those transitions enter `FETCHING` or `AWAIT_RESET`; remove/re-add already changes object identity. Its internal committed-offset application therefore retains the initialization-lifetime scope rather than opening a new intent.

## Close establishes the terminal phase before collecting work

**Problem:** the application normally enqueues commit-close and stop-discovery events before stopping the network thread. With an exhausted close budget, cleanup can begin before those events are processed. A deterministic probe left a commit pending with an unknown coordinator and called the network thread's close collector directly; the commit future remained incomplete because the collector only called `pollOnClose`, while CommitRequestManager's terminal logic was reachable only after a separate `signalClose` plus normal `poll`.

**Forces:** every admitted commit must terminate; known-coordinator commits should still be staged within the close budget; coordinator discovery must stop before dependent close work is collected; zero-timeout close cannot depend on application-event processing; normal and forced close should share domain error semantics; all owners must observe the close phase before any owner is asked for final work.

**Solution:** the close collector first calls `signalClose` on every request manager, then performs the existing ordered `pollOnClose` pass. CommitRequestManager's close hook delegates to its normal poll after entering close: a known coordinator drains pending commits into the final request batch, while an unknown coordinator completes them with the existing `CommitFailedException`. Repeated signals only set existing close state and are idempotent.

**Alternatives:** relying on queued close events leaves a zero-budget race. Closing managers one at a time before signaling peers lets earlier dependent owners observe a coordinator that has not entered its close phase. Reimplementing commit failure rules in network cleanup duplicates domain policy. Silently dropping pending futures makes callback and waiter outcomes depend on timing.

The direct network-close probe failed with an incomplete future before the change and passes afterward. Tests also cover staging exactly one known-coordinator commit and verify that every owner receives the close signal before the first close poll. CommitRequestManager and ConsumerNetworkThread targeted suites passed 175 tests. The broad run passed 2,599 tests across 75 suites with zero failures, errors or skips; Checkstyle, Spotless and SpotBugsMain passed.

## Fetch continuation is independent per node

**Problem:** fetch preparation is requested by the application poll loop, while broker responses wake its fetch buffer. Treating any in-flight fetch as sufficient reason for an unbounded application wait assumes that every other partition depends on that same response. A deterministic two-node probe sent A, left B in reconnect backoff, and observed an unbounded application condition. B's backoff could expire without an input that woke the application to request another preparation.

**Forces:** avoid periodic application wakeups when every fetchable candidate is already represented by an in-flight request; retain a finite recheck for a free or reconnecting peer node; preserve one request per node and existing fetch-session rules; consider preferred replicas without expiring their lease during a pure scheduling query; do not retain a second copy of fetch request data.

**Solution:** the application condition inspects fetchable, unbuffered partitions per node. It remains idle only when each candidate leader and currently recorded preferred replica already has an in-flight request whose completion will wake the buffer. If a leader is unknown or any candidate node lacks an in-flight request, the existing fetch retry backoff remains as an application recheck. The query peeks at the preferred-replica identifier without expiring or clearing it; actual preparation remains the sole owner of lease expiry, metadata requests, availability checks and request construction.

**Alternatives:** a global in-flight bit admits the reproduced starvation. Always retaining the retry timer wakes a single-node consumer repeatedly while its request is legitimately in flight. Calling full fetch preparation from the query mutates metrics, metadata and fetch sessions. A per-node continuation queue duplicates data already derivable from subscription, buffer and in-flight-node state.

The corrected probe shows A in flight while B retains a 100 ms application recheck; after B's 500 ms reconnect backoff, preparation sends B independently, then becomes idle once both nodes are represented. Fetch, subscription and AsyncKafkaConsumer targeted suites passed 308 tests. The broad run passed 2,600 tests across 75 consumer-internals suites with zero failures, errors or skips; Checkstyle, Spotless and SpotBugsMain passed.

## Timer-backed conditions advance at observation time

**Context:** a heartbeat owner stores its interval and retry timing in Kafka's `Timer`. The former unconditional
manager poll happened to update that timer before any caller used the reported delay. Under selective polling, the
condition query is the first place that observes the timer while the owner is otherwise inactive.

**Problem:** `timeToNextHeartbeatMs(currentTimeMs)` accepted the current time but read the timer without updating it.
The published condition therefore retained the original 5-second delay forever. Six asynchronous broker tests hung
in auto-commit rebalance, delayed revocation, session-timeout recovery and multi-consumer progress while their classic
counterparts passed.

**Forces:** retain the existing heartbeat interval, retry and request-timeout state machine; do not call the mutating
send-eligibility path merely to age time; make the condition accurate on every observation; keep time bookkeeping
separate from sending, resetting or completing a request; verify the change with real rebalance and recovery paths.

**Solution:** update the heartbeat timer from the supplied clock sample before reading its remaining duration. This
advances only the timer's view of time. `canSendRequest`, response processing and retry logic remain the owners of
request lifecycle transitions. A focused timer test proves that the condition reaches zero without an intervening
send check.

**Alternatives:** depending on an unconditional manager pass recreates the coupling this KIP removes. Calling
`canSendRequest` from the condition query would mix observation with request-state transitions. Keeping the stale
relative value can permanently hide a real heartbeat deadline.

The original failing asynchronous broker cases pass after the correction. The regular poll, commit and close classes
passed 53 tests, followed by an extended six-class run covering assignment, subscription, callbacks, fetch, seek,
broker failure and recovery: 194 tests with zero failures, errors or skips.

## Position initialization admits obligations at its operation boundary

**Context:** `updateFetchPositions` handles two kinds of work: partitions that are still initializing and partitions
that already await an explicit or out-of-range reset. A committed-offset lookup may delay the reset stage, during
which another seek or reset can establish a newer position intent.

**Problem:** the first scoped implementation carried only the initializing-partition predicate into the reset stage.
A partition already in `AWAIT_RESET` when the operation began was absent from that predicate, so the operation
completed without creating its ListOffsets continuation. Five asynchronous broker tests for seek, earliest/latest/
duration out-of-range recovery and broker-bounce recovery timed out. Expanding the scope by inspecting all resets
after a pending OffsetFetch would fix that liveness bug but allow an older operation to adopt a newer reset intent.

**Forces:** include every due reset obligation visible when the operation starts; preserve the internal
committed-offset-to-default-reset fallback for admitted initializing partitions; reject a seek or reset that arrives
while the committed-offset request is pending; fence each partition independently; let retained reset owners continue
through metadata and backoff without duplicating their payload or owner.

**Solution:** capture two scopes at `updateFetchPositions` entry. The initializing scope combines partition-state
identity with its position-intent version. The reset scope captures the identity and intent of partitions already due
for reset. After committed-offset processing, initialization may internally transition only the first scope to its
default reset. ListOffsets ownership is then restricted to the union of those two captured scopes and narrowed again
against current intent before reuse or creation. A reset requested later is left for the next update operation.

**Alternatives:** using only the initializing scope reproduces the five hangs. Reading all current reset partitions
after OffsetFetch completion expands an old operation into new input. Capturing topic-partition names alone admits
remove/re-add and same-lifetime ABA. Incrementing intent for the internal fallback would sever one initialization
operation between its committed-offset and ListOffsets stages.

A deterministic unit probe failed before the correction because the update future completed with no reset owner. It
passes afterward, and a second interleaving test proves that a reset arriving behind a pending committed-offset fetch
is not adopted by the old operation but is owned by the next one. The five previously failing public broker cases pass;
the extended broker run passed all 194 tests. The final consumer-internals regression passed 2,605 tests across 75
suites with zero failures, errors or skips, together with Checkstyle, Spotless and SpotBugsMain.

## Progress completion is distinct from owner termination

**Context:** one reset attempt can address several partitions and broker nodes. A response may install a position for
one partition while another partition remains with the same retained owner, waiting for metadata and retry backoff.
`AsyncPollEvent` must prepare fetches after the current position-update attempt, while the network loop must retain the
unfinished reset without requiring another application poll to recreate it.

**Problem:** using the reset owner's terminal future as the completion of `updateFetchPositions` coupled those two
lifetimes. In the full clients suite, the CONSUMER protocol received a successful latest-offset response for A and a
retriable response for B. A had a valid position, but fetch preparation never started because B's owner correctly
remained unfinished. The public poll timed out; the CLASSIC protocol made progress.

**Forces:** let every partition with a valid position become fetchable promptly; retain retry ownership for partitions
still awaiting metadata or backoff; wait for all broker responses in the currently staged reset attempt before
starting fetch preparation; preserve callers that explicitly observe the owner's terminal result; avoid completing a
future for work that has not reached either a response or a dependency wait.

**Solution:** a reset owner exposes two completion boundaries. Its terminal future completes when no valid partition
remains. Its current-attempt future completes after all requests staged in that attempt respond, or immediately when
the owner can stage no request and is waiting for metadata/backoff. `updateFetchPositions` joins the current-attempt
future, so successful partitions proceed to fetch preparation. The owner remains registered, and its condition retains
metadata, retry and response progress for unfinished partitions. Direct reset callers continue to join the terminal
future. A later retry installs a new attempt future without replacing the terminal owner.

**Alternatives:** waiting for owner termination reproduces cross-partition starvation. Completing and discarding the
owner after each response restores fetch progress but loses the retained continuation. Completing after the first
node responds can start fetch preparation while another response from the same attempt still owns applicable position
updates. A periodic retry timer in the application loop would hide the lifecycle coupling instead of representing it.

The exact full-suite failure is archived: 13,661 tests ran with one failure in
`KafkaConsumerTest.testFetchProgressWithMissingPartitionPosition(CONSUMER)`. A deterministic component probe failed at
the same boundary before the change and passes afterward while the retry owner remains present. The focused public
test passes for CLASSIC and CONSUMER, and 234 related offsets/application-event/async-consumer tests pass. The final
consumer-internals run passes 2,606 tests across 75 suites. The complete clients module passes 13,662 tests across 446
suites with zero failures or errors and four existing skips; Checkstyle, Spotless and SpotBugsMain pass. On that same
source revision, the five broker cases that originally exposed reset admission—explicit seek, three out-of-range reset
policies and broker-bounce recovery—also pass.

## Compose deadline conditions without allocation chains

**Context:** the network loop combines one fallback deadline with every manager's immutable condition after each ordered manager pass. The manager count is currently eleven, and the same primitive also combines conditions inside managers.

**Problem:** the first implementation represented every `either` call as a new binary wrapper. A JMH baseline measured an eleven-condition composition of already-created deadlines at 42.912 ns/op and 288 B/op. The binary tree carried no extra meaning when both operands were absolute deadlines: only the earlier deadline could determine readiness or remaining time.

**Forces:** keep `NextPollCondition` open to domain-specific implementations; preserve `ready` and `idle` algebra; keep absolute-deadline saturation semantics, including `Long.MAX_VALUE`; avoid mutable accumulators or a network-loop-only numeric side channel; treat performance as secondary to the scheduling and continuation contracts.

**Solution:** use algebraic fast paths in `either`. `ready` absorbs either operand, `idle` returns the other operand, and two built-in deadline conditions return the earlier existing object. Arbitrary implementations still use the general immutable `EitherPollCondition`, so the interface remains extensible and either reason can make the result ready. `at` exposes no mutable state and the selected deadline object remains safe to publish to the application thread.

**Alternatives:** retaining wrapper chains preserves semantics but allocates in every composition. Converting conditions back to numeric delays would lose custom readiness behavior and recreate two scheduling contracts. A mutable builder would add publication and ownership rules to a value that is otherwise immutable. Restricting the interface to built-in variants would prevent domain-specific conditions.

Two tests verify identity reuse for deadline composition and preservation of arbitrary custom-condition semantics. The full consumer-internals regression passed 2,602 tests across 75 suites with zero failures, errors or skips; Checkstyle, Spotless and SpotBugsMain passed. With the same JMH configuration (two forks, five 500 ms warmups, eight 500 ms measurements and the GC profiler), eleven existing conditions improved from 42.912 to 21.834 ns/op and from 288 to 24 B/op. At 32 conditions the result improved from 136.085 to 61.203 ns/op and from 792 to 24 B/op. Fresh-deadline composition at eleven conditions improved from 60.200 to 23.320 ns/op and from 552 to 288 B/op; those remaining allocations are the deadline values created by the benchmark, rather than composition wrappers. The numeric minimum is retained only as a benchmark reference and is not part of the design.

## Public interfaces, compatibility and migration

This design changes no public Consumer, ShareConsumer or Streams API. It adds the internal `NextPollCondition` contract
to `RequestManager`, replaces the internal numeric network/application wait contract, and keeps request batches focused
on transport output. There are no wire-protocol, record-format, configuration, metric-name or callback-thread changes.
Classic Consumer does not use the background manager loop and retains its scheduler; shared public and subscription
state behavior is covered by the paired CLASSIC/CONSUMER tests and the complete clients-module regression.

The implementation must land atomically across all background request-manager implementations because the internal
interface has no compatibility default. Existing owners translate their authoritative readiness, dependency and
deadline state directly. There is no user migration, feature flag or rolling broker dependency. Downgrade restores the
former internal loop with no persisted state conversion. A follow-up can split implementation work by independently
testable owner families, but the loop contract, application-wait publication and every compiled manager implementation
must remain one coherent change.

Compatibility acceptance uses three kinds of evidence. Deterministic component tests establish condition, continuation,
scope and publication behavior. Local broker suites exercise regular, Share and Streams behavior, including recovery
and close. The complete clients module covers unchanged public and transport paths. Historical fixes already present at
the baseline are treated as invariants; this proposal claims only the new model and corrections exposed by selectively
invoking owners.

## Rejected alternatives

The numeric minimum remains useful as a JMH reference but is rejected as the programming contract because it discards
the enabling reason and overloads a sentinel as quiescence. A global ready queue or change-generation registry is
rejected because it requires every producer to publish, replace and cancel registrations while authoritative state
still has to be rechecked. An actor or general continuation runtime is rejected because existing request managers,
futures and queues already own the payload and lifecycle; adding another owner would not solve partition or assignment
validity. Unconditionally invoking every manager is rejected because it hides missing dependencies and lets synchronous
no-op completions become scheduling inputs.

Per-owner retained state remains valid where an operation truly spans attempts. Offset reset and validation therefore
retain scoped owners; ShareConsume retains one input bit for changes that cannot be safely reconstructed from its
mutable session planner. Applying either mechanism globally was rejected. A single assignment generation was also
rejected because it invalidates unaffected partitions; state-object lifetime plus position intent fences each partition
at the operation boundary.

## Final acceptance

The corrected source revision passes 13,659 complete clients-module tests across 445 suites, with zero failures or
errors and two existing skips. Its three affected broker-integration classes pass 27 tests, including assignment
listener recovery in both KRaft layouts, share callback-triggered wakeup ordering and share renewal polling.
Checkstyle and SpotBugsMain pass. Earlier broad local evidence contains 194 regular-consumer, 62 ShareConsumer and 18
Streams broker tests, supplemented by 383 Streams consumer-boundary tests and a 2,606-test consumer-internals run.
The source also passes RAT and JMH compilation. Baseline/candidate JMH results
use two forks, five 500 ms warmups, eight 500 ms measurements and the GC profiler; the measured condition-composition
allocation chain was removed and all correctness suites were rerun afterward.

The issue-to-contract table and use-case map are normative acceptance inputs for this local design. They cover public
consumer surfaces, all application-event families, every request-manager composition, callback and buffer publication,
close, late results and the three required end-to-end cases. Transport fixes already present in the baseline remain
compatibility requirements rather than new claims. Remote CI and e2e results are recorded separately from this design;
upstream publication or community approval is not implied.
