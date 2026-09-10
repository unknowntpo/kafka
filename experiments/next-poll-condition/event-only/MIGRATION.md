# Production migration boundary

The executable experiment proves scheduling mechanics, not a complete consumer migration.
All entries in the synthetic workload are condition-based. Existing Kafka managers remain
unchanged by this experiment. The generated loop must not be used with AsyncKafkaConsumer.

| Owner | Events that must make it ready | Timer responsibilities to preserve |
|---|---|---|
| CoordinatorRequestManager | discovery completion, coordinator invalidation, close | request retry backoff; current PoC already condition-based |
| AbstractHeartbeatRequestManager / StreamsGroupHeartbeatRequestManager | coordinator availability, membership transitions, completion, AsyncPollEvent, leave | heartbeat interval, retry, max.poll.interval, application liveness notification while blocked |
| CommitRequestManager | commit/offset-fetch enqueue, coordinator/member epoch changes, completion, close | per-request backoff/deadline; auto-commit due must notify application or move its initiation with equivalent semantics |
| AbstractMembershipManager / StreamsMembershipManager | heartbeat assignment, subscription changes, callback completion, metadata needed for reconciliation | preserve any reconciliation and leave timeout responsibilities; do not rely on unconditional maybeReconcile calls |
| OffsetsRequestManager | requestsToSend transitions to nonempty, metadata onUpdate, completion and application commands | preserve request deadlines/retries across the existing offset pipeline |
| TopicMetadataRequestManager | new request, response/failure and connectivity readiness | request expiration and retry eligibility; current poll prunes expired requests |
| FetchRequestManager | createFetchRequests, completion, fetchability/subscription/position changes, metadata/connectivity progress | replace retryBackoff fallback with complete notification coverage or explicit reconnect timer |
| ShareConsumeRequestManager | member readiness, fetch demand, acknowledgement enqueue/completion, metadata/session changes, close | acknowledgement retry/expiry and close deadlines |
| StreamsGroupTopologyDescriptionRequestManager | topology required/epoch changes, member/coordinator readiness, response | retry backoff and nextPushTime throttle |

This inventory is an initial source audit, not proof that every transition has been enumerated.
Concrete source anchors include RequestManagers.entries, AbstractHeartbeatRequestManager.poll
and maximumTimeToWait, CommitRequestManager.maximumTimeToWait, FetchRequestManager.maximumTimeToWait,
OffsetsRequestManager.requestsToSend/onUpdate, TopicMetadataRequestManager.poll, and
StreamsGroupTopologyDescriptionRequestManager.maximumTimeToWait.

## Application side

The real consumer currently computes a timeout in AsyncKafkaConsumer.pollForFetches from
ApplicationEventHandler.maximumTimeToWait AND its own retryBackoff safeguards, then waits on
FetchBuffer. BackgroundEventHandler.add enqueues events but does not publish this experiment's
gate. A new gate alone cannot replace these paths.

A complete migration must capture a notification generation before checking all relevant state,
then wait with the user's remaining poll timeout. Fetched data, background errors, membership
callbacks, position/metadata changes, auto-commit due and liveness timers must publish after their
state is visible. Public consumer.wakeup and close/interrupt behavior must remain distinct and
correct. FetchBuffer notifications and the new gate must share a race-safe wait protocol; two
independent waits can lose responsiveness even if each primitive is individually correct.

The experiment conservatively publishes progress after a processed scheduler batch. It tests
an application waiter and the generated loop's gate separately; it does not test the real consumer
waiting on that gate, OS selector wake behavior, or an end-to-end consumer/broker exchange.

## Remaining timing

No per-manager maximumTimeToWait scan does NOT mean no timeout calculation. The event loop still
bounds selector waiting by the nearest registered deadline and transport housekeeping. The user
poll timeout, request deadlines, heartbeat liveness and close deadlines remain. This is consistent
with event-driven runtimes having deadline timers.
