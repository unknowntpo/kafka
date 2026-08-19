# Async Consumer Reactor Evidence

## Causal Standard

A bug belongs in the main KIP argument only when its failure path demonstrates at least one of these conditions:

1. the same logical state transition is split across execution contexts;
2. separate components decide urgency and feasibility from different state snapshots;
3. a cross-thread operation can miss or duplicate its terminal acknowledgement.

Generic null checks, isolated cleanup mistakes, metrics defects, and unrelated implementation bugs should not be
attributed to the reactor design.

## Direct Evidence

| Issue / change | Observed invalid state | Missing contract |
| --- | --- | --- |
| [KAFKA-17066 / PR 16885](https://github.com/apache/kafka/pull/16885) | `updateFetchPositions` was split between application and background threads | one owner for a complete position update |
| [KAFKA-18641 / PR 18737](https://github.com/apache/kafka/pull/18737) | application position advancement raced with a background auto-commit snapshot, allowing record loss | ordered position and commit-snapshot transition |
| [KAFKA-15529 / PR 21476](https://github.com/apache/kafka/pull/21476) | consumed state became visible before the corresponding position, allowing a duplicate fetch | publish a completed transition atomically |
| [KAFKA-17439 / PR 17035](https://github.com/apache/kafka/pull/17035) | application and background threads inspected fetch-buffer state at different times | one fetch scheduling decision from one snapshot |
| [KAFKA-17182 / PR 18795](https://github.com/apache/kafka/pull/18795) | a buffer-state race caused unnecessary fetch-session removal and recreation | keep decision and execution under one owner |
| [KAFKA-20426 / PR 22018](https://github.com/apache/kafka/pull/22018) | a zero wait was returned although manual assignment made heartbeat progress impossible | combine urgency with progress feasibility |
| [KAFKA-20253 / PR 22836](https://github.com/apache/kafka/pull/22836) | heartbeat urgency produced CPU spin while the coordinator was unavailable or backing off | one progress decision across heartbeat and coordinator state |
| [KAFKA-20854 / PR 23014](https://github.com/apache/kafka/pull/23014) | an ambiguous empty fetch result caused application/network wakeup ping-pong | typed outcome and transition-specific notification |
| [KAFKA-20397 / PR 21991](https://github.com/apache/kafka/pull/21991) | metadata error publication raced with waiting on the fetch buffer | one completion protocol and wait set |
| [KAFKA-18160 / PR 18089](https://github.com/apache/kafka/pull/18089) | wakeup or interruption could skip a callback-completed event | exactly-once terminal acknowledgement |

## Supporting Lifecycle Evidence

| Issue / change | Observed invalid state | Relationship |
| --- | --- | --- |
| [KAFKA-19357 / PR 19914](https://github.com/apache/kafka/pull/19914) | close stopped coordinator discovery while pending commits still required it | contradictory lifecycle decisions across managers |
| [KAFKA-19394 / PR 20792](https://github.com/apache/kafka/pull/20792) | network-thread initialization failure could leave application close waiting forever | incomplete startup/close handshake |

These are supporting rather than primary examples because a narrower lifecycle coordinator could also address them.

## KIP Positioning

[KIP-945](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/255073708/KIP-945+Update+threading+model+for+Consumer)
already records the motivation for changing the consumer threading model, but its detailed threading, data-flow,
and network-I/O sections remain incomplete. A proposal should first determine whether to complete KIP-945 or create
a successor.

The design claim is not that a reactor automatically prevents every bug. The narrower claim is:

> The existing model permits recurring invalid states because ownership, progress feasibility, and cross-thread
> completion are separate implicit contracts. Making those contracts explicit removes entire classes of invalid
> intermediate states and gives focused tests a stable boundary.

The main KIP motivation should use one correctness example (KAFKA-18641), one liveness example (KAFKA-20854 or
KAFKA-20426), and one handshake example (KAFKA-18160 or KAFKA-20397). The remaining cases belong in an appendix.

## POC Adversarial Review Gate

A Claude Fable read-only review challenged the direct removal of the fetch wait rescan. Its high-severity finding
was that a typed deadline is insufficient unless publication and notification form one protocol: the application
may already be waiting with an older snapshot, and consumers without a group id do not read
`maximumTimeToWait()` at all.

The POC therefore requires these properties before the rescan is removed:

| Risk | Required property | POC evidence |
| --- | --- | --- |
| stale long wait | publish the new immutable snapshot before waking the waiter | `testShorterDecisionIsPublishedBeforeApplicationWakeup` |
| groupless consumer ignores application wait | publish and deadline expiry produce retained wakeups; the real consumer chain retries after exactly one reconnect backoff | `testDeadlineWakeupReleasesGrouplessApplicationWaitWithoutReadingMaximumTimeToWait`, `AsyncKafkaConsumerTest.testGrouplessPollRetriesFetchWhenReconnectBackoffExpires` |
| deadline drift | store an absolute deadline and subtract elapsed time at the reader | `ConsumerReactorProgressTest.testApplicationWaitSubtractsElapsedTime` |
| mixed partitions | retry conditions win over event-only in-flight conditions | `testRetryDeadlineWinsWhenInFlightAndReconnectConditionsAreMixed` |
| wakeup ping-pong | no wake for `NO_FETCHABLE_PARTITIONS`; terminal request completion remains a real transition | fetch-manager wakeup tests |

The POC now covers the groupless publication protocol at two deterministic levels: a real application-side
`FetchBuffer` wait running concurrently with the reactor scheduler, and the complete
`AsyncKafkaConsumer -> ApplicationEventHandler -> ConsumerNetworkThread -> FetchRequestManager -> FetchBuffer`
chain with only the socket replaced by a controllable `MockClient`. The real KRaft `PlaintextConsumerPollTest`
suite also remains green. A broker-restart test still belongs to production integration rather than this POC, so
the review leaves two gates open:

1. add an end-to-end manual-assignment/groupless reconnect-backoff test;
2. prove that unsent-request expiration and every terminal failure path reach the fetch completion wakeup.
