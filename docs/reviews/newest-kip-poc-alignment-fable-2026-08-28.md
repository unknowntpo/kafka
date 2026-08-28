# KIP-1371 newest-KIP / POC alignment review

Date: 2026-08-28  
Reviewer: external Claude Fable, read-only fresh review  
Reviewed branch: `codex/async-consumer-reactor-poc`

## Review question

Find production implementation, tests, diagrams, or KIP claims that do not match the newest reactor model:

- one `PollResult` output model: `NetworkCommand`, `ManagerEvent`, and `NextPollCondition`;
- manager-owned local progress decisions;
- owner-routed cross-manager facts and version fencing;
- one published schedule before reactor-owned application effects;
- explicit distinction between verified POC behavior and remaining migration work.

## Findings and disposition

| Finding | Impact | Disposition |
| --- | --- | --- |
| `StateTransition` remained as a second, unused manager-output family. | The code and KIP described two names for manager facts, although producers had already migrated to `ManagerEvent`. | Resolved by `64c2cd33e2`: removed `StateTransition`, `PendingStateTransitions`, the `PollResult` field, and the reactor collection path. |
| Topic-metadata and share-acknowledgement retries returned an input wait while a positive retry deadline existed. | The reactor could not publish the real earliest retry time. | Resolved by `ddb66b5d52`: both managers publish a finite `RetryAfter`; deterministic tests verify the delay. |
| Coordinator fatal errors were read and cleared by heartbeat managers. | The coordinator owner did not control delivery, and the application-visible error bypassed the typed event/action ordering model. | Resolved by `d9fafb212d`: the coordinator emits one `CoordinatorFatalError`; policy selects error publication and final wake after schedule publication. Heartbeats no longer mutate coordinator fatal state. |
| Heartbeat request admission sometimes retained a heartbeat interval or zero delay after a request was already in flight. | The same local state had conflicting work-admission and wait meanings. | Resolved by `d9fafb212d`: regular and Streams heartbeat request-producing paths publish `AwaitInput(NETWORK_COMPLETION)`. |
| Streams topology-description push used compatibility `PollResult` constructors and an undifferentiated empty wait. | Coordinator input, membership input, network completion, and retry delay were indistinguishable. | Resolved by `d2e5ff6eb7`: the manager publishes typed input causes or a finite retry and captures coordinator snapshot/version. |
| `FetchBuffer.addAll(...)` still calls `signalAll()` directly. | The POC cannot claim every async fetch-data wake is ordered after `ReactorSchedule` publication. The method is also used by the classic consumer, so deleting the signal globally is unsafe. | Open and now stated explicitly in the KIP as a compatibility side channel. A later slice must separate async notification before removing it. |
| Share fetch request production still uses raw/compatibility `PollResult` construction. | The share path has not yet proved the complete typed manager-output model. | Open migration item; the KIP no longer describes all share output as migrated. |
| The KIP described `awaitEvent()` and `StateTransition` as retained adapters after code removal. | Reviewer-facing design and implementation evidence disagreed. | Resolved in the KIP update accompanying this report. |
| Some KIP evidence labels implied end-to-end fixes where only a mechanism/component test exists. | Reviewers could mistake POC evidence for a completed historical-bug reproduction. | Tightened: verified slices, compatibility limitations, and pending end-to-end reproductions are now separated. |

## Validation completed

- coordinator, policy, reactor, regular heartbeat, and Streams heartbeat suites: 384 tests passed;
- Streams topology-description and Streams heartbeat suites: 271 tests passed;
- Spotless, Checkstyle main/test, and SpotBugs main passed for the changed slices.

## Remaining alignment gates

1. Separate async `FetchBuffer` notification from the classic-consumer condition signal and prove the
   `FetchBufferHasData -> ReactorAction -> schedule-before-wake` path end to end.
2. Migrate remaining share fetch and other raw-delay `PollResult` producers.
3. Replace compatibility application waits only when each enabling input has a deterministic wake/completion proof.
4. Keep historical issue rows labelled `verified`, `partial`, or `pending` according to the strongest actual test.
