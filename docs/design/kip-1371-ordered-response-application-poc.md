# Ordered response application experiment

See [callback safety contract](kip-1371-callback-safety-contract.md) for the current
decision, two-mode assertions, negative control, and final validation receipt.

Baseline: `bcc1e49d4e`. Rollback checkpoint remains
`checkpoint/kip-1371-design-v0-2026-09-06`.
Status: local experimental POC, not a completed compatibility proof.

Current decision: inline delivery remains the default. Package-local
`enableResponseBatching()` enables the alternative only in explicit POC tests;
there is no new public consumer configuration. The shared safety assertions run
against both modes. Batching has not been selected as a correctness prerequisite.

## Question

Can transport completion only capture a result, with owner response logic invoked
from a named `runOnce` boundary rather than inside `NetworkClientDelegate.poll`?

## Rejected first implementation

Capturing only Commit/OffsetFetch responses and leaving Heartbeat inline reordered
cross-owner effects. The unchanged
`testCompletionObserverIsNotABatchSnapshot(heartbeatFirst=false)` failed: a commit
completion that previously observed a known coordinator now observed it unknown,
because a later heartbeat response ran first. Of the 34 selected tests, one failed.
This is an internal completion-observation counterexample, not evidence that a
public OffsetCommitCallback contract promises a batch snapshot.

That implementation was removed rather than changing the existing assertion.
The failure demonstrated a changed intermediate observation, not necessarily an
invalid business outcome. Keeping that observation test documents compatibility;
it does not make exact callback order the sole architecture acceptance criterion.

## Current candidate

`NetworkClientDelegate` captures result records for requests registered through
`add` while the normal reactor transport call is active. Each record retains the
original completion handler, response/error, and observation timestamp. The queue
preserves transport callback order across owners; it is not grouped by RM.

`ConsumerNetworkThread.runOnce` opens capture, invokes network polling, then drains
the observed batch in `finally`. Completing the request's existing future invokes
its existing owner response logic in that explicit response-application phase.
This reuses response handlers; it does not remove every callback from the program.

Delivery occurs before the optional post-I/O manager pass, before new application
inputs are admitted, and before application-event timeout reaping. It is not
skipped just because application input is queued or normal request building stops.
Standalone delegate polling and the existing close polling path remain inline.

If `trySend` already captured a send-time failure, the subsequent network poll is
non-blocking so delivery is not held behind an unrelated wait. The failure is
removed from the unsent queue and applied once that transport call returns; this
is a ready-result rule, not an empty-output immediate-poll convention. A focused
test checks the zero network wait and that owner handling receives the original
failure timestamp rather than the later application-phase time.

Each batch is finite in the sense that only the observed transport results are
drained, without another I/O poll or a recursive fixed-point loop. This is not a
configured capacity limit. Retained response payloads and per-completion allocations
add memory/CPU cost that has not been benchmarked.

## What the boundary does and does not guarantee

- Owner response handlers no longer execute inside the wrapped normal transport poll.
- Their original relative response order remains meaningful; no all-owner snapshot
  is promised to intermediate operation completions.
- Captured request scope/version checks still decide whether state mutation is valid.
- Existing synchronous future continuations still execute during response application.
  Arbitrary direct peer mutation or manually recursive completions are not prevented.
- Coordinator invalidation still uses existing version-fenced owner calls. This is
  not the separate fatal-error value-routing migration.
- Transport metadata/error side channels and callbacks outside these registered
  request handlers are not made part of a universal publication barrier.
- Fatal cleanup, timeout races involving real elapsed I/O time, every consumer variant,
  and public-consumer/broker behavior require further validation.

The initial direct proof checks that Commit's future remains incomplete inside the
transport call even after the response is observed, then completes after return.
Queued application input does not suppress delivery or get drained recursively.
An injected transport exception after observation also leaves the observed commit
completed before the exception propagates out of `runOnce`; this is not a full
fatal-cleanup proof.
The original response-order, stale-owner, input-cutoff, and timeout tests remain
unchanged as compatibility checks.
