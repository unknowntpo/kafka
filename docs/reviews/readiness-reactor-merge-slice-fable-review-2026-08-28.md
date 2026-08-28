# Fable Review: Minimal Readiness and Reactor Merge Slice

## Reviewed baseline

- Experiment: `codex/reactor-readiness-model-slice` at `cb8d3c7f06`
- Main POC comparison point: `codex/async-consumer-reactor-poc` at `0f27b10e2f`
- Review mode: independent read-only reconstruction of code, tests, and the experimental design document

## Verdict

**Merge with follow-ups.** The slice is small, contains no manager-specific branch in `ConsumerReactor`, and adds no
second wakeup channel. Two findings were required before merging.

## Required findings

### 1. Correct the claimed failure

The comparison baseline already contains the KAFKA-20253 safety guard. When the auto-commit interval expires while a
commit is in flight, it waits another `autoCommitInterval`; it does not return zero. The slice therefore removes an
arbitrary timer fallback and derives an input-driven wait from the same state used to reject duplicate work. It must
not claim to fix a currently present zero-timeout loop.

### 2. Prove completion liveness and publication order

Unit tests for `CommitRequestManager` prove its local plan, but the reviewed baseline lacked a vertical test using a
real `CommitRequestManager` inside `ConsumerReactor`. The required proof is:

1. an auto-commit remains in flight after its timer expires;
2. no timer deadline is retained for that manager;
3. the request completes during `NetworkClientDelegate.poll()`;
4. the completion marks and re-polls its owning manager;
5. `ConsumerReactor` publishes the newer `ReactorSchedule` generation; and
6. only then does it wake the application thread.

## Verified strengths

- `AwaitCause` is diagnostic data only; production code does not use it to register a signal.
- Production `PollResult` validation is runtime enforcement, not a Java assertion.
- The Coordinator pre-step and post-step plan prevents an in-flight request from retaining an immediate-poll state.
- The reactor remains generic: it stages manager outputs, aggregates timing, publishes, and executes effects.
- Owner-version fencing and readiness solve different problems and remain separate.

## Non-blocking follow-ups

- Refine `AwaitCause` if diagnostics must distinguish a request waiting for coordinator discovery from a request
  actually awaiting network completion.
- Reduce overlapping factory vocabulary among `awaitEvent`, `awaitInput`, and `waitFor` after migration call sites are
  known.
- Do not claim Heartbeat implementation proof; the current work is analysis only.
- Revisit the legacy Commit application-wait bridge only after compatibility behavior is measured.
- Retain the comparison prompt artifact only if reproducible external review is considered useful project evidence.

## Resolution in this branch

- The experiment document now describes elimination of the periodic fallback rather than a pre-existing zero loop.
- `ConsumerReactorCommitReadinessTest` supplies the required real-manager completion and publish-before-wakeup proof.
- The proof table distinguishes tests introduced by this slice from evidence inherited from the main POC.
- The external-review prompt is retained intentionally as reproducibility evidence for this experimental branch.
