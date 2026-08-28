# Independent review: deadline and manager-eligibility semantics

Reviewed branch: `codex/async-consumer-reactor-poc`  
Reviewed commit: `42c7c423ba5160fb0171833b80791ef4c354ffc2`

## Verdict

**Partly accurate.** The information-loss argument applies to the numeric wait field of an empty `PollResult`, not to
the complete result. Request presence already distinguishes produced transport work, and `WAIT_FOREVER` is an
established sentinel rather than an ordinary large finite delay.

## Verified evidence

| Issue | Verified behavior | Classification |
| --- | --- | --- |
| KAFKA-20253 / PR 22836 | Missing coordinator, heartbeat, and in-flight commit guards allowed expired timers to keep returning zero; the fix added local guards and substituted intervals or `WAIT_FOREVER`. | Strong deadline/blocker-classification evidence, but still a local predicate defect. |
| KAFKA-20426 / PR 22018 | Manual assignment skipped heartbeat while an initial zero heartbeat interval produced an immediate application wait; the fix returned `Long.MAX_VALUE` when heartbeat was skipped. | State-to-time projection defect. |
| KAFKA-20854 / PR 23014 | Fetch woke the application for every empty preparation result; the fix distinguished buffered-data readiness from blocked states and moved wakeup to request termination. | Wake/effect classification, not primarily deadline information loss. |
| KAFKA-20970 / PR 23227 | The open PR demonstrates an expired auto-commit timer remaining at zero while asynchronous coordinator discovery cannot yet progress. | Deadline projection plus configuration interaction; not merged historical evidence. |

## Defensible thesis

> Current `PollResult` pairs produced requests with a numeric wait hint. Request presence shows whether transport work
> was produced, but the wait hint itself says only when another poll may occur. For an empty result, it does not state
> whether passage of time can change eligibility or whether progress requires an external input. Existing code
> represents those cases by convention—zero, a finite delay, or `WAIT_FOREVER`—which leaves eligibility and blocking
> cause implicit.

The broader configuration slogan was too strong. Defensible wording is:

> Once a feature and operating mode are fixed, timing-only configurations should control cadence, retry spacing, and
> timeout budgets. They should not be reused as proxies for whether work is currently eligible or which external
> input is required before progress can resume.

## Counterarguments and limits

- Typed conditions do not eliminate incorrect manager-local predicates.
- `AwaitInput(cause)` is diagnostic; it does not subscribe to an input or prove wake-path completeness.
- KAFKA-20854 can be fixed with a local readiness/effect flag and should not be presented as deadline causality.
- Effect ordering, lifecycle timeout enforcement, and lost wakeups remain separate correctness problems.
- A smaller representation—output plus an optional timed retry—could encode the three states; an `AwaitCause` enum is
  optional unless it supports diagnostics or wake-path validation.

## Compatibility gap discovered

`retry.backoff.ms=0` is legal, while the current `RetryAfter` requires a positive delay. The compatibility path can
turn an empty zero-delay result into `AwaitInput`, which may suppress a required retry. The KIP must define and test
zero-delay time-driven retry semantics before claiming configuration compatibility.

## Claims to avoid

- Do not claim the numeric field caused every cited issue.
- Do not claim full `PollResult` cannot distinguish output from no output.
- Do not describe `WAIT_FOREVER` as an ordinary very long delay.
- Do not claim typed conditions guarantee liveness or correct wakeup routing.
- Do not claim Kafka configuration should never affect state semantics.
