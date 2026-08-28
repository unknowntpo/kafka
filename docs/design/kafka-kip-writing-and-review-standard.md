# Kafka KIP Writing and Review Standard

This is the working standard for KIP drafts in this repository. It is not an ASF policy. Its purpose is to make a
proposal easy to evaluate: why the change is needed, what behavior it defines, whether it is compatible, and what
evidence supports it.

## Source of truth

- Maintain one authoritative Markdown draft. Generated HTML, diagrams, review notes, and POC reports are supporting
  artifacts.
- Keep current Kafka behavior, current POC behavior, and the target model visibly separate.
- A KIP defines behavior and compatibility. A POC demonstrates feasibility; its class layout is not automatically
  part of the proposal.

## Progressive disclosure

Present information in this order:

1. **Summary:** the existing problem, the proposed behavioral change, and the affected implementations.
2. **Motivation:** observable failures and evidence. Do not introduce the solution here.
3. **Public Interfaces:** protocol, API, configuration, metric, and compatibility changes.
4. **Proposed Changes:** one short model overview, followed by the minimum detail needed to define its behavior.
5. **Case Studies:** representative before/after flows with component interactions and test evidence.
6. **Compatibility and Migration:** current POC, target state, transition steps, and unresolved gates.
7. **Test Plan:** externally meaningful assertions, not a list of private methods.
8. **Appendix:** detailed POC matrices, benchmark records, and implementation diary material.

Define a new term at its first use and attach a concrete Kafka example. Do not require readers to understand the POC
before they understand the proposal.

## Evidence rules

Classify every material claim:

| Claim | Required support |
| --- | --- |
| Current behavior | Current source, test, or official design document. |
| Historical failure | JIRA, PR, or commit. Describe the observed failure; do not overstate causality. |
| POC behavior | Pinned commit, named test, and result. |
| Performance | Reproducible benchmark configuration, raw result, and a narrow conclusion. |
| Target behavior | Normative wording such as `must` or `will`, plus a test obligation. |
| Unverified behavior | Mark `Pending`, `Partial`, or `Open`; do not present it as implemented. |

For each important behavior, write an intent-to-assertion chain:

```text
Intent -> Given -> When -> Then -> Evidence
```

Cover the happy path and the relevant retry, zero/max delay, stale completion, ordering, timeout, cancellation,
close, and consumer-variant cases. Mock-based proof is not equivalent to an end-to-end proof.

## Narrative and style

- Lead with the reviewer decision. A KIP is a decision document, not a design diary.
- Use one memorable model and one representative lifecycle before introducing individual types.
- Keep one judgment per paragraph. Prefer concrete subjects and actions.
- Use Kafka terminology. Prefer examples over abstract labels.
- State scope positively. Add a non-goal only when it prevents a likely compatibility or ownership misunderstanding.
- Keep the main text to representative cases; move exhaustive issue and test matrices to an appendix.
- Use FAQ entries only for genuine edge questions that are not already answered by the main narrative.

Avoid:

- repeated `not X, but Y` constructions and long `does not` lists;
- repeated explanations across Summary, Proposed Changes, Case Studies, and FAQ;
- meta narration such as “the following section will explain”;
- unsupported adjectives such as “robust”, “comprehensive”, or “seamless”;
- abstract terms such as “contract”, “generic”, “unified”, or “explicit” when a concrete behavior is clearer;
- making internal POC names part of the community decision without a compatibility reason.

## Complexity budget

Every section, type, and mechanism must contribute at least one of these:

- a decision the community must make;
- evidence for a material claim;
- a behavioral or compatibility guarantee; or
- navigation needed to understand the next layer.

Otherwise merge it with adjacent material or remove it. Prefer the smallest model that closes the motivating
failures. New indirection requires a named owner, lifecycle, boundedness rule, failure behavior, and test.

## Discussion-ready checklist

- A maintainer can explain the problem and proposed behavior after reading the Summary.
- Motivation contains evidence and failure mechanisms without solution leakage.
- The model overview defines ownership, data flow, timing, and application-visible effect ordering.
- Current Kafka, current POC, and target behavior are distinguishable.
- Every invariant has at least one deterministic assertion and a named remaining gate.
- Public API, protocol, configuration, metrics, callback, and thread compatibility are stated.
- Legal boundary values and lifecycle paths have defined behavior.
- Open questions and incomplete evidence are visible; no unresolved correctness issue is hidden in an appendix.
- Terms are introduced once and used consistently.
- The draft contains no duplicated section whose removal would preserve the same decision information.

## Reference patterns

- [KIP-500](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A%2BReplace%2BZooKeeper%2Bwith%2Ba%2BSelf-Managed%2BMetadata%2BQuorum): introduces one memorable model before detailed mechanics.
- [KIP-848](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217387038/KIP-848%2BThe%2BNext%2BGeneration%2Bof%2Bthe%2BConsumer%2BRebalance%2BProtocol): moves from operational pain to goals and a “nutshell” model.
- [KIP-98](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98%2B-%2BExactly%2BOnce%2BDelivery%2Band%2BTransactional%2BMessaging): defines the observable guarantee and concrete failure before protocol machinery.
- [KIP-631](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=173082410): builds on an earlier architectural decision instead of repeating its full rationale.
