# KIP-1371 / ConsumerReactor POC — Fable independent review

## Review metadata

- Reviewed branch: `codex/async-consumer-reactor-poc`
- Reviewed HEAD: `3448f9bd25`
- Reviewer: Claude Fable (`claude-fable-5`, effort `max`)
- Mode: first-exposure, read-only, adversarial architecture review
- Verdict: **REQUEST CHANGES**
- Scope: KIP, diagrams, companion documents, consumer implementation, tests, and benchmark evidence
- Criteria: correctness, ownership, deterministic ordering, liveness, compatibility, extensibility,
  debuggability, and strength of evidence

The response below is preserved as returned by Fable. Resolution notes will be appended after each remediation pass.

---

## Fable response

### Context

審查對象：branch `codex/async-consumer-reactor-poc`（62 commits vs `trunk`，86 files）。範圍：KIP 全文
（`docs/design/kip-introduce-consumer-reactor-state-management-event-processing.md`）、四份圖（`docs/images/kip-1371-*`）、
五份 companion docs、`ConsumerReactor` 與所有 request manager 變更、`ConsumerReactorTest`/`AsyncKafkaConsumerTest`/
`FetchRequestManagerTest` 等測試、`BaseConsumerTest.scala` 整合測試。方法：source trace + 兩個 read-only Explore
sweep（cross-owner mutation 清單、Test Plan 逐條對照）。未執行 gradle，未修改任何檔案。

結論：**REQUEST CHANGES**。核心機制（ReactorSchedule 聚合、publish-before-wake、legacy 0 ms 抑制、exception 隔離、
coordinator version fencing）經 trace 成立，未發現資料遺失或 deadlock；但有一個由「deferred routing + 同 phase
affected re-poll」組合造成的 liveness/效率退化（H1），以及兩個會讓 KIP 投票無法成立的敘述失真（H2、H3）。

### Findings

#### H1 — Deferred coordinator invalidation 讓 post-I/O phase 對 stale coordinator 送出新 request（行為退化）

Trace（STABLE member 收到 `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE`）：

1. `AbstractHeartbeatRequestManager.onErrorResponse:410-411`：`onFailedAttempt` → `HeartbeatRequestState.onFailedAttempt`
   把 `heartbeatTimer.reset(0)`；`case NOT_COORDINATOR:416-428` 只把 `CoordinatorUnavailableObserved` 放進
   `pendingManagerEvents`，接著 `heartbeatRequestState.reset()` 清掉 backoff（`RequestState.reset` 設 `lastReceivedMs=-1`）。
2. 事件要等下一次 `poll()` 才 publish，再等下一輪 `runOnce()` 開頭 `routeDeferredManagerEvents`（`ConsumerReactor:261`）才套用。
3. 但 heartbeat manager 是 affected manager，`pollAffectedManagers`（`ConsumerReactor:484-504`）在**同一個 post-I/O phase**
   重新 poll 它：`coordinator().isEmpty()` 為 false（`AbstractHeartbeatRequestManager.poll:169`），`canSendRequest` 為 true
   （timer expired 且 backoff 0）→ `makeHeartbeatRequest:333` 用 stale `CoordinatorSnapshot(node-1, v7)` 建 request → 回傳
   `progress([hb])` → `networkClientDelegate.addAll`（`ConsumerReactor:501`）。
4. 下一輪：事件 route → coordinator unknown（v8）→ FindCoordinator 建立 → `networkClientDelegate.poll()` 的 `trySend` 同時送出
   FindCoordinator **與 stale heartbeat**。

後果：每次 invalidation 多一個無效 RPC 與一行重複的 INFO log；`heartbeatRequestState.requestInFlight()` 在 stale 回應前保持
true，`poll:203-204` 因此回 `awaitEvent()`，對**新** coordinator 的第一個 heartbeat 被延後一個 stale RTT。
`StreamsGroupHeartbeatRequestManager.poll:490-493` 同樣（`reset()` 於 `:768/:781`；`canSendRequest` 前無 in-flight 檢查）。
Trunk 的 `markCoordinatorUnknown` 是同步套用，不存在此視窗；這是 POC 新引入的。

與文件矛盾：KIP step 5「heartbeat and commit may wait for its completion」；`routeDeferredManagerEvents` Javadoc「every
dependent manager observe the event before the next NetworkClientDelegate.poll()」——dependent manager 觀察到的是 stale
state，而它的輸出正是下一次 poll 送出的東西。

測試沒抓到：`ConsumerReactorTest.testRealHeartbeatInvalidationIsRoutedBeforeNextNetworkPoll:586` 把 member 放在
JOINING（`:648`，本來就會立即 heartbeat），`heartbeatRequest` 用 `compareAndSet(null, …)` 只記第一個 request（`:657`），
斷言只涵蓋 FindCoordinator 順序。

修正方向（擇一並加測試）：post-I/O 順序改為「publish → route deferred events → re-poll affected」；或 affected re-poll 前先
route；或 heartbeat/commit 對「剛回報 unavailable 的同一 snapshot version」不再 admit request。

#### H2 — KIP 不變式 3 與程式行為不符，但被寫成設計依賴的 invariant

KIP：「State and the resulting ReactorSchedule are published before completing futures, publishing data or events, or waking
the application.」實際：

- `AbstractFetch.handleFetchSuccess:232` → `fetchBuffer.add` → `FetchBuffer:109-110` 設 `wokenup` + `signalAll`，發生在
  `networkClientDelegate.poll()`（`ConsumerReactor:301`）內，早於 post-I/O `publishReactorSchedule`（`:312`）。
  `ShareConsumeRequestManager:1019` 同。
- 所有 `CompletableApplicationEvent` future 在 `processApplicationEvents`（`:262`，早於 `:291`）與 response handler
  （`CommitRequestManager:902-1393`、`NetworkClientDelegate.FutureCompletionHandler:544-565`）內直接完成。
- `CompletableEventReaper.reap`（`ConsumerReactor:323`）在最後一次 `executeReactorActions` 之後完成 future，沒有 republish，
  也沒有 wake。
- `ShareAcknowledgementEventHandler.add:48` 是第二條未緩衝的 application queue；`OffsetCommitCallbackInvoker`
  （`CommitRequestManager:398`）是第三條。

Summary 的「executes ReactorAction values only after the corresponding state and schedule are visible」只對五種
`ReactorAction.Type` 成立。要求：把不變式 3 的範圍縮到 `ReactorAction` + staged `BackgroundEvent`，明列尚未納入的
mailbox/future 路徑，並把 KAFKA-18641 / KAFKA-15529 的「publication race」從「本設計解決」改為 Phase 3 目標。

#### H3 — 圖與實作、KIP 正文互相矛盾；dead transition 常數

- `docs/images/kip-1371-kafka-20253-progress.dot:90-99`「ConsumerReactor re-polls heartbeat in the same post-I/O phase」、
  `kip-1371-kafka-20253-before-after.dot:165-177`「re-polls heartbeat in the same phase」描述的是 commit `57b973bc6a`
  （"Use full pass for cross-manager progress"）已移除的分支；KIP 正文現在說相反（Test Plan「without a hard-coded same-phase
  manager branch」、Phase 2「Remove hard-coded coordinator dependency branches」）。這兩張圖未被 KIP 引用
  （KIP 只引用 `:217`、`:253` 兩張），是 branch 內的孤兒產物，但讀者會信。
- `StateTransition.COORDINATOR_DISCOVERED` 仍由 `CoordinatorRequestManager.poll:111-118` 發出，但
  `ConsumerReactor.collectStateTransitions:557-570` 只收 `requiresApplicationWakeup()` 的 transition，沒有任何消費者；
  `COORDINATOR_INVALIDATED` 從未被產生。兩者是「typed transition contract」裡的 dead code。

#### M1 — Strict factory 把 scheduling 契約錯誤升級成使用者可見的例外

`PollResult.retryAfter(0)` / `progress(…, <0)` 拋 `IllegalArgumentException`（`NetworkClientDelegate:391-397`）；
`ConsumerReactor.pollManager:452-460` 接住後 `stageBackgroundError` → `ErrorEvent` → `AsyncKafkaConsumer.processBackgroundEvents`
→ 使用者的 `poll()` 拋出。Trunk 的 `runOnce` catch 只 log 不上拋。同類缺陷在 legacy 路徑只是 metric + `awaitEvent()`。
目前 call sites 受正 delay/backoff guard 保護而不可達，但一次 refactor 就可能觸發。KIP Compatibility 也未提及此行為變更。

#### M2 — Manager 無法撤回或延後自己未到期的 deadline

`ManagerPollCache.PollState.update:89-110` + `preservePendingDeadline:129-135`：同一 manager 後續回 `awaitEvent()` 或更長
delay，仍保留較早的 absolute deadline。跨 manager 的保留是設計意圖，但同 manager 的保留與 KIP `PollResult` 表格
「`delay` describes the next manager poll after that state change」矛盾，會造成多餘 re-poll。`ManagerPollCacheTest` 無
「同 manager `retryAfter → awaitEvent`」案例。

#### M3 — Test Plan bullet 6 被自己的測試反證

`ConsumerReactorTest.testWakeupDeduplicationIsPerReactorPhase:415` 斷言一次 `runOnce()` 內 `wakeupApplicationThread()`
被呼叫 `times(3)`；Test Plan 卻列「one complete reactor iteration combines … into at most one primitive synthetic wakeup」。
正文另一處承認 per-phase 是 migration gap，但 Test Plan 應標示「現況 per-phase；per-iteration 為 Phase 3」。
`reactor-application-wakeup-total` 描述為「coalesced application wakeups」，實際計的是 per-phase。

#### M4 — 證據歸因：兩個 component proof 證明的是 trunk 既有修正，不是 reactor

- `AsyncKafkaConsumerTest.testReactorPreservesNewPartitionAcrossOlderOffsetFetchCompletion:2481` 通過的原因是
  `OffsetsRequestManager` 既有 KAFKA-17674 修正；negative baseline `6744a718c2` 是 pre-17674，不是 pre-reactor。
- `testPausedPartitionDoesNotProduceNoProgressWakeup` 證明的是 PR 23014 已在 trunk 交付的性質。
- KIP Motivation 的多數列在 evidence doc 中仍標為 Pending / Partial / Unit only。KIP 應區分「regression guard」與
  「本設計改變的機制」。

#### M5 — Test Plan 缺口

- 同一 pass 內各 manager 使用同一 snapshot version：無測試。
- owner 只保留 current snapshot、in-flight 只保留 version + bounded scope：無測試。
- 無跨 owner mutation：無 ArchUnit/import-control，且 main code 仍有多個 KIP 豁免或尚未遷移的 cross-owner calls。
- share acknowledgement、topic metadata 無新 classification 測試。
- `ConsumerReactorTest` 從未以 share manager set 跑 kernel。
- 整合測試只 shutdown coordinator broker，且以 `pollRecordsUntilTrue` 觀察恢復。

#### M6 — Manager state → Result 表混用已實作與目標列

- Auto-commit in-flight 尚未在 `CommitRequestManager.poll` 建模。
- `TopicMetadataRequestManager.poll` 仍回 `EMPTY`，不是 `retryAfter(remainingBackoffMs)`。
- Share acknowledgement in-flight 是 trunk 既有行為，不是本 POC 新增。

#### M7 — `FetchRequestManager.createFetchRequests` 語意變更未在 KIP 揭露

最多一個 waiter，其餘呼叫者立即成功完成；若隨後 preparation 拋例外，duplicate caller 不會收到。
`FetchRequestManagerTest` 由「0 done」放寬為「size-1 done」。現有 regression 只證明 retained waiter 收到例外。

#### M8 — Benchmark 證據過時且範圍窄

benchmark 比較的是較舊 HEAD，只有 idle CPU 與 first-record latency，未覆蓋 KIP 要求的 throughput、allocation/record、
reconnect recovery gate；Jenkins 專用 harness 已進 tree。

### Low-severity notes

- `ConsumerReactorGateway.submit` Javadoc 宣稱 configured capacity，但 queue 無界。
- `ReactorSchedule.withApplicationDeadlineDelivered` republish 不遞增 generation。
- `CoordinatorRequestManager.handleCoordinatorDisconnect` 已無 main caller；`markCoordinatorUnknown` 仍 public。
- 第三個 action execution point 沿用前一次 publication；KIP 只描述兩個 phase。
- Classic 路徑被動到但 KIP 稱未 retrofit，需在 Compatibility 說明。
- PollResult violation 每輪 `log.error` 無 rate limit。
- ownership 圖漏列兩個 `MARK_ASYNC_POLL_*` action。
- Commit subject `KAFKA-1371` 對應到無關的舊 JIRA，branch 另有 placeholder subjects。

### Verified claims

- `ConsumerReactor` 無 consumer-type 分支；wake target 於 composition 決定。
- 沒有 manager 繞過 `PollResult` 直接送 request；`affectedManagers` 標記完整。
- FetchBuffer latch 讓 publish-then-wake 不會 lost wakeup；更短 schedule 與 legacy 0 ms 抑制有測試。
- `PollResult(List)` 使用 `WAIT_FOREVER`，share 空結果不會觸發 violation。
- 舊 `ConsumerNetworkThreadTest` 14 個測試皆有對應；無新增 `@Disabled`。
- Coordinator version fencing 與其測試一致。
- Failover 整合測試是真實 end-to-end coordinator-loss recovery 證明。

### Required changes

1. 修 H1：調整 post-I/O 順序或 admit 規則，並加 regular/Streams stale-heartbeat regression。
2. 修 H2：改寫不變式 3 與 Summary，明列未覆蓋路徑；調整 Motivation/evidence 歸因。
3. 修 H3：重生或刪除兩張 20253 圖；移除或接上 dead coordinator transitions。
4. factory `IllegalArgumentException` 與 legacy violation 同等處理，或在 Compatibility 明寫新行為並加測試。
5. 文件化或修改同 manager deadline 覆寫規則，加測試。
6. Test Plan 與 result table 逐列標示 current / target。
7. 恢復 fetch waiter fan-out，或補測 duplicate caller 不遮蔽失敗。
8. 以 HEAD 重跑 A/B，補 throughput 與 reconnect gate；決定 harness 去留。

### Test gaps

H1 情境測試；同 manager `retryAfter → awaitEvent` cache 測試；share manager set 跑 reactor kernel；
share/topic-metadata classification；snapshot/version retention；architecture rule；broker restart；
duplicate fetch caller failure propagation。

### Open questions

- Deferred routing 的 phase consistency 與 affected re-poll 的 latest truth 如何取捨？
- `usesLegacyApplicationWait` bridge 的移除時程與替代 event 是什麼？
- `MAX_POLL_TIMEOUT_MS=5000` 對 `awaitEvent()` 是 safeguard，還是 topic-metadata 的實際 scheduler？

### Final verdict

**REQUEST CHANGES**

---

## Resolution log

### Remediation pass 1

- Code commit: `804ca84868` (`CONSUMER: Fence deferred coordinator observations`)
- Documentation and diagram updates: staged in the following documentation commit

| Finding | Resolution |
| --- | --- |
| H1 stale post-I/O request | Heartbeat, Streams heartbeat, and commit now publish a pending coordinator observation before admitting another request. The reactor component test counts heartbeat attempts and proves that no stale heartbeat is staged before owner routing and rediscovery. |
| H2 overbroad publication invariant | The KIP now limits the current invariant to `ReactorAction` and staged `BackgroundEvent` values. Fetch-buffer signals, generic futures, callback invokers, acknowledgement events, and timeout reaping are explicitly listed as Phase 3 compatibility paths. |
| H3 stale diagrams and dead transitions | Both KAFKA-20253 diagrams now show the next full ordered pass rather than a same-phase dependent re-poll. Dead coordinator `StateTransition` values and discovery-publication state were removed. |
| M1 manager factory exception visibility | A manager polling failure is logged, counted, and replaced by `awaitEvent()` without creating a user-visible `ErrorEvent` or skipping network I/O. |
| M2 deadline withdrawal | A finite early poll still cannot postpone its manager's earlier deadline, while an explicit `awaitEvent()` now withdraws that manager's obsolete finite deadline. A deterministic cache regression covers the transition. |
| M3 wake-count claim | The KIP and metric description now say that the current implementation coalesces once per execution phase; once per complete iteration remains a Phase 3 target. |
| M4 evidence attribution | Motivation labels each issue as current mechanism evidence, an existing regression guard, or a later migration target. |
| M5 test-plan gaps | Current evidence and target exit criteria are labelled separately, including snapshot consistency, architecture enforcement, share coverage, and exactly-once completion. |
| M6 mixed manager-result table | Every row now has an explicit current, existing, or target status. |
| M7 fetch waiter semantics | The KIP documents the bounded single-current-waiter contract and the already-completed duplicate behavior rather than implying failure fan-out. |
| M8 stale benchmark evidence | The companion report is marked preliminary and historical, identifies its old baseline, and is no longer presented as Phase 3 acceptance evidence. Current-HEAD throughput, allocation, and reconnect gates remain required before that phase is accepted. |
| Low-severity notes | Gateway capacity Javadoc was corrected; deadline delivery now increments schedule generation; dead coordinator mutation entrypoints were removed or narrowed; the KIP describes all three current action phases and Classic-helper compatibility; repeated contract-violation logs are rate-limited per continuous violation; and the action diagram lists both async-poll mark actions. |

Verification after the remediation:

- 496 coordinator, commit, regular heartbeat, Streams heartbeat, and topology manager tests passed;
- 469 reactor, schedule, cache, transport, fetch, regular/share consumer, application-event, background-event, and metrics tests passed;
- `SslConsumerTest.testConsumerProtocolCoordinatorFailoverReactorRecovery` passed against real embedded brokers with `group.protocol=consumer`;
- Spotless, Checkstyle main/test, and SpotBugs main passed as part of the verification runs.

A second independent Fable verdict will be appended below after the remediation is committed and pushed.

---

## Fable re-review response

### Review metadata

- Reviewed branch: `codex/async-consumer-reactor-poc`
- Reviewed HEAD: `9164c413ae`
- Reviewer: a fresh Claude Fable instance (`claude-fable-5`, effort `max`)
- Mode: independent PMC-style, read-only review in an isolated clean clone
- Verdict: **APPROVE WITH NON-BLOCKING FOLLOW-UPS**

The reviewer was instructed to form an independent view from the KIP, diagrams, code, and tests before reading the
earlier review or this resolution log. The response is recorded below; repeated absolute `file://` citations are
normalized to repository-relative file and symbol names.

### Executive summary and verdict

Fable concluded that the implementation at exact HEAD `9164c413ae` fulfills the core architecture, concurrency,
state-ownership, and stale-work-fencing requirements of the proposal. It identified these strengths:

1. **Single-owner state and fenced cross-manager facts.** Coordinator-dependent requests capture an immutable
   `CoordinatorSnapshot`; response handlers publish typed `CoordinatorUnavailableObserved` facts; the coordinator
   owner rejects observations from an older version.
2. **Deterministic timing publication and busy-spin prevention.** `ReactorSchedule` aggregates retained manager
   deadlines, while malformed empty zero-delay results are counted and replaced with `awaitEvent()`.
3. **Explicit publication boundaries.** Migrated `ReactorAction` and staged `BackgroundEvent` effects run only after
   the corresponding schedule publication, with application wakeup last in each execution phase.
4. **Fault isolation.** Manager polling and action failures cannot skip network I/O or suppress later independent
   actions.
5. **Shared kernel without consumer-type branching.** Regular, share, and Streams request-manager compositions use
   the same reactor mechanism while retaining protocol-specific rules outside the kernel.

Fable summarized the reviewed iteration as:

```text
route deferred ManagerEvent facts to their owners
  -> process application events
  -> run the full ordered manager pass
  -> publish ReactorSchedule
  -> publish staged BackgroundEvents and execute pre-I/O ReactorActions
  -> NetworkClientDelegate.poll()
  -> poll affected owners after I/O
  -> republish schedule, events, and post-I/O actions
  -> reap compatibility-path application events and drain selected final actions
```

### Verification of the previous findings

| Previous finding | Fable's re-review conclusion |
| --- | --- |
| H1 stale post-I/O request | **Resolved.** Heartbeat, Streams heartbeat, and commit publish a pending coordinator observation with `awaitEvent()` before they can admit another request. `testRealHeartbeatInvalidationIsRoutedBeforeNextNetworkPoll` proves that no stale heartbeat is staged before routing and rediscovery. |
| H2 overbroad publication invariant | **Resolved.** The KIP limits the current guarantee to `ReactorAction` and staged `BackgroundEvent`; direct fetch-buffer signals, generic futures, callback invokers, acknowledgement events, and timeout reaping are Phase 3 compatibility paths. |
| H3 stale diagrams and dead transitions | **Resolved.** The diagrams describe the next full ordered pass, and the unused coordinator transition values were removed. |
| M1 manager-factory exception visibility | **Resolved.** Manager polling failures are logged, counted, and converted to an event wait without synthesizing a user-visible `ErrorEvent` or skipping network I/O. |
| M2 manager deadline withdrawal | **Resolved.** `awaitEvent()` withdraws the manager's obsolete finite deadline; a finite early re-poll still cannot move an unexpired deadline later. |
| M3 wake-count claim | **Resolved.** The KIP and metric describe current per-phase coalescing and retain per-iteration coalescing as a Phase 3 goal. |
| M4 evidence attribution | **Resolved.** The KIP separates mechanisms introduced by the POC, existing regression guards, and future migration evidence. |
| M5/M6 test-plan and table status | **Resolved.** Current evidence and target exit criteria are labelled separately. |
| M7 fetch waiter disclosure | **Resolved.** The bounded single-current-waiter and already-completed duplicate behavior are documented. |
| M8 benchmark scope | **Resolved.** The older report is labelled historical preliminary evidence; current-HEAD throughput, allocation, and reconnect gates remain required for the later phase. |
| Low-severity findings | **Resolved.** Fable confirmed the Gateway Javadoc, schedule-generation increment, dead coordinator API cleanup, phase description, compatibility wording, log rate limiting, and action-diagram changes. |

### Test and evidence assessment

Fable considered the layered evidence appropriate for the current foundational slice:

- `ConsumerReactorTest` covers deterministic ordering, malformed-result handling, manager-failure isolation, deadline
  retention, affected-owner polling, and the real-manager heartbeat invalidation chain;
- `ManagerPollCacheTest`, `ReactorScheduleTest`, and `PendingManagerEventsTest` cover timing and bounded-event
  invariants;
- `AsyncKafkaConsumerTest` covers application integration, schedule generations, paused-partition wake suppression,
  and retained fetch-preparation failure delivery; and
- `BaseConsumerTest.testConsumerProtocolCoordinatorFailoverReactorRecovery` provides real-broker coordinator-loss,
  rediscovery, resumed-heartbeat, consume, and commit evidence for `group.protocol=consumer`.

### Non-blocking follow-ups

1. **Phase 2 — subscription and assignment ownership.** Continue moving cross-thread assignment reconciliation and
   position validation behind reactor-owned application events.
2. **Phase 3 — one synthetic wake per iteration.** Replace the current pre-I/O, post-I/O, and final-drain coalescing
   scopes after the remaining direct futures and publications move behind `ReactorAction`.
3. **Phase 3 — current performance evidence.** Re-run throughput, allocation-per-record, and reconnect-recovery A/B
   gates on the accepted implementation and integrate the harness with normal Kafka performance validation.

### Open questions for community discussion

1. Confirm whether `MAX_POLL_TIMEOUT_MS = 5000` is only a socket-poll safety ceiling or still acts as the scheduler
   for any unmigrated manager, especially topic metadata.
2. Define the eventual `RegularConsumerDriver`, `ShareConsumerDriver`, and `StreamsConsumerDriver` boundaries before
   expanding the snapshot/event families.

### Final verdict

> **APPROVE WITH NON-BLOCKING FOLLOW-UPS**
>
> The implementation at exact HEAD `9164c413ae` fulfills the core architectural, concurrency, state ownership, and
> fencing requirements. The previous H1-H3 and M1-M8 findings are resolved with focused regressions and accurate
> current-versus-target documentation. Remaining work is cleanly scoped to later migration phases and does not block
> approval of the foundational reactor kernel.

### Recorder's accuracy notes

Two symbol-level phrases in the generated response were broader or more literal than the implementation, without
affecting the review reasoning or verdict:

- Fable wrote that `ConsumerReactor.pollManager()` catches `Throwable`; the code intentionally catches
  `RuntimeException`.
- Fable described a `ManagerPollCache.withdrawPendingDeadline()` operation; the behavior exists, but it is implemented
  directly in `PollState.update(...)` rather than as a method with that name.
