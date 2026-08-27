# Fable review: unified manager-output experiment

- Reviewer: external Claude Fable (`claude-fable-5`), read-only
- Reviewed branch: `codex/manager-output-algebra-poc`
- Reviewed HEAD: `1fc9e97acd`
- Review posture: fresh Apache Kafka PMC-level reviewer with no prior conversation context
- Scope: KIP, static diagrams, implementation, tests, phase ordering, liveness, extensibility, debugging, and progressive disclosure

# KIP-1371 審查（HEAD `1fc9e97acd`，唯讀）

## Verdict：**REVISE**

核心方向站得住：`PollResult = NetworkCommand + ManagerEvent + NextPollCondition` 的三分法在語義上是乾淨的，coordinator 這條 vertical slice（snapshot version → observation → deferred command → fenced apply → next full pass → next network poll）有真實 manager 的測試證明。但目前有兩個 P1 級的「失敗模式會讓 consumer 停擺或丟 fact」設計缺口、KIP 與程式碼／圖表在 current-vs-target 上有幾處不一致，且 share 路徑實質未遷移。這些都是小修改可解，不需要推翻設計。

---

## P1 發現

### P1-1 pre-I/O fail-fast 會丟棄 fact 並跳過 network poll
- 證據：`ConsumerReactor.stageManagerEventBatch` (`ConsumerReactor.java:464-470`) 在 `PollPhase.PRE_IO` 收到 cross-owner command 時 `throw IllegalStateException`。此時 `PendingManagerEvents.publishWith` 已 `events.clear()`（`PendingManagerEvents.java:56-58`），command 尚未加入 `deferredManagerCommands` → observation 永久遺失；`run()` 吞掉例外後下一輪繼續，但本輪 `networkClientDelegate.poll` 未執行。
- 可達路徑：post-I/O `pollAffectedManagers` 呼叫的 manager `poll` 拋 RuntimeException → `pollManager` 回 `awaitEvent()`（`ConsumerReactor.java:478-483`），manager 內的 `pendingManagerEvents` 仍保留 → 下一輪 pre-I/O 發布 → throw。
- KIP 自己承認「The fail-fast guard is not the final contract」（KIP L471），但目前的 guard 不是安全失敗。
- 最小修改：pre-I/O 遇到 command 改為 defer 到下一輪（與 post-I/O 相同語義）並計數；若堅持「不可達」，用 metric + `log.error` 取代 throw。

### P1-2 未註冊的 `ManagerEvent` 會讓 reactor 每輪在 network poll 前中止
- 證據：`ManagerCoordinationPolicy.handle` (`ManagerCoordinationPolicy.java:57-59`) 對找不到 handler 的 event class 拋 `IllegalArgumentException`；`standard()` 是手動維護的三個 handler。若日後 share/streams 新增一種 event 而漏註冊，`runOnce` 每輪在 `stageManagerEventBatch(PRE_IO)` 拋出，heartbeat 永遠送不出去，consumer 被 group 踢出。`testUnknownManagerEventFailsFast` 把這個行為當成正確性來測。
- 另一個脆弱點：handler 以 `event.getClass()` 為 key，`ManagerEvent.LocalProgress` 若有 constant body 會變成匿名子類而 lookup 失敗。
- 最小修改：建構 `ManagerCoordinationPolicy` 時對 `ManagerEvent.Type` 全集做覆蓋檢查（fail at composition time）；或以 `Type` 而非 class 做 dispatch，讓 `PendingManagerEvents` 與 policy 用同一個 identity。

## P2 發現

### P2-1 `ReactorAction` 把 regular-consumer policy 帶進 kernel 型別
`ReactorAction.Type` 含 `MARK_ASYNC_POLL_RECONCILIATION_COMPLETE / MARK_ASYNC_POLL_VALIDATE_POSITIONS_COMPLETE / COMPLETE_ASYNC_POLL`，直接綁 `AsyncPollEvent`（`ReactorAction.java:31-37, 62-126`）。`ConsumerReactor` 本身確實無 consumer-type switch（claim 成立），但 God-object 的風險轉移到兩處：`ReactorAction` 的封閉 enum，以及 `RequestManagers` 現在同時是 registry、policy owner、wakeup target、background-event publisher、command dispatcher（`RequestManagers.java:53, 69-71, 181-216`）。`applyManagerCommands` 用 `switch(command.type())`，與 event 用 typed handler 的作法不對稱。

### P2-2 `StateTransition` 已是死碼，但 KIP 與圖仍以它為 algebra
- `grep` 顯示 main 裡沒有任何 `StateTransition.X` 生產者，`PendingStateTransitions` 無人使用；`PollResult` 仍保留 `stateTransitions` 欄位、`withStateTransitions`、四個 legacy constructor；`ConsumerReactor.collectStateTransitions` 永遠處理空集合。
- `docs/images/kip-1371-reactor-action-ownership.dot:54,59` 仍寫 `unsentRequests + stateTransitions / timeUntilNextPollMs / awaitEvent()`；`.mmd` 寫 `await event`；`NetworkCommand`/`NextPollCondition` 只出現在 `kip-1371-reactor-architecture.html`。dry-run 的 DR-01 說「Agreed direction; KIP and code are not aligned」——程式碼已對齊，文件沒跟上。

### P2-3 current-vs-target 誠實度：application-side rescans 已移除
KIP Phase 2 列「Replace application-side mutable-state rescans … after equivalent tests exist」為 target，但 `AsyncKafkaConsumer.poll` 的 `numAssignedPartitions()==0 / !hasAllFetchPositions → retryBackoffMs` 已刪除（diff L548-577），且已有 `testPollWaitUsesOnlyPublishedReactorDecision`。這是行為變更（app thread 現在可能等滿使用者 timeout 而非每 `retryBackoffMs` 重掃），應在「Current implementation status」明示並列出其 liveness 依賴：assignment 到達靠 `BackgroundEvent` 發布喚醒、positions 失敗靠 `FETCH_POSITIONS_UPDATE_FAILED`。

### P2-4 `retryAfter(0)` 在 manager.poll 內拋例外，且 poll-failure log 無 rate limit
`AbstractHeartbeatRequestManager.poll` L211-213 與 `StreamsGroupHeartbeatRequestManager.poll` L500 把 `timeToNextHeartbeatMs` 直接餵給 `PollResult.retryAfter`；`NextPollCondition.RetryAfter` 對 0 拋 IAE。我追過 `HeartbeatRequestState.timeToNextHeartbeatMs`，在該分支通常 >0，但這是靠隱含不變量而非型別保證。一旦發生，`pollManager` 每輪 `log.error` 一次（`ConsumerReactor.java:479`，不像 contract violation 有 `managersWithPollResultViolation` 去重）。

### P2-5 network poll 使用過期時間戳（DR-09 已知）
`runOnce` 在 manager pass 前取 `currentTimeMs`，pass、policy、action 執行後仍以它呼叫 `networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs)`（`ConsumerReactor.java:267, 306`）。timeout 會系統性偏長。

### P2-6 Share 路徑未遷移、無 reactor 級測試
`ShareConsumeRequestManager` 仍回 `PollResult.EMPTY` / `new PollResult(requests)`（L155,165,313,526）；`ConsumerReactorTest` 沒有 share composition 的案例。KIP「Regular, share, and Streams consumers use the same execution model」目前只靠繼承 `AbstractHeartbeatRequestManager` 成立。

### P2-7 每輪 iteration 仍可能 wake 兩次
pre-I/O 與 post-I/O 各自 `executeReactorActions`，KIP L470 誠實列為 Phase 3 target；但 `FETCH_REQUEST_TERMINATED` 對每個空 fetch response 都會 wake（`AbstractFetch.java` `onFetchRequestTerminated`），與 trunk 行為等價，並未比 KAFKA-20854 前進。

## P3
- 熱迴圈配置：`managerPollCache.states()`、`retainManagers`、`ReactorSchedule.from` 每輪各配一次集合；註解說避免 Streams API 卻沒避免配置。
- `pollAffectedManagers` 對已是 `Set` 的 `affectedManagers` 再建一個 `scheduledManagers` set（`ConsumerReactor.java:511-516`）。
- `ManagerEvent.source()` 是字串 class name，policy 不使用，只作診斷。
- `awaitEvent()` alias 與 `awaitInput()` 並存，`PollResult` 有 9 個建構入口。

---

## 各項 claim 分類

| Claim | 判定 | 證據 |
|---|---|---|
| 三種輸出語義不重疊 | **Proven**（型別層）| `NetworkCommand`/`ManagerEvent`/`NextPollCondition` 各自 javadoc 與 `PollResult.progress` 驗證；但 `stateTransitions` 第四欄位仍在（P2-2） |
| 空輸出 + 立即重試在結構上不可表達 | **Proven** | `PollResult.progress` 拋 IAE；`satisfiesProgressContract` + `testPersistentEmptyImmediatePollResultIdentifiesManager` |
| 減少 special case | **Partial** | reactor 無 domain switch；但 `RequestManagers.applyManagerCommands` switch、`ReactorAction` 封閉 enum、`BackgroundEventHandler.pendingEvents` 與 `ApplicationEventProcessor.pendingReactorActions` 兩條繞過 `PollResult` 的側通道 |
| Publish-before-effect | **Proven（限 ReactorAction / BackgroundEvent）** | `testManagerEventActionIsExecutedAfterSchedulePublication` 等以 `deadlineObservedByWakeup` 驗證；fetch 資料由 callback 直接 `fetchBuffer.add` 在 schedule 發布前即可見，KIP 應縮小措辭 |
| 較早 deadline 不被抹除 | **Proven** | `ManagerPollCache.preservePendingDeadline`、`testEarlierManagerDeadlineDoesNotEraseLaterManagerDeadline` |
| 過期 compat deadline 不造成 busy loop | **Partial** | `testPersistentZeroCompatibilityWaitDoesNotBusyLoop` 顯示仍有一次 `poll(0)`，之後才 `MAX_POLL_TIMEOUT_MS`；KAFKA-20970 的根因（commit `maximumTimeToWait` 回 0）未遷移 |
| Stale-observation fencing | **Proven** | `CoordinatorRequestManager.handleCoordinatorUnavailableObserved` 嚴格比對 version；`coordinatorVersion++` 只在 known→unknown 與 node 變更；`testDelayedObservationCannotInvalidateRediscoveredCoordinator`、`PendingManagerEventsTest` |
| Cross-owner command 在下一次 network poll 前生效 | **Proven** | `testRealHeartbeatInvalidationIsRoutedBeforeNextNetworkPoll` 用真 manager + `InOrder` 驗證 apply → addAll(FindCoordinator) → poll；integration `testConsumerProtocolCoordinatorFailoverReactorRecovery` |
| 沒有 manager 直接 mutate 他人狀態 | **Proven（coordinator slice）** | main 裡 `markCoordinatorUnknown` 只剩 classic 路徑呼叫；已改 package-private |
| Bounded state | **Proven** | `PendingManagerEvents` 以 `EnumMap<Type>` 上限 = type 數；`deferredManagerCommands` ≤ managers×types；`pendingReactorActions` 每輪 drain |
| Lost wakeup 防護 | **Proven** | `FetchBuffer.wokenup` latch + 先寫 `reactorSchedule` 再 wake；`deliverExpiredApplicationDeadline` 先標 delivered 再 wake |
| 無 consumer-type 分支 | **Partial** | `ConsumerReactor` 成立；`ReactorAction` 綁 `AsyncPollEvent`（P2-1） |
| Share/streams 走同一 kernel | **Partial** | streams heartbeat/topology 已用 snapshot；share consume 未遷移、無測試（P2-6） |
| App-side rescans 已由 schedule 取代 | **Proven 但 KIP 未宣稱** | P2-3 |

## 建議的最小修改（依序）
1. P1-1：pre-I/O command 改 defer + metric；刪 `testPreIoCrossOwnerCommandFailsBeforeNetworkPoll` 改測「不遺失」。
2. P1-2：`ManagerCoordinationPolicy` 建構時檢查 `ManagerEvent.Type.values()` 全覆蓋；dispatch 改用 `Type`。
3. 刪除 `StateTransition`、`PendingStateTransitions`、`collectStateTransitions` 與 `PollResult.stateTransitions`；同步 KIP「Current implementation status」與 `.dot`/`.mmd`。
4. KIP 補記 rescans 已移除及其 liveness 依賴；把「publish before effect」限縮到 ReactorAction/BackgroundEvent。
5. `pollManager` 例外路徑加入去重 log；`retryAfter` 呼叫端以 `Math.max(1, …)` 或改回 `awaitInput` 保護。
6. `runOnce` 在 network poll 前重取 `time.milliseconds()`。

## 建議補測
- post-I/O manager `poll` 拋例外後，下一輪 observation 仍被 route 到 coordinator owner（對應 P1-1）。
- 未註冊 event 在 `RequestManagers` 建構時失敗，而非 runtime。
- share composition：`ShareHeartbeatRequestManager` 收 NOT_COORDINATOR → 下一次 network poll 前送出 FindCoordinator。
- 完整 iteration 只 wake 一次（Phase 3 gate，先以 `@Disabled` 標記目標）。
- close 時 `cleanup` 先 `applyDeferredManagerCommands` 再 `runAtClose` 的順序。
- app thread 在無 assignment 時等待，assignment 到達後由 BackgroundEvent 喚醒（驗證 P2-3 的 liveness）。

## PMC 可能提問
1. 為何 `ReactorAction` 的封閉 enum 含 `AsyncPollEvent` 專屬型別？share 的 acknowledgement completion 要加到同一個 enum 嗎？
2. `FetchBufferHasData`/`LocalProgress` 只映射到 wake，為何不直接是 `ReactorAction`？「fact vs effect」的界線在這兩個型別上看不出實益。
3. Pre-I/O 為何「不可能」產生 cross-owner command？請給出完整枚舉或改為 defer。
4. 移除 app-side rescans 後，`poll(Duration)` 在 metadata 尚未到、無 assignment 的情境下最長等多久？是否有 integration 測試？
5. `reactor-*` 四個 metric 是 public contract 嗎？KIP 寫 `-total` 後綴，sensor 名稱沒有；KIP-1000 級的 metric 命名需對齊。
6. `maximumTimeToWait` 與 `NextPollCondition` 兩條 timing 通道並存到哪個 phase 結束？commit 的 persistent-zero 何時遷移？
7. `CoordinatorSnapshot.version` 與 `memberEpoch` 的關係已說明；但 `OffsetsRequestManager` 的 scope fencing（KAFKA-17066）沒用同一機制，是否另有 snapshot family？
8. Benchmark 只有 idle harness；saturated throughput / allocation-per-record 的 gate 數據在哪？

## 下一步
本審查未修改任何檔案（僅寫入 plan 檔紀錄）。若要處理，建議先做 P1-1、P1-2 與 StateTransition 清理，再更新 KIP/圖表；這三項都能在不動 coordinator slice 語義的前提下完成。

---

## Post-review disposition

This section records the experiment owner's verification after receiving the read-only review. It does not alter
Fable's response above.

| Finding | Disposition | Evidence or remaining limit |
| --- | --- | --- |
| P1-1 pre-I/O fact loss | **Fixed at `de69dca791` for the reachable callback path.** | Callback-produced pending facts are drained at the next input boundary; owner commands apply before any manager poll or network poll. A post-I/O manager-poll failure therefore cannot strand the fact. A fact first created by the pre-I/O manager pass is preserved and diagnosed, but generic stale-send safety remains unproven without replay/cancellation metadata. |
| P1-2 runtime handler omission | **Fixed at `de69dca791`.** | Dispatch now uses `ManagerEvent.Type`; missing, duplicate, or empty handler coverage fails during `ManagerCoordinationPolicy` construction. |
| P2-2 stale output-algebra diagram | **Fixed in the documentation follow-up.** | The action-ownership diagram now shows `ManagerEvent`, `NetworkCommand`, and `NextPollCondition`. Legacy `StateTransition` remains an explicitly isolated compatibility adapter, not part of the target algebra. |
| P2-3 application-side rescans | **KIP corrected.** | Current status now says the POC already removed assignment/position rescans and states the remaining liveness proof obligation. |
| Metric `-total` naming question | **Disproved.** | The constants without `-total` are sensor names. `AsyncConsumerMetricsTest` verifies the exported metrics named `reactor-*-total`, matching the KIP. |
| P2-1, P2-4 through P2-7 | **Open migration/review items.** | Variant-specific `ReactorAction` types, legacy `StateTransition` removal, retry invariant hardening, fresh-time network timeout calculation, share recovery proof, and complete-iteration wake coalescing remain outside this bounded hardening slice. |

Validation after the P1 fixes: 53 focused policy/pending/reactor/composition tests and 681 broader request-manager
tests passed with zero failures; Spotless Java, Checkstyle main/test, and SpotBugs main passed. No change was merged
into the formal POC branch.
