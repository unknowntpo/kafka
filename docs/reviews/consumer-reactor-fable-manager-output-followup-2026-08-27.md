# Fable follow-up: hardened manager-output experiment

- Reviewer: external Claude Fable (`claude-fable-5`), read-only
- Reviewed branch: `codex/manager-output-algebra-poc`
- Reviewed HEAD: `4045577961`
- Purpose: verify the two prior P1 fixes and judge the bounded experiment independently from trunk integration

我已讀完實作、測試、KIP、圖表、dry-run 與前次審查／disposition，現在整理結論。

# KIP-1371 manager-output POC 後續審查（HEAD `4045577961`，唯讀）

## Verdict：**ADOPT**（僅限 bounded experiment，非 trunk 合併）

兩個 P1 都已在 `de69dca791` 真正修掉，且修法是結構性的（input-boundary drain + Type dispatch），不是補丁。剩餘問題全是 P2/P3，且 KIP 對殘餘限制的描述誠實。本審查未執行測試（plan mode 唯讀），disposition 宣稱的 53+681 測試通過我只驗證了測試存在與斷言有效，未重跑。

---

## 已驗證的修復

### P1-1（fact 遺失 / 跳過 network poll）— **已修，含 post-I/O 失敗恢復**
- 新路徑：callback → `PendingManagerEvents.add` → 下一輪 `runOnce` 第一步 `applyDeferredManagerCommands()`，第二步 `stageManagerEvents(requestManagers.drainPendingManagerEvents(), INPUT)`（`ConsumerReactor.java:264-265`）。INPUT phase 直接 `applyManagerCommands`（L471-474），**早於** `processApplicationEvents`、full manager pass（L282-289 建 request）與 `networkClientDelegate.poll`（L307）。
- Post-I/O poll 拋例外：`pollManager` 吞掉並回 `awaitEvent()`（L489-498），fact 仍留在 producer-local buffer（各 manager 的 `publishWith` 未執行到），下一輪 INPUT drain。`testPendingEventSurvivesPostIoPollFailureAndAppliesBeforeNextFullPass`（`ConsumerReactorTest.java:382-432`）以 `ownerCommandApplied` 計數在第三次 manager poll 與第二次 network poll 前斷言 =1，證據充分。
- 真 manager 證明：`testRealHeartbeatInvalidationIsRoutedBeforeNextNetworkPoll`（L707-844）用 `InOrder` 驗 `applyManagerCommands → addAll(FindCoordinator) → poll`，且 stale heartbeat 未送出（`heartbeatRequestCount == 1`）。
- 保險：每個 producer 在 poll 開頭 `if (pendingManagerEvents.hasPendingEvents()) return publishWith(awaitEvent())`（`AbstractHeartbeatRequestManager.java:172-173`、`CommitRequestManager.java:186-187`、Streams L458-459），持有未發布 observation 的 manager 自己不會用 stale coordinator 建 request。

### P1-2（runtime handler 漏註冊）— **已修**
- `ManagerCoordinationPolicy` 建構時對 `EnumSet.allOf(ManagerEvent.Type)` 做覆蓋、重複、空宣告三項檢查（L33-50）；dispatch 改 `handlers.get(event.type())`（L68）。`RequestManagers` 以 field initializer 呼叫 `standard()`（L53），所以 composition 時就失敗。
- 測試：`testStandardPolicyCoversEveryDeclaredSemanticType`、`testMissingSemanticTypeFailsAtPolicyConstruction`、`testDispatchUsesSemanticTypeRatherThanConcreteEventClass`。舊的 `testUnknownManagerEventFailsFast` 已移除。

### 文件對齊
- `kip-1371-reactor-action-ownership.dot:54` 已改為 `ManagerEvent[] / NetworkCommand[] / NextPollCondition`。
- Dry-run DR-01～DR-04、DR-18 狀態已更新為「Implemented / Accepted for the experiment」，並把舊的 fail-fast 要求改寫成 input-boundary + containment（`git show 4045577961`）。
- KIP L366-369 明寫 post-I/O poll 失敗路徑；L478 明寫 app-side rescans 已移除及其 liveness 義務。

---

## 殘餘 PRE_IO 情境的誠實度判斷（問題 3）

**足夠誠實。** KIP L480 與 L510-513 說得很準：「preserved and diagnosed」「containment rather than generic stale-send recovery」「producer must move the fact to the input/post-I/O admission path, or provide dependency-aware cancellation/replay」。程式碼 `ConsumerReactor.java:476-482` 的註解與 `log.error` 一致，`testUnexpectedPreIoCrossOwnerCommandIsRetainedAndEventuallyApplied` 證明不遺失。

我追了可達性：目前所有 `CoordinatorUnavailableObserved` 生產點都在 response callback（heartbeat L383/435/446、commit L917/1065/1279、streams L778/791/893、topology L144/177），callback 只在 `networkClientDelegate.poll` 內觸發，因此 INPUT drain 一定先於 PRE_IO。PRE_IO 出現 command 的唯一窗口是 drain 之後、manager pass 期間有人完成 request future（例如 manager 在 poll 內把過期 request `completeExceptionally`）。這條路徑對實驗來說可接受，但**不該被當成「不可達」**——KIP 沒這麼宣稱，OK。

---

## 剩餘問題

### P2
1. **INPUT/PRE_IO phase 沒有例外隔離，會重現舊 P1 形狀。** `pollManager` 有 try/catch，但 `stageManagerEvents`→`planManagerEvents`→`handler.eventClass().cast(event)`（`ManagerCoordinationPolicy.java:74`）在「新 event class 宣告既有 `Type` 但不是該 handler 的 class」時拋 `ClassCastException`；`applyManagerCommands` 拋例外同理。發生在 INPUT 時 drain 已清空 → fact 遺失；發生在 PRE_IO 時每輪在 `networkClientDelegate.poll` 之前中止。`testDispatchUsesSemanticTypeRatherThanConcreteEventClass` 只因 handler 宣告 `ManagerEvent.class` 才通過，沒覆蓋這個 shape。最小修：`stageManagerEvents` 加 try/catch + metric（與 `pollManager` 同型），或讓 `ManagerEventHandler` 用 `instanceof` 檢查後回 empty plan。
2. **God-object 風險位移，未消除。** `ConsumerReactor` 本體乾淨（無 consumer-type switch，claim 成立）。但：`RequestManagers` 同時是 registry、policy owner（L53）、command dispatcher（L194-208 的 `switch`）、wakeup target（L109/153）、background-event publisher（L218-224）；`ReactorAction.Type` 仍含三個 `AsyncPollEvent` 專屬型別（`ReactorAction.java:31-37`）；reactor 有三條繞過 `PollResult` 的側通道（`applicationEventProcessor.drainReactorActions`、`backgroundEventHandler`、已死的 `pendingStateTransitions`）。command 用 `switch`、event 用 typed handler，不對稱。這是實驗可接受、整合前必須收斂的邊界問題。
3. **`StateTransition` 已是死碼但 KIP 仍稱 adapter。** main 裡零生產者（僅 `NetworkClientDelegate.java:506`、`ConsumerReactor.java:123` 的容器宣告），只剩測試在用（`ConsumerReactorTest.java:449`）。KIP L250/L473 說「remain adapters」比實況保守；`PollResult` 仍有 9 個建構入口、4 個欄位。建議直接刪除或在 KIP 標「dead, pending removal」。
4. **DR-09 未動：network poll 用過期時間戳。** `runOnce` L268 取 `currentTimeMs`，經 manager pass、policy、action 執行後仍以它呼叫 `poll(pollWaitTimeMs, currentTimeMs)`（L307）。disposition 已列 open。
5. **`retryAfter(0)` 靠隱含不變量。** `AbstractHeartbeatRequestManager` / Streams heartbeat 直接把 `timeToNextHeartbeatMs` 餵給 `PollResult.retryAfter`，`NextPollCondition.RetryAfter` 對 0 拋 IAE，被 `pollManager` 攔下後每輪 `log.error` 無去重。前次已列，未改。
6. **Share 路徑仍未遷移、無 reactor 級 recovery 測試。** `ShareConsumeRequestManager` 6 處 legacy `PollResult`；KIP L482 已誠實列出。

### P3
- `.mmd:65` 仍寫「await event」，目標詞彙是 `awaitInput`；`kafka-20253-before-after.dot:68` 的 `timeUntilNextPollMs = 0` 是 before 圖，可接受。
- `pollAffectedManagers` 對已是 `Set` 的 `affectedManagers` 再建 set（L526-531）。
- `awaitEvent()` alias 與 `awaitInput()` 並存。

---

## 算法與文件重新評估（問題 4、5）

- **`PollResult` 三分法**：`NetworkCommand`（transport intent）/ `ManagerEvent`（fact）/ `NextPollCondition`（local timing）在型別層互不重疊；`progress` 拒絕空輸出 + `PollImmediately`（`NetworkClientDelegate.java:416-417`），`RetryAfter` 拒絕 0 與 ∞。KIP §2 的表格與範例和程式碼一致。`FetchBufferHasData`/`LocalProgress` 只映射到 wake，fact-vs-effect 的界線在這兩型上實益仍薄（前次 PMC 提問 2 未回答），但不影響實驗。
- **Current-vs-target**：KIP 的「Current implementation status」表（L471-482）逐項分欄，Phase 1-3 與 Test Plan 用「current:」「target:」前綴標記；§7 的三個 Driver 明寫「do not yet exist」。§2 的 `record PollResult` 標為 target contract，實作是 class + public 欄位，一致。
- **Progressive disclosure**：Summary → Motivation → §1-7 模型 → 現況表 → 名稱對照（明寫「implementation evidence, not prerequisites」）→ Phases → Test Plan。順序合理。
- **Claimed tests**：`ConsumerReactorTest` 中 8 個與本 slice 相關的測試存在且斷言具體；`ManagerCoordinationPolicyTest` 3 個、`PendingManagerEventsTest` 3 個、`RequestManagersTest.testCoordinatorInvalidationIsRoutedToStateOwner` 存在。**未由本審查執行**。

---

## 整合前的最低 gate

1. 補 P2-1 的 phase 級例外隔離與對應測試（mis-typed event 不得遺失 fact、不得跳過 network poll）。
2. 刪除 `StateTransition` / `PendingStateTransitions` / `collectStateTransitions`，`PollResult` 收斂到 typed 建構子 + 一個 legacy adapter。
3. 用 `handler` 或 typed command 取代 `RequestManagers.applyManagerCommands` 的 `switch`，並把 `ReactorAction` 的 AsyncPoll 型別移出 kernel enum（或在 KIP 明確承認 `ReactorAction` 是 regular-consumer policy）。
4. 執行並記錄：`./gradlew clients:test --tests "*ConsumerReactorTest" --tests "*ManagerCoordinationPolicyTest" --tests "*PendingManagerEventsTest" --tests "*RequestManagersTest"`，加上 share composition 的 NOT_COORDINATOR → FindCoordinator 測試。

上述 1-2 不動 coordinator slice 語義；3 是設計討論，可與 KIP 討論串並行。
