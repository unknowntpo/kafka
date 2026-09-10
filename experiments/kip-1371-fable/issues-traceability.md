# KIP-1371 Issue Traceability（以 trunk HEAD 74fbd50061 對照）

- 對照基準：worktree `kip-1371-fable`，HEAD `74fbd50061` == upstream/trunk（2026-09-10）。
- 來源：KIP 頁面（cwiki 449282795）、umbrella JIRA KAFKA-20995 與其引用的 issue、對應 GitHub PR diff、以及 `clients/src/main/java/org/apache/kafka/clients/consumer/internals/` 的 HEAD 原始碼。
- 「Root cause（程式碼觀點）」一律由 HEAD 程式碼與 PR diff 推導，不沿用 KIP 文字。
- 行號皆為 HEAD 行號；路徑前綴 `…/consumer/internals/` 省略。

## 模型缺口分類（Model-gap category）

| 代號 | 定義 |
|---|---|
| (a) | 被阻塞的工作回報 0 等待時間 → busy loop |
| (b) | 請求/回應的 scope 洩漏到別的操作，或遲到的回應套用到較新的狀態 |
| (c) | position / consumed / auto-commit / 結果交付 之間的發布順序 |
| (d) | 錯誤交付與「無進度的 wakeup」/ 交錯等待 |
| (e) | callback 確認與中斷 |
| (f) | close 與終止時對 pending 工作的責任歸屬 |
| (g) | 其他 |

## 總表

| Issue | 標題（縮寫） | 狀態 | Fix PR / merge commit | 在 HEAD？ | 分類 | 修法範圍 | trunk 迴歸測試 |
|---|---|---|---|---|---|---|---|
| KAFKA-20995 | KIP-1371 umbrella | Open | — | n/a | — | — | — |
| KAFKA-20253 | High CPU loop after failed re-authentication | Resolved/Fixed (4.2.2, 4.3.2, 4.4.0) | #22836 `28de22de34`（async/share）；#22073 `01f50af8b8`（classic，本文不涉） | 是 | (a) | 局部（3 個 manager 各加 guard） | `ConsumerHeartbeatRequestManagerTest.testMaximumTimeToWaitWhenCoordinatorUnavailableDoesNotSpin`、`ShareHeartbeatRequestManagerTest.testMaximumTimeToWaitWhenHeartbeatShouldBeSkippedDoesNotSpin`、`CoordinatorRequestManagerTest.testNoBusyPollWhileFindCoordinatorRequestInFlight` |
| KAFKA-20426 | group.id + assign() busy loop | Resolved/Fixed (4.3.0) | #22018 `44bafc60e7`（#22173 `e1a062cc07` 為 4.3 backport，不在 HEAD 祖先鏈） | 是 | (a) | 局部（單一 manager） | `AsyncKafkaConsumerTest.testPollWithManualAssignmentDoesNotBusyLoop`、`ConsumerHeartbeatRequestManagerTest.testMaximumTimeToWaitWhenHeartbeatShouldBeSkipped` |
| KAFKA-20970 | auto.commit.interval.ms < bootstrap.resolve.timeout.ms busy loop | Resolved/Fixed (4.4.0) | #23227 `ec65e09a04` | 是 | (a) | 局部（Commit + Streams HB 兩個 manager） | `CommitRequestManagerTest.testMaximumTimeToWaitWhenCoordinatorUnknownDoesNotSpin`、`…DoesNotSpinDuringRealBootstrapDnsResolution`、`StreamsGroupHeartbeatRequestManagerTest.testMaximumTimeToWaitWhenCoordinatorUnknownDoesNotSpin` |
| KAFKA-21010 | JOINING + DNS 未完成 busy loop | Resolved/Fixed (4.4.0) | #23348 `db6f147797` | 是 | (a) | 局部（3 處把 interval 換成 retryBackoff） | `ConsumerHeartbeatRequestManagerTest.testMaximumTimeToWaitWhenJoiningAndCoordinatorUnknownDoesNotSpin`、`…WhenFencedWaitsRetryBackoff`、`…WhenFatalReturnsMaxValue`、Share 同名測試 |
| KAFKA-21031 | heartbeat in flight + timer 已過期 busy loop | Open (fixVersion 4.5.0) | #23357 OPEN | **否** | (a) | 局部（`HeartbeatRequestState`） | 無（PR 內含 `testMaximumTimeToWaitWhileHeartbeatInFlightDoesNotSpin`） |
| KAFKA-20854 | KIP-909 後更明顯的 fetch busy loop | Resolved/Fixed (4.4.0, blocker) | #23014 `7ff5d7b71c` | 是 | (d)（兼 (a)） | 結構性（fetch 路徑：新 `FetchRequestPreparationResult`、`FetchRequestManager.maximumTimeToWait`、`pollForFetches` 重寫） | `FetchRequestManagerTest.testNoFetchablePartitionsDoesNotWakeUpBuffer`、`testPartitionsSkippedDueToBackoffDoesNotWakeUpBuffer`、`testMaximumTimeToWait*`（4 個）、`testEmpty/Failed/FetchSessionErrorResponseWakesUpBuffer` |
| KAFKA-17066 | updateFetchPositions 全部移到背景執行緒 | Resolved/Fixed (4.0.0) | #16885 `6744a718c2` | 是 | (b) | 結構性（整個流程改為單一背景 event） | `OffsetsRequestManagerTest.testUpdatePositionsWithCommittedOffsets`、`…ReusesRequest`、`…DoesNotApplyOffsetsIfPartitionNotInitializingAnymore` |
| KAFKA-17674 | 新增 partition 在取 committed offset 前被 reset | Resolved/Fixed (4.0.0) | #17342 `1962917436` | 是 | (b) | 局部（固定 `initializingPartitions` 集合） | `OffsetsRequestManagerTest.testUpdatePositionsDoesNotResetPositionBeforeRetrievingOffsetsForNewlyAddedPartition` |
| KAFKA-15529 / PR 21476 | isConsumed 與 position 更新的 race | Resolved/Fixed (4.4.0) | #21476 `5d03ccff57` | 是 | (c) | 局部（`CompletedFetch` + `FetchCollector`） | `FetchCollectorTest.testPositionUpdatedBeforeDrainOnExhaustedFetch` |
| KAFKA-18641 | auto-commit 可能弄丟紀錄 | Resolved/Fixed (4.0.0) | #18737 `709bfc506a` | **部分** | (c) | 結構性（PollEvent/CommitEvent 加 future、app thread 阻塞） | `ConsumerMembershipManagerTest.testPollMustCallsMaybeReconcileWithFalse`、`ApplicationEventProcessorTest.testAsyncCommitEvent/testSyncCommitEvent`；interval auto-commit 順序已無測試 |
| KAFKA-20397 | metadata error 與 fetch-buffer 等待的 race | Open | #21991 OPEN | **否** | (d) | 局部（proposed：block 前再檢查一次） | 無（PR 內含 `testPollSurfacesMetadataErrorWithoutWastingFetchWaitInterval`） |
| KAFKA-18160 | onPartitionsAssigned 被 wakeup/interrupt 時 CallbackCompletedEvent 被跳過 | Resolved/Fixed (4.0.0) | #18089 `0815d70592` | 是 | (e) | 局部 | `ConsumerIntegrationTest.testFetchPartitionsAfterFailedListenerWithGroupProtocolConsumer`、`…WithAlwaysFailedListener…`、`ConsumerMembershipManagerTest.testOnPartitionsLostError` |
| KAFKA-18569 | close 等待不需要的 FindCoordinator | Resolved/Fixed (4.0.0) | #18590 `9dd73d43b0`；#19402 `6c3995b954`（share port） | 是 | (f) | 局部（新 event + `signalClose`） | `ShareConsumerImplTest.testStopFindCoordinatorOnClose`、`ConsumerBounceTest.testRollingBrokerRestartsWithSmallerMaxGroupSizeConfigDisruptsBigGroup`（已對所有 protocol 參數化）；`CoordinatorRequestManagerTest.testSignalOnClose` 在 HEAD 已不存在 |
| KAFKA-19357 | close hang：commitAsync 因無 coordinator 永不完成 | Resolved/Fixed (4.2.0) | #19914 `92169b8f08` | 是 | (f) | 局部（單一 manager） | `CommitRequestManagerTest.testPollWithClosingAndPendingRequests`、`testPollWithFatalErrorDuringCoordinatorIsEmptyAndClosing`、`PlaintextConsumerCommitTest.testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose` |

分類統計（不含 umbrella，共 14 件）：(a) 5、(b) 2、(c) 2、(d) 2、(e) 1、(f) 2、(g) 0。

HEAD 上仍未修復：KAFKA-21031、KAFKA-20397；KAFKA-18641 的 interval auto-commit 順序保證已被後續變更削弱（見該節）。另有相關 open issue：KAFKA-21049（retry.backoff.ms=0 時 fetch 等待仍為 0，分類 (a)）、KAFKA-20540（Streams HB manager 在 UNSUBSCRIBED 仍可能 busy loop，分類 (a)）、KAFKA-19804（heartbeat interval 初值 0）。

## KIP 頁面摘要

- 標題：KIP-1371 Formalize Consumer Reactor Cross-Manager Coordination and Publication（JIRA 內連結標題為「Introduce a Consumer Reactor for State Management and Event Processing」，兩者為同一頁）。狀態 Draft。
- 公開介面變更：**無** Consumer / ShareConsumer API、callback 執行保證、thread 名稱、既有 metrics 的變更。新增 4 個單調累加 metric：`reactor-invalid-poll-result-total`、`reactor-manager-poll-failure-total`、`reactor-action-failure-total`、`reactor-application-wakeup-total`。
- 提出的機制：
  - `PollResult(networkCommands, events, nextPoll)`，其中 `NextPollCondition` 三選一：`progress(...)`（有輸出時才可 poll immediately）、`retryAfter(delayMs)`（時間可解鎖）、`awaitInput()`（只有外部輸入可解鎖）。空輸出 + PollImmediately 視為無效。
  - Reactor 每輪固定順序：套用上一輪 `ManagerCommand` → drain `ManagerEvent` → drain application command → 依固定順序 poll 所有 manager → 驗證 pre-I/O 結果不首次產生 cross-owner fact → 發布一份不可變 `ReactorSchedule` → 釋出 `BackgroundEvent` → 執行 `ReactorAction` → 網路 I/O（不超過已發布 deadline）→ 對完成的 manager 再 poll。
  - Single-writer state + 版本化觀察（`ManagerEvent(observedVersion)` → `ManagerCommand` → 由 owner 比對版本後才套用）。
  - Publication-before-effect：先發布 schedule/state，再 complete / notify / wake application。
  - `ConsumerNetworkThread` 更名為 `ConsumerReactor`；分三個 phase 遷移。
- 明列的 rejected alternatives：只做局部修補；由 reactor 從原始 delay 推斷進度；通用 readiness kernel / signal registry；把所有 consumer 邏輯集中到 reactor；regular 與 share 各自一套 reactor；一次到位全部遷移；動態 manager 依賴圖；pre-I/O 發現 cross-owner fact 後 replay/cancel transport work。

## 各 issue 細節

### KAFKA-20253 — High CPU loop on consumer after failed re-authentication

- 狀態：Resolved/Fixed，fixVersions 4.2.2 / 4.3.2 / 4.4.0。async/share 修法 #22836（`28de22de34`，在 HEAD）；classic 修法 #22073（`01f50af8b8`）。另有兩個未合併的替代 PR #21714、#21790。
- Root cause（程式碼觀點）：re-auth 失敗會讓 coordinator 變 unknown。此時 `AbstractHeartbeatRequestManager.poll()`（`AbstractHeartbeatRequestManager.java:165-169`）直接回 `EMPTY`、不送 heartbeat，但 heartbeat timer 一直維持過期。修法前的 `maximumTimeToWait()` 沒有對應的 guard，掉到 timer 分支後回 0；同樣地 `CommitRequestManager.AutoCommitState.remainingMs()` 在 timer 過期但前一個 auto-commit 仍 in flight 時回 0；`CoordinatorRequestManager.poll()` 在 FindCoordinator in flight 而 backoff 已耗盡時回 `PollResult(0)`。三個 0 經 `ConsumerNetworkThread.runOnce()`（`ConsumerNetworkThread.java:222-237`）取 min 後同時把背景 `networkClientDelegate.poll(0)` 與 app thread 的 `pollForFetches` timeout 壓成 0。
- HEAD 證據：`AbstractHeartbeatRequestManager.java:275-277`（coordinator 空或 shouldSkipHeartbeat → `retryBackoffMs()`，經 KAFKA-21010 從 `heartbeatIntervalMs()` 改過）；`CommitRequestManager.java:1567-1569`（timer 過期且 `hasInflightCommit` → 回 `autoCommitInterval`）；`CoordinatorRequestManager.java:113-115`（in flight → `EMPTY`）。
- 分類：(a)。修法範圍：局部，三個 manager 各自重建「poll() 不會送出請求」的條件。KIP 敘述與程式碼一致。

### KAFKA-20426 — Using both group.id and assign() causes a busy loop

- 狀態：Resolved/Fixed，4.3.0。#22018（`44bafc60e7`，在 HEAD）。#22173（`e1a062cc07`）是 4.3 分支 backport，不在 HEAD 祖先鏈。
- Root cause（程式碼觀點）：`assign()` 搭配 `group.id` 時 member 停在 `UNSUBSCRIBED`，`poll()` 因 `shouldSkipHeartbeat()` 回 `EMPTY`。`HeartbeatRequestState` 以 interval 0 建立（`AbstractHeartbeatRequestManager.java:117`），所以 `heartbeatTimer` 立刻過期，`timeToNextHeartbeatMs()`（`HeartbeatRequestState.java:71-76`）落到 `remainingBackoffMs()`，而 `lastReceivedMs == -1` 使其為 0；`maximumTimeToWait()` 最後一行 `Math.min(pollTimer.remainingMs()/2, 0)` 回 0，app thread 的 `pollForFetches` 立即返回。
- HEAD 證據：`AbstractHeartbeatRequestManager.java:259-261`（UNSUBSCRIBED / FATAL → `Long.MAX_VALUE`）。
- 分類：(a)。修法範圍：局部。相關 open：KAFKA-20540（`StreamsGroupHeartbeatRequestManager.maximumTimeToWait()` `StreamsGroupHeartbeatRequestManager.java:531-551` 沒有對應的 UNSUBSCRIBED 分支）、KAFKA-19804（interval 初值 0 的根因未動）。

### KAFKA-20970 — busy loop if auto.commit.interval.ms < bootstrap.resolve.timeout.ms

- 狀態：Resolved/Fixed，4.4.0。#23227（`ec65e09a04`，在 HEAD）。
- Root cause（程式碼觀點）：KIP-909 後 bootstrap DNS 解析期間 coordinator 為空，`CommitRequestManager.poll()`（`CommitRequestManager.java:183-193`）回 `EMPTY` 不送 auto-commit，timer 卻不會被 reset（reset 只在成功送出 auto-commit 後，`CommitRequestManager.java:299`）。`maximumTimeToWait()` 直接回 `remainingMs()`，interval 一過就永遠 0。Streams HB manager 的 `shouldNotWaitForHeartbeatInterval()` 分支同樣在 coordinator 未知時持續回 0。
- HEAD 證據：`CommitRequestManager.java:234-236`（coordinator 空 → `retryBackoffMs`）；`StreamsGroupHeartbeatRequestManager.java:544-546`。
- 分類：(a)。修法範圍：局部（兩個 manager）。KIP 敘述與程式碼一致。

### KAFKA-21010 — busy loop if member is JOINING and bootstrap DNS resolution not finished

- 狀態：Resolved/Fixed，4.4.0。#23348（`db6f147797`，在 HEAD；4.4 cherry-pick `d12e95da90`）。
- Root cause（程式碼觀點）：KAFKA-20253/20970 的 guard 在 coordinator 未知時回 `heartbeatRequestState.heartbeatIntervalMs()`，但 interval 在收到第一個 heartbeat response 前一直是 0（`AbstractHeartbeatRequestManager.java:117`、`HeartbeatRequestState.java:102-107` 才更新），所以 JOINING 且 DNS 未完成時 `maximumTimeToWait()` 仍回 0。修法把三處回傳改為 `retryBackoffMs()`（`HeartbeatRequestState.java:63-65` 新增），並把 FATAL 併入 `Long.MAX_VALUE` 分支。
- HEAD 證據：`AbstractHeartbeatRequestManager.java:273-277`、`StreamsGroupHeartbeatRequestManager.java:544-546`、`CommitRequestManager.java:234-236`。
- 分類：(a)。修法範圍：局部。這是 20253 → 20970 → 21010 三次針對同一個 `maximumTimeToWait()` 的連續補丁，正是 KIP 所謂「每個 guard 都得重建依賴」的實例。

### KAFKA-21031 — busy loop while a heartbeat is in flight and the heartbeat timer is expired

- 狀態：Open（fixVersion 4.5.0）。#23357 OPEN。HEAD **未修**。
- Root cause（程式碼觀點，HEAD 可直接讀出）：`AbstractHeartbeatRequestManager.maximumTimeToWait()` 第 278 行的 `shouldHeartbeatNow() && !requestInFlight()` 在 in flight 時為 false，接著第 281 行呼叫 `timeToNextHeartbeatMs()`；timer 已過期時它回 `remainingBackoffMs()`（`HeartbeatRequestState.java:72-73`），而 `RequestState.remainingBackoffMs()`（`RequestState.java:137-139`）是從 `lastReceivedMs` 算起，request 送出後沒有新的 response，所以 (1) JOINING 首個 heartbeat：interval 0 + `lastReceivedMs=-1` → 0；(2) STABLE 之後 response 慢於 interval：`backoffMs = retry.backoff.ms` 從上一個 response 起算，過了就 0。`poll()` 第 194-195 行也回同一個值，所以背景 thread 一起 spin，直到 request 完成或 `request.timeout.ms`。
- 修法方向（PR 23357）：在 `timeToNextHeartbeatMs()` 加 `requestInFlight()` 分支回 `max(1, initialInterval)`。分類：(a)。修法範圍：局部。此案說明 KAFKA-20253 在第 278 行加的 `!requestInFlight()` guard 只擋住「立即 heartbeat」分支，卻讓 in-flight 狀態掉到同樣回 0 的 fallthrough。

### KAFKA-20854 — A more obvious busy loop due to KIP-909

- 狀態：Resolved/Fixed，4.4.0（blocker）。#23014（`7ff5d7b71c`，在 HEAD）。完成 KAFKA-20904，KAFKA-20915 標為 duplicate。
- Root cause（程式碼觀點）：修法前 `FetchRequestManager.poll()` 只要 `prepareFetchRequests()` 回空 map 就無條件 `fetchBuffer.wakeup()`；但空 map 的原因包含「沒有 fetchable partition」「leader 未知」「node 在 reconnect backoff」「node 已有 in-flight fetch」等無法立即進展的狀態。app thread 在 `pollForFetches` 的 `fetchBuffer.awaitWakeup()` 被叫醒後回到 `poll()` 迴圈，`checkInflightPoll` 再送一個 `AsyncPollEvent`，背景再度回空、再 wakeup，形成 app/background ping-pong。KIP-909 讓 DNS 解析期間「沒有 node」的窗口拉長到 `bootstrap.resolve.timeout.ms`，使問題明顯。
- HEAD 證據：`AbstractFetch.java:434-442`（只有「所有 fetchable partition 都已 buffered」才允許 wake）、`AbstractFetch.java:496`（有 fetchable 但都被 skip 時不 wake）、`FetchRequestManager.java:165-172`（依 flag 決定 wake）、`FetchRequestManager.java:83-84`（新 `maximumTimeToWait`：無 in-flight fetch 時回 `retryBackoffMs`，否則 `MAX_VALUE`）、`AbstractFetch.java:290-297`（每個 node 的 request 完成一律 wake）、`AsyncKafkaConsumer.java:1988-2013`（`pollForFetches` 以 app-thread 狀態把等待時間收斂到 `retryBackoffMs`）。
- 分類：(d)「無進度的 wakeup」，同時具有 (a) 性質（空結果被當成可立即重試）。修法範圍：結構性（fetch 路徑內引入型別化的「可否 wake」結果，接近 KIP 的 `NextPollCondition` 雛形）。後續 open：KAFKA-21049（`retry.backoff.ms=0` 時 `FetchRequestManager.maximumTimeToWait()` 與 `pollForFetches` 的下限都是 0）。

### KAFKA-17066 — updateFetchPositions should perform all operations in background thread

- 狀態：Resolved/Fixed，4.0.0。#16885（`6744a718c2`，在 HEAD）。
- Root cause（程式碼觀點）：修法前 `AsyncKafkaConsumer.updateFetchPositions()` 在 app thread 先讀 `subscriptions.initializingPartitions()` 送 `FetchCommittedOffsetsEvent`，等結果回來後再在 app thread 讀 `subscriptions.partitionsNeedingReset` 送 reset event。兩次讀取之間背景 thread 可能完成 reconciliation 把新 partition 加成 INITIALIZING，於是第二步把從未嘗試取 committed offset 的 partition reset 到 partition offset。修法把整個流程改成單一 `CheckAndUpdatePositionsEvent`，由背景的 `OffsetsRequestManager.updateFetchPositions()` 一次完成。
- HEAD 證據：`AsyncKafkaConsumer.java:2094-2105`（app thread 只送一個 event）、`ApplicationEventProcessor.java:456-459`、`OffsetsRequestManager.java:289-318, 334-349`。
- 分類：(b)（operation scope 跨兩個 thread、兩次取樣）。修法範圍：結構性。

### KAFKA-17674 — reset positions for newly added partitions before retrieving committed offsets

- 狀態：Resolved/Fixed，4.0.0。#17342（`1962917436`，在 HEAD）。
- Root cause（程式碼觀點）：17066 移到背景後，`initWithCommittedOffsetsIfNeeded()` 內部自己讀一次 `initializingPartitions()`，而接在 `thenCompose` 後面的 `initWithPartitionOffsetsIfNeeded()` 呼叫 `subscriptionState.resetInitializingPositions()` 時又重新掃全部 `shouldInitialize()` 的 partition。OffsetFetch 回應期間新加入的 partition 會被第二步以 partition offset reset，跳過 committed offset。修法把 `initializingPartitions` 在 operation 開頭固定一次，並以 predicate 傳入 `resetInitializingPositions(...)`。
- HEAD 證據：`OffsetsRequestManager.java:340-348`、`OffsetsRequestManager.java:391-396`。
- 分類：(b)（遲到的 completion 套用到 operation 建立後才 admitted 的 scope）。修法範圍：局部。KIP 敘述與程式碼一致。

### KAFKA-15529 / PR 21476 — race between isConsumed and position updates in fetch path

- 狀態：JIRA KAFKA-15529 標題是 tiered-storage 的 flaky test `ReassignReplicaShrinkTest.executeTieredStorageTest`（Resolved/Fixed，4.4.0）；PR #21476（`5d03ccff57`，在 HEAD）是它的根因修法。KIP 頁面把 PR 21476 歸在 KAFKA-18641 底下，與 PR 實際掛的 JIRA 不符（見末節）。
- Root cause（程式碼觀點）：`FetchCollector.fetchRecords()` 在 app thread 先把 `CompletedFetch` 讀到底（舊碼在耗盡時立刻 `drain()`，把非 volatile 的 `isConsumed` 設 true），之後才 `subscriptions.position(tp, nextPosition)`。背景 thread 的 `FetchBuffer.bufferedPartitions()` / `AbstractFetch.prepareFetchRequests()` 以 `isConsumed` 判斷該 partition 已無 buffered 資料，可在 position 更新前讀到舊 position 而發出重複 fetch。修法：`isConsumed` 改 volatile，耗盡時只標 `exhausted`，等 position 更新後才 `drain()`。
- HEAD 證據：`CompletedFetch.java:81`（volatile）、`CompletedFetch.java:200`（`exhausted = true`）、`FetchCollector.java:185-192`（先 position 後 drain）。
- 分類：(c)（position 發布順序）。修法範圍：局部。

### KAFKA-18641 — AsyncKafkaConsumer could lose records with auto offset commit

- 狀態：Resolved/Fixed，4.0.0。#18737（`709bfc506a`，在 HEAD 祖先鏈）。**在 HEAD 為部分保留。**
- Root cause（程式碼觀點）：`FetchCollector` 在 app thread 於回傳紀錄「之前」就推進 position（`FetchCollector.java:185`），而 `CommitRequestManager.maybeAutoCommitAsync()` 在背景 thread 自由讀 `subscriptions.allConsumed()`（`CommitRequestManager.java:293`）。背景在 app thread 推進 position 後、應用程式處理完紀錄前送出 commit，應用程式此時當掉即遺失紀錄。原修法在 `PollEvent` 加 `reconcileAndAutoCommit` future，app thread 在 `poll()` 進入 fetch 前阻塞等待背景做完 `maybeReconcile(true)` 與 `updateTimerAndMaybeCommit()`；`CommitEvent` 加 `offsetsReady`，讓 `commitSync/commitAsync` 的 `allConsumed()` 在背景取樣後才放行 app thread。
- HEAD 狀況：
  - 保留：`CommitEvent.offsetsReady`（`AsyncKafkaConsumer.java:1152`、`ApplicationEventProcessor.java:258-259, 276-277`）；`maybeReconcile(true)` 在 `AsyncPollEvent` 處理最前面（`ApplicationEventProcessor.java:731-732`）；rebalance 需要 commit 時 app thread 在 `collectFetch()` 等 `reconciliationCheckFuture`（`AsyncKafkaConsumer.java:2048-2066`，KAFKA-20332 `b954b35d0a` 加回）。
  - 被削弱：KAFKA-18376（`72532b6f73`）移除了 app thread 對 `reconcileAndAutoCommit` 的阻塞；HEAD 的 `markReconciliationCheckComplete()`（`ApplicationEventProcessor.java:736`）在 `updateTimerAndMaybeCommit()`（`:740`）之前，且只有 `hasPendingReconciliation` 時 app thread 才等待。因此 interval 驅動的 auto-commit 讀 `allConsumed()` 與 app thread 的 `FetchCollector` 推進 position 之間沒有順序保證，18641 描述的第一種情境（純 interval auto-commit）在 HEAD 又回到可競爭狀態。此判斷來自程式碼閱讀，尚無測試證明；trunk 也沒有針對 interval auto-commit 順序的迴歸測試（18641 當時只修改既有測試，未新增專屬測試；HEAD 測試碼已無任何 `reconcileAndAutoCommit` 參照，`ApplicationEventProcessorTest.testPollEvent` 亦隨 18376 移除）。
- 分類：(c)。修法範圍：結構性（跨 app/background 的 future 交握）。

### KAFKA-20397 — metadata error check before blocking on buffer data

- 狀態：Open。#21991 OPEN。HEAD **未修**。
- Root cause（程式碼觀點，HEAD 可直接讀出）：背景 thread 在 `ConsumerNetworkThread.runOnce()` 末尾以 `maybeFailOnMetadataError(uncompletedEvents)`（`ConsumerNetworkThread.java:241, 441-447`）呼叫 `AsyncPollEvent.onMetadataError()`（`AsyncPollEvent.java:136-137`）把 inflight poll 標成錯誤，但**沒有**任何 `fetchBuffer.wakeup()` 或 `wakeupTrigger` 通知。app thread 只在 `checkInflightPoll()`（`AsyncKafkaConsumer.java:1034-1042, 1069-1076`）讀取 `inflightPoll.error()`，之後進入 `pollForFetches()` 並在 `fetchBuffer.awaitWakeup(pollTimer)`（`:2024`）睡到 timeout。錯誤若在 `checkInflightPoll` 與 `awaitWakeup` 之間產生，最長要等一整個 fetch wait（可達 `maximumTimeToWait()` 或 poll timeout）才拋出。
- 修法方向（PR 21991）：在 `awaitWakeup` 前多檢查一次 `inflightPoll.error().isPresent()`。這是 check-then-block，仍留有窗口；沒有把「錯誤發布」接到 wakeup 路徑。分類：(d)。修法範圍：局部。

### KAFKA-18160 — wakeup/interrupt in onPartitionsAssigned skips ConsumerRebalanceListenerCallbackCompletedEvent

- 狀態：Resolved/Fixed，4.0.0。#18089（`0815d70592`，在 HEAD）。
- Root cause（程式碼觀點）：app thread 的 `invokeRebalanceCallbacks()` 只把 listener 回傳的一般例外包成 event；`WakeupException` / `InterruptException` 從 `rebalanceListenerInvoker` 直接拋出，導致 `applicationEventHandler.add(invokedEvent)`（`AsyncKafkaConsumer.java:284`）不執行，背景 `AbstractMembershipManager` 等待的 callback future 永不完成，reconciliation 卡住。第二個問題：callback 失敗後 `enablePartitionsAwaitingCallback(addedPartitions)` 因 `addedPartitions` 已清空而不再啟用任何 partition，後續 poll 取不到資料。
- HEAD 證據：`AsyncKafkaConsumer.java:2514-2516`（捕捉兩種例外轉成 error event）、`AbstractMembershipManager.java:1266`（改用 `assignedPartitions`）。
- 分類：(e)。修法範圍：局部。

### KAFKA-18569 — close may wait on unneeded FindCoordinator

- 狀態：Resolved/Fixed，4.0.0（blocker）。#18590（`9dd73d43b0`）；#19402（`6c3995b954`）將同樣做法移植到 ShareConsumer。皆在 HEAD。
- Root cause（程式碼觀點）：`close()` 依序做 auto-commit → leave group → 關閉 network thread（`AsyncKafkaConsumer.java:1655-1666`）。commit/leave 完成後、`ConsumerNetworkThread.cleanup()` 前，背景 `runOnce()` 仍每輪 poll 所有 manager，`CoordinatorRequestManager.poll()` 在 coordinator 未知時繼續產生 FindCoordinator；`cleanup()` 的 `sendUnsentRequests()`（`ConsumerNetworkThread.java:396-403`）只要 `hasAnyPendingRequests()` 就迴圈到 close timeout。修法新增 `StopFindCoordinatorOnCloseEvent` → `CoordinatorRequestManager.signalClose()`，之後 `poll()` 直接回 `EMPTY`。
- HEAD 證據：`AsyncKafkaConsumer.java:1657-1658, 1765-1770`、`ApplicationEventProcessor.java:503-508`、`CoordinatorRequestManager.java:85-88, 102-103`。
- 分類：(f)。修法範圍：局部（用一個 close 專用 event 對單一 manager「發訊號」，與 `CommitOnCloseEvent` 同型）。註：HEAD 找不到 `CoordinatorRequestManagerTest` 針對 `signalClose()` 的單元測試（原 `testSignalOnClose` 已不在），剩 `ShareConsumerImplTest.testStopFindCoordinatorOnClose` 與 `ConsumerBounceTest`。

### KAFKA-19357 — close hangs because commitAsync never completes with missing coordinator

- 狀態：Resolved/Fixed，4.2.0。#19914（`92169b8f08`，在 HEAD）。由 KAFKA-19352（flaky `testCommitAsyncCompletedBeforeConsumerCloses`）發現。
- Root cause（程式碼觀點）：KAFKA-16103 讓 `close()` 等待 pending 的 commitAsync（`awaitPendingAsyncCommitsAndExecuteCommitCallbacks`，`AsyncKafkaConsumer.java:1663-1664`）；但 `CommitRequestManager.poll()` 在 coordinator 未知時回 `EMPTY` 並把 commit 留在 `pendingRequests`（`CommitRequestManager.java:183-193`），而 KAFKA-18569 的 `signalClose()` 又讓 coordinator 不會再被找到，於是 commit future 永不完成，close 等到 timeout。修法：`closing && coordinator 未知 && 有 unsent` 時以 `CommitFailedException` 讓全部 pending commit 失敗（`CommitRequestManager.java:186-191`）。
- 分類：(f)。修法範圍：局部。這是兩個 close 修補（16103、18569）在不同 manager 各自決策後互相衝突的實例，對應 KIP「distributed lifecycle dependencies」。

### KAFKA-20995 — umbrella

- Open，無 fix。描述把上列 issue 分成四種 failure shape：urgent work without progress（20253、20426、20970）、ambiguous empty manager results（20854）、publication/wait ordering races（18641、20397）、distributed lifecycle dependencies（18569、19357）。JIRA 描述的第一頁未列 17066/17674/18160/21010/21031，這些出現在 KIP 頁或 issue link 中。

## KIP 敘述與程式碼不一致之處

1. **PR 21476 的歸屬**：KIP 把 PR 21476 列在 KAFKA-18641 下；PR 標題與 merge commit 都掛 KAFKA-15529（tiered-storage flaky test）。兩者確實都是 position 發布順序問題，但 JIRA 對應應更正。
2. **KAFKA-18641 在 HEAD 的狀態**：KIP 與 JIRA 都視為已修；HEAD 上 interval auto-commit 與 position 推進之間的順序保證已被 KAFKA-18376 拿掉、只由 KAFKA-20332 補回 rebalance 情境。若 KIP 要以 18641 當「publication ordering」證據，應說明它目前是「部分修復」而非「已由局部修法解決」。
3. **KAFKA-20253 的 in-flight guard**：KIP 把 20253 描述為 heartbeat/coordinator/auto-commit 三者的 deadline 問題，這與 diff 一致；但 HEAD 顯示該修法在 heartbeat manager 只擋了「立即 heartbeat」分支，in-flight 狀態仍由 `timeToNextHeartbeatMs()` 回 0（KAFKA-21031）。KIP 若引用 20253 為「已由局部 guard 修好」，需補充 21031 仍 open。
4. **KAFKA-20854 的分類**：KIP 稱其為「empty fetch-preparation results triggered application wakeups」，程式碼確認無誤；但 PR 同時新增 `FetchRequestManager.maximumTimeToWait()` 並重寫 `pollForFetches` 的等待收斂，實際修法同時處理 (a) 與 (d)，且留下 KAFKA-21049（`retry.backoff.ms=0`）尚未收斂。
5. **KAFKA-20397 的修法方向**：KIP 說根因是「metadata-error publication raced with application entering its fetch-buffer wait」，程式碼確認。但目前唯一的 PR 21991 只是 block 前再檢查一次，並未讓錯誤發布觸發 wakeup；若 KIP 想主張「publication before effect」能根治，應指出 21991 仍是 check-then-block。

## 附：本次核對用到的 HEAD 位置速查

| 主題 | 位置 |
|---|---|
| 背景迴圈與 wait 聚合 | `ConsumerNetworkThread.java:210-242`（`runOnce`）、`:343-345`（`cachedMaximumTimeToWait`） |
| heartbeat wait 計算 | `AbstractHeartbeatRequestManager.java:164-200`（`poll`）、`:253-282`（`maximumTimeToWait`）；`HeartbeatRequestState.java:63-76`；`RequestState.java:78-92, 137-139` |
| auto-commit wait 計算 | `CommitRequestManager.java:181-209`（`poll`）、`:223-238`（`maximumTimeToWait`）、`:1540-1571`（`AutoCommitState`） |
| FindCoordinator | `CoordinatorRequestManager.java:85-88, 101-118` |
| fetch wake / wait | `FetchRequestManager.java:83-84, 155-172`；`AbstractFetch.java:290-297, 425-442, 496`；`FetchBuffer.java:165-199` |
| app thread poll 路徑 | `AsyncKafkaConsumer.java:948-977`（`poll`）、`:993-1088`（`checkInflightPoll`）、`:1980-2030`（`pollForFetches`）、`:2042-2066`（`collectFetch`） |
| AsyncPollEvent 處理 | `ApplicationEventProcessor.java:728-770` |
| position 初始化 | `OffsetsRequestManager.java:289-352, 391-396, 418-446` |
| position 發布 | `FetchCollector.java:178-192`；`CompletedFetch.java:81, 144-149, 200` |
| close | `AsyncKafkaConsumer.java:1642-1672, 1700-1708, 1749-1770`；`ConsumerNetworkThread.java:379-435` |
| rebalance callback | `AsyncKafkaConsumer.java:255-285, 2491-2526`；`AbstractMembershipManager.java:875, 1266, 1467` |
