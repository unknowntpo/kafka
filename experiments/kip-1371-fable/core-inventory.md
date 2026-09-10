# Async consumer background core 程式碼盤點（KIP-1371 前置）

- 基準：apache/kafka trunk，worktree HEAD `74fbd50061`。
- 路徑縮寫：`I/` = `clients/src/main/java/org/apache/kafka/clients/consumer/internals/`，`E/` = `I/events/`，`S/` = `streams/src/main/java/org/apache/kafka/streams/processor/internals/`。
- 所有 `檔案:行號` 皆對應 HEAD 原始檔實際行號。
- 本文只描述現況，不做設計建議。

---

## 0. 三種 consumer 共用的核心元件

| 元件 | 檔案 | 角色 |
|---|---|---|
| `ConsumerNetworkThread` | `I/ConsumerNetworkThread.java` | 背景 thread；事件消化 + manager poll + network I/O |
| `ApplicationEventHandler` | `E/ApplicationEventHandler.java` | app → background 的 queue 包裝，附帶 `wakeup` |
| `ApplicationEventProcessor` | `E/ApplicationEventProcessor.java` | 在背景 thread 上把 event 轉成 manager 呼叫 |
| `BackgroundEventHandler` | `E/BackgroundEventHandler.java` | background → app 的 queue（無 wakeup） |
| `CompletableEventReaper` | `E/CompletableEventReaper.java` | 依 `deadlineMs` 讓 `CompletableEvent` 逾時失敗 |
| `NetworkClientDelegate` | `I/NetworkClientDelegate.java` | `unsentRequests` 佇列 + `KafkaClient.poll` 包裝 |
| `RequestManagers` | `I/RequestManagers.java` | manager 集合與 `entries()` 順序 |
| `SubscriptionState` | `I/SubscriptionState.java` | 兩個 thread 共享的 `synchronized` 狀態 |
| `FetchBuffer` / `ShareFetchBuffer` | `I/FetchBuffer.java`、`I/ShareFetchBuffer.java` | 背景 → app 的資料通道，含 condition wakeup |

三種 consumer 的組裝點：

| Consumer | 組裝 | `notifyMetadataErrorsViaErrorQueue` |
|---|---|---|
| `AsyncKafkaConsumer`（含 Streams 模式） | `RequestManagers.supplier(...)` `I/RequestManagers.java:161-333` | `false`（`I/AsyncKafkaConsumer.java:530`、`:733`）→ metadata error 走 `getAndClearMetadataError` |
| `ShareConsumerImpl` | `RequestManagers.supplier(...)` `I/RequestManagers.java:340-412` | `true`（`I/ShareConsumerImpl.java:297`、`:407`）→ metadata error 直接變 `ErrorEvent` |

Streams 模式：同一個 `AsyncKafkaConsumer`，建構子帶入 `Optional<StreamsRebalanceData>`（`I/AsyncKafkaConsumer.java:445`）；`RequestManagers.supplier` 依 `streamsRebalanceData.isPresent()` 決定建 `StreamsMembershipManager` + `StreamsGroupHeartbeatRequestManager` + `StreamsGroupTopologyDescriptionRequestManager`（`I/RequestManagers.java:229-266`）還是 `ConsumerMembershipManager` + `ConsumerHeartbeatRequestManager`（`:268-301`）。

---

## 1. 背景迴圈：`ConsumerNetworkThread`

### 1.1 `run()` 與 `runOnce()` 的精確順序

`run()`（`I/ConsumerNetworkThread.java:143-173`）：`initializeResources()`（`:149`，失敗則設 `initializationError` 並 return，`:150-155`）→ `initializationLatch.countDown()`（`:157`）→ `while (running) runOnce()`（`:160-167`，任何 Throwable 只記 log 不中斷）→ `finally cleanup()`（`:171`）。

`runOnce()`（`:210-242`）每一輪固定做：

| 步驟 | 程式碼 | 說明 |
|---|---|---|
| 1 | `processApplicationEvents()` `:212` | `drainTo` 整個 queue（`:249`），對每個 event：若是 `CompletableEvent` 先 `applicationEventReaper.add`（`:258-260`）；若是 `MetadataErrorNotifiableEvent` 先檢查 metadata error，有則 `onMetadataError` 並 `continue`（`:264-267`）；否則 `applicationEventProcessor.process(event)`（`:268`）；例外時把 completable event 的 future 標 fail（`:271-273`） |
| 2 | 取 `currentTimeMs` 一次 `:214`，記錄 `timeBetweenNetworkThreadPoll` `:215-218` | 之後所有 manager 都拿同一個 `currentTimeMs` |
| 3 | `pollWaitTimeMs = MAX_POLL_TIMEOUT_MS(5000)` `:220`；對 `requestManagers.entries()` 逐一 `rm.poll(currentTimeMs)` → `networkClientDelegate.addAll(pollResult)` → `pollWaitTimeMs = min(pollWaitTimeMs, timeoutMs)` `:222-226` | **poll 與 addAll 交錯**，每個 manager 的 request 立即進 `unsentRequests` |
| 4 | `networkClientDelegate.poll(pollWaitTimeMs, currentTimeMs)` `:228` | 見 1.2 |
| 5 | 第二次掃描 `rm.maximumTimeToWait(currentTimeMs)` 取 min，寫入 `volatile cachedMaximumTimeToWait` `:230-237` | 注意用的是步驟 2 的舊 `currentTimeMs`，而非 network poll 之後的時間 |
| 6 | `reapExpiredApplicationEvents(currentTimeMs)` `:239` → `applicationEventReaper.reap(currentTimeMs)` `:284-286` | 逾時的 `CompletableEvent` future 被 `completeExceptionally(TimeoutException)`（`E/CompletableEventReaper.java:100-118`） |
| 7 | `uncompletedEvents()` + `maybeFailOnMetadataError` `:240-241` | 只對仍在 tracked 的 `MetadataErrorNotifiableEvent` 傳遞 metadata error（`:441-462`；`getAndClearMetadataError` `I/NetworkClientDelegate.java:282-286`） |

### 1.2 network poll timeout 計算

`NetworkClientDelegate.poll(timeoutMs, currentTimeMs, onClose)`（`I/NetworkClientDelegate.java:161-172`）：

1. `trySend(currentTimeMs)`（`:209-229`）：逐一 `unsent.timer.update`，逾時（`request.timeout.ms`，由 `add` 時 `setTimer` 設定 `:323`）則 `handler.onFailure(TimeoutException)`（`:214-219`）；否則 `doSend`，node 不可用 / 未 ready 則留在 queue（`:231-246`）。
2. **若 `unsentRequests` 非空，`pollTimeoutMs = min(retryBackoffMs, timeoutMs)`**（`:165-167`）。這是第二層截斷：任何送不出去的 request 都會讓迴圈以 `retry.backoff.ms` 節奏醒來。
3. `client.poll(pollTimeoutMs, currentTimeMs)`（`:168`）。`NetworkClient.poll` 內部再取 `min(timeout, metadataTimeout, telemetryTimeout, defaultRequestTimeoutMs)`（`clients/.../NetworkClient.java:708-711`）。
4. `maybePropagateMetadataError()`（`:174-187`）：依 `notifyMetadataErrorsViaErrorQueue` 決定送 `ErrorEvent` 或存到 `metadataError` 欄位（`:189-195`）。`BootstrapResolutionException` 只傳一次（`:177-183`）。
5. `checkDisconnects(currentTimeMs, onClose)`（`:248-265`）：指定 node 且連線失敗 → `onFailure(authException)`；`onClose` 且未指定 node → `onFailure(NETWORK_EXCEPTION)`。

所以真正的等待時間 = `min(5000, min over manager.timeUntilNextPollMs, [有 unsent 則 retryBackoffMs], metadataTimeout, telemetryTimeout, request.timeout.ms)`，再被 `Selector.wakeup()` 打斷。

### 1.3 `maximumTimeToWait` 的意義

`RequestManager.maximumTimeToWait` 預設 `Long.MAX_VALUE`（`I/RequestManager.java:79-81`）。它**不影響背景 thread**，只被快取到 `cachedMaximumTimeToWait`（`:237`），供 app thread 的 `ApplicationEventHandler.maximumTimeToWait()`（`E/ApplicationEventHandler.java:122-124`）讀取，用來決定 `pollForFetches` 在 `FetchBuffer` 上最多等多久（見 §4.2）。javadoc 明說 app thread 不得直接碰 manager，所以用快取（`I/ConsumerNetworkThread.java:331-342`）。

### 1.4 誰喚醒背景 thread（`Selector.wakeup` 呼叫鏈）

`NetworkClientDelegate.wakeup()` → `client.wakeup()`（`I/NetworkClientDelegate.java:292-294`）→ `NetworkClient.wakeup()` → `selector.wakeup()`（`NetworkClient.java:777-779`）。呼叫者：

| 呼叫者 | 位置 | 觸發時機 |
|---|---|---|
| `ConsumerNetworkThread.wakeup()` | `I/ConsumerNetworkThread.java:325-329` | 下列兩者 |
| `ApplicationEventHandler.add(event)` | `E/ApplicationEventHandler.java:96-105` | **每一個** application event 入列後都 `wakeupNetworkThread()`（`:104`） |
| `ApplicationEventHandler.wakeupNetworkThread()` | `:110-112` | `ShareConsumerImpl.collect`/`sendShareAcknowledgeAsyncEvent` 額外再叫一次（`I/ShareConsumerImpl.java:750`、`:771`、`:789`） |
| `ConsumerNetworkThread.closeInternal` | `:382-384` | `running=false` 後 `wakeup()` 再 `join()` |

沒有其他喚醒來源：response 到達由 `Selector` 自己回傳；manager 內部狀態改變（例如 listener 觸發 `transitionTo(ACKNOWLEDGING)`）**不會**喚醒，只能等當前 poll timeout 到期或下一個 app event。

### 1.5 `runAtClose` / `cleanup`

見 §6.4。

---

## 2. `RequestManager` contract 的實際實作

### 2.1 介面

`I/RequestManager.java`：`PollResult poll(long)`（`:46`）、`default PollResult pollOnClose(long)` 回 `EMPTY`（`:64-66`）、`default long maximumTimeToWait(long)` 回 `Long.MAX_VALUE`（`:79-81`）、`default void signalClose()`（`:86`）。

`PollResult`（`I/NetworkClientDelegate.java:328-350`）：`WAIT_FOREVER = Long.MAX_VALUE`、共用單例 `EMPTY = new PollResult(WAIT_FOREVER)`（`:330`）；`PollResult(List)` 與 `PollResult(UnsentRequest)` 的 `timeUntilNextPollMs` 都是 `WAIT_FOREVER`（`:339-345`）。

### 2.2 `entries()` 順序

Consumer / Streams（`I/RequestManagers.java:94-105`）：

1. `CoordinatorRequestManager`（有 group.id 才有）
2. `CommitRequestManager`（同上）
3. `ConsumerHeartbeatRequestManager`（consumer 模式）
4. `ConsumerMembershipManager`（consumer 模式）
5. `StreamsGroupHeartbeatRequestManager`（streams 模式）
6. `StreamsGroupTopologyDescriptionRequestManager`（streams 模式）
7. `StreamsMembershipManager`（streams 模式）
8. `OffsetsRequestManager`（一定有）
9. `TopicMetadataRequestManager`（一定有）
10. `FetchRequestManager`（一定有）

Share（`I/RequestManagers.java:128-133`）：

1. `CoordinatorRequestManager`
2. `ShareHeartbeatRequestManager`
3. `ShareMembershipManager`
4. `ShareConsumeRequestManager`

順序的實際影響：heartbeat 在 membership 之前 poll，所以同一輪內 membership 的 `maybeReconcile` 造成的狀態變化要到**下一輪**才會反映在 heartbeat；`OffsetsRequestManager` 在 `FetchRequestManager` 之前，所以同一輪內由 `AsyncPollEvent` 產生的 ListOffsets/OffsetFetch request 會先進 unsent，再輪到 fetch。

### 2.3 每個 manager 的 `poll` / `maximumTimeToWait` 行為

#### `CoordinatorRequestManager`（`I/CoordinatorRequestManager.java`）

| 狀態 | `poll` 回傳 | 行 |
|---|---|---|
| `closing` 或已知 coordinator | `EMPTY` | `:102-103` |
| 未知且 `canSendRequest` | `PollResult(FindCoordinator request)`（`WAIT_FOREVER`） | `:105-108` |
| 未知且 request in flight | `EMPTY` | `:113-115` |
| 未知且 backoff 中 | `new PollResult(remainingBackoffMs)` | `:117` |

`maximumTimeToWait`：預設 `MAX`。狀態改變來源：response callback `onResponse`/`onFailedResponse`（`:130-138`、`:203-249`）；其他 manager 呼叫 `markCoordinatorUnknown`（`:168-189`）、`handleCoordinatorDisconnect`（`:150-154`）。fatal error 存在 `fatalError` 欄位（`:215`、`:220`），由 heartbeat manager 的 `maybePropagateCoordinatorFatalErrorEvent` 轉成 `ErrorEvent`（`I/AbstractHeartbeatRequestManager.java:300-303`），或 `CommitRequestManager.PendingRequests.maybeFailOnCoordinatorFatalError` 用來 fail 未送出的 commit/fetch（`I/CommitRequestManager.java:1509-1517`）。沒有 0 回傳分支。

#### `AbstractHeartbeatRequestManager`（consumer 與 share 共用；`I/AbstractHeartbeatRequestManager.java`）

| 狀態 | `poll` 回傳 | 行 |
|---|---|---|
| 無 coordinator 或 `shouldSkipHeartbeat()`（UNSUBSCRIBED/FATAL/STALE/FENCED，`I/AbstractMembershipManager.java:801-807`） | `EMPTY`；順帶 `onHeartbeatRequestSkipped` + 傳 coordinator fatal error | `:165-169` |
| `pollTimer` 過期且不在 leaving | 送 leave heartbeat；`PollResult(heartbeatIntervalMs, [leave])` | `:170-185` |
| 不能送（`!canSendRequest && !heartbeatNow`） | `new PollResult(timeToNextHeartbeatMs)` | `:194-195` |
| 可送 | `PollResult(heartbeatIntervalMs, [hb])` | `:198-199` |

`heartbeatNow` = `shouldSendLeaveHeartbeatNow()`（consumer：`state == LEAVING` 且非 dynamic+REMAIN_IN_GROUP，`I/ConsumerHeartbeatRequestManager.java:217-228`）或（`membershipManager().shouldHeartbeatNow()` = ACKNOWLEDGING/LEAVING/JOINING，`I/AbstractMembershipManager.java:739-742`）且無 in-flight（`:189-192`）。

`maximumTimeToWait`（`:252-282`）：UNSUBSCRIBED/FATAL → `MAX`；`pollTimer` 過期 → **0**（`:264-266`）；無 coordinator 或 skip → `retryBackoffMs`（`:275-277`）；`shouldHeartbeatNow && !inFlight` → **0**（`:278-280`）；否則 `min(pollTimer.remainingMs/2, timeToNextHeartbeatMs)`（`:281`）。

狀態改變來源：response callback（`:320-328` → `onResponse` `:365-380` / `onErrorResponse` `:382-492` / `onFailure` `:347-363`）；app event `AsyncPollEvent` 經 `resetPollTimer`（`:289-298`，由 `E/ApplicationEventProcessor.java:746` 呼叫）；membership 狀態由 listener/event 改變。

**零等待分支（KAFKA-21031 型）**：`HeartbeatRequestState` 建構時 `heartbeatIntervalMs = 0`（`:117`；streams 同 `I/StreamsGroupHeartbeatRequestManager.java:419`），`makeHeartbeatRequest` 送出後 `heartbeatRequestState.resetTimer()`（`:310`）把 `heartbeatTimer` reset 成 0 → 立即過期；下一輪 `poll` 走 `:194-195`，`timeToNextHeartbeatMs` 因 timer 已過期回 `remainingBackoffMs`（`I/HeartbeatRequestState.java:71-76`），而初始 `backoffMs = 0`、`lastReceivedMs = -1`（`I/RequestState.java:32-34`、`:137-140`）→ 回 **0**。request 在 in-flight、manager 無法前進，但 `pollWaitTimeMs = 0` → 背景 thread busy loop 直到第一個 heartbeat response 更新 interval（`:374`）。同樣邏輯也發生在任何 `heartbeatRequestState.reset()` 之後（`:182`、`:399`、`:409`、`:448`、`:457`）：`reset()` 把 `backoffMs = backoff(0)`（`I/RequestState.java:75`），但 `lastReceivedMs = -1` → `remainingBackoffMs = max(0, backoff - (now+1)) = 0`。

#### `ConsumerHeartbeatRequestManager` / `ShareHeartbeatRequestManager`

只差 request 內容與錯誤處理：`buildHeartbeatRequest` 用 `coordinatorRequestManager.coordinator()` 當目標 node（`I/ConsumerHeartbeatRequestManager.java:171-175`、`I/ShareHeartbeatRequestManager.java:143-147`）；`HeartbeatState.buildRequestData` 讀 `SubscriptionState.subscription()` 與 membership 的 `currentAssignment()`（`I/ConsumerHeartbeatRequestManager.java:257-323`），只送有變化的欄位（`SentFields`）。share 版 `shouldSendLeaveHeartbeatNow` = `state == LEAVING`（`I/ShareHeartbeatRequestManager.java:189-192`）。

#### `StreamsGroupHeartbeatRequestManager`（`I/StreamsGroupHeartbeatRequestManager.java`，不繼承 Abstract）

`poll`（`:451-486`）：無 coordinator 或 skip → `EMPTY`（`:452-456`）；poll timer 過期 → `onPollTimerExpired` + leave hb（`:458-472`）；LEAVING 且 dynamic+REMAIN_IN_GROUP → skip leave（`:473-479`）；`shouldHeartbeatBeforeIntervalExpires()`（leave 或 JOINING/ACKNOWLEDGING 且無 in-flight，`:573-577`）或 `canSendRequest` → 送（`:480-482`）；否則 `new PollResult(timeToNextHeartbeatMs)`（`:484`，同樣有上述第一個 heartbeat 零等待問題）。

`maximumTimeToWait`（`:531-551`）：poll timer 過期 → 0；無 coordinator/skip → `retryBackoffMs`；`shouldNotWaitForHeartbeatInterval && !inFlight` → 0；否則 `min(pollTimer/2, timeToNextHeartbeatMs)`。**沒有** UNSUBSCRIBED → `MAX` 的分支（與 Abstract 版 `:259-261` 不同），所以 streams consumer 即使未 join，app thread 的 `pollForFetches` 也只會等 `retryBackoffMs`。

成功 response（`:662-698`）：更新 `heartbeatRequestState` 與 `streamsRebalanceData`（interval / taskOffsetInterval / acceptableRecoveryLag / partitionsByHost / statuses / topologyPushRequired，皆為 `Atomic*` 欄位，`I/StreamsRebalanceData.java:364-380`），再呼叫 `membershipManager.onHeartbeatSuccess(response)`（`:697`）。

#### `StreamsGroupTopologyDescriptionRequestManager`（`I/StreamsGroupTopologyDescriptionRequestManager.java`）

`poll`（`:64-85`）：`shouldSendTopologyDescriptionUpdate`（`:101-113`：backoff 可送、未被 throttle、`topologyPushRequired`、有 wire description、memberId 非空、有 coordinator）才送；否則 `EMPTY`。`maximumTimeToWait`（`:88-99`）：不需 push → `MAX`；否則 `max(backoff, throttle)`，若為 0 且可送 → **0**，否則 `MAX`。response（`:115-182`）處理 throttle（`:137-139`）與 coordinator error（`:148-153`）。

#### `AbstractMembershipManager`（consumer；`I/AbstractMembershipManager.java`）與 `ShareMembershipManager`

`poll`：consumer 版 `maybeReconcile(false)` 後回 `EMPTY`（`:1466-1469`）；share 版 `maybeReconcile(true)` 後回 `EMPTY`（`I/ShareMembershipManager.java:177-180`）。`maximumTimeToWait` 預設 `MAX`。

`maybeReconcile(canCommit)`（`:875-977`）：只在 RECONCILING 且目標 != 當前、且無 reconciliation in progress 時進行；`canCommit == false` 且（autoCommit 開啟或有 revoked partitions）時直接 return（`:929`）——即 **consumer 模式的 reconciliation 只由 `AsyncPollEvent`（`canCommit=true`，`E/ApplicationEventProcessor.java:731-732`）驅動**，背景 poll 只處理「無 revoke 且 autocommit 關閉」的情況。流程：`markPendingRevocationToPauseFetching`（`:949` → `subscriptions.markPendingRevocation`，`:1320-1329`）→ `signalReconciliationStarted`（consumer：`commitRequestManager.maybeAutoCommitSyncBeforeRebalance(rebalanceTimeout deadline)`，`I/ConsumerMembershipManager.java:236-244`）→ `revokeAndAssign`（`:993-1035`）→ `revokePartitions`（`:1191-1232`，透過 `PartitionsRemovedEvent` 跑 `onPartitionsRevoked`）→ `assignPartitions`（`:1248-1284`，透過 `PartitionsAssignedEvent`；成功後 `enablePartitionsAwaitingCallback` `:1266`）→ `currentAssignment = resolved`、`transitionTo(ACKNOWLEDGING)`（`:1025-1031`）。

狀態改變來源：heartbeat manager 回呼 `onHeartbeatSuccess`（`:299-340`）、`onHeartbeatFailure`（`:362-372`）、`transitionToFenced`（`:448-496`）、`transitionToFatal`（`:503-532`）、`onHeartbeatRequestGenerated`（`:749-773`，ACKNOWLEDGING→STABLE/RECONCILING、LEAVING→STALE/UNSUBSCRIBED）、`onHeartbeatRequestSkipped`（`:780-788`）；app event：`onSubscriptionUpdated`（`:542-544`，只設 `AtomicBoolean`）+ `onConsumerPoll`（`:551-555`，才真的 `transitionToJoining`）、`leaveGroup`/`leaveGroupOnClose`（`:599-672`）、`consumerRebalanceListenerCallbackCompleted`（`I/ConsumerMembershipManager.java:452-474`）、`applyAssignment`（`:486-489`）。每次 `transitionTo` 都同步通知 `MemberStateListener.onMemberStateChange`（`:249`）；epoch 變化通知 `onMemberEpochUpdated`（`:722-724`、`:1358-1370`）；assignment 變化通知 `onGroupAssignmentUpdated`（`:731-733`）。

#### `StreamsMembershipManager`（`I/StreamsMembershipManager.java`）

`poll`：`state == RECONCILING` 才 `maybeReconcile()`，回 `EMPTY`（`:1129-1134`）。**無 `canCommit` 閘門**，reconciliation 由背景 poll 直接驅動（`:1144-1228`）。revoke → `StreamsOnTasksRevokedCallbackNeededEvent`（`:1383-1387`）；assign → `StreamsTasksAssignedEvent`（`:1399-1411`）；lost → `StreamsOnAllTasksLostCallbackNeededEvent`（`:1377-1381`）。owned partitions 一致性檢查失敗會丟 `IllegalStateException`（`:1193-1198`，會被 `runOnce` 的 catch 吃掉並 log）。

#### `CommitRequestManager`（`I/CommitRequestManager.java`）

| 狀態 | `poll` 回傳 | 行 |
|---|---|---|
| 無 coordinator | 先 `maybeFailOnCoordinatorFatalError`；`closing` 且有 unsent → 全部以 `CommitFailedException` 失敗；回 `EMPTY` | `:183-193` |
| `closing` | `drainPendingOffsetCommitRequests()`：全部無視 backoff 送出，`PollResult(MAX, requests)` | `:196-197`、`:733-738` |
| 無 unsent | `EMPTY` | `:200-201` |
| 有 unsent | `pendingRequests.drain(currentTimeMs)`；`timeUntilNextPoll = min(剩餘 unsent commit/fetch 的 remainingBackoffMs)`，無剩餘則 `MAX` | `:203-208`、`:240-245` |

`drain`（`:1452-1485`）先 `failAndRemoveExpiredCommitRequests`（`:1458`，`maybeExpire` 只在 `numAttempts > 0 && isExpired()` 才過期，`:1011-1017`），再把可送的 commit/fetch 轉成 `UnsentRequest`（node = `coordinatorRequestManager.coordinator()`，`:1022-1026`），fetch 另放進 `inflightOffsetFetches`（`:1476`）。剩餘者都是 `!canSendRequest` 且非 in-flight，故 `remainingBackoffMs > 0`：**無 0 回傳分支**。

`maximumTimeToWait`（`:223-238`）：無 autocommit → `MAX`；無 coordinator → `retryBackoffMs`；否則 `autoCommit.remainingMs`（timer 過期但有 in-flight commit 時回整個 `autoCommitInterval`，`:1559-1571`；timer 過期且無 in-flight → **0**）。

狀態改變來源：app event `AsyncCommitEvent`/`SyncCommitEvent`/`FetchCommittedOffsetsEvent`/`AssignmentChangeEvent`/`AsyncPollEvent`（後兩者 `updateTimerAndMaybeCommit` `:760-763` → `maybeAutoCommitAsync` `:290-302`）、`CommitOnCloseEvent` → `signalClose`（`:212-214`）；membership listener `onMemberEpochUpdated`（`:694-703`，更新 `memberInfo` 供下次 request 用）；`OffsetsRequestManager` 直接呼叫 `fetchOffsets`（`I/OffsetsRequestManager.java:434-435`）；`ConsumerMembershipManager` 直接呼叫 `maybeAutoCommitSyncBeforeRebalance`/`resetAutoCommitTimer`（`I/ConsumerMembershipManager.java:243`、`:252`）；response callback（`:1035-1051`、`:863-943`、`:1215-1368`）。

#### `OffsetsRequestManager`（`I/OffsetsRequestManager.java`）

`poll`（`:169-174`）：**無條件** `new ArrayList<>(requestsToSend)` + `requestsToSend.clear()` + `new PollResult(list)`（`WAIT_FOREVER`）。從不回傳 timeout；`maximumTimeToWait` 預設 `MAX`。

request 產生點：`fetchOffsets`（ListOffsets，`:188-224`，`prepareFetchOffsetsRequests` `:572-585`，leader 未知時放 `requestsToRetry` 等 metadata `onUpdate` `:587-600`）、`updateFetchPositions`（`:289-318`：`validatePositionsIfNeeded` `:559-566` → OffsetsForLeaderEpoch；`updatePositionsWithOffsets` `:334-360` → `initWithCommittedOffsetsIfNeeded` `:418-456`（走 `commitRequestManager.fetchOffsets`，deadline = `max(deadlineMs, now + defaultApiTimeoutMs)` `:433`，同 partition 集合的 in-flight 會被重用 `:431-453`）→ `initWithPartitionOffsetsIfNeeded` `:391-405` → `resetPositionsIfNeeded` `:530-545` → ListOffsets）、`currentLag`（`:242-270`，one-shot ListOffsets）。

重試節奏完全依賴 `SubscriptionState` 內的 `nextAllowedRetry`：送 request 前 `setNextAllowedRetry(now + requestTimeoutMs)`（`:728-729`、`:808`）；response 有部分失敗或整體失敗時改成 `now + retryBackoffMs`（`I/OffsetFetcherUtils.java:343`、`:360`、`:375-376`）；`partitionsNeedingReset(nowMs)`/`partitionsNeedingValidation(nowMs)` 過濾（`I/SubscriptionState.java:922-936`）。**沒有任何 timer 會喚醒背景 thread 來重試**，只有下一個 `AsyncPollEvent`/`CheckAndUpdatePositionsEvent` 再次呼叫 `updateFetchPositions`。

#### `TopicMetadataRequestManager`（`I/TopicMetadataRequestManager.java`）

`poll`（`:86-107`）：先把 `isExpired()` 的 request `expire()`（future 失敗、移除，`:90-97`）；再對每個 in-flight state 呼叫 `send`（`canSendRequest` 才送，`:178-189`）；有 request → `new PollResult(0, requests)`（`:106`，**0 但附帶 request**，下一輪因 in-flight 而回 `EMPTY`，不是 busy loop）；否則 `EMPTY`。**在 backoff 中回 `EMPTY`（`WAIT_FOREVER`）**，所以重試只靠其他 manager 的 timeout 或 5 秒上限。`maximumTimeToWait` 預設 `MAX`。

#### `FetchRequestManager`（`I/FetchRequestManager.java`）

`poll`（`:118-124` → `pollInternal` `:152-200`）：`pendingFetchRequestFuture == null` → `EMPTY`（`:155-157`）；否則 `prepareFetchRequests()`（`I/AbstractFetch.java:420-497`）：無 unbuffered fetchable partition → 若仍有 fetchable partition則 `fetchBuffer.wakeup()`（`:164-170`）、complete future、回 `EMPTY`；有 request → `PollResult(requests)`（`WAIT_FOREVER`）並 complete future（`:175-190`）；例外 → future fail（`:191-196`）；`finally pendingFetchRequestFuture = null`（`:198`）。

`createFetchRequests()`（`:94-112`）由 `AsyncPollEvent`（`E/ApplicationEventProcessor.java:763`）與 `CreateFetchRequestsEvent`（`:244-247`）呼叫；已有 pending 則串接（`:97-106`）。**因此背景 thread 只會在收到 app event 後的下一次 `poll` 建立 fetch request；response 抵達後不會自動續送下一個 fetch。**

`maximumTimeToWait`（`:82-85`）：`nodesWithPendingFetchRequests.isEmpty() ? retryBackoffMs : MAX`。response 處理：`handleFetchSuccess`（`I/AbstractFetch.java:151-249`）把每個 partition 包成 `CompletedFetch` 丟進 `fetchBuffer.add`（`:225`）；`finally removePendingFetchRequest` → `nodesWithPendingFetchRequests.remove` + **無條件 `fetchBuffer.wakeup()`**（`:290-298`，失敗路徑 `:258-271` 亦同）。`prepareFetchRequests` 會跳過 in-flight node（`:462-464`）與有 buffered partition 的 node（`:465-470`）。

#### `ShareConsumeRequestManager`（`I/ShareConsumeRequestManager.java`）

`poll`（`:150-314`）：`memberId == null`（尚未收到 epoch）→ `EMPTY`（`:151-156`；closing 時順便 complete `closeFuture`）；`processAcknowledgements`（`:484-537`）有 acknowledge request → `PollResult(requests)`；有未完成 ack state → `EMPTY`（`:527-529`）；`closing` → complete `closeFuture` 並 `EMPTY`（`:530-535`）；否則回 null 繼續；`!fetchMoreRecords` → `EMPTY`（`:164-166`）；否則建 ShareFetch（`:169-313`），**即使 `requests` 為空也 `new PollResult(requests)`**（`:313`）。從不回傳 timeout；`maximumTimeToWait` 預設 `MAX`。ack request 在 backoff 中（`canSendRequest` false，`:589-593`）同樣沒有 timer 喚醒。

狀態改變來源：app event `ShareFetchEvent`（`fetch()` `:448-456`，設 `fetchMoreRecords=true` 並合併 ack）、`ShareAcknowledgeSyncEvent`/`ShareAcknowledgeAsyncEvent`/`ShareAcknowledgeOnCloseEvent`（`commitSync` `:676-741`、`commitAsync` `:750-812`、`acknowledgeOnClose` `:824-903`）、`ShareAcknowledgementCommitCallbackRegistrationEvent`（`:543-545`）；membership listener `onMemberEpochUpdated`（`:1355-1358`，只用來拿 memberId）；response callback（`handleShareFetchSuccess` `:905-1046`，有 acquired records 時 `fetchMoreRecords=false` `:1013-1015`；`handleShareAcknowledgeSuccess/Failure` `:1101-1210`）。

### 2.4 零等待分支總表（`timeUntilNextPollMs == 0` 或 `maximumTimeToWait == 0`）

| 位置 | 條件 | 是否能前進 | 判定 |
|---|---|---|---|
| `I/AbstractHeartbeatRequestManager.java:194-195` + `I/HeartbeatRequestState.java:71-76` | 第一個 heartbeat in-flight（interval 仍為 0，`:117`），或任何 `heartbeatRequestState.reset()` 之後 request in-flight | 否（等 response） | **背景 busy loop**（KAFKA-21031） |
| `I/StreamsGroupHeartbeatRequestManager.java:484` + `:419` | 同上 | 否 | **背景 busy loop** |
| `I/TopicMetadataRequestManager.java:106` | 有 request 要送 | 是（同輪送出） | 無害 |
| `I/AbstractHeartbeatRequestManager.java:264-266` | poll timer 過期 | app 端 | app thread 以 0 等待迴圈，直到下一個 `AsyncPollEvent` 被背景處理並 `resetPollTimer` |
| `I/AbstractHeartbeatRequestManager.java:278-280`、`I/StreamsGroupHeartbeatRequestManager.java:547-549` | ACKNOWLEDGING/LEAVING/JOINING 且無 in-flight | app 端 | 通常同一輪 `poll` 已送出（poll 先於掃描），實務上罕見 |
| `I/CommitRequestManager.java:237` + `:1559-1571` | autocommit timer 過期且無 in-flight commit | app 端 | app thread 短暫 0 等待，直到下一個 `AsyncPollEvent` 觸發 `maybeAutoCommitAsync` |
| `I/StreamsGroupTopologyDescriptionRequestManager.java:98` | 需要 push 且可送 | app 端 | 同輪已送 |
| `I/NetworkClientDelegate.java:165-167` | 任何 unsent 送不出去（node 不可用/未 ready） | 否 | 每 `retry.backoff.ms` 醒來一次（非 0 但為固定短週期） |

---

## 3. 跨 manager 依賴

| 讀方 → 被讀方 | 機制 | 位置 |
|---|---|---|
| Heartbeat → Coordinator | 直接呼叫 `coordinator()`（每輪 poll 與 maximumTimeToWait 各一次）、`getAndClearFatalError`、`markCoordinatorUnknown`、`handleCoordinatorDisconnect` | `I/AbstractHeartbeatRequestManager.java:165`、`:275`、`:301`、`:351`、`:397`、`:407`；streams 同 `I/StreamsGroupHeartbeatRequestManager.java:452`、`:544`、`:610`、`:645`、`:752`、`:764`、`:862` |
| Heartbeat → Membership | 直接呼叫（`shouldSkipHeartbeat`/`shouldHeartbeatNow`/`isLeavingGroup`/`state`/`onHeartbeat*`/`transitionTo*`/`memberEpoch`/`currentAssignment`） | `I/AbstractHeartbeatRequestManager.java:165-178`、`:308`、`:362`、`:376`、`:446`、`:455`、`:491`、`:503`；`I/ConsumerHeartbeatRequestManager.java:257-320` |
| Heartbeat → SubscriptionState | 直接讀 `subscription()`/`subscriptionPattern()` 組 request | `I/ConsumerHeartbeatRequestManager.java:281-292`、`I/ShareHeartbeatRequestManager.java:237` |
| Membership → Commit | 直接呼叫 `maybeAutoCommitSyncBeforeRebalance`、`resetAutoCommitTimer`（consumer 模式） | `I/ConsumerMembershipManager.java:243`、`:252` |
| Membership → Commit / app thread / ShareConsume | `MemberStateListener`（`onMemberEpochUpdated`/`onGroupAssignmentUpdated`/`onMemberStateChange`）同步回呼 | 註冊：`I/RequestManagers.java:239-240`、`:291-292`、`:401`；觸發：`I/AbstractMembershipManager.java:249`、`:722-733`；`I/StreamsMembershipManager.java:418-429`；接收：`I/CommitRequestManager.java:694-703`、`I/AsyncKafkaConsumer.java:425-440`、`I/ShareConsumeRequestManager.java:1355-1358` |
| Membership → SubscriptionState | 直接寫：`assignFromSubscribedAwaitingCallback`、`enablePartitionsAwaitingCallback`、`markPendingRevocation`、`unsubscribe`、`assignFromSubscribed`、`setAssignedTopicIds` | `I/ConsumerMembershipManager.java:487`；`I/AbstractMembershipManager.java:438`、`:562`、`:633`、`:675`、`:1266`、`:1328` |
| Membership → Metadata | `metadata.topicNames()` 解析 topic id、`requestUpdate(true)` | `I/AbstractMembershipManager.java:1136-1148`、`:1160` |
| Membership → app thread（callback） | `CompletableBackgroundEvent`（`PartitionsAssignedEvent`/`PartitionsRemovedEvent`/Streams 三種）+ future；app 以 `*CallbackCompletedEvent` 回覆 | `I/ConsumerMembershipManager.java:415-441`；`I/StreamsMembershipManager.java:1377-1411`；回覆 `E/ApplicationEventProcessor.java:473-482`、`:674-699` |
| Commit → Coordinator | `coordinator()`、`fatalError()`、`markCoordinatorUnknown`、`handleCoordinatorDisconnect` | `I/CommitRequestManager.java:183`、`:234`、`:901`、`:1025`、`:1044`、`:1254`、`:1510` |
| Commit → SubscriptionState | `allConsumed()`（autocommit 與 rebalance 前 commit） | `:293`、`:350`、`:374` |
| Commit → Metadata | `topicIds()`/`topicNames()`/`updateLastSeenEpochIfNewer` | `:743`、`:809`、`:875-876`、`:1173` |
| Offsets → Commit | 直接呼叫 `fetchOffsets`（可為 null，無 group 時走 `initWithPartitionOffsetsIfNeeded`） | `I/OffsetsRequestManager.java:341-349`、`:434-435` |
| Offsets → SubscriptionState / Metadata / PositionsValidator | 直接讀寫（`hasAllFetchPositions`、`initializingPartitions`、`resetInitializingPositions`、`setNextAllowedRetry`、`completeValidation`；`currentLeader`、`requestUpdate`、`addTransientTopics`、cluster listener） | `:148`（listener 註冊）、`:201`、`:299`、`:340`、`:396`、`:728`、`:803`、`:808`、`:973-977`；`I/OffsetFetcherUtils.java:189`、`:210` |
| Offsets → NetworkClientDelegate | `tryConnect(node)` 以取得 ApiVersions | `:794` |
| Fetch → SubscriptionState / Metadata / FetchBuffer | `fetchablePartitions`、`position`、`topicIds`、`bufferedPartitions` | `I/AbstractFetch.java:343-349`、`:426-432`、`:448` |
| Fetch → NetworkClientDelegate | `isUnavailable`/`maybeThrowAuthFailure` | `I/FetchRequestManager.java:65-73` |
| ShareConsume → SubscriptionState / Metadata / ShareFetchBuffer | `fetchablePartitions`、`currentLeader`、`fetch()`、`bufferedNodes` | `I/ShareConsumeRequestManager.java:170-198`、`:254`、`:1334-1336` |
| Streams HB / Topology → `StreamsRebalanceData` | 直接讀寫 `Atomic*` 欄位（與 app/Stream thread 共享） | `I/StreamsGroupHeartbeatRequestManager.java:168-202`、`:681-692`；`I/StreamsGroupTopologyDescriptionRequestManager.java:72-73`、`:105`、`:126`、`:144` |
| ApplicationEventProcessor → 各 manager | 直接呼叫（見 §5） | `E/ApplicationEventProcessor.java:231-770` |

---

## 4. Application thread 側

### 4.1 `AsyncKafkaConsumer.poll`（`I/AsyncKafkaConsumer.java:933-982`）

```
do {
  wakeupTrigger.maybeTriggerWakeup()      :953
  checkInflightPoll(timer, firstPass)      :955
  fetch = pollForFetches(timer)            :957
  if (!fetch.isEmpty()) { sendPrefetches(timer); return interceptors.onConsume(...) }  :958-972
} while (timer.notExpired())               :975
```

`checkInflightPoll`（`:993-1030`）：

1. `inflightPoll != null && (firstPass || isComplete)` → `maybeClearPreviousInflightPoll`（`:998-1000`、`:1032-1065`）：完成且有 error → 清掉並 **throw**（`:1036-1042`）；完成且 buffer 空 → 清掉（`:1045-1050`）；完成且 buffer 非空 → **不清**（`:1057`，讓下一輪不重送）；未完成但 `isExpired && isValidatePositionsComplete` → 清掉（`:1060-1064`）。
2. `inflightPoll == null` → `new AsyncPollEvent(calculateDeadlineMs(timer), now)` 並 `applicationEventHandler.add`（`:1004-1009`，**不阻塞**，不進 reaper——`AsyncPollEvent` 不是 `CompletableEvent`，`E/AsyncPollEvent.java:44`）。
3. `offsetCommitCallbackInvoker.executeCallbacks()` + `processBackgroundEvents()`（`:1014-1015`）；例外則清掉 inflight 並 throw（`:1016-1023`）。
4. `maybeClearCurrentInflightPoll`（`:1067-1088`）。

`AsyncPollEvent` 在背景的處理（`E/ApplicationEventProcessor.java:728-770`）：`consumerMembershipManager.maybeReconcile(true)`（`:731-732`）→ `markReconciliationCheckComplete`（`:736`）→ `commitRequestManager.updateTimerAndMaybeCommit`（`:740`）→ `maybeUpdatePatternSubscription` + `membershipManager.onConsumerPoll()` + `hrm.resetPollTimer`（`:742-753`，consumer 與 streams 各一組）→ `offsetsRequestManager.updateFetchPositions(deadlineMs)`（`:756`）→ `markValidatePositionsComplete`（`:757`，**在 future 完成前就標記**）→ future 完成後 `fetchRequestManager.createFetchRequests()`（`:763`）→ `completeSuccessfully`（`:767`）。`TimeoutException` 被忽略、不算失敗（`:780-783`）。

### 4.2 `pollForFetches`（`:1980-2034`）——所有把等待縮短到 `retry.backoff.ms` 的分支

1. `collectFetch()` 非空 → 直接回（`:1983-1986`）。
2. `pollTimeout = min(applicationEventHandler.maximumTimeToWait(), timer.remainingMs())`（`:1988`）。
3. 若 `pollTimeout > retryBackoffMs`（`:1992`）：
   - `subscriptions.numAssignedPartitions() == 0` → `retryBackoffMs`（`:1993-1997`）
   - `!subscriptions.hasAllFetchPositions()` → `retryBackoffMs`（`:1998-2003`）
   - 有 fetchable 且未 buffered 的 partition（`hasFetchablePartitions(tp -> !buffered.contains(tp))`）→ `retryBackoffMs`（`:2005-2011`）
4. `wakeupTrigger.setFetchAction(fetchBuffer)`（`:2018`）→ `fetchBuffer.awaitWakeup(pollTimer)`（`:2024`）→ `clearTask`（`:2030`）→ `collectFetch()`（`:2033`）。

再加上 `FetchRequestManager.maximumTimeToWait` 在無 in-flight fetch 時本來就是 `retryBackoffMs`（`I/FetchRequestManager.java:84`），實務上 app thread 幾乎永遠只等 100 ms 就重跑一輪（重新 `checkInflightPoll`，但只有 `inflightPoll == null` 時才送新事件）。

`FetchBuffer.awaitWakeup`（`I/FetchBuffer.java:165-194`）：`while (!wokenup.compareAndSet(true,false))` 等 condition；`wokenup` 由 `addAll`（`:109`）與 `wakeup()`（`:199`）設定。因為 `wokenup` 是黏著旗標，前一輪殘留的 wakeup 會讓下一次 `awaitWakeup` 立即返回一次。

### 4.3 `collectFetch`（`:2042-2083`）

1. `hasPendingReconciliation && inflightPoll != null && !isReconciliationCheckComplete` → 以 `wakeupTrigger.setActiveTask` **阻塞**在 `reconciliationCheckFuture` 直到 `deadlineMs`（`:2048-2066`）——這是 app thread 唯一會等 `AsyncPollEvent` 內部 future 的地方（`hasPendingReconciliation` 由 listener 在 `RECONCILING` 時設定，`:437-439`）。
2. `positionsValidator.canSkipUpdateFetchPositions()`（`I/PositionsValidator.java:141-150`：先丟快取的驗證錯誤，再看 metadata version 未變且 `hasAllFetchPositions`）→ 直接 `fetchCollector.collectFetch(fetchBuffer)`（`:2074-2075`）。
3. 否則 `inflightPoll != null && !isValidatePositionsComplete` → `Fetch.empty()`（`:2078-2079`）；否則 collect（`:2082`）。

`FetchCollector.collectFetch`（`I/FetchCollector.java:91-147`）在 app thread 上：`initialize` 檢查 `hasValidPosition` 與 position == fetchOffset（`:222-263`）、更新 HW/LSO/logStart（`:285-319`）；`fetchRecords`（`:149-217`）在 `isAssigned && isFetchable && nextFetchOffset == position.offset` 時取出 records 並**同步 `subscriptions.position(tp, nextPosition)`**（`:178-186`）。

### 4.4 其他 API

| API | 事件 / 行為 | 阻塞？ | 位置 |
|---|---|---|---|
| `position(tp, timeout)` | 迴圈：`subscriptions.validPosition` 有值即回；否則 `updateFetchPositions(timer)` = `CheckAndUpdatePositionsEvent` `addAndGet`（`wakeupTrigger.setActiveTask`） | 是 | `:1235-1257`、`:2094-2105` |
| `committed(partitions, timeout)` | `FetchCommittedOffsetsEvent` `addAndGet`，`setActiveTask` | 是 | `:1265-1292` |
| `seek(tp, offset)` / `seek(tp, OffsetAndMetadata)` | `SeekUnvalidatedEvent` `addAndGet`（背景執行 `subscriptions.seekUnvalidated`，`E/ApplicationEventProcessor.java:618-631`） | 是（default.api.timeout） | `:1157-1201` |
| `seekToBeginning/End` | `ResetOffsetEvent` `addAndGet` | 是 | `:1204-1227` |
| `commitAsync` | `AsyncCommitEvent`；`commit()` 只阻塞到 `offsetsReady`（背景已讀 `allConsumed`，`E/CommitEvent.java:68-74`、`E/ApplicationEventProcessor.java:258-259`）；結果經 `whenComplete` 排進 `OffsetCommitCallbackInvoker` | 部分（等 offsetsReady，最多 default.api.timeout） | `:1117-1154` |
| `commitSync` | `SyncCommitEvent`；先 `awaitPendingAsyncCommitsAndExecuteCommitCallbacks`（`:1830-1853`），再 `setActiveTask(commitFuture)` + `getResult(commitFuture, timer)` | 是 | `:1810-1828` |
| `assign` | `fetchBuffer.retainAll` 後 `AssignmentChangeEvent` `addAndGet` | 是 | `:1890-1931` |
| `subscribe(topics)` | `TopicSubscriptionChangeEvent` `addAndGet` | 是 | `:2299-2335` |
| `unsubscribe` | `UnsubscribeEvent` `add` 後 `processBackgroundEvents(future, timer, ..., skipAssignmentEvents=true)` 迴圈（每 100 ms 檢查一次，`:2457-2489`） | 是 | `:1934-1963` |
| `updateAssignmentMetadataIfNeeded`（Streams 用） | `UpdatePatternSubscriptionEvent` `addAndGet` + `processBackgroundEvents` + `updateFetchPositions` | 是 | `:2130-2144` |
| `wakeup` | `wakeupTrigger.wakeup()`：ActiveFuture → `completeExceptionally(WakeupException)`；FetchAction → `fetchBuffer.wakeup()` | — | `:1785-1787`、`I/WakeupTrigger.java:40-63` |

`addAndGet`（`E/ApplicationEventHandler.java:138-148`）= `add` + `ConsumerUtils.getResult(future)`（**無 timeout 的 `future.get()`**，`I/ConsumerUtils.java:239-249`），完全依賴背景 reaper 在 `deadlineMs` 讓 future 失敗。

### 4.5 `ShareConsumerImpl.poll`（`I/ShareConsumerImpl.java:597-659`）

`processBackgroundEvents`（`:603`）→ `handleCompletedAcknowledgements`（`:606`）→ implicit 模式 `acknowledgeAll`（`:609`）→ explicit 模式檢查（`:612`）→ `shouldSendShareFetchEvent = true`（`:620`）→ 迴圈：`maybeTriggerWakeup`、`checkInFlightPoll`（`SharePollEvent`，只做 `maybeReconcile(true)`+`onConsumerPoll`+`resetPollTimer`，`E/ApplicationEventProcessor.java:231-242`）、`pollForFetches`（`:704-730`）：`pollTimeout = min(maximumTimeToWait, remaining)`（**沒有 retryBackoffMs 縮短分支**）、`collect(acks)`（`:732-782`：buffer 空且需要時送一次 `ShareFetchEvent` + 額外 `wakeupNetworkThread`）、`fetchBuffer.awaitNotEmpty(pollTimer)`（`I/ShareFetchBuffer.java:145-168`，等到有資料或 `wokenUp`）、再 `collect`；每輪 `processBackgroundEvents` + `metadata.maybeThrowAnyException()`（`:643-644`）。

Share 的 ack 結果不走 `BackgroundEventHandler`，而走獨立的 `ShareAcknowledgementEventHandler` queue（`E/ShareAcknowledgementEventHandler.java:45-59`），由 app thread 在 `handleCompletedAcknowledgements` → `processAcknowledgementEvents`（`:1185-1200`、`:1252-1263`）消化，觸發使用者 `AcknowledgementCommitCallback` 與 `currentFetch.renew`（`:136-143`）。

### 4.6 app thread 阻塞在哪些 future

| 會阻塞 | 不阻塞 |
|---|---|
| 所有 `CompletableApplicationEvent` 走 `addAndGet`（seek/assign/subscribe/position/committed/listTopics/offsetsForTimes/leaveGroupOnClose/ApplyAssignmentEvent 等）；deadline 由背景 `CompletableEventReaper` 執行（`E/CompletableEventReaper.java:86-122`） | `AsyncPollEvent`（`add`，app 只讀 `isComplete/error/isValidatePositionsComplete`，`E/AsyncPollEvent.java:48-50`，volatile） |
| `SyncCommitEvent.future()`（`getResult(commitFuture, requestTimer)` 自帶 timer，`:1821`） | `AsyncCommitEvent.future()`（除了 `offsetsReady`） |
| `CommitEvent.offsetsReady()`（`:1152`，帶 default.api.timeout） | `CreateFetchRequestsEvent`（`sendPrefetches` 只 `add`，`:2120-2127`） |
| `AsyncPollEvent.reconciliationCheckFuture()`（僅 `hasPendingReconciliation` 時，`:2055-2056`） | `CommitOnCloseEvent`、`StopFindCoordinatorOnCloseEvent`、`ConsumerRebalanceListenerCallbackCompletedEvent`、Streams `*CallbackCompletedEvent`、`SharePollEvent`、`ShareFetchEvent`、`ShareAcknowledgeAsyncEvent`、`ShareAcknowledgementCommitCallbackRegistrationEvent` |
| `UnsubscribeEvent`/`ShareUnsubscribeEvent` 以 100 ms 輪詢 `processBackgroundEvents(future, timer, ...)` 等待（`:2457-2489`、`I/ShareConsumerImpl.java:1344-1385`） | |
| `lastPendingAsyncCommit`（`commitSync`/close 前等待，`:1830-1853`） | |

---

## 5. 結果 / 通知通道

### 5.1 background → app：`BackgroundEventHandler`

`add`（`E/BackgroundEventHandler.java:53-58`）只入 `LinkedBlockingQueue`，**不喚醒 app thread**；app 只在 `processBackgroundEvents`（`I/AsyncKafkaConsumer.java:2370-2411`；呼叫點 `:1015`、`:2141`、`:2462`）主動 `drainEvents`（`:65-70`）時處理。Share 版同（`I/ShareConsumerImpl.java:1288-1317`）。

事件類型（`E/BackgroundEvent.java:28-35`）與 app 端處理（`I/AsyncKafkaConsumer.java:192-363`）：

| 事件 | 產生者 | app 端處理 |
|---|---|---|
| `ErrorEvent` | heartbeat fatal（`I/AbstractHeartbeatRequestManager.java:432`、`:502`）、coordinator fatal（`:302`）、metadata error（share，`I/NetworkClientDelegate.java:191`）、streams（`I/StreamsGroupHeartbeatRequestManager.java:611`、`:791`、`:883`；`I/StreamsMembershipManager.java:985`） | `throw event.error()`（`:227-229`）；`processBackgroundEvents` 收集 `firstError` 後重新丟出（`:2395-2408`） |
| `PartitionsAssignedEvent` | `ConsumerMembershipManager.enqueuePartitionsAssignedEvent`（`:434-441`） | `applyNewAssignment`：**同步 `addAndGet(ApplyAssignmentEvent)`** 回背景執行 `subscriptions.assignFromSubscribedAwaitingCallback`（`:255-268`、`E/ApplicationEventProcessor.java:707-726`）；無 listener 則直接 complete future（`:241-242`），否則跑 `onPartitionsAssigned` 並送 `ConsumerRebalanceListenerCallbackCompletedEvent`（`:244`、`:274-288`） |
| `PartitionsRemovedEvent` | `enqueueConsumerRebalanceListenerCallback`（revoked/lost，`:415-424`） | `invokeRebalanceCallbackAndNotifyBackgroundThread`（`:270-288`）；callback 例外會被包進 completed event 且 rethrow（`:285-287`） |
| `StreamsTasksAssignedEvent` | `StreamsMembershipManager:1399-1411` | `addAndGet(ApplyAssignmentEvent)` 後 `invokeTasksAssigned`，送 `StreamsOnTasksAssignedCallbackCompletedEvent`（`:312-336`） |
| `StreamsOnTasksRevokedCallbackNeededEvent` / `StreamsOnAllTasksLostCallbackNeededEvent` | `:1383-1387`、`:1377-1381` | `invokeTasksRevoked`/`invokeAllTasksLost` → completed event（`:290-304`） |

`CompletableBackgroundEvent` 的 `deadlineMs` 一律 `Long.MAX_VALUE`（`E/PartitionsAssignedEvent.java:46`、`E/PartitionsRemovedEvent.java:38`、`E/StreamsTasksAssignedEvent.java:47` 等），app 端 `backgroundEventReaper.reap(now)`（`:2405`）實際上永不逾時，只在 close 時 `reap(queue)` 一次性失敗（`:1671-1672`）。

`*CallbackCompletedEvent` 走 application queue 回背景（`E/ConsumerRebalanceListenerCallbackCompletedEvent.java:31-65`，非 completable），背景 `consumerRebalanceListenerCallbackCompleted` 完成原 future（`I/ConsumerMembershipManager.java:452-474`），推動 reconciliation 的 `whenComplete` 鏈。

### 5.2 commit callback：`OffsetCommitCallbackInvoker`

背景 thread 只 enqueue（`enqueueInterceptorInvocation` 由 autocommit callback `I/CommitRequestManager.java:393` 呼叫；user callback 由 app 端 `commitAsync` 的 `whenComplete` enqueue `I/AsyncKafkaConsumer.java:1121-1134`——注意這個 `whenComplete` 跑在**背景 thread**）；app thread 在 `poll`（`:1014`）、`commit()`（`:1142`）、`commitSync` 前（`:1852`）、`updateAssignmentMetadataIfNeeded`（`:2131`）執行 `executeCallbacks`（`I/OffsetCommitCallbackInvoker.java:59-66`）。

### 5.3 network thread 喚醒 app thread 的每個點

| 位置 | 動作 |
|---|---|
| `I/AbstractFetch.java:225` `fetchBuffer.add` | 收到 fetch response 的每個 partition → `addAll` → `wokenup=true` + `signalAll`（`I/FetchBuffer.java:102-114`） |
| `I/AbstractFetch.java:297` `fetchBuffer.wakeup()` | 每個 fetch request 完成（成功、失敗、close session）後無條件 |
| `I/FetchRequestManager.java:169` `fetchBuffer.wakeup()` | 建 fetch request 時發現沒有 unbuffered partition 可 fetch，但仍有 fetchable partition |
| `I/ShareConsumeRequestManager.java:1019` `shareFetchBuffer.add` | ShareFetch response（`I/ShareFetchBuffer.java:80-88`，只 signal，不設 `wokenUp`） |
| `I/WakeupTrigger.java:54`、`:58` | 使用者 `wakeup()`（app thread 自己觸發） |

`CompletableFuture` 完成（`addAndGet` 的等待）由背景 thread 在 callback 中 `complete`，這是另一種喚醒。

---

## 6. Timeout、取消、close、遲到 response

### 6.1 `CompletableEventReaper`

- 背景 reaper：`processApplicationEvents` 在**處理前**就 `add`（`I/ConsumerNetworkThread.java:258-260`）；每輪 `reap(currentTimeMs)`（`:285`）：已完成者移除，`currentTimeMs >= deadlineMs` 者 `completeExceptionally(TimeoutException)` 並移除（`E/CompletableEventReaper.java:91-119`）。
- **reaper 只失敗 future，不通知任何 manager**。manager 內部的 request state、`whenComplete` 鏈、unsent request 全部繼續存在；遲到的 `complete` 對已完成 future 是 no-op。
- close 時 `reap(Collection)`：tracked + queue 中未處理的 completable event 全部以 "could not be completed before the consumer closed" 失敗（`:143-153`、`:186-210`）。

### 6.2 request 層 deadline

| 層級 | 機制 | 位置 |
|---|---|---|
| `UnsentRequest` | `add` 時 `setTimer(request.timeout.ms)`（`I/NetworkClientDelegate.java:321-326`）；`trySend` 逾時 → `onFailure(TimeoutException)`（`:213-219`）；送出後 `newClientRequest(..., timer.remainingMs())`（`:277`），由 `NetworkClient` 處理 in-flight 逾時 | |
| `RequestState` | 只有 backoff（`I/RequestState.java:78-92`、`:137-140`），無 deadline | |
| `TimedRequestState` | `deadlineTimer(time, deadlineMs)` + `isExpired()`/`remainingMs()`（`I/TimedRequestState.java:51-68`）；用於 `TopicMetadataRequestState`、`OffsetCommitRequestState`、`OffsetFetchRequestState`、`AcknowledgeRequestState` | |
| `AsyncPollEvent.deadlineMs` | = poll timeout；只用於 app 端 `isExpired` 與 `updateFetchPositions(deadlineMs)`（OffsetFetch deadline 取 `max(deadline, now+default.api.timeout)`，`I/OffsetsRequestManager.java:433`；`cacheExceptionIfEventExpired` `:372-379`） | |
| Heartbeat / FindCoordinator / ListOffsets / OffsetsForLeaderEpoch / Fetch / ShareFetch | 無 deadline，只有 `request.timeout.ms` 與 backoff | |

### 6.3 事件被 reap 之後，各 manager 的遲到處理

| Manager / 路徑 | 事件被 reap 後的 manager 行為 |
|---|---|
| `CommitRequestManager.commitSync` | `commitSyncWithRetries`：retriable error 時才檢查 `requestAttempt.isExpired()`（`:486-493`）；成功遲到 → `result.complete` no-op。**尚未送出過的 commit 不會過期**（`maybeExpire` 需 `numAttempts > 0`，`:1012`），coordinator 出現後照送，broker 端會被 commit。close 且無 coordinator 時才以 `CommitFailedException` 失敗（`:186-191`）。 |
| `CommitRequestManager.commitAsync` | deadline `Long.MAX_VALUE`（`:420`），無重試（`:424-430`），失敗即回報。 |
| `CommitRequestManager.fetchOffsets` | 重試前檢查 `isExpired()`（`:619`、`:646`），過期回 `TimeoutException` 或部分結果；同 partition 集合的重複請求會 `chainFuture` 到既有的（`:1426-1444`）。 |
| `OffsetsRequestManager.updateFetchPositions` | 若 update 在 event 過期後才失敗，例外存進 `cachedUpdatePositionsException`，**下一次** `updateFetchPositions` 直接以該例外失敗（`:320-327`、`:372-379`）。`pendingOffsetFetchEvent` 讓下一個 poll 重用 in-flight OffsetFetch（`:431-453`）。 |
| `OffsetsRequestManager.fetchOffsets`（ListOffsets） | `ListOffsetsRequestState` **沒有 deadline**（`:873-920`）；leader 未知的 partition 放 `requestsToRetry`，每次 metadata `onUpdate` 重建 request（`:587-600`），並保持 `metadata.addTransientTopics` 直到完成（`:201`、`:209`）。 |
| `OffsetsRequestManager` reset/validate | 失敗只寫 `SubscriptionState.nextAllowedRetry` 與 `cachedResetPositionsException`/`PositionsValidator.cachedValidatePositionsException`（`I/OffsetFetcherUtils.java:343`、`:360-366`；`I/PositionsValidator.java:102-112`），下次 poll 才丟出。 |
| `TopicMetadataRequestManager` | state 自帶相同 deadline，`poll` 時 `expire()` 移除（`:93-96`）；retriable error 且已過期 → `completeFutureAndRemoveRequest`（`:216-218`）。 |
| `FetchRequestManager` | `CreateFetchRequestsEvent` 被 reap 後 `pendingFetchRequestFuture` 仍在，下一輪 `poll` 照建 request（`:155-190`）。 |
| Membership `leaveGroup` | `UnsubscribeEvent`/`LeaveGroupOnCloseEvent` 被 reap 後 `leaveGroupInProgress` future 仍存在，heartbeat response 或 skip 時 `maybeCompleteLeaveInProgress` 完成它（`:378-385`）；狀態機不回滾。 |
| `ShareConsumeRequestManager` | `AcknowledgeRequestState.maybeExpire` 同樣需 `numAttempts > 0`（`:1617-1619`）；過期時 `handleAcknowledgeTimedOut` 以 `REQUEST_TIMED_OUT` 完成並送 `ShareAcknowledgementEvent`（`:577-587`、`:1549-1557`）。 |

### 6.4 close 序列

`AsyncKafkaConsumer.close(timeout, op, swallow)`（`I/AsyncKafkaConsumer.java:1642-1692`）：

1. `wakeupTrigger.disableWakeups()`（`:1647`）。
2. `closeTimer = min(timeout, request.timeout.ms)`（`:1694-1698`）。
3. `autoCommitOnClose`（`:1700-1708`）：autocommit 開啟時 `commitSyncAllConsumed`（阻塞 `SyncCommitEvent`，例外只 warn，`:1773-1782`）；再 `add(CommitOnCloseEvent)` → 背景 `commitRequestManager.signalClose()`（`E/ApplicationEventProcessor.java:484-489`）。
4. `stopFindCoordinatorOnClose` → `StopFindCoordinatorOnCloseEvent` → `coordinatorRequestManager.signalClose()`（`:1765-1770`、`E/ApplicationEventProcessor.java:503-508`）。
5. `runRebalanceCallbacksOnClose`（`:1710-1747`）：**在 app thread 直接**依 `groupAssignmentSnapshot`/`memberEpoch` 呼叫 revoked 或 lost（streams 則 `invokeAllTasksRevoked`/`invokeAllTasksLost`）。
6. `leaveGroupOnClose` → `addAndGet(LeaveGroupOnCloseEvent)`（`:1749-1763`）→ `leaveGroupOnClose(op)` → `leaveGroup(runCallbacks=false)`（`I/AbstractMembershipManager.java:599-602`、`:627-672`）→ `transitionToSendingLeaveGroup` → LEAVING；heartbeat manager 下一輪送 leave hb；response（或 skip）完成 future。逾時只 warn。
7. `awaitPendingAsyncCommitsAndExecuteCommitCallbacks(closeTimer, false)`（`:1663-1664`）。
8. `applicationEventHandler.close(remaining)`（`:1665-1666`）→ `ConsumerNetworkThread.closeInternal`：`running=false; closeTimeout=timeout; wakeup(); join()`（`I/ConsumerNetworkThread.java:379-391`）→ 背景 `cleanup()`（`:412-436`）：
   - `runAtClose`（`:311-319`）：每個 manager `pollOnClose` + `addAll`。heartbeat：`isLeavingGroup()` 才送 leave hb（`I/AbstractHeartbeatRequestManager.java:230-237`；streams `I/StreamsGroupHeartbeatRequestManager.java:506-512`）；fetch：`createFetchRequests()` 後送 close-session request（`I/FetchRequestManager.java:129-140`）；commit 沒有 `pollOnClose`，其 `closing` 只影響一般 `poll`（此時已不再呼叫）；其餘回 `EMPTY`。
   - `sendUnsentRequests(timer)`（`:396-410`）：`while (timer.notExpired() && hasAnyPendingRequests()) poll(remaining, now, onClose=true)`；未指定 node 的 unsent 直接以 `NETWORK_EXCEPTION` 失敗（`I/NetworkClientDelegate.java:258-263`）。
   - `applicationEventReaper.reap(applicationEventQueue)`（`:430`）：所有未完成/未處理 completable event 失敗。
   - `closeQuietly(requestManagers)`（`:432`）：只關 `Closeable` 的 manager——`FetchRequestManager`（經 `AbstractFetch.close` → `fetchBuffer.close` → `retainAll(∅)` drain，`I/AbstractFetch.java:669-682`、`I/FetchBuffer.java:261-273`）與 `ShareConsumeRequestManager`（`:1346-1353`）。
   - `closeQuietly(networkClientDelegate)`（`:433`）。
9. `backgroundEventReaper.reap(backgroundEventQueue)`（`:1671-1672`）→ 尚未處理的 rebalance callback event 失敗。
10. 關 interceptors / metrics / deserializers / telemetry（`:1674-1683`）。

被丟棄 vs 完成：

| 工作 | 結果 |
|---|---|
| unsent commit（有 coordinator） | `closing` 後下一輪 `poll` 無視 backoff 全部送出（`:196-197`）；但 step 8 之後不再有一般 `poll`，只剩 `sendUnsentRequests` 送已在 queue 者 |
| unsent commit（無 coordinator） | `CommitFailedException`（`:186-191`） |
| in-flight request | 等到 `closeTimer` 用完；之後 `NetworkClient.close` 丟棄 |
| 未處理 application event | `TimeoutException`（reaper） |
| `FetchBuffer` 內資料 | drain 丟棄 |
| Share：`ShareAcknowledgeOnCloseEvent`（`I/ShareConsumerImpl.java:1088-1090`）→ `acknowledgeOnClose` 為每個 session 建 CLOSE ack request，等 `closeFuture`；再 `ShareUnsubscribeEvent` 以 100 ms 輪詢等待（`:1093-1109`） | |

---

## 7. 現行依賴的順序保證

| 保證 | 機制 | 位置 |
|---|---|---|
| position 更新發生在 records 回傳之前、且在 app thread | `FetchCollector.fetchRecords` 先 `subscriptions.position(tp, nextPosition)` 再回傳 `Fetch`（`:178-186` → `:204`）；`poll` 回傳前 `interceptors.onConsume` | `I/FetchCollector.java`、`I/AsyncKafkaConsumer.java:972` |
| autocommit 讀到的是「上一批已回傳」的 position | 背景 `maybeAutoCommitAsync` 讀 `subscriptions.allConsumed()`（`I/CommitRequestManager.java:293`，`allConsumed` 只含 `hasValidPosition` 的 partition，`I/SubscriptionState.java:819-827`），觸發點是**下一個** `AsyncPollEvent`（`E/ApplicationEventProcessor.java:738-740`）；`AsyncCommitEvent` 無 offsets 時亦在背景讀 `allConsumed`（`:258`），app 端只等到 `offsetsReady` | |
| 同一 partition 不會有兩個 in-flight fetch、不會在 buffer 未清空時再 fetch | `prepareFetchRequests` 跳過 buffered partition 與 in-flight/buffered node（`I/AbstractFetch.java:429-432`、`:462-470`）；`FetchCollector.initialize` 丟棄 offset 不符的 stale response（`:258-263`） | |
| 新 assignment 在 callback 前已寫入 `SubscriptionState`，但不可 fetch | `PartitionsAssignedEvent` → app `addAndGet(ApplyAssignmentEvent)` → 背景 `assignFromSubscribedAwaitingCallback`（`I/SubscriptionState.java:982-986`，`pendingOnAssignedCallback=true` → `isFetchable()` false `:1345-1346`）→ app 跑 `onPartitionsAssigned` → completed event → 背景 `enablePartitionsAwaitingCallback`（`I/AbstractMembershipManager.java:1266`）→ 之後 `AsyncPollEvent` 的 `updateFetchPositions` 才會初始化 position | |
| revoke 期間停止 fetch 與回傳 | `markPendingRevocation`（`:949`、`:1201`）→ `isFetchable()` false；`FetchCollector.fetchRecords` 對不可 fetch 的 partition drain（`:155-162`、`:213-216`）；autocommit-before-rebalance 在 revoke callback 之前（`:953-968`） | |
| ACKNOWLEDGING 的 heartbeat 帶新 assignment | `currentAssignment = resolvedAssignment` 先於 `transitionTo(ACKNOWLEDGING)`（`:1025-1030`）；heartbeat `buildRequestData` 讀 `currentAssignment()`（`I/ConsumerHeartbeatRequestManager.java:308-314`）；送出時 `onHeartbeatRequestGenerated` 才轉 STABLE（`:749-761`） | |
| 錯誤傳遞：`ErrorEvent` vs future | fatal group 錯誤 → `ErrorEvent`，在下一次 `processBackgroundEvents` 於 app thread 拋出（`I/AsyncKafkaConsumer.java:227-229`、`:2407-2408`）；單一 API 的錯誤 → 該 event 的 future（`E/ApplicationEventProcessor.java:795-802`）；`AsyncPollEvent` 的錯誤 → volatile `error` 欄位，於 `checkInflightPoll` 拋出（`:1036-1042`、`:1071-1076`）；metadata error（consumer）→ 只給 `MetadataErrorNotifiableEvent`（`AsyncPollEvent`、`CheckAndUpdatePositionsEvent`、`ListOffsetsEvent`、`TopicMetadataEvent`/`AllTopicsMetadataEvent`）：在 step 1 處理前檢查一次（`I/ConsumerNetworkThread.java:264-267`），之後只有仍被 reaper 追蹤的 completable event 會在 step 7 再收到（`:240-241`）；`AsyncPollEvent` 不是 completable，所以只在入列處理當下有機會收到 | |
| Streams：`reconciledAssignment` 由 app thread 在 `onTasksAssigned` 內寫入，heartbeat 才會回報 | `S/DefaultStreamsRebalanceListener.java:112`、`:123` 寫 `AtomicReference`；`I/StreamsGroupHeartbeatRequestManager.java:168-176` 讀並比對 `lastSentFields` | |
| Streams：`statuses`/`partitionsByHost` 由背景寫、StreamThread 在 `handleStreamsRebalanceData` 讀 | `I/StreamsGroupHeartbeatRequestManager.java:692`、`:704`；`S/StreamThread.java:1578-1640` | |

---

## 8. `poll()` 穩態空路徑上的配置（allocation）清單

「穩態空路徑」= 沒有新 event、沒有 request 要送、沒有 response 到達的一輪 `runOnce`。

| 位置 | 配置物 | 每輪必發生？ |
|---|---|---|
| `I/ConsumerNetworkThread.java:248` | `new LinkedList<>()` 供 `drainTo`（即使 queue 為空） | 是 |
| `I/ConsumerNetworkThread.java:240` → `E/CompletableEventReaper.java:166` | `uncompletedEvents()` 的 `new ArrayList<>()` | 是 |
| `I/ConsumerNetworkThread.java:442` | `maybeFailOnMetadataError` 的 `new ArrayList<>()`（step 7，只在 uncompleted 非空時；step 1 每個 event 一次） | 有未完成 event 時 |
| `I/ConsumerNetworkThread.java:222`、`:232` | `entries()` 的 `Iterator`（unmodifiableList → ArrayList iterator） | 是（×2） |
| `I/CoordinatorRequestManager.java:257` | `Optional.ofNullable(coordinator)`：heartbeat `poll`+`maximumTimeToWait`、commit `poll`+`maximumTimeToWait`、topology 各一次 | 是（每輪 ≥ 4 個 Optional） |
| `I/CoordinatorRequestManager.java:117` | backoff 中 `new PollResult(long)` | 只在 coordinator 未知時 |
| `I/AbstractHeartbeatRequestManager.java:195` | 成員活躍時每輪 `new PollResult(timeToNextHeartbeatMs)` | 是（STABLE 狀態） |
| `I/StreamsGroupHeartbeatRequestManager.java:484` | 同上 | 是（streams） |
| `I/AbstractHeartbeatRequestManager.java:184`、`:199`、`:234` | 送 hb 時 `Collections.singletonList` + `PollResult` + request data | 送 hb 時 |
| `I/CommitRequestManager.java:203-208` | `drain` 的多個 stream/`ArrayList`/`Map`（`:1454-1484`）、`new PollResult`；`findMinTime` 的 stream（`:241-244`） | 只在有 unsent 時；空路徑回 `EMPTY` 無配置 |
| `I/OffsetsRequestManager.java:171-173` | `new ArrayList<>(requestsToSend)` + `new PollResult(list)`（內含 `unmodifiableList` 包裝，`I/NetworkClientDelegate.java:336`） | **是，無條件** |
| `I/TopicMetadataRequestManager.java:88`、`:99` | `inflightRequests.iterator()`（LinkedList）+ `new ArrayList<>()` | **是，無條件** |
| `I/FetchRequestManager.java:155-157` | 無 pending 時回 `EMPTY`，無配置；有 pending 時 `prepareFetchRequests` 配置 `HashMap`/`HashSet`/`List`/lambda/stream（`I/AbstractFetch.java:424-496`、`I/FetchRequestManager.java:175-190`） | 只在有 pending 時 |
| `I/AbstractMembershipManager.java:1466-1469`、`I/ShareMembershipManager.java:177-180`、`I/StreamsMembershipManager.java:1129-1134` | 非 RECONCILING 早退，無配置 | 否 |
| `I/ShareConsumeRequestManager.java:485-486` | `processAcknowledgements` 每輪 `new ArrayList<>()` + `new AtomicBoolean()` + entrySet iterator | 是（memberId 已知後） |
| `I/ShareConsumeRequestManager.java:169-313` | `fetchMoreRecords` 時 `HashMap`、`partitionsToFetch()`（`subscriptions.fetchablePartitions` 新 list，呼叫兩次 `:172`、`:201`）、`HashSet`（`:200`）、`bufferedNodes()` 新 `HashSet`、stream + lambda、`new PollResult(requests)`（`:313`，即使空） | 等待 fetch 期間每輪 |
| `I/NetworkClientDelegate.java:210`、`:250` | `unsentRequests.iterator()` ×2（`ArrayDeque`） | 是 |
| `I/NetworkClientDelegate.java:171` | metrics record（可能有 boxing） | 是 |

---

## 9. Streams 與 Share 對同一核心的使用方式摘要

### Streams

- 入口：`StreamThread` 建立 `StreamsRebalanceData`（`S/StreamThread.java:575-576`、`:704-735`），透過 `KafkaConsumer` → `AsyncKafkaConsumer(config, k, v, Optional<StreamsRebalanceData>)`；以 `subscribe(topics, StreamsRebalanceListener)`（`I/AsyncKafkaConsumer.java:2159`）註冊 `DefaultStreamsRebalanceListener`（`S/StreamThread.java:892-893`）。
- 背景側新增三個 manager（§2.2 第 5-7 項），其餘（Coordinator/Commit/Offsets/TopicMetadata/Fetch）與 consumer 相同；`CommitRequestManager` 與 app-thread listener 註冊到 `StreamsMembershipManager`（`I/RequestManagers.java:239-240`）。
- 資料交換：heartbeat request 從 `StreamsRebalanceData` 讀 topology / processId / endpoint / clientTags / taskOffsetSum / reconciledAssignment（`I/StreamsGroupHeartbeatRequestManager.java:125-204`）；response 寫回 interval/statuses/partitionsByHost/topologyPushRequired（`:662-698`）。`TaskManager.taskOffsetSumSnapshot`（`S/TaskManager.java:1284`）與 `DefaultStateUpdater.taskEndOffsetSumSnapshot`（`S/DefaultStateUpdater.java:1213`）以 `AtomicReference` 提供 supplier。
- Rebalance callback：三種 `CompletableBackgroundEvent` → app thread `BackgroundEventProcessor`（`I/AsyncKafkaConsumer.java:290-336`）→ `DefaultStreamsRebalanceListener.onTasksRevoked/onTasksAssigned/onAllTasksLost`（`S/DefaultStreamsRebalanceListener.java:70-127`，內部呼叫 `TaskManager.handleRevocation/handleAssignment/handleLostAll` 並改 `StreamThread` state）→ `*CallbackCompletedEvent` 回背景。
- StreamThread 每輪 `handleStreamsRebalanceData()`（`S/StreamThread.java:1249`、`:1407`、`:1578-1640`）讀 statuses（SHUTDOWN_APPLICATION / MISSING_SOURCE_TOPICS / INCORRECTLY_PARTITIONED_TOPICS）與 `partitionsByHost`。

### Share

- `ShareConsumerImpl` 用同一個 `ConsumerNetworkThread`/`ApplicationEventHandler`/`ApplicationEventProcessor`/`BackgroundEventHandler`（`I/ShareConsumerImpl.java:170-184`），manager 只有四個（§2.2）。
- 沒有 `OffsetsRequestManager`/`FetchRequestManager`/`CommitRequestManager`：`RequestManagers` 對應欄位為 `null`/`empty`（`I/RequestManagers.java:116-126`）。
- `ShareMembershipManager.signalPartitionsAssigned` 直接在背景 `assignFromSubscribedAwaitingCallback` 並回傳已完成 future（`I/ShareMembershipManager.java:152-158`）——**沒有 app-thread callback 往返**；但 `enablePartitionsAwaitingCallback` 由 `AbstractMembershipManager.assignPartitions` 的 `whenComplete` 立即執行（`:1258-1266`）。
- ack 結果經 `ShareAcknowledgementEventHandler`（§4.5），`BackgroundEventHandler` 只承載 `ErrorEvent`（`I/ShareConsumerImpl.java:152-168`）。
- metadata error 走 `ErrorEvent`（`notifyMetadataErrorsViaErrorQueue=true`），另外 `poll` 迴圈每輪 `metadata.maybeThrowAnyException()`（`:644`）。
