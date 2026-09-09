# CONSUMER-NG 04：M2 設計——在 M1 引擎上做出完整的 `Consumer<K,V>`

M1 證明了 fetch 路徑（03）。M2 的目標是**同一條 I/O 執行緒**上加回 group membership、heartbeat、commit、coordinator 與 positions，讓 `NgKafkaConsumer` 實作完整的 `Consumer<K,V>`，先過 Jenkins `consumer_test.py`（前一條線的門檻，48 個 case），再過其餘 system test。效能與功耗目標不變：加回這些之後，03 的四張表不能退化，閒置喚醒只能來自協定本身（heartbeat 間隔、`fetch.max.wait.ms`）。

## 1. 重用什麼、重寫什麼（依 01 §2.1 的四關）

| 元件 | 決定 | 理由 |
|---|---|---|
| `ConsumerMembershipManager`（member 狀態機、reconcile、callback 排程） | **重用** | 協定細節多、有完整測試；它只依賴 `SubscriptionState`、`CommitRequestManager`、`ConsumerMetadata`、`BackgroundEventHandler` |
| `ConsumerHeartbeatRequestManager`、`CoordinatorRequestManager`、`CommitRequestManager`、`OffsetsRequestManager` | **重用** | 契約單純：`poll(now) → PollResult`；請求以 `UnsentRequest`（builder + node + future）交給網路層 |
| `SubscriptionState`、`ConsumerMetadata`、`ConsumerRebalanceListenerInvoker`、`OffsetCommitCallbackInvoker`、`BackgroundEventHandler` 與其 event | **重用** | app thread 與 I/O thread 之間既有的、有測試的通道 |
| `NetworkClientDelegate` | **重用作為 manager 請求的送出路徑** | 它包住我們的 `NetworkClient`，`addAll(PollResult)` + `poll()`；fetch 仍由 M1 引擎直送同一個 `NetworkClient` |
| `ConsumerNetworkThread`（每圈全掃、`maximumTimeToWait`） | 不用 | 前一條線量到它是平局，且是 busy loop 類 bug 的來源 |
| `ApplicationEventHandler` / `ApplicationEventProcessor`（app→bg 的 event bus） | 不用 | 每次 poll 兩個 event 的固定成本；改為命令佇列（只有 subscribe / commit / seek / close 這類真正的命令）+ poll 序號（volatile） |
| `FetchRequestManager` / `FetchBuffer` / `FetchCollector` | 不用 | M1 取代 |
| `AsyncKafkaConsumer` | 不用 | app 側重寫，但方法語意逐一對照它 |
| 前一條線的 `ManagerTask` / `LoopTimer` / `WaitCondition` / `LifecycleSequencer` | **移植** | 已在 `consumer_test.py` 48/48 驗證過的排程模型，含 R3 / R11 的修正與測試 |

## 2. 執行模型

一條 I/O 執行緒（M1 的 `FetchEngine` 執行緒擴充），每個 pass：

1. 處理命令佇列（subscribe / assign / seek / commit / pause / close…；每個命令帶 future 給 app thread 等）。
2. **app poll 的 housekeeping**（每個 poll 迭代一次，R11）：`membershipManager.maybeReconcile(true)`、`onConsumerPoll`、auto-commit timer、heartbeat 的 poll timer、pattern 訂閱刷新、positions 初始化。
3. 只跑「輸入變了」或「自己 timer 到期」的 manager（`ManagerTask`：ANY_INPUT / OWN_COMPLETION / TIMER_ONLY 的宣告與版本驗證）；把 `PollResult` 交給 `NetworkClientDelegate`。
4. M1 的 fetch 續發（credit 允許就發）。
5. 發佈一份不可變的 `PassDecision`（positions 是否齊、reconcile 是否進行中、有無 background event），只在 app thread 等的欄位改變時叫醒它（R3）。
6. 阻塞在 `NetworkClient.poll` 直到最早的 timer 或有輸入；沒有 100 ms 的安全網 timer。

閒置時的活動只剩：heartbeat（`heartbeat.interval.ms`，通常 3–5 s）、空 fetch 回應（`fetch.max.wait.ms`）、metadata 刷新（`metadata.max.age.ms`）。目標：閒置每秒 ≤ 3 次喚醒。

## 3. app 側（`NgKafkaConsumer<K,V>`）

- `poll(timeout)`：處理 background event（rebalance callback 需求 → `ConsumerRebalanceListenerInvoker`；錯誤事件）→ 執行 commit callback → M1 `RecordReader` 交付 records；position 以 `SubscriptionState` 為準（交付即前進，與 trunk 相同）；每個等待迭代登記一次 poll 輸入（R11）。
- assignment 的變化來自 I/O thread 的 reconcile：新增的 partition 建 M1 佇列、撤銷的 partition 清佇列與 credit；app thread 透過 callback 得知。
- `commitSync` / `commitAsync` / `committed` / `position` / `seek*` / `close`：命令 + future，語意逐一對照 `AsyncKafkaConsumer`（含 `close` 的順序：commit → leave → coordinator 查找停止 → 網路關閉，用 `LifecycleSequencer`）。
- 不做的假設：`Deserializer` 不要求 thread-safe（反序列化留在 app thread）；interceptor 與 callback 都在 app thread。

## 4. 分段

- **M2a**（門檻：Jenkins `consumer_test.py` 全綠）：`subscribe(topics, listener)`、membership + heartbeat + reconcile callback、auto-commit、`commitSync/Async`、`committed`、`position`、`seek/seekToBeginning/seekToEnd`、`assign`、`close`（leave group）、`wakeup()`、`pause/resume`、static membership（`group.instance.id`）。
- **M2b**：pattern 訂閱、`offsetsForTimes` / `beginningOffsets` / `endOffsets`、`partitionsFor` / `listTopics`、`currentLag`、`enforceRebalance`、metrics 名稱對齊、interceptor、telemetry、`groupMetadata`。
- **M2c**：接進 `ConsumerDelegateCreator`（`group.protocol=consumer` 走新實作），跑完整 system test 矩陣與 Streams。

每一段結束都重跑 03 的四張表（T2/T1 穩態、閒置長視窗、256 MB heap）。

## 5. 風險與對策

- **manager 期待被定期 poll**：`CommitRequestManager` 的 auto-commit timer 與 membership 的 `maybeReconcile(true)` 都在 app poll 路徑上；用前一條線的 housekeeping + R11（每個 poll 迭代是輸入）處理，測試沿用（一次長 `poll()` 內完成 join、callback、fetch）。
- **兩條執行緒互相喚醒**：R3 的閘門（只在 reconcile 進行中才因 reconcile 序號叫醒 app thread）與閒置長視窗量測是必做項目。
- **重用的 manager 帶著 `maximumTimeToWait()`**：不呼叫；它們的 `PollResult.timeUntilNextPollMs` 是唯一的 timer 來源。
- **`OffsetsRequestManager` 需要 `NetworkClientDelegate`**：用同一個 delegate 實例；positions 初始化（committed offset → reset）走它，M1 引擎的「未知位置就從 earliest 開始」拿掉。
