# KIP-1371（新版）：Consumer 背景 event loop 的 programming model

狀態：v1 草稿（2026-09-10）。分支 `fable/kip-1371-event-loop`，base upstream/trunk `74fbd50061`。用 Pattern Language 的結構：Context → Intent → Problem Domain → Forces → 替代方案 → 機制。「已觀察」附來源（`core-inventory.md`、`issues-traceability.md`、`../next-poll-condition/fable-review/REPORT.md`）；「假設」與「未驗證」明標。

設計方針（使用者 2026-09-10 指示）：不發明複雜 scheduler；偏好簡單、有界、可各自成 PR 的契約與機制。

## 1. Context

`AsyncKafkaConsumer`（`group.protocol=consumer`）、`ShareConsumerImpl` 與 Streams 群組協定共用同一個背景核心：

- 一條 `ConsumerNetworkThread`，每輪（pass）：drain application events → 依 `RequestManagers.entries()` 順序 poll 每個 manager 並立即 `addAll` → `NetworkClient.poll(min(各 manager 的 `timeUntilNextPollMs`, 5 s))` → 掃每個 manager 的 `maximumTimeToWait` 快取給 application thread → reaper 使逾期的 `CompletableApplicationEvent` 失敗 → 對仍未完成的 metadata-notifiable event 投遞 metadata error。（`core-inventory.md` §1.1）
- Application thread 用 event 下命令：大部分 `addAndGet` 阻塞等 future（deadline 由背景 reaper 執行），`AsyncPollEvent` 不阻塞；records 從 `FetchBuffer` 取；callback 需求與錯誤從 `BackgroundEvent` queue 取，但 **`BackgroundEventHandler.add` 不喚醒 app**（§5.1）。
- Consumer 七個 managers、Share 四個、Streams 八個（§2.2）；classic 不在範圍。

已觀察的成本結構（本機真 broker A/B，async-profiler）：

- 背景執行緒 82–88% 的 CPU 在 `Selector.select`／socket read；七個 manager 的 poll 在 profile 中 < 1%。
- 減少 manager poll 次數的原型（NextPollCondition）consume CPU/record +31%，來源是每次 poll 的物件配置與 app↔network 交接次數。
- 唯一量到的大成本是「阻塞卻回零等待」（coordinator unavailable：trunk 1.6 core，no-spin 後 < 0.05 core）。
- app thread 實際上是 100 ms 的 ticker：`FetchRequestManager.maximumTimeToWait` 無 in-flight 時回 `retry.backoff.ms`，`pollForFetches` 另有三個分支縮短到 `retry.backoff.ms`（§4.2）。

## 2. Intent

給寫 `RequestManager` 與 event processor 的人一個小而完整的模型，讓他們能**明確表達**、核心能**機械地保證或用測試釘住**：

1. 這次執行後，工作是可以推進、在等 deadline、在等別人的狀態改變、還是在等自己的操作完成。
2. 跨 manager 的依賴。
3. 受阻或某階段完成後，誰保存未完成的需求、誰負責 continuation。
4. timeout、wakeup、取消、close 與晚到回應的責任。
5. 狀態發佈、結果交付、通知與 callback 的順序。

不在 Intent 內：換 `RequestManager` 拓撲、actor／continuation framework、單一 state owner、每 pass 的 immutable snapshot、跳過未 ready 的 manager。這些都是候選機制，只有對應到具體 Force 且有證據才進來；目前證據反對前三者（見 §6）。

## 3. Problem Domain

### 3.1 工作的生命週期欄位

| 欄位 | 意義 |
|---|---|
| identity／owner | 哪個 manager 擁有它；代表哪一次 app 呼叫或哪個協定義務 |
| scope | admission 時捕捉的 partition 集合／assignment 世代／coordinator 世代 |
| blocker | 現在不能推進的原因：deadline、依賴狀態、in-flight request、app callback 未回覆 |
| milestone | admitted → attempted → (retry)* → completed／failed／cancelled；哪個 milestone 對 observer 有意義 |
| observer | app 端的 future／event 與它自己的 deadline；observer 結束 ≠ operation 結束 |
| late response | 回應在 scope 失效或 observer 結束後到達時，owner 要做什麼 |
| termination | close／fatal 時誰以什麼結果結束它 |

### 3.2 工作清單與現況（`core-inventory.md` §2.3、§6.3）

| 工作 | owner | blocker 表達方式（現況） | observer | late response（現況） | 缺口 |
|---|---|---|---|---|---|
| FindCoordinator | Coordinator | in-flight → `EMPTY`；backoff → 數字 | 無（其他 manager 讀 `coordinator()`） | 回應寫 `coordinator`，其他 manager 下一輪看到 | close 後仍可能送（18569 已修） |
| Heartbeat | Heartbeat | in-flight 時 `timeToNextHeartbeatMs()` 可為 0（**21031 open**） | 無 | response 更新 membership | 兩條等待通道都可為 0 |
| commitSync／Async | Commit | coordinator unknown → `EMPTY`，request 留在 unsent | future + deadline（sync 由 app timer） | 成功遲到 no-op；**未送出過的 commit 永不過期**（`maybeExpire` 需 `numAttempts > 0`） | observer 結束後仍會送出 |
| auto-commit | Commit | timer；只由 `AsyncPollEvent` 觸發 | 無 | callback 排進 invoker | 讀 `allConsumed()` 與 app `collectFetch` 推進 position 之間無順序保證（**18641 殘留**） |
| OffsetFetch（positions） | Offsets→Commit | `initializingPartitions` 已在 admission 捕捉（17674） | `AsyncPollEvent`／`CheckAndUpdatePositionsEvent` | 過期後例外快取到下一次呼叫 | 錯誤時間位移（屬於哪次呼叫不明） |
| ListOffsets（reset／timestamps／end） | Offsets | leader 未知 → `requestsToRetry`，metadata update 重送 | future + deadline（reaper） | **無 manager 端 deadline**；transient topic 留在 metadata 直到完成 | 永久重試 |
| OffsetsForLeaderEpoch（validation） | Offsets | `SubscriptionState.nextAllowedRetry`，無 timer | `AsyncPollEvent` | 錯誤快取到 `PositionsValidator` | 沒有喚醒來源，靠 app 100 ms ticker |
| Fetch | Fetch | 只在 app 設 `pendingFetchRequestFuture` 時建 request；response 不自動續發 | `CreateFetchRequestsEvent` | 舊 offset 的 response 由 `FetchCollector.initialize` 丟棄 | 續發靠 app 往返；無 in-flight 時 app 100 ms 醒 |
| Membership reconcile | Membership | consumer 只由 `AsyncPollEvent(canCommit=true)` 驅動；callback 由 app 回覆 event | `CompletableBackgroundEvent`（deadline ∞） | leave future 由 heartbeat 完成 | app 不會被 `BackgroundEvent` 喚醒 |
| TopicMetadata | TopicMetadata | 自帶 deadline，poll 時 expire | future | 過期移除 | — |
| close | app + 各 manager | 8 步序列（§6.4） | closeTimer | unsent commit（有 coordinator）在 step 8 後不再送 | 順序分散在三個 event + `pollOnClose` |

### 3.3 原始問題的形狀（`issues-traceability.md`）

| 形狀 | 案例（HEAD 狀態） | 一句話 |
|---|---|---|
| A 阻塞卻回零等待 | 20253、20426、20970、21010（已修，三次修同一個 `maximumTimeToWait`）；**21031 open** | 等待時間由「只在送出時更新」的 timer 算出，in-flight／coordinator unknown／未 join 時變 0 |
| B scope 跨階段外洩 | 17066、17674（已修） | 非同步鏈後段讀「現在」的 assignment，而不是 admission 時捕捉的集合 |
| C 發佈順序 | 15529／PR 21476（已修）；**18641 部分殘留** | position 前進、records 交付、`isConsumed`、auto-commit 快照的順序沒有明文契約 |
| D 錯誤交付與無進展喚醒 | 20854（已修，結構性）；**20397 open** | 錯誤屬於哪次呼叫不明；背景標記錯誤後不喚醒 app |
| E callback acknowledgement | 18160（已修） | callback 例外／中斷未回報背景 |
| F close 終止責任 | 18569、19357（已修，且互相衝突過） | 各 manager 各自決定 close 行為 |

相關 open：KAFKA-21049（`retry.backoff.ms=0` 時 fetch 等待仍 0）、KAFKA-20540（Streams HB UNSUBSCRIBED）、KAFKA-19804（HB interval 初值 0）。

## 4. Forces

- **F1 Liveness**：受阻的工作在 blocker 解除時必須被重新評估。「每 pass 全部 poll」內建這點；任何「跳過 manager」都把證明責任轉給 owner（S1 原型的兩個 High 回歸即來自此）。
- **F2 No busy loop**：受阻的工作不得要求零等待。零等待只能伴隨進展（送出請求）。兩條等待通道（network、application）都適用。
- **F3 Scope**：結果只能套用到請求時的 scope；失效的結果丟棄但仍清 in-flight。
- **F4 Publication order**：先改狀態、再發佈、再 signal；position 前進到 records 交付之間不得有可拋出的中斷點；auto-commit 只能快照已交付的 position。
- **F5 Observer ≠ operation**：app 的 timeout／wakeup／cancel 結束觀察，不取消 RPC；但「從未嘗試」的操作在 observer 結束後不應再開始。
- **F6 Thread confinement**：manager 狀態只在 network thread 改；user callback 只在 app thread；app 完成的 future 若觸發背景 continuation，經 event 回到背景。
- **F7 Cost**：成本在 socket I/O 與交接；新機制以 CPU/record、配置量、喚醒次數為代價指標。
- **F8 Compatibility**：`Consumer` API、`ConsumerRebalanceListener` 契約、metrics 名稱；share／Streams 共用核心；classic 不動。
- **F9 Simplicity／reviewability**：概念少、diff 小、每個改動有「改前失敗、改後通過」的測試、可各自成 PR。

張力：F1 vs F7（全 poll 看似浪費，量測說可忽略）；F2 vs 真到期（真到期的零值是進展，不能一律加 floor）；F4 vs F7（順序需要 volatile／lock）；F5 vs F9（晚到回應多一層責任）。

## 5. 替代方案

| 方案 | 內容 | 對 Forces | 狀態 |
|---|---|---|---|
| S0 現狀 | 全 poll；兩條數字通道；契約隱含 | F1 內建；F2 靠自律（A 類反覆出現） | 基線 |
| S1 NextPollCondition／Signal 排程（Codex 原型） | 未 ready 不 poll | F1 轉嫁；F7 +31% | 已量測，不採 |
| S2 每 pass immutable snapshot + 版本等待（consumer-ng R3） | 背景每 pass 發佈決定 | F4 強；需改寫多條等待路徑；app 自己推進 position 不在 snapshot 內 | 未在 trunk 驗證 |
| S3 Operation／continuation framework | 操作物件化 | F3、F5 明確；F9、F7 差 | 不採 |
| S4 Codex 目前主線（contract-guided：post-I/O pass、CoordinatorAccess、response batching） | 局部契約 + 一次額外 pass | 額外 pass 量到 CPU +5–6%（Jenkins 930/931）且 ablation 反向；typed activation write-only | 部分想法可用，機制不採 |
| **S5 契約 + 最小機制，保留全 poll** | 五條契約；每條一個小機制或只有測試 | F1 內建；F2 機械；F3–F6 契約＋測試；F7 不變或改善；F9 最小 | **採用** |

## 6. 契約（S5）

每條契約：定義 → 現況違反處 → 機制（最小）→ 釘住的測試 → 對應 issue。

### C1 等待原因契約（F2）

**定義。** 每個 manager 的 `poll()` 回傳三種之一：
- `progress`：本次送出請求（`timeUntilNextPollMs` 可為 0，只因 request 要立刻送）；
- `retryAfter(delay)`：等時間；
- `awaitInput`：等自己的 in-flight 完成或等別的 owner 改狀態（`Long.MAX_VALUE`；下一 pass 因 response／event 自然重評估）。
「沒送出請求卻回 0」是契約違反。

`maximumTimeToWait()` 的定義改為：**application thread 必須主動呼叫 poll() 才能推進、且現在可執行的動作**的最早期限。可執行的動作只有：poll timer 刷新（HB）、auto-commit 觸發（需 coordinator 已知且無 in-flight commit）、Streams topology push。等 in-flight、等 coordinator、等 DNS 都不是 app 可執行的動作 → `Long.MAX_VALUE`。這條把 20253→20970→21010→21031 四次修同一方法的 pattern 變成規則。

**機制。**
1. `PollResult` 加三個 static factory（`progress(requests)`、`retryAfter(ms)`、`awaitInput()`），只是命名，不加欄位。
2. `ConsumerNetworkThread.runOnce`：manager 回 `timeUntilNextPollMs == 0` 且 `unsentRequests` 為空 → 記 metric `network-thread-zero-wait-without-progress-total`，並把該值視為 `retry.backoff.ms`（下限 1 ms）。這是安全網加偵測器，不是排程器。
3. `HeartbeatRequestState.timeToNextHeartbeatMs()` in-flight 分支（= KAFKA-21031，#23357 的修法）；`maximumTimeToWait` 各 manager 依定義修正（多數已由 20253/20970/21010 完成）。
4. app 端 `pollForFetches` 的 `retry.backoff.ms` 縮短分支加 1 ms 下限（KAFKA-21049）。

**測試。** 每個 manager 在「同一狀態、時間不前進」下連續 poll 兩次不得回 0 而無請求（表格驅動）；in-flight／coordinator unknown／JOINING 三種阻塞狀態的兩條通道；`ConsumerNetworkThreadTest` 對違反契約的 mock manager 驗證 clamp 與 metric。

### C2 Scope 與晚到回應契約（F3、F5）

**定義。** 跨多次 poll 的操作在 admission 時捕捉 scope；回應套用前驗證；失效的結果丟棄但清 in-flight。observer 結束不取消已送出的 RPC；**從未嘗試**的操作在 observer deadline 過後不再開始。

**現況違反。** 未送出過的 commit 永不過期（`CommitRequestManager.maybeExpire` 需 `numAttempts > 0`）；ListOffsets 無 manager 端 deadline；validation 錯誤時間位移。

**機制。** (1) `RetriableRequestState.maybeExpire` 對「有 deadline 且從未嘗試」的請求同樣過期（行為變更：commitSync 逾時後不再事後送出；需 reviewer 確認）；(2) `ListOffsetsRequestState` 帶 observer deadline，過期時退出 `requestsToRetry` 並移除 transient topic；(3) 只加測試：OffsetFetch／ListOffsets／validation 在 seek／assign 後的晚到回應不得套用。

**測試。** 每種操作三個案例：observer 逾時後回應到達、scope 變更後回應到達、close 後回應到達。

### C3 發佈與通知契約（F4，C／D 類）

**定義。**
- 背景可見效果的順序：改狀態 → 發佈 → signal。
- 背景把任何「app 必須處理的東西」放進 queue 或標記到 `AsyncPollEvent` 後，必須喚醒 parked 的 app：records（已有）、`BackgroundEvent`（**現況不喚醒**）、metadata／fatal error 標記（**20397 open：不喚醒**）。
- auto-commit 只能快照「上一次完成的 poll() 已交付」的 position。

**機制。** (1) `BackgroundEventHandler.add` 與 `maybeFailOnMetadataError` 之後呼叫一個 `applicationWakeup` hook（實作為 `fetchBuffer.wakeup()`，Share 為 `shareFetchBuffer.wakeup()`）；(2) `FetchBuffer.wakeup()` 在沒有 waiter 時不取 lock（配合 (1) 的頻率）；(3) interval auto-commit 的 offsets 快照改由 app thread 在 poll() 進入時、`collectFetch` 之前捕捉並隨 `AsyncPollEvent` 帶入，只在背景發佈的 `autoCommitDueMs` 已到時才捕捉（避免每次 poll 複製 map）。這修復 18641 殘留而不恢復 18376 拿掉的阻塞。

**測試。** 20397：metadata error 在 app parked 後標記，poll 必須在短時間內拋出；`BackgroundEvent` 入列後 parked app 必須醒來；interval auto-commit 不包含本次 poll 才推進的 position（`EventLoopContractRegressionTest`，改前失敗）。

### C4 終止契約（F5、F6，F 類）

**定義。** close 是一份明文序列（§6.4 的八步）；每種 pending 工作有唯一的終止結果（成功送出／`CommitFailedException`／`TimeoutException`／丟棄），寫在表裡而不是散在各 manager 的 `closing` 旗標。

**機制。** 只加文件與測試，不新增 sequencer；若測試顯示某 pending 工作沒有終止者，補最小修正。

**測試。** close 時每種 pending 工作（unsent commit 有／無 coordinator、in-flight commit、pending positions、pending ListOffsets、未完成 rebalance callback）的結果與時限；晚到回應在 close 後不得 NPE、不得重開工作。

### C5 執行緒歸屬契約（F6，E 類）

**定義。** manager 狀態只在 network thread；user callback 只在 app thread；app 對背景 future 的完成一律經 `*CallbackCompletedEvent`；`SubscriptionState` 的 position 由 app thread 推進、其餘欄位由背景改，並列出每個欄位的 writer。

**機制。** 文件（欄位 × writer 表）+ 既有測試（18160）+ 針對 `WakeupException`／`InterruptException` 在每種 callback 中的回報測試（目前 async 只有一個案例）。

## 7. 效能（只保留與契約直接相關的部分）

KIP 正文只放兩類量測：(1) 證明契約修的問題有成本（busy loop：trunk unavailable 1.6 core → < 0.05 core）；(2) 證明契約本身不退步（consume CPU/record、idle 喚醒次數，JMH 交錯 A/B）。JMH 基線：loop-level（真 `ConsumerNetworkThread.runOnce` + `MockClient`：idle／blocked／consume／recovery）與 end-to-end（本機真 broker）。

邊角優化（`FetchBuffer.wakeup` 無 waiter 不取 lock、`Selector.wakeup` 只在 parked 時、每 pass 的 `LinkedList`／`ArrayList`／`Optional` 配置、HB STABLE 每輪 `new PollResult`）**不放進 KIP**；各自以 MINOR PR 附 JMH 處理，見附錄 `perf-notes.md`。唯一例外是「移除 app 端 100 ms retry」：它是 C3 通知契約的直接後果，放在 C3 的效益段落，且必須在通知到位後才做。

## 8. 假設與未驗證

- H1：C1 的 factory 與 clamp 不改變任何 share／Streams 測試行為（全套測試）。
- H2：C3 通知到位後移除 app 端短 retry，idle 喚醒次數下降且 time-to-first-record 不退（JMH + 真 broker）。
- H3：C2 的行為變更（未嘗試的 commit 在 deadline 後過期）被 reviewer 接受；fallback 是只在 close 路徑實作。

## 9. KIP 的章節結構（一份 KIP，先重點再深入）

reviewer 偏好（`reviewer-preferences.md`）：最小修法、每個 manager 一個 `...DoesNotSpin` 測試、內部結構不開 KIP、不要新框架。因此 KIP 的正文只講契約與它們修的問題；機制放在各契約的「最小落地」小節，量測與替代方案放附錄。

1. **Summary**（半頁）：五條契約各一句；解決的 issue 家族；public interface 變更只有一個 metric；沒有 scheduler、沒有新執行緒模型。
2. **Motivation**：3.3 的六種形狀 + 「同一方法修四次」的證據（issues-traceability）。
3. **Contracts**：C1–C5 各一節，固定格式：定義 → 現況違反處（file:line）→ 最小落地 → 釘住的測試 → 對應 issue。
4. **Public Interfaces**：`network-thread-invalid-poll-result-total`；C2 的行為變更（明列前後差異）。
5. **Compatibility**：Consumer／Share／Streams 各自受影響的契約；classic 不動。
6. **Test Plan**：契約測試表（每條契約 × 每個 manager）；整合與系統測試清單。
7. **Rejected Alternatives**：S1（NextPollCondition 排程，+31%）、S2（pass snapshot）、S3（operation framework）、S4（post-I/O pass，+5–6%），各附量測來源。
8. **Appendix**：core inventory、issue traceability、benchmark 方法與原始數據、邊角優化清單（不在 KIP 範圍）。

實作切片（同一分支、依序 commit，但每個 commit 對應一條契約，方便 reviewer 逐條看）：P0 改前失敗測試 → P1 C1 → P2 C3 通知 → P3 C3 快照 → P4 C2 → P5 C4/C5 文件與測試 → JMH 基線與契約相關量測。

## 10. 待補

- `use-case-matrix.md`（use case × consumer 類型 × 契約 × 測試）
- `test-inventory.md`、`codex-model-summary.md`、`reviewer-preferences.md`（agent 產出）
- 各切片的實作與量測記錄
