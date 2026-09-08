# AsyncKafkaConsumer v2 設計文件

- 承接 `01-problem-analysis`（問題）與 `02-deadline-model-and-baseline`（量測與實驗）；基準 commit `820533b870`
- **本文件只講新架構本身改了什麼、帶來什麼。** 舊架構也能套用的優化（fetch 深度 / prefetch、`RecordHeaders`、per-record 解碼路徑、M3 的判斷）在 `04-portable-optimizations`，不在這裡
- 目標：`KafkaConsumer` / `Consumer<K,V>` 公開 API 與行為契約不變，`group.protocol=consumer` 下穩態吞吐 ≥ 2 × 現行 `AsyncKafkaConsumer`，且 CPU 秒/GB 不高於現行
- 非目標：不改 wire protocol、不改 `NetworkClient` / `Selector`、不改 `SubscriptionState` 的對外語意、不改 `Deserializer` 契約、不新增需要 KIP 的公開設定

---

## 摘要：設計是什麼、解決了什麼

**設計核心（三句話）**

1. **一條事件驅動的背景執行緒**（`ConsumerEventLoop`）：三種工作來源直接分派——網路 response 交給擁有它的元件、每個 manager 自己的 timer、應用端送來的 command。沒有「對所有 manager 取最小等待時間」這回事。
2. **Fetch 有唯一的 owner**（`FetchPipeline` + `FetchPipelineModel`）：response 直接回到它，在同一個回呼裡做 epoch 檢查、放進 sink、決定續發，不經應用執行緒、不等下一輪。所有資料帶 position epoch，seek / reset 後舊資料自動作廢；續發規則與 fencing 是四條明列的不變式，不是散在 handler 裡的 flag。（在途資料的深度只是模型的一個參數，它的增益屬於 04 文件。）
3. **無鎖交接**（`RecordSink` + `Parker` + `CreditReturnQueue`）：背景執行緒 publish、應用執行緒 take，沒有鎖、沒有 condition variable；應用端只在「有資料、有事件、有回呼」時被叫醒，消費完把 credit 還回去。穩態下 `poll()` 不產生任何 event、不做任何 syscall。

**解決了現行 async consumer 的哪些問題**（對照 01 文件的分類）

| 問題類別 | 原本 | 現在 |
|---|---|---|
| **B 續發的粒度** | 續發要等一整輪迴圈加應用執行緒的處理，因為 fetch 沒有 owner，只有 gate 與 flag | response 回到 owner，同一個 stack 內續發；fencing 是資料模型的不變式。（深度本身的增益在 04 文件） |
| **C 每次 poll 的固定成本** | 每 poll 2 個 event + 2 次 selector wakeup、`collectFetch` 跑兩次、`FetchBuffer` 單鎖 + `signalAll`、100 ms 空喚醒 | 零 event、零 wakeup；喚醒合併（`LoopSignal`）；狀態式 park 沒有空喚醒；等待上限就是使用者的 timeout |
| **E 背景執行緒節奏**（deadline-driven） | 迴圈節奏被最沒耐心的 manager 綁架，任一 manager 回 0 就 busy loop；六張 JIRA | 每個 manager 自己的 timer、1 ms 下限、只重跑該 manager；response 直接分派 |
| **D 共享狀態** | `FetchBuffer` 所有操作共用一把 `ReentrantLock`，每個 partition 一次 `signalAll`；**結構性風險，未實際量到競爭**（CPU profile 看不到被鎖擋住的時間，且現行每 node 一個 in-flight 使碰撞機會低） | 狀態有唯一 owner；跨執行緒只走四個明列通道（sink、credit、command、result），沒有鎖可競爭 |
| inflightPoll 生命週期（KAFKA-20780 / 20844 一類） | 分散在五個方法的 future + flag 狀態機 | 不存在；每次 poll 的簿記由 loop 用 manager 的既有公開方法完成 |

**不在這份文件裡的**：舊架構也能套用的優化（fetch 深度 / prefetch、`RecordHeaders`、per-record 解碼、M3）在 04 文件；它們的增益不算架構的。架構本身的貢獻用 §2 的 2×2 實驗衡量：每次 poll 的固定成本歸零（CPU 秒/GB −10~30%，高 poll 頻率下讓同樣的 fetch 深度從 1.54× 變 2.60×）與 busy-loop 類 bug 的結構性消失。

**刻意不動的**：六個協定 manager、`ApplicationEventProcessor`、`SubscriptionState`、`NetworkClient`，以及公開 API。既有 manager 累積的邏輯與 bug fix 原樣保留，由 `ManagerTask` 透過既有的 `RequestManager.poll()` 契約驅動（§5）。

---

## 主張（Semantics）：這個設計保證什麼

仿 KIP-1371 的寫法，先列主張，再列機制。每條主張是一句可以驗證的規範句；違反它就是 bug。它們合起來要涵蓋 KIP-1371（KAFKA-20995）的四類問題與 06 文件的五類 busy loop，同時保留 §1.5 的成本目標。細則在 06 文件（R1–R10），實作順序在 07 文件。

| # | 主張 | 保證什麼 | 對應的問題 | 機制 | 驗證 | 狀態 |
|---|---|---|---|---|---|---|
| **S1 推進宣告** | 一個 manager 的每次執行，結束時必須是三者之一：`WORK_NOW`（已送出 request 或改變狀態）、`RETRY_AT(deadline)`、`WAIT_FOR(input)`；它只會在「自己的 timer 到期」「宣告的輸入到達且版本嚴格大於宣告時的版本」「command」三者之一時被再次執行 | 沒有「延遲 0 卻什麼都沒做」；自我觸發類 busy loop 對已宣告的 manager 在結構上不可能 | KIP-1371 問題 1、2；busy loop 第 1、5 類 | `RequestManager.waitCondition()`（預設 `ANY_COMPLETION` = 安全退路）+ `ManagerTask` 版本驗證 + `LoopTimer` 1 ms 下限 | 每個 manager 一個凍結時鐘的執行次數上界測試；`testResetUsingDurationBasedAutoResetPolicy` | 07 第 2 步；下限已有 |
| **S2 輸入有身分** | 到達迴圈的每個輸入都有身分（哪個 owner 的哪個 request 完成、哪個 command、哪個 metadata 版本），且讓單一的 `stateVersion` 前進；沒有任何工作是「因為迴圈醒了」而做 | 排程決定可以針對輸入而不是重新掃描；版本比對有共同的座標 | KIP-1371 問題 2；S1 的基礎 | `ConsumerEventLoop.stateVersion`；`ManagerTask` 對每個 `UnsentRequest` 掛回呼 | `decisionIsPublishedEveryPassAndItsVersionAdvancesOnlyOnInputsWithIdentity` | 版本已做；身分標記在第 2 步 |
| **S3 完成不等於進展** | 沒有改變狀態的完成不觸發工作；同步完成的 future 永遠不觸發重跑 | 不經 timer 的自我觸發空轉不存在 | KIP-1371 問題 1；busy loop 第 1 類 | `maybeUpdateFetchPositions` 的非同步旗標；S1 落實後由版本比對統一保證 | `positionsAttemptThatCompletesWithoutProgressDoesNotRerunManagersOnItsOwn` | 已做（個案）；第 2 步統一 |
| **S4 每 pass 一份決定，先發佈再阻塞** | 迴圈每個 pass 結束時發佈一份不可變的 `PassDecision`（版本、下一個 deadline、position 狀態、reconciliation 序號、待交付事件），用阻塞前一刻的新時間計算；應用執行緒的等待帶版本，只在它可能等待的欄位改變時被叫醒；所有生產者先發佈再 signal | 應用執行緒永遠不會在看不到釋放條件的狀態下等待，也不會為沒有改變的狀態醒來；等待上限與它看到的狀態來自同一個 pass | KIP-1371 問題 3；02 文件 §2.3、§2.4；R3、R7 | `PassDecision`、`publishDecision`、`Parker` / `FetchBuffer` 的版本式等待、`LoopSignal.prepareToPark` | `applicationIsWokenOnlyWhenADecisionFieldItMayWaitForChanges`、`LoopSignalTest` 交錯案例 | 已做（07 第 1 步） |
| **S5 唯一 owner、列舉的通道、資源守恆** | 每個可變狀態有唯一的 owner 執行緒；跨執行緒只經由列舉過的通道；從通道取出的東西在所有路徑上都有 owner 或已釋放 | 沒有兩條執行緒同時改一個狀態的競態；例外路徑不會讓 batch、credit、事件消失 | KIP-1371 問題 3、4；R4、R5 | 欄位分組與 javadoc；`SinkCollector`（完整版）的 ownership 規則 | `SinkCollectorTest`；執行緒斷言待補 | 大致已做；斷言與 property test 待補 |
| **S6 生命週期單一排序** | close、leave-group、commit-on-close、停止 coordinator 尋找、停執行緒，以及 fatal error 的「commit 先讀、heartbeat 後清」，由迴圈執行緒上的一個狀態機以固定順序執行，共用一個 timer | 沒有元件能各自決定關閉順序；順序依賴不再靠 `entries()` 的排列 | KIP-1371 問題 4；R9 | `LifecycleSequencer`，取代三個 close 事件 | coordinator 不可用時 close 仍完成（`ConsumerBounceTest.testAsyncClose`）；fatal error 重排測試 | 07 第 4 步 |
| **S7 無進展有界且可見** | 跑了工作但 `stateVersion` 沒前進、且沒有 timer 或 command 的 pass 會被計數；連續發生時 manager pass 以有上限的指數退避；計數以 metric 暴露 | 元件間循環與外部風暴從「以 RTT 速率轉」變成有界、可觀測 | busy loop 第 2、3 類 | 進展帳、`passes-without-progress` metric | 兩個互相觸發但淨狀態不變的 mock manager，pass 頻率必須衰減 | 07 第 3 步 |
| **S8 每次 poll 零 event、至多一次條件式 wakeup** | 應用執行緒的穩態 `poll()` 只寫 volatile 序號；「請建下一批 fetch」是旗標；只在迴圈已 park 時才 wakeup | 每次 poll 的固定成本與 poll 頻率脫鉤 | §1.5 第 2 列；busy loop 第 4 類 | `onApplicationPoll`、`requestFetch`、`LoopSignal.wakeupIfParked` | 三方 A/B：mpr50 +17%、CPU/GB −11% | 已做 |

**與 KIP-1371 三條主張的對照**：KIP-1371 主張 (i) manager 結果三分、(ii) 跨 manager 的觀察依產生 request 的狀態版本排序並由 owner 套用、(iii) 一份不可變的彙總 timing 決定先於所有可見效果發佈。S1 對應 (i)；S2 加 S5 對應 (ii)；S4 對應 (iii)，差別在我們不把效果暫存到輪尾——續發仍在 response 回呼裡發生（§2.1 的收益來源）——而是讓等待帶版本，對等待者提供同樣的保證（永遠看得到釋放條件）而不延後資料路徑。S3、S6、S7、S8 是 KIP-1371 沒有明列、但四類問題與 busy loop 分類要求的補充。

---

## 0. 設計原則（回應「不要犧牲通用性的 hack」）

1. **每個效能手段都必須是一個有名字的抽象，而不是一個旗標。** 實驗分支裡的每個 hack 在本設計中的正規對應：

| 實驗 hack | 為什麼是 hack | 正規做法（本文章節） |
|---|---|---|
| `-Dkafka.exp.prefetch.depth` system property | 繞過設定系統、無文件、無驗證 | `FetchPipeline` 的 credit 模型，上限由既有設定推導；可調參數走 `ConsumerConfig.defineInternal`（§4.3） |
| sessionless full fetch（`FetchMetadata.LEGACY`） | 放棄 fetch session，broker 端每次全量 | 保留 incremental session；每 node 一個 in-flight 本來就滿足 epoch 序列要求，只是把「續發時機」從應用執行緒改到 response 到達（§4.2） |
| `expPrefetchOffsets` 無 fencing | seek / reset / assignment 變更後狀態失效，實測 seek 後偶爾停滯 | `PartitionFetchState` 帶 **position epoch**，所有 buffered / in-flight 資料標記 epoch，不匹配即丟棄（§4.4） |
| 三個 `HashMap` 計數器散落在 `AbstractFetch` | 狀態沒有 owner、沒有不變式 | 單一 `PartitionFetchState` 物件，不變式明列（§4.1），單執行緒擁有 |
| 從 `AbstractFetch` 的 `finally` 呼叫 hook 重設 gate | 隱式控制流 | `FetchPipeline` 本身就是 response handler 的 owner，沒有 gate（§4.2） |

2. **狀態有唯一 owner 執行緒；跨執行緒只走明列的通道**（§3.3）。沒有「兩邊都會碰、靠 `synchronized` 保平安」的物件。
3. **不重寫已經正確且與效能無關的東西**：KIP-848 的 membership / heartbeat / commit / coordinator / offsets / topic-metadata 六個 request manager 保留，用 adapter 掛進新的執行模型（§5）。它們的每輪固定成本是雜訊（01 文件 E 類），重寫它們是風險不是收益。
4. **新舊並存、範圍限定**，與當年 Classic → Async 相同：新實作是新類別，由 `ConsumerDelegateCreator` 切換；`AsyncKafkaConsumer` 原封不動。**改寫只針對 `group.protocol=consumer` 的一般 consumer**，也就是 deadline-driven 的 `ConsumerNetworkThread` 這一段；Kafka Streams 與 share consumer 繼續使用 `AsyncKafkaConsumer` / `ConsumerNetworkThread`，不在本次範圍（§7.4，2026-09-08 決定）。
5. **每個里程碑有可量測的退出條件**（§8），用 02 文件建立的 benchmark 驗證，不憑感覺。

---

## 1. 一句話架構

> 一條 **事件驅動** 的背景執行緒（reactor：I/O readiness + per-task timer + command queue），擁有 **資料驅動** 的 `FetchPipeline`（response 一到就用 credit 決定續發），透過 **無鎖 per-partition 佇列** 把已完成的 fetch 交給應用執行緒；應用執行緒的 `poll()` 在穩態下 **不產生任何 event、不做任何 syscall**，只有拿資料、更新 position、歸還 credit。（背景執行緒的閒置 pass 仍有 metrics、空 queue、metadata 版本、timer 頂端的檢查——是「便宜」，不是「零」。）

```
應用執行緒                                   背景執行緒 (ConsumerEventLoop)
────────────────────────                     ─────────────────────────────────────────────
poll(timeout)                                loop:
 ├─ drainResults()  ◄── ResultQueue ──────── │  ├─ dispatch I/O readiness ──► NetworkClient.poll(0)
 │   (commit callbacks, rebalance 回呼,       │  │     └─ FetchResponse ──► FetchPipeline.onResponse()
 │    錯誤, 完成的 future)                     │  │            ├─ epoch 檢查 → RecordSink.publish(batch)
 ├─ RecordSink.take()  ◄── SPSC/partition ── │  │            ├─ credit 扣減
 │   └─ RecordDecoder: CRC/解壓/解析/反序列化 │  │            └─ 立刻 maybeIssueFetch(node)
 ├─ position 更新 (SubscriptionState)         │  ├─ dispatch timers ──► ManagerTask.poll() / auto-commit / HB
 ├─ credit 歸還 ──► CreditCounter ─(閾值喚醒)─► │  ├─ dispatch commands ◄── CommandQueue ◄── subscribe/seek/commit/...
 └─ 空則 park(直到 sink 非空 | result | 逾時)  │  └─ select(next timer deadline)
```

---

## 1.5 Deadline-driven 究竟哪裡不好

KAFKA-14246 的目標是「可預測、事件驅動的通訊」（附錄 A），但 `ConsumerNetworkThread.runOnce()` 落地成的是一個 **polling scheduler**：每一輪醒來，不管是誰、為什麼把它叫醒，都做同一件事——問每個 `RequestManager`「你現在要不要送請求、下次多久要再被問」，取所有答案的最小值當下一次阻塞的上限。事件（response 到達、應用端送來 event、timer 到期）只負責**打斷阻塞**，不攜帶「該做什麼」的資訊；真正的決策永遠是「全部重評估一遍」。

這個模型有五個具體的壞處。前四個是效能，第五個是正確性；每一項都有 02 文件的量測或公開 JIRA 佐證，右欄是 v2 對應的做法。

| # | 壞處 | 機制（現行程式碼） | 證據 | v2 的做法 |
|---|---|---|---|---|
| 1 | **推進的粒度是「一整輪」，而不是「這個事件」** | fetch response 在 `NetworkClient.poll` 內同步進 `FetchBuffer`，但下一個 fetch 要等：本輪結束 → 應用執行緒被喚醒、消費整批 → 送 `CreateFetchRequestsEvent` + selector wakeup → 背景執行緒**下一輪**才 `FetchRequestManager.poll` 建請求。應用執行緒的處理時間被串進管線，管線深度 1 | 100B 場景三方（應用、背景、broker）各只用 22–29% CPU，全部在等彼此；只把「續發」改成 response 到達即發，356 → 766 MB/s（2.15×，02 文件 §3.7） | §4 `FetchPipeline`：response 一到，在同一個回呼裡用 credit 決定續發，不經應用執行緒、不等下一輪 |
| 2 | **每輪成本固定，而輪數隨應用端 poll 頻率成長** | 每次 `poll()` 送 1–2 個 event，每個 event 一次 selector wakeup（pipe write），每次 wakeup 一整輪：N 個 manager 的 `poll` + N 個 `maximumTimeToWait` + 兩個 reaper 的 list 掃描 + sensor | `max.poll.records=50`：背景 13k 輪/s，CPU 秒/GB 比 classic 多 37%、sys 多 79%；`IOUtil.write1` 佔應用執行緒 4.9%（02 文件 §3.4–3.5） | §3.2 穩態 `poll()` 不產生 event、不做 syscall；§3.4 喚醒合併；loop 的 pass 只跑「髒」的東西（command、到期 timer、被標記的 manager） |
| 3 | **節奏被最沒耐心的 manager 綁架，而且是全域的** | `pollWaitTimeMs = min(所有 timeUntilNextPollMs)`、`cachedMaximumTimeToWait = min(所有 maximumTimeToWait)`。一個 manager 回 100 ms（`FetchRequestManager` 無 in-flight 時回 `retryBackoffMs`），整條執行緒就每 100 ms 醒一次；一個回 0，其他 manager 全部陪著被每輪重問 | 閒置的 consumer 兩條執行緒各每 100 ms 醒一次（02 文件 §2.2） | §5 `ManagerTask`：每個 manager 自己排自己的 timer，到期只重跑它自己；沒有跨 manager 取最小值的地方 |
| 4 | **時間只在阻塞前取樣一次** | `currentTimeMs` 在 `select()` 之前取，阻塞最多 5 s 後，`maximumTimeToWait`、reaper、`maybeFailOnMetadataError` 用的都是舊時間；`cachedMaximumTimeToWait` 給應用執行緒的又是上一輪的值 | deadline 精度是「一輪」而非毫秒；應用端拿到的等待上限天生過時（02 文件 §2.3） | §3.1 `LoopTimer` 用執行時的時間判斷到期；應用執行緒不需要背景執行緒告訴它「可以等多久」，它 park 在 sink 狀態上（§3.2） |
| 5 | **正確性依賴每個 manager 都把 wait time 算對** | 模型沒有「誰喚醒我、為什麼」的資訊；任何一個 manager 在任何一個狀態算出 0，就是 busy loop，而且症狀（CPU 100%）與根因（某個 manager 的某個分支）相距很遠 | KAFKA-20426、20540、20970、21010、21031、19804 六張 JIRA 同一根因，三張仍 open（02 文件 §4） | §3.1：timer 有 1 ms 下限（`LoopTimer.MIN_DELAY_MS`），到期只重跑該 manager；「某個 manager 回 0」只會讓它自己每 1 ms 跑一次，不會拖動整個 loop。這只消除 **timer 路徑**的這類 bug；loop 自己的其他觸發仍要逐條證明有界（附錄 F 的 F4 就是一條漏掉的） |

還有一個沒有直接量到、但決定了設計方向的問題：**所有關注點在同一輪裡序列化，沒有優先權**——一個大 fetch response 的處理、heartbeat、commit、metadata 更新、rebalance reconcile 都排在同一個 `runOnce()` 裡（02 文件 §2.6）。v2 沒有引入優先權佇列（那會是另一種複雜度），而是讓每個來源的工作變得夠小、夠獨立：fetch response 的處理只剩「epoch 檢查 + 放進 sink + 續發」，per-record 工作全部在應用執行緒；manager 只在自己被觸發時才跑。

一句話：deadline-driven 把「什麼時候醒來」算得很仔細，卻把「醒來後要做什麼」交給全量輪詢。事件驅動反過來：事件本身就攜帶要做的事（response → 它的 owner，timer → 它的 task，command → 它的 handler），loop 只是分派。附錄 B 的結果是這個差別的量化：100B 場景 2.33×、`max.poll.records=50` 場景 2.83×，而 CPU 秒/GB 回到 classic 水準（2.56 vs 2.48；baseline 是 3.57）。

---

## 1.6 與 KIP-1371 問題清單的對照，以及「這算不算事件驅動」

KIP-1371（KAFKA-20995，Consumer Reactor）列了四類問題。本設計的目標是 §1.5 的吞吐與固定成本，不是 KIP-1371 的狀態一致性，所以只有部分重疊，逐條說明：

| KIP-1371 的問題 | 本設計 | 判定 |
|---|---|---|
| 1. 沒有可行進展的緊急工作：manager 的 timer 回 0，但 coordinator 不可用或前一個 request 還在飛 | timer 1 ms 下限、per-manager timer、到期只跑該 manager：把傷害從「整條執行緒 busy loop」縮成「那一個 manager 每 1 ms 跑一次」。manager 仍然回 0，結果仍不表達可行性；附錄 F 的 F4 就是本設計自己迴圈裡同類的一個實例 | **有界化，未解決** |
| 2. 空結果語意不明：空的 `PollResult` 不說是時間、網路完成、還是其他輸入才能推進 | 完全保留 `RequestManager.poll()` 契約，沒有改結果型別。迴圈用「任何 request 完成就跑全部 manager」補償，正因為結果語意不明所以只能粗粒度 | **未解決**（KIP-1371 / `NextPollCondition` 改的正是這裡） |
| 3. 發佈與等待的順序競態：狀態、錯誤、完成、喚醒以不同順序可見，應用執行緒可能在沒看到釋放條件時進入等待 | 應用端的等待是狀態式（`Parker.await(condition)` 醒來後重新評估；`LoopSignal.prepareToPark` 是 Dekker 式），`RecordSink` 先 publish 再 signal，背景事件佇列會叫醒；`FetchPositionsErrorEvent` 在交付時檢查是否過期。但沒有 KIP-1371 的「一份不可變的彙總 timing 決定先於所有可見效果發佈」，不同來源各自 signal，跨來源順序沒有保證（附錄 F 的 F1 / F7 是這類缺口） | **部分解決**（等待必看到釋放條件：有；一致版本：無） |
| 4. 生命週期依賴分散：coordinator 發現、commit、leave-group、shutdown 由不同元件各自啟停，沒有最終一致的視角 | close 路徑照抄 `AsyncKafkaConsumer`（`CommitOnCloseEvent`、`LeaveGroupOnCloseEvent`、`pollOnClose`）。Commit「讀」與 Heartbeat「清」fatal error 的順序依賴（兩份審查都指出）就是這一類，仍在 | **未解決** |

**這算不算事件驅動？** 兩層要分開講：

- **分派層與 fetch 路徑：是。** I/O readiness → response 回呼 → owner；`FetchPipeline` 在同一個 stack 內決定並續發；timer 各自到期；command 直接進 handler；應用端的交接是狀態式的 park / signal。§1.5 的第 2、3、5 列與 §2.1 量到的增益都來自這一層。
- **六個協定 manager：不是，是「事件觸發的輪詢」。** manager 仍是 pull 式 API（`poll(now)` 回傳 request 與延遲），迴圈只決定**何時**呼叫它們；而且因為問題 2（結果語意不明、依賴藏在 future 鏈裡），每次有 request 完成就得全部輪詢一次。這與舊迴圈的差別是「有事情完成才輪詢」而不是「每輪都輪詢」，是量的改善，不是模型的改變。

所以正確的描述是：**事件驅動的迴圈，裝著未改動的 pull 式 manager**。要讓 manager 也事件化（每個 manager 只對自己的事件反應、不再被 poll），必須改 `RequestManager` 的契約——這正是 KIP-1371 / Codex `NextPollCondition` 的方向，也是同時解掉上表第 1、2 項的唯一辦法。本設計刻意不動 manager 契約（§0 原則、§5），代價就是這一層停留在輪詢；`ManagerTask` 是為了讓那個遷移可以逐個 manager 進行而留的接縫，不是遷移本身。

---

## 2. 架構本身的貢獻怎麼量

架構的貢獻必須與 fetch 深度（prefetch）分開量，因為後者在舊架構也做得到（04 文件）。方法是 2×2：舊架構用 02 文件的實驗分支（`-Dkafka.exp.prefetch.depth` 0 / 2），新架構用 M1 的 jar（`internal.fetch.prefetch.factor` 1 / 2）；四個變體交錯執行、3 輪取中位數，100B 40M 筆，`fetch.MB.sec` 與 CPU 秒/GB。

**控制變因並不相等，所以這是「方向性」而非精確的 ablation**（Codex 審查 F8，附錄 F）：舊實驗的 depth 限制的是「buffered batch 數 + in-flight 數」，新版的 factor 限制的是位元組額度；`factor=1` 在 response 小於 `max.partition.fetch.bytes` 時仍允許續發（100B 場景 response 都接近 1 MB，實際上等於不預抓，但這是場景性質，不是控制保證）；M1 jar 含 `RecordHeaders` 改動（profile 上限 1.3%）而舊架構 jar 不含；舊實驗用 sessionless full fetch，新版用 incremental session。要做成精確的 ablation，兩邊必須用同一套 admission（同樣以位元組計的 credit、同樣的 session 行為、同樣的共用解碼碼），並記錄實際的 outstanding batch / bytes 與峰值記憶體，而不只是設定名稱；列在 §9 的待辦。

原始數據：`ASYNC-CONSUMER-V2-bench/results-arch-2x2.csv`。

| 場景 | 舊架構 | 舊架構 + prefetch | 新架構，無 prefetch | 新架構 + prefetch |
|---|---:|---:|---:|---:|
| 1p 100B，MB/s | 444 | 849（1.91×） | 484（1.09×） | **1039（2.34×）** |
| 1p 100B，CPU 秒/GB | 2.50 | 2.53 | 2.21 | 2.27 |
| 1p 100B `max.poll.records=50`，MB/s | 337 | 519（1.54×） | 447（1.33×） | **877（2.60×）** |
| 1p 100B `max.poll.records=50`，CPU 秒/GB | 3.44 | 3.41 | 2.37 | 2.53 |

（倍率以同場景的舊架構為分母。這台機器同一變體在不同時段會有 ±15% 的飄移，所以只看交錯執行的相對值。）

讀法：

- **預設 poll 頻率下，2× 幾乎全部是 prefetch 的功勞**：舊架構加上 prefetch 就 1.91×，架構本身只多 9%。prefetch 可以回移，且回移後舊架構也能拿到接近 2×（04 文件）。
- **架構的貢獻在 poll 頻率高的時候才顯現**：`max.poll.records=50`（應用端每秒約 1 萬次 poll）時，舊架構加 prefetch 只到 1.54×，因為每次 poll 的固定成本（event、selector wakeup、一整輪 `runOnce`）把 prefetch 省下的時間吃掉了；新架構把同樣的 prefetch 推到 2.60×，在 prefetch 之上再 **+69%**，CPU 秒/GB **−26%**。
- **CPU 效率的差異在兩個場景都存在**：架構讓 CPU 秒/GB 少 10%（預設）與 26–31%（mpr50），與 prefetch 無關。這就是 §1.5 第 2 列「每輪成本固定、輪數隨 poll 頻率成長」的量化。

所以切換架構的理由要這樣講：**不是「2×」**（那是 prefetch，可以回移），而是 (1) 每次 poll 不再產生 event 與 selector wakeup，使得 prefetch 的增益在高 poll 頻率下不被吃掉（mpr50：1.54× → 2.60×，方向性數字，見上段），(2) CPU 秒/GB 少 10–30%，(3) timer 路徑上的 busy-loop 這一類 bug 消失（§1.5 第 5 列），(4) 續發與 fencing 是資料模型的一部分而不是又一組跨執行緒 event。誰在乎 (1)(2)？小 `max.poll.records`、低延遲、每次 poll 只拿少量紀錄的使用者，以及每 GB 付 CPU 錢的人。

一個附帶的誠實註記：「舊架構 + prefetch」欄用的是 02 文件的 100 行實驗 hack（sessionless full fetch、沒有 seek fencing），不是正式實作；正式版要在舊架構裡補上 fencing 與 credit 通道，成本只會比這一欄高，不會低。

### 2.1 只有事件迴圈的變體：把邊角優化全部拿掉之後，迴圈本身解決了什麼

§2 的 2×2 仍有混雜（附錄 F 的 F8）：新架構那一欄同時含 `RecordSink` 無鎖交接、`RecordHeaders`、`FetchPipeline` 的 admission 邏輯。為了只看事件迴圈，另建一個變體，保存在獨立 worktree：

| | 位置 |
|---|---|
| Worktree / 分支 | `/Users/unknowntpo/repo/unknowntpo/kafka/async-consumer-v2-loop-only`，分支 `async-consumer-v2-loop-only`（自 trunk `820533b870`），commit `da14ac1721` |
| 有的 | `ConsumerEventLoop`、`ManagerTask`、`LoopTimer`、`LoopSignal`、`FetchPositionsErrorEvent`；`EventLoopKafkaConsumer`（由 `ConsumerDelegateCreator` 在 `group.protocol=consumer` 時選用）：poll 零 event（poll 序號是 volatile 寫入；「請建下一批 fetch」是旗標 `requestFetch()`，只在 loop 已 park 時才 wakeup） |
| 沒有的 | `FetchPipeline`、`RecordSink`、`Parker`、credit、M0 `RecordHeaders`、M2 解碼路徑。fetch 路徑是 trunk 原封不動的 `FetchRequestManager`（每次 poll 一次 `createFetchRequests`，深度 1）、`FetchBuffer`（原鎖、permit 語意）、`FetchCollector` |
| 驗證 | `KafkaConsumerTest` 225/225、`ConsumerEventLoopTest` 15/15、`LoopTimerTest` / `LoopSignalTest`；checkstyle、spotbugs 通過 |

這個變體與 trunk 的唯一差別就是背景執行緒的推進模型（deadline-driven → 事件驅動）與 app→background 的協定（每次 poll 2 個 event + 2 次 wakeup → 0 event、至多 1 次條件式 wakeup）。所以「baseline vs loop-only」量的是 §1.5 第 2、3、5 列；「loop-only vs 完整版」量的是 fetch 深度、無鎖交接與 admission。

**三方交錯 A/B（2026-09-08，3 輪中位數；原始數據 `ASYNC-CONSUMER-V2-bench/results-loop-only-3way.csv`）**。baseline 是 trunk（實驗分支 jar、`depth=0` 即原行為）；loop-only 是上表的變體（`da14ac1721`）；完整版是 `async-consumer-v2-review-fixes` @ `8daa179a5a`。量測期間機器 1 分鐘負載 12–43（Codex 的容器仍在跑），只看交錯的相對值：

| 場景 | trunk | loop-only（只有事件迴圈） | 完整版 |
|---|---:|---:|---:|
| 1p 100B，MB/s | 387 | 415（**+7%**） | 1000（2.6×） |
| 1p 100B，CPU 秒/GB | 2.63 | 2.54（−3%） | 2.27（−14%） |
| 1p 100B `max.poll.records=50`，MB/s | 320 | 374（**+17%**） | 910（2.8×） |
| 1p 100B `max.poll.records=50`，CPU 秒/GB | 3.52 | 3.14（**−11%**） | 2.57（−27%） |
| 6p 1KB，MB/s | 510 | 805（**+58%**） | 1300（2.5×） |
| 6p 1KB，CPU 秒/GB | 2.02 | 1.98 | 1.91 |
| 閒置 30 s（空 topic），整個 JVM 的 user+sys 秒 | 2.83 / 2.67 | 2.62 / 2.59 | 2.52 / 2.62 |

讀法：

- **事件迴圈本身解決的問題是 §1.5 第 2 列（每次 poll 的固定成本），而且只在 poll 頻率高時看得見**：單 partition 預設設定只 +7%、CPU/GB −3%；`max.poll.records=50`（每秒約 1 萬次 poll）時 +17%、CPU/GB −11%。這與附錄 G 的分類一致——省下的是 C 類（app↔background 的 event 與 wakeup 往返），不是 A 類（掃描 manager）。
- **多 partition 時迴圈本身的增益放大到 +58%**（6p）。fetch 路徑完全相同（同一個 `FetchRequestManager`、同樣的 node 層級排除、深度 1），差別只在「下一批 fetch 的請求」從 event 往返變成旗標加條件式 wakeup；多 partition 下每次 poll 只消費一個 partition 的 batch，其餘仍 buffered 而擋住整個 node，所以續發的時機更常落在 app→background 的往返延遲上。這是量測觀察，機制推論待 profile 證實。
- **2× 不是迴圈給的**：loop-only → 完整版的差距（415 → 1000、374 → 910、805 → 1300）是 fetch 深度、admission 與無鎖交接，也就是 04 文件裡標為「可回移」的部分。
- **閒置 CPU 沒有可量到的差別**：三者在 30 s 內都約 2.5–2.8 s（含 JVM 啟動與 JIT），§1.5 第 3 列「閒置時每 100 ms 醒一次」在這個量測裡沒有 CPU 上的證據；那一列只剩正確性面向（busy-loop 類 bug）的意義。

結論（回答「事件迴圈的優化是不是真的有解決問題」）：有，但範圍是明確的——它解決的是高 poll 頻率下的固定成本與多 partition 下的續發延遲，在單 partition、大批次的場景幾乎沒有差別；吞吐 2× 要靠 fetch 路徑。切換架構的論證（§9）應據此收斂：迴圈的價值是 C 類成本、busy-loop 類 bug 的 timer 路徑保證、以及讓 fetch 路徑的改動能以資料模型而非 event 表達；不是「2×」。

---

## 3. 執行模型：`ConsumerEventLoop`

### 3.1 三種事件來源，沒有全域 wait time

```java
interface EventLoop {
    void execute(Command c);                 // MPSC，任何執行緒可呼叫
    TimerHandle schedule(long delayMs, Runnable task);   // 只能在 loop 執行緒呼叫
    void onReadable(...)                      // 由 NetworkClient response 回呼驅動
}
```

每輪：
1. `NetworkClient.poll(0)`（非阻塞）處理 readiness，response 回呼直接分派給 owner（`FetchPipeline` 或 `ManagerTask`）。
2. 執行到期的 timer（`LoopTimer`：PriorityQueue heap，`O(log n)` 插入、`O(1)` 看頂端；n 是 manager 數加 reaper，個位數）。
3. drain `CommandQueue`，每個 command 直接呼叫對應 handler。
4. 阻塞在 `select(min(nextTimerDeadline − now, MAX))`；只在真的沒事時阻塞。

**沒有 `maximumTimeToWait`，沒有對所有 manager 取最小值。** 每個 manager 自己排自己的 timer（§5）。注意 manager 這一層是「事件觸發的輪詢」而非事件驅動（§1.6）。「某個 manager 回 0」不再能造成 busy loop：timer 有 1 ms 下限（`LoopTimer.MIN_DELAY_MS`），且 timer 到期只重跑該 manager。這個保證**只涵蓋 timer 路徑**；其他觸發（response、command、每次 poll 的簿記）各自由事件率限制，而「完成但沒有進展」的自我觸發（Codex 審查 F4，附錄 F）已經修掉。另外，request 完成會重跑**全部** manager，不是只有送出它的那個：manager 之間的依賴藏在 future 鏈裡（commit manager 的 OffsetFetch 完成會在 offsets manager 排入 ListOffsets），沒有宣告就不能做針對性喚醒；這與舊迴圈每輪都跑全部一樣，差別只在「只有事情完成時才跑」。

### 3.2 穩態 `poll()` 不產生 event

現行每次 `poll()` 送 `AsyncPollEvent` 的三個理由，以及本設計的替代：

| 現行理由 | 替代 |
|---|---|
| 重設 heartbeat 的 poll timer（`max.poll.interval.ms` 存活性） | 應用執行緒寫 `volatile long lastPollMs`；heartbeat `ManagerTask` 在自己的 timer 讀它。零 event |
| 觸發 auto-commit timer | auto-commit 是 loop 上的 timer，到期讀 `SubscriptionState.allConsumed()`。零 event |
| 觸發 `updateFetchPositions` / pattern 訂閱重算 / reconcile | 這些由各自的觸發源驅動：metadata 更新回呼、assignment 變更 command、heartbeat response。`poll()` 只需在「有 partition 沒有 valid position」時送一次 `EnsurePositions` command（coalesced：同一時間最多一個 outstanding） |
| 產生 fetch request | `FetchPipeline` 自主續發（§4） |

`poll()` 的流程：

```
poll(timeout):
  ensureOpen; recordPollStart; lastPollMs = now
  loop until timeout:
    drainResults()                          // 執行 callback；有錯誤則丟
    records = sink.take(maxPollRecords)     // 無鎖；含 decode 與 position 更新（§6.0）
    if (!records.isEmpty()) { creditCounter.release(bytes); return interceptors.onConsume(records) }
    if (needsPositions()) loop.execute(EnsurePositions.coalesced())
    park(until: sink.seq > seenSeq || results.nonEmpty || deadline)   // LockSupport，狀態式，不是 permit
```

`park` 是**狀態式**：先讀 `sink.sequence`，publish 端在寫入後 `unpark` 已登記的 parker。沒有 permit 旗標，所以沒有空喚醒；沒有 100 ms 上限，逾時就是使用者的 `timeout`。

### 3.3 執行緒擁有權與跨執行緒通道

| 狀態 | Owner | 其他執行緒的存取方式 |
|---|---|---|
| `PartitionFetchState`（fetch offset、epoch、credit 帳）| loop | 不可存取；應用端只透過 `CreditCounter` 與 `RecordSink` |
| `SubscriptionState` | 沿用現行 monitor（M1 不改） | 應用端：position 讀寫、seek；loop：assignment、validation。熱路徑上每 batch 一次 position 更新 |
| `RecordSink` per-partition 佇列 | loop publish / 應用端 take | SPSC：`volatile` sequence + `LockSupport`；無鎖 |
| `CreditCounter`（每 partition 已消費位元組） | 應用端 add / loop read | `LongAdder`-style；達閾值且 loop parked 才 `wakeup()` |
| `CommandQueue` | 任何執行緒 offer / loop drain | MPSC（`ConcurrentLinkedQueue`）+ 合併喚醒（§3.4） |
| `ResultQueue`（callback、錯誤、future 完成） | loop offer / 應用端 drain | MPSC；offer 後 `unpark` 應用端 |
| 六個協定 manager 的內部狀態 | loop | 只透過 command 與 `ManagerTask` |

規則：**loop 執行緒永遠不阻塞在應用端的鎖上**；應用端永遠不阻塞在 loop 的鎖上（只 park 等訊號）。

### 3.4 喚醒合併

`CommandQueue.offer()` 後只在 `loopState == PARKED` 時呼叫 `selector.wakeup()`（`AtomicInteger` state：RUNNING / PARKED），同一輪多個 command 共用一次 wakeup。`CreditCounter` 的喚醒同理，並加閾值（累積 ≥ ½ 個 `max.partition.fetch.bytes` 才喚醒），因為 loop 醒來也只是要發 fetch，半個 fetch 以下的 credit 沒有續發價值。這把 02 文件量到的 `IOUtil.write1` 4.9% 與每 poll 兩次 wakeup 降到接近零。

---

## 4. 資料路徑：`FetchPipeline`

### 4.1 `PartitionFetchState` 與不變式

```java
final class PartitionFetchState {
    final TopicPartition tp;
    long positionEpoch;        // 每次 position 被應用端/協定改寫時 +1（seek、reset、assign、pause→resume）
    long nextFetchOffset;      // 下一個 fetch 的起點
    long bufferedBytes;        // 已 publish 到 sink 但尚未被消費歸還的位元組
    boolean inFlight;          // 此 partition 目前在某個 in-flight request 裡
    Optional<Integer> leaderEpoch, lastFetchedEpoch;
}
```

不變式（所有轉移後都成立）：
- **I1 連續性**：sink 內該 partition 的 batch 依 offset 遞增且連續；`nextFetchOffset` = 最後一個已接受 batch 的 `lastOffset + 1`，若 sink 為空且無 in-flight 則 = `SubscriptionState.position(tp).offset`。
- **I2 單 in-flight**：每個 partition 同時最多一個 in-flight request（協定限制：不知道前一個 response 的結尾就無法定址下一個）。
- **I3 credit 上限**：`bufferedBytes + inFlightBytesEstimate ≤ prefetchLimitBytes(tp)`。
- **I4 epoch 一致**：sink 內與 in-flight 的每個 batch 都標記發出時的 `positionEpoch`；`positionEpoch` 改變後，舊 epoch 的資料在 publish 或 take 時被丟棄，且 `nextFetchOffset` 重新從 position 取。

### 4.2 續發時機

```
onResponse(node, response):
  session.handleResponse(...)                    // 保留 incremental fetch session
  for each partition in response:
     st = state(tp)
     st.inFlight = false
     if (st.positionEpoch != request.epochOf(tp)) continue      // I4：丟棄
     if (error) { handleError; continue }
     batch = wrap(records, st.positionEpoch)      // 不解析、不拷貝，只 slice
     st.nextFetchOffset = batch.lastOffset + 1;  st.bufferedBytes += batch.sizeInBytes
     sink.publish(tp, batch)                      // volatile write + unpark 應用端
  maybeIssueFetch(node)                           // 同一輪、同一個 stack 立刻續發
```

`maybeIssueFetch(node)`：對該 node 上所有 fetchable partition，若 `!inFlight && bufferedBytes < prefetchLimitBytes` 則以 `nextFetchOffset` 加入 request；有任何 partition 就送。**續發不經過應用執行緒、不經過 command、不等下一輪。** 這一段就是 02 文件 2.15× 的來源。

觸發 `maybeIssueFetch` 的四個時機：response 到達（上）、credit 歸還達閾值（應用端消費後）、assignment / position 變更 command 處理完、node 從 backoff 恢復（timer）。

### 4.3 在途資料的上限

每 partition 的 credit 上限是 `prefetchFactor × max.partition.fetch.bytes`（`ConsumerConfig.defineInternal("internal.fetch.prefetch.factor")`，預設 2）；每 node 一個 in-flight，response ≤ `fetch.max.bytes`。架構本身不依賴 factor：factor=1 就是現行的記憶體上界，而 §2 的 2×2 顯示此時架構仍有 9–33% 吞吐與 10–30% CPU/GB 的差距。factor 的推導、記憶體代價與深度本身的增益在 04 文件 §2。

### 4.4 Position epoch fencing 的來源

`positionEpoch` 在 loop 執行緒上遞增，觸發點：
- 應用端 `seek*`、`assign`、`subscribe`、`unsubscribe`、`resume`：這些 API 本來就送 command 到 loop（現行也是），handler 在改 `SubscriptionState` 後 `state(tp).bumpEpoch()`。
- 協定端 offset reset / validation 完成、assignment reconcile（loop 內部）。
- `pause` 不 bump（資料仍有效，只是不交付）；`resume` 也不需要 bump——現行 `pause` 期間 buffered 資料保留，語意一致。

應用端 `take()` 時也檢查 epoch（因為 seek 與 take 之間可能有 race）：sink 內 batch 的 epoch ≠ 目前 `positionEpoch`（透過 `volatile` 讀）即跳過。這取代現行「offset 相等才接受」的隱式檢查，而且明確處理了實驗中 seek 後停滯的情況。

### 4.5 `max.poll.records` 與部分消費

batch 是 sink 的最小單位；應用端一次 `take(maxRecords)` 可停在 batch 中間，游標保留在應用端的 `PartitionCursor`（不是 loop 的狀態）。部分消費不影響 credit（credit 以 batch 消費完為單位歸還）也不影響續發（續發看 `bufferedBytes`，不看是否消費到一半），所以 01 文件 B5 的「部分消費連鎖停擺」消失。

### 4.6 多 partition 與 fetch session

沒有 credit 的 partition（buffered 已達上限，或有尚未處理的錯誤）**完全不放進 request**，因此會被 incremental session 忘掉、有 credit 時再加回來，與舊實作對「有 buffered 資料的 partition」的處理相同。原設計想用「帶原 offset 但 `maxBytes=0`」讓它留在 session 裡，這**不成立**（Codex 審查 F3，附錄 F）：broker 以 `minOneMessage=true` 開始讀，request 裡第一個非空的 partition 不論 `maxBytes` 都會拿到至少一個 batch（`ReplicaManager.readFromLocalLog` → `LogSegment.read` 把 budget 提高到第一個 batch 的大小），所以一個有 backlog 的閒置 partition 會每個 request 多長一個 batch，沒有上界。`FetchPipelineTest` 驗證沒有 credit 的 partition 不在 request 裡。

---

## 5. 協定 manager 的掛接：`ManagerTask`

保留 `CoordinatorRequestManager`、`CommitRequestManager`、`ConsumerHeartbeatRequestManager`、`ConsumerMembershipManager`、`OffsetsRequestManager`、`TopicMetadataRequestManager`，不改它們的程式碼，用 adapter 驅動：

```java
final class ManagerTask {
    void run(long now) {                       // 由 timer、response、command 三種來源觸發
        PollResult r = manager.poll(now);
        for (UnsentRequest u : r.unsentRequests) {
            u.whenComplete((resp, err) -> { /* 原 handler */; loop.execute(this::runNow); });
            network.send(u);
        }
        timer.reschedule(clamp(r.timeUntilNextPollMs, MIN_DELAY_MS, MAX_DELAY_MS));
    }
}
```

- `maximumTimeToWait` **不再被呼叫**（應用端的等待由 §3.2 決定）。
- 每個 manager 的 `timeUntilNextPollMs` 只影響自己的 timer；1 ms 下限是 `TimerWheel` 的性質，不是特例。
- 觸發來源明確：自己的 timer、自己 request 的 response、指名給它的 command（例如 `Commit` → `CommitRequestManager`）。現行 `ApplicationEventProcessor` 的 `switch` 拆成各 command 的 handler，每個 handler 只碰它需要的 manager。
- `AsyncPollEvent` 的三件事（reconcile 觸發、auto-commit timer、positions）分別歸位：reconcile 由 heartbeat response 觸發；auto-commit 是 loop timer；positions 由 §3.2 的 `EnsurePositions` command 觸發 `OffsetsRequestManager`。

長期（M4 之後）可以把 `RequestManager` 介面改成純事件式，但那是清理，不是效能需求。

---

## 6. Per-record 工作的歸屬

（每位元組與每筆紀錄的成本優化——`MemoryPool`、解碼路徑、M3——是舊架構也能做的，在 04 文件。）

### 6.0 為什麼 per-record 工作留在應用執行緒，而不是背景執行緒

這不是為了「利用閒置的應用執行緒」；重疊只是副作用。理由依重要性：

1. **背景執行緒是協定的關鍵路徑**（heartbeat、commit、reconcile）。§1.5 提到 v2 不引入優先權佇列，前提是背景執行緒上的每件工作都很小；CRC、解壓、解析、反序列化是唯一會大到讓 heartbeat 排隊的工作，所以它不能在那裡。把它搬上去，迴圈就變成 CPU-bound，那時才真的需要優先權。
2. **反序列化與 interceptor 是使用者程式碼**：可能慢、阻塞、非 thread-safe、丟例外。現行契約是例外從 `poll()` 在呼叫者執行緒拋出，`RecordDeserializationException` 讓使用者 seek 過壞紀錄。這是 §7.2 的相容契約，不是效能取捨。
3. **按需解碼對齊 back-pressure 與作廢**：每次 poll 只解 `max.poll.records` 筆；pause / seek / rebalance 後未解碼的 batch 由 epoch 檢查直接丟掉，成本為零。預先解碼作廢的會是已配置的 `ConsumerRecord`。
4. **記憶體上界以原始位元組計**（credit = 2 × `max.partition.fetch.bytes`）。解碼後每筆數個物件，100B 紀錄的物件開銷是 payload 的數倍；預先解碼會把上界從位元組變成物件，且多為短命垃圾。
5. **v2 沒有搬動 per-record 工作**：classic 與 async 本來就在應用執行緒做（01 文件）。v2 改的是「下一個 fetch 不再等它」。04 文件 §5 量到 lz4 場景應用執行緒只忙 24%，所以連「搬到 worker 以增加平行度」（M3）都沒有收益。

能搬出應用執行緒的只有沒有使用者程式碼的部分（CRC、解壓），而它們該去的是獨立的 worker pool（04 文件 M3），不是事件迴圈。

---

## 7. 相容邊界與並存策略

### 7.1 新類別，不動舊類別

- 新增 `PipelinedKafkaConsumer<K,V> implements ConsumerDelegate<K,V>`（暫名；套件 `clients.consumer.internals`）。
- `ConsumerDelegateCreator`：`group.protocol=consumer` → 新類別；例外包裝契約（`KafkaException` 直接拋、其他包成「Failed to construct Kafka consumer」）照舊。保留注入式建構子（`LogContext, Time, ConsumerConfig, Deserializer×2, KafkaClient, SubscriptionState, ConsumerMetadata`）以支援 `KafkaConsumerTest` 的注入路徑。
- `AsyncKafkaConsumer` **一行不改**：Streams（`StreamThread` 直接 `new`、`ConsumerWrapper` SPI）與 `ShareConsumerImpl`（`CompletableEventReaperFactory`、`ConsumerNetworkThread`）繼續用它。
- 範圍：只有 `KafkaConsumer` + `group.protocol=consumer` 走新類別。`PipelinedKafkaConsumer` 不帶 Streams 專用程式碼（沒有 `StreamsRebalanceData` 建構參數、沒有 `subscribe(Collection, StreamsRebalanceListener)`、不處理 `Streams*Event`）；Streams 與 share consumer 維持現狀，`AsyncKafkaConsumer` 與 `ConsumerNetworkThread` 因此**保留**，不做移除。

### 7.2 必須保留的行為契約

- `poll()` 的 `WakeupException` / `InterruptException` 語意、`wakeup()` 對所有阻塞 API 的中斷（`WakeupTrigger` 的角色由 `ResultQueue` 的一個特殊結果取代；阻塞 API 統一透過 `awaitResult(future, timer)` 等，`wakeup()` 對它們 `completeExceptionally`）。
- rebalance 回呼在應用執行緒、在 `poll()` 內執行；`onPartitionsRevoked` 完成後才回覆 heartbeat——維持現行 background → app → background 的 round trip（這是 API 契約，不是效能路徑）。
- commit callback 與 interceptor 在應用執行緒執行；順序與現行一致。
- `close()` 的 leave group / commit-on-close / 等待 in-flight 行為與 timeout。
- 所有既有 metrics 名稱保留；KIP-1068 的 event-queue 類 metrics 對應到 `CommandQueue` / `ResultQueue`（語意相同：佇列大小與停留時間）。
- 設定鍵不新增公開項；`internal.fetch.prefetch.factor` 為內部設定。

### 7.3 測試策略

- **facade 層**：`KafkaConsumerTest`（135 個，`@EnumSource(GroupProtocol)`）與 `ConsumerConfigTest` 不改，必須全過。
- **整合層**：`clients-integration-tests` 約 400 個 `@ClusterTest` 與 `core` 172 處 `groupProtocol` 參數化測試，`consumer` 分支必須全過。這是行為契約的主要保證。
- **新單元測試**：`FetchPipelineTest`（以 §4.1 四條不變式為屬性測試：隨機 seek / reset / response 序列後不變式恆成立）、`RecordSinkTest`（SPSC 正確性、park/unpark 無遺失喚醒）、`ConsumerEventLoopTest`（timer、command、wakeup 合併、「manager 回 0 不會 busy loop」）、`RecordDecoderTest`（與 `CompletedFetch` 輸出逐筆相等）。
- **不變式測試工具**：`OffsetCheck` 正式化為整合測試（連續性、無重複、seek 後恢復、`max.poll.records` 小於 batch）。
- **benchmark**：02 文件的 `bench/` 進 repo（`ASYNC-CONSUMER-V2-bench/`），每個里程碑跑同一組場景。

---

## 8. 里程碑與退出條件

| 里程碑 | 內容 | 退出條件（以 02 文件 baseline 為分母） |
|---|---|---|
| **M0** | benchmark 工具進 repo；`FetchPipeline` / `RecordSink` / `EventLoop` 介面與不變式測試骨架（`RecordHeaders` 部分屬 04 文件） | 工具可一鍵重跑 02 文件全部場景 |
| **M1** | `ConsumerEventLoop` + `ManagerTask` + `FetchPipeline` + `RecordSink`；decode 沿用 `CompletedFetch`；新類別接上 `ConsumerDelegateCreator` | 架構本身：CPU 秒/GB ≤ 現行、`max.poll.records=50` 場景 CPU/GB ≤ classic、facade 與整合測試全過（§2 的 2×2 是最終的衡量；原訂「100B ≥ 2×」含 fetch 深度，屬 04 文件） |
| M2、M3 | per-record 解碼、`MemoryPool`、worker 解碼 | 舊架構也能做，移至 04 文件 |
| ~~**M4**~~ | ~~Streams 支援與遷移~~ **取消**：Streams 與 share consumer 不在範圍（附錄 E） | — |
| ~~**M5**~~ | ~~移除 `AsyncKafkaConsumer`、`ConsumerNetworkThread`~~ **取消**：兩者仍被 Streams 與 share consumer 使用 | — |

風險與對策：
- **fetch session 語意**：§4.6 的 `maxBytes=0` 技巧經 Codex 審查（F3）證實不成立，已改為「沒有 credit 的 partition 不放進 request」（session forget / re-add，與舊實作相同）。
- **記憶體上界翻倍**（§4.3，04 文件 §2.2）：文件化；`prefetchFactor=1` 可回到現行上界。
- **協定 manager 對 `maximumTimeToWait` 的隱含依賴**：M1 以測試證明六個 manager 在純 timer 驅動下行為一致（heartbeat 間隔、commit backoff、coordinator 重試）。
- **Streams 與 share consumer 的耦合**：不在範圍內，舊類別保留，零風險。

---

## 9. 給 reviewer 的論點（2026-09-08 整理）

**我們在要求什麼**：把 `group.protocol=consumer` 路徑的 deadline-driven scheduler 換成事件迴圈，request manager 一行不改。這是完成 KAFKA-14246 的原始目標（事件驅動，附錄 A），不是推翻它。Streams、share consumer、classic 不動（附錄 E）。

**不在這份文件裡、獨立提交的**：04 文件的所有項目（fetch 深度、`RecordHeaders`、解碼路徑）。它們的增益不是切換理由。

**架構獨有的四個論點**

1. **timer 路徑上的一整類 bug 消失**（§1.5 第 5 列）：六張 busy-loop JIRA 同一根因；新迴圈 per-manager timer、1 ms 下限、到期只重跑自己。loop 的其他觸發要逐條證明有界（附錄 F 的 F4 是 Codex 找到、已修的一條）。
2. **每次 poll 不再產生 event 與 wakeup，可量化**（§2 的 2×2，方向性）：CPU 秒/GB 預設 −10%、`max.poll.records=50` −26–31%；同樣的 prefetch 在舊架構只到 1.54×，新架構 2.60×。控制變因尚未完全相等（§2），精確 ablation 列在待辦。受益者：低延遲、小批次、多 partition 少紀錄、按 CPU 付費的使用者。
3. **狀態有 owner、不變式明列**（§0、§4.1）：續發與 fencing 是 `FetchPipelineModel` 的四條不變式加 property test，不是散在 handler 的 switch 與 volatile flag。
4. **風險低、可退回**（§7）：新類別由 `ConsumerDelegateCreator` 切換，`AsyncKafkaConsumer` 原封不動，公開 API 不變，manager 用 adapter 原樣驅動；`prefetch.factor=1` 回到現行記憶體上界。證據：`KafkaConsumerTest` 225/225、clients 整合測試 278/278、core Scala 22/22、真實 broker offset 連續性檢查全過。

**預期的反對與回答**

| 反對 | 回答 |
|---|---|
| 「2× 是 prefetch，不是架構」 | 同意，所以 prefetch 在 04 文件而不在這裡；架構的貢獻看 §2 的 mpr50 列與 CPU/GB |
| 「多一份實作要維護」 | 最真實的代價：classic、async（Streams / share）、pipelined 三份。範圍限定是刻意的；新抽象只有 `ConsumerEventLoop` 與 `FetchPipeline`，manager 沒有分叉 |
| 「記憶體上界翻倍」 | 文件化；`internal.fetch.prefetch.factor=1` 可回 |
| 「單機 loopback 的數字」 | 目前最弱的一環；提交前需補真實叢集、有網路延遲的數據 |
| 「metrics 與行為對等」 | 行為契約由整合測試覆蓋；metrics 名稱沿用，但 `time-between-network-thread-poll` 一類語意已變，需註明 |

**建議順序**：先送 04 文件的小項（`RecordHeaders`、解碼路徑）→ 帶著 §2 的 2×2 表開 discussion 談架構 → 最後決定 fetch 深度落在哪一邊。每一步的反對理由只剩該步自己的。

---

## 附錄 A：當初為什麼要做 AsyncKafkaConsumer（v2 必須保留的約束）

來源：KAFKA-14246「Update threading model for Consumer」（Philip Nee，2022-09-20）、cwiki「Consumer threading refactor design」（Philip Nee 撰、Kirk True 2024-01 更新）、KIP-848 Motivation、dev@ 討論串 2022-09-13（Philip Nee 發起，Guozhang Wang、Kirk True、Luke Chen、Divij Vaidya、Erik van Oosten 參與）。

當時列出的問題，全部是**正確性與可維護性**問題，沒有一項是吞吐：

| 當時的問題 | 原文重點 | v2 的處置 |
|---|---|---|
| Coordinator 通訊分散在兩條執行緒 | 「coordinator communication can happen in both the heartbeat thread and the polling thread, which makes it challenging to diagnose the source of the issue」；具體案例 KAFKA-13563 的 race | 保留：所有協定通訊只在 loop 執行緒（§3.3、§5） |
| Rebalance 阻塞 polling thread | classic 的 JoinGroup / SyncGroup 在 `poll()` 內同步進行 | 保留：reconcile 在 loop，應用端只跑回呼（§7.2） |
| `HeartbeatThread` 帶來額外複雜度 | 與 app thread 共用 `ConsumerNetworkClient`，靠鎖協調 | 保留：沒有 heartbeat thread，heartbeat 是 loop 的 timer（§5） |
| 程式碼可讀性因多年 patch 劣化 | 「Patches and hotfixes in the past years have heavily impacted the readability of the code」 | v2 的回應是 §0 的原則：狀態有 owner、不變式明列、沒有旗標式 hack |
| 無法支援 KIP-848 | KIP-848 要求 heartbeat 與 assignment reconcile 由 client 在背景持續進行，不受應用端 `poll()` 節奏影響；「the entire rebalance process is driven by the group coordinator」，client 端做「incremental reconciliation」 | 保留：六個協定 manager 原樣掛進 loop（§5） |

Refactor 的四個目標（wiki）：polling 與背景職責清楚分離；coordinator 通訊集中到單一背景執行緒；rebalance 對 polling 非同步、不阻塞；透過「可預測、事件驅動的通訊」簡化邏輯；且不破壞相容性。

兩點值得注意：

1. **「事件驅動」是原始目標，但落地成了 deadline-driven 的 polling scheduler**（具體哪裡不好見 §1.5，量測在 02 文件第 2 節）。v2 不是推翻當初的方向，而是把「事件驅動」真正做出來。
2. **當初沒有效能目標，也沒有效能量測**（KAFKA-16110 至今為空）。Async consumer 的 fetch 路徑是從 `Fetcher` 抽出來的 `AbstractFetch`（子任務 KAFKA-14365 / 14758 / 14274），管線深度、buffer 配置、per-record 成本全部原樣繼承。這解釋了為什麼 01 文件的 A、B 類問題在新舊兩版一模一樣，也解釋了 v2 為什麼要從資料路徑下手。

### 7.4 其他 consumer 變體的涵蓋範圍

| 變體 | 現況 | v2 的涵蓋 |
|---|---|---|
| `ClassicKafkaConsumer`（`group.protocol=classic`） | 舊協定、單執行緒 + heartbeat thread；社群方向是逐步淘汰 | 不在目標內。02 文件的實驗顯示同一個 prefetch 機制對 classic 也有 1.5×，未來可把 `FetchPipelineModel` 接進 `Fetcher`，但那是獨立的工作 |
| Kafka Streams（`AsyncKafkaConsumer` + `StreamsRebalanceData`） | `StreamThread` 直接 `new AsyncKafkaConsumer`，用 Streams 專屬的三個 request manager 與 `StreamsRebalanceListener` | **不在範圍**（附錄 E）：Streams 維持 `AsyncKafkaConsumer`。技術上可行（Streams 的 manager 是一般 `RequestManager`，M4 的試作在 `StreamThreadTest` 168/168 與 6 個 streams-protocol 整合測試類 242/242 通過），但決定不動 Streams；`PipelinedKafkaConsumer` 也因此移除了 Streams 專用程式碼 |
| `KafkaShareConsumer`（`ShareConsumerImpl`） | 用同一套 `ConsumerNetworkThread` / `ApplicationEventHandler` / `RequestManagers`（share 版）；fetch 協定不同：broker 端 acquire、client 端 acknowledge、沒有 offset/position/seek | **執行模型可直接重用**：`ConsumerEventLoop` 只依賴 `RequestManager` 與 `ApplicationEventProcessor`，share 版的 manager 可原樣掛進去，`RecordSink` / `Parker` / `LoopSignal` 也不假設 offset 語意。**資料模型不同**：share 的「credit」是 acquired 但尚未 acknowledge 的紀錄數（受 `max.poll.records` 與 acquisition lock 時限約束），且 acknowledge 要 piggyback 在下一個 ShareFetch 上。這需要一個 `ShareFetchPipelineModel`（不變式：acquired ≤ 上限、每個 acquired batch 恰好 ack 一次、lock 到期前送出），與 `FetchPipelineModel` 平行，不是它的子類。**不在範圍**（附錄 E）：share consumer 維持 `ConsumerNetworkThread` |

結論：loop、sink、parker、signal 這一層在設計上對所有變體共用，per-variant 的是「credit 模型」與「應用端的 collector」；但本次只落地在一般 consumer。

---

## 附錄 B：M1 結果（2026-09-07，commit `1eaa25718c`）

v2 的總數字（含 fetch 深度）在 04 文件 §6；架構單獨的貢獻在 §2。這裡只留與迴圈本身有關的觀察與驗證狀態。

- 正確性：`OffsetCheck`（每 partition offset 連續、無重複、消費完後 `seekToBeginning` 能重新讀到 offset 0）在 1p / 6p、`max.poll.records` 500 / 50、lz4 全部通過。
- facade 測試 `KafkaConsumerTest` 225/225；`AsyncKafkaConsumerTest`、`ShareConsumerImplTest` 等舊類別測試不受影響。

**M1 實作過程學到、且已回寫進程式碼的事**

1. 舊迴圈「每輪輪詢全部 manager」隱含涵蓋了四種觸發，新迴圈必須明確追蹤：過期 event 的 reap（每個 pass 都要做，不能只靠 timer，否則 `close(Duration.ZERO)` 在時鐘不前進時永遠等）、metadata 版本變更（透過 `NetworkClient` 內部到達，不經 request future）、同一時間戳的重複 poll（改用 poll 序號）、以及儲存的 metadata 錯誤（要主動傳給應用端）。
2. 兩個只有真實 broker 才會暴露的 bug：incremental fetch response 會**省略沒有新資料的 partition**，per-partition in-flight 狀態必須對整個 request 清除；seek 到「已抓取範圍內」的位置，loop 端的範圍檢查看不出來，必須由 seek / reset command 標記該 partition 重啟 epoch（設計 §4.4 說的就是這件事，實作時漏掉了）。
3. 迴圈的 catch-all 絕不能在錯誤時全速重試並每次 log ERROR：一個 `MockClient` matcher 失配讓單一測試產生 1.6 GB stdout。現在錯誤後至少等 100 ms，且 log 每秒一次。
4. Fetching 在第一次 `poll()` 之後才開始（與舊版一致；測試依賴 request 順序，且避免抓取使用者馬上要 seek 走的位置）。

**整合測試（commit `ce00f020f0`）**：`clients-integration-tests` 的 12 個 consumer 類別（`PlaintextConsumer*` 8 個、`ConsumerIntegrationTest`、`ConsumerBounceTest`、`ConsumerTopicCreationTest`、`ConsumerWithLegacyMessageFormatIntegrationTest`）共 278 個案例全部通過；其中 `testSingleCoordinatorOwnershipAfterPartitionReassignment` 在一次完整跑失敗、單獨重跑 3/3 通過，是時序敏感（斷言 broker 端 coordinator partition 已載入，而 v2 的 join + commit 比舊版快），不是 consumer 缺陷。整合測試又抓到兩個單元測試抓不到的行為契約，都已修正：(1) 一次 `updateFetchPositions` 可能因 leader 未知而未解析完，舊版靠每次 poll 重試，現在在 poll、metadata 變更、manager pass 之後都會重試；(2) 應用執行緒阻塞在 `poll()` 裡的時間不能算進 `max.poll.interval.ms`，舊版每 100 ms 的 poll event 順便重設 heartbeat 的 poll timer，現在由 loop 在 `applicationInPoll` 期間定期重設。

`core` 的 Scala 整合測試（`PlaintextConsumerAssignorsTest`、`SslConsumerTest`、`SaslSslConsumerTest`、`ConsumerBounceTest`，groupProtocol 參數化）22/22 通過。



---

## 附錄 C、D：移至 04 文件

M2（per-record 解碼路徑）的結果與 receive-buffer 重用不可行的設計修正、M3 不做的判斷，都是舊架構也適用的內容，移至 `04-portable-optimizations` §4.4 與 §5。附錄 C 裡的兩個量測方法註記保留在此：

- **交錯 A/B 是唯一可信的跨變體比較**：同一變體在同一台機器不同時段可從 630 擺到 1050 MB/s（熱節流恢復期），跨時段比較不可信。
- **loop 的 pass 頻率是 I/O 驅動的**：100B 場景 `time-between-network-thread-poll-avg` 約 0.05 ms（每秒約 2 萬 pass）；把「新的 poll」從 `hasPendingWork` 移除後數字不變，代表一個 1 MB response 分多個 TCP segment 到達、`Selector` 每次可讀就醒一次，舊迴圈也是同一個數量級。所以 loop 的**每個 pass 必須便宜**（閒置 pass 只有：空 command queue、metadata 版本比較、timer 檢查、`poll(0)`），這比減少 pass 次數更重要。

---

## 附錄 E：範圍決定——Streams 與 share consumer 不遷移（2026-09-08）

使用者決定：這次改寫**限制在 deadline-driven 的部分**，也就是 `group.protocol=consumer` 一般 consumer 所用的 `ConsumerNetworkThread` 這一段。原本同樣使用 `ConsumerNetworkThread` 的 Kafka Streams（`StreamThread` 直接 `new AsyncKafkaConsumer`）與 share consumer（`ShareConsumerImpl`）都**不動**。

因此：

- M4（Streams 遷移）與 M5（移除 `AsyncKafkaConsumer` / `ConsumerNetworkThread`）取消。`AsyncKafkaConsumer`、`ConsumerNetworkThread`、`ApplicationEventHandler` 長期保留給 Streams 與 share consumer。
- `PipelinedKafkaConsumer` 移除了從 `AsyncKafkaConsumer` 複製過來、但在新範圍下無人呼叫的 Streams 程式碼：`StreamsRebalanceData` 建構參數、`StreamsRebalanceListenerInvoker` 欄位、`subscribe(Collection, StreamsRebalanceListener)`、背景事件的 `STREAMS_*` 三個分支與對應 callback 方法、`runRebalanceCallbacksOnClose` 的 Streams 分支。公開建構子變成 `(ConsumerConfig, Deserializer, Deserializer)`。`ConsumerDelegateCreator` 隨之調整。
- 驗證：`KafkaConsumerTest`、`ConsumerEventLoopTest`、pipeline 套件 263/263；clients 的 checkstyle / spotbugs 通過。
- 曾經試作的 M4（新增 `StreamsGroupConsumer` 介面、`StreamThread` 改 `new PipelinedKafkaConsumer`）在 `StreamThreadTest` 168/168、6 個 streams-protocol 整合測試類 242/242 通過後整個還原，沒有提交；記錄在此只是說明「技術上可行」，不是計畫。

最終的邊界：

| 入口 | 實作 | 執行緒模型 |
|---|---|---|
| `KafkaConsumer`，`group.protocol=consumer` | `PipelinedKafkaConsumer`（本專案） | `ConsumerEventLoop`（事件驅動） |
| `KafkaConsumer`，`group.protocol=classic` | `ClassicKafkaConsumer` | 不變 |
| Kafka Streams（streams group protocol） | `AsyncKafkaConsumer` | `ConsumerNetworkThread`（deadline-driven，不變） |
| `KafkaShareConsumer` | `ShareConsumerImpl` | `ConsumerNetworkThread`（不變） |

---

## 附錄 F：對 Codex 獨立審查的逐項回應（2026-09-08）

審查：`review/async-consumer-v2-by-codex` @ `0a57c9d721`（被審版本 `241f83c59c`），報告 `REVIEW-async-consumer-v2-by-codex.md`，9 個重現測試在 `08f4986662`。修正在分支 `async-consumer-v2-review-fixes`。Codex 的測試檔沒有納入本分支（避免修改他們的斷言），而是複製到暫存位置對修正後的程式碼重跑；每項的驗證測試是本分支自己寫的。

| # | 判定 | 原因 | 修正 | 本分支的測試 | Codex 的重現測試（對修正後程式碼） |
|---|---|---|---|---|---|
| F1 已推進 position 的結果因後續 partition 錯誤遺失 | **接受（blocker）** | `SinkCollector.collect` 沒有 `FetchCollector.collectFetch` 的延後錯誤政策；A 的 position 已在 `fetchRecords` 推進，例外卻讓 A 的紀錄沒回傳 | `SinkCollector`：捕捉 `KafkaException`；已收集到資料就回傳、錯誤留給下一次；初始化失敗的 batch 由本呼叫持有並放回 `setAside` 前端（含紀錄的 batch 保留到 position 移動；不含紀錄的錯誤 batch 在錯誤拋出後釋放） | `SinkCollectorTest.recordsCollectedBeforeALaterPartitionErrorAreReturnedAndTheErrorIsRaisedByTheNextCall` | `shouldReturnAlreadyCollectedRecordsBeforeSurfacingLaterError` 通過 |
| F2 initialize 例外讓 batch 與 credit 同時失去 owner | **接受（major）** | 從 sink 取出後未指派 owner 即拋出 | 同上：離開 sink 的 entry 永遠是 `current`、`setAside` 之一，或已 `discard`（歸還 credit / error block） | `SinkCollectorTest.batchWhoseInitializationFailsIsReleasedWithItsCreditWhenItCarriesNothingElse`、`...IsRetainedSoTheErrorRepeatsUntilThePositionMoves` | `shouldNotLoseBatchAndCreditWhenInitializationThrows` 通過 |
| F3 `maxBytes=0` 不能停取 | **接受（blocker）** | 核對 `ReplicaManager.readFromLocalLog`（`minOneMessage` 直到第一個非空 partition）與 `LogSegment.read`（budget 提高到第一個 batch）：request 裡第一個非空 partition 不論 `maxBytes` 都拿到一個 batch；有 credit 的 B 讓 request 持續發出，閒置的 A 就無上界增長 | 沒有 credit（含未處理錯誤）的 partition 不放進 request；`offsetIfIdle` 移除；文件 §4.6、§8 更正 | `FetchPipelineTest.partitionWithoutCreditIsLeftOutOfTheRequestUntilCreditReturns`、`noRequestIsSentWhenNoPartitionHasCredit` | `ZeroBudgetFetchCodexReviewTest`（storage，行為前提）未重跑；其結論已由 broker 原始碼核對接受 |
| F4 無進展的 position 完成反覆設 dirty | **接受（major）** | 完成回呼設 `managersDirty` → `hasPendingWork` → `poll(0)` → 下一輪再嘗試、再完成：不經 timer 的自我觸發 | 完成只設 `fetchDirty`；只有「嘗試發出了 request（future 未完成）」才觸發一次 manager pass 去送它們 | `ConsumerEventLoopTest.positionsAttemptThatCompletesWithoutProgressDoesNotRerunManagersOnItsOwn`、`positionsAttemptWithRequestsOutstandingTriggersAManagerPass` | `missingPositionsWithNoProgressDoNotDirtyEveryIdlePass` 通過 |
| F5 timer callback 拋例外後不再排程 | **接受（major）** | `LoopTimer` 先標記完成再執行；`ManagerTask.run` 在 `poll()` 拋出時沒重排 | `ManagerTask.run`：例外時先以 `FAILURE_RETRY_MS`（100 ms）重排再拋；`runManagers` 讓一個 manager 的例外不擋住同輪其他 manager；reaper timer 同樣 try/finally 重排 | `ConsumerEventLoopTest.managerTimerIsRearmedAfterPollThrows`、`aManagerThatThrowsDoesNotStopTheOthersInTheSamePass` | `managerTimerRetriesAfterOneTransientPollFailure` 通過 |
| F6 error credit 覆蓋既有資料額度 | **接受（major）** | `errorReceived` 把 `bufferedBytes` 覆寫成上限，先前資料的 credit 歸還就解鎖 | 錯誤與資料額度分開表示：`PartitionFetchState.pendingErrors`；`errorReceived` 只遞增、`errorConsumed` 遞減；error entry 以 `FetchedBatch.error`（size 0、`isError()`）發佈，`CreditReturnQueue.releaseError` 必喚醒 loop；`offsetToFetch` 在 `pendingErrors > 0` 時為空 | `FetchPipelineModelTest.errorBlocksFetchingIndependentlyOfDataCredit`、`staleErrorReturnDoesNotAffectTheNewEpoch` | `existingDataCreditReturnMustNotUnblockPendingError` 通過 |
| F7 只有 paused buffered data 時 poll 空轉 | **接受（major）** | `hasBufferedData` 把 paused 的 batch 當成可進展；舊 `AsyncKafkaConsumer` 的 `FetchBuffer.awaitWakeup` 是 permit 語意不會空轉，所以這是回歸 | `SinkCollector.hasDeliverableData()`：`current` / set-aside / sink 中**非 paused** 的資料才算；`RecordSink.hasReady(predicate)`；`poll()` 的 park 條件改用它。`resume()` 在應用執行緒自己呼叫，重新評估條件即可 | `SinkCollectorTest.onlyPausedDataIsNotDeliverable`、`partiallyConsumedBatchOfAPausedPartitionIsNotDeliverable` | （Codex 未附此項測試） |
| F8 2×2 控制量不等價 | **部分接受** | 同意：depth（批次數）與 factor（位元組）不等價；`factor=1` 不是嚴格的無預抓控制；`RecordHeaders` 只在新 jar；session 模式不同。不接受的部分：CSV 數字本身與「方向」仍成立（Codex 也未否定），100B 場景 response 接近 1 MB 使 `factor=1` 實際上不預抓（可由 `fetch-size-avg` 佐證，待補） | 文件 §2、§9 改為「方向性、非精確 ablation」，列出精確 ablation 的做法為待辦；不宣稱 9% / 69% 是架構的純貢獻 | — | `factorOneStillAllowsLookAheadWithoutConsumption`、`factorTwoAllowsThirdOutstandingBatchWithoutConsumption` 通過（行為前提，與修正後一致） |
| F9 仍有全量 manager activation；同輪重複執行；文件描述過強 | **部分接受** | 重複執行：接受，已修。全量 activation：接受描述需修正，但**不改為針對性喚醒**——試做 per-manager dirty 後 `KafkaConsumerTest.testResetUsingDurationBasedAutoResetPolicy` 逾時：commit manager 的 OffsetFetch 完成會在 offsets manager 排入 ListOffsets，只喚醒 commit manager 就送不出去。manager 依賴藏在 future 鏈裡，沒有宣告就不能針對性喚醒；文件改為如實描述。timer wheel → heap、1 s 安全間隔、100 ms reaper：接受，文件更正 | `ManagerTask` 以 pass 序號去重（timer 與 dirty pass 同輪只跑一次）；文件 §1、§1.5、§3.1、§9 更正 | `ConsumerEventLoopTest.dueTimerAndDirtyPassRunAManagerOnceInTheSamePass`、`aCompletedRequestRerunsEveryManager` | `dueManagerRunsOnlyOnceWhenPollAlsoMakesManagersDirty` 通過 |

**不成立的部分**：沒有一項整體不成立。F8、F9 各有一部分不接受，理由與最小反例在上表（F9 的反例是 `KafkaConsumerTest.testResetUsingDurationBasedAutoResetPolicy` 在 per-manager 喚醒下的逾時）。

**驗證**（修正後）：`KafkaConsumerTest` 225/225、`ConsumerEventLoopTest` 16/16、`SinkCollectorTest` 5/5、`FetchPipelineTest` 2/2、pipeline 套件 30/30（合計 278）；Codex 的 4 個 clients 測試檔 8 案例全部通過（原 6 失敗）；checkstyle / spotbugs 通過。整合測試：

`clients-integration-tests` 12 個 consumer 類別 278 案例，並行執行時 5 個失敗，逐一判定：
- `testSingleCoordinatorOwnershipAfterPartitionReassignment`：已知 timing-flaky（附錄 B）。
- `ConsumerBounceTest.testAsyncClose`、`PlaintextConsumerCloseTest.testAsyncConsumerCloseWithTimeoutIgnoresFetchMaxWaitMs`、`PlaintextConsumerFetchTest.testAsyncConsumerFetchInvalidOffset`：隔離重跑全部通過；並行負載下的 timing。其中 invalid-offset 案例暴露了一個真實缺口並已加固：loop 自己發起的 position 嘗試失敗時投遞的 `ErrorEvent` 不綁定任何一次 `poll()`，可能在使用者已 `seek()` 之後才被下一次 poll 丟出（舊實作把 position 錯誤綁在該次 poll 的 event future 上，沒有這個問題）。現在改投 `FetchPositionsErrorEvent`，應用執行緒只在仍有 partition 缺 position 時才丟出，否則視為過期丟棄。
- `PlaintextConsumerFetchTest.testAsyncConsumerFetchHonoursFetchSizeIfLargeRecordNotFirst`（期望第一次 poll 只有 1 筆、得到 2 筆）：**不是本次修正造成**——在修正前的 `241f83c59c` 隔離重跑三次，兩次同樣失敗。原因是 fetch 深度（04 文件）：第一個 response（小紀錄）發佈後立刻續發，第二個 response（大紀錄）在應用端收集前抵達，一次 poll 就回傳兩筆。這個測試斷言的是深度 1 的實作細節（一次 poll 只含一個 response 的紀錄），不是公開契約（`max.poll.records` 才是上界），但它顯示需要一個真正的「深度 1」模式：`factor=1` 因為以位元組計，在小 response 時仍會續發（正是 F8 指出的），要有以「未消費 response 數」計的上限才能同時解決 F8 的控制變因與這類測試的決定性。列為待辦 6。

**真 broker 驗證（修正後的 jar，2026-09-08）**：`OffsetCheck`（每 partition offset 連續、無重複、消費完後 `seekToBeginning` 能重讀 offset 0）在 1p 1KB × `max.poll.records` 500 / 50、6p 1KB、1p lz4 全部通過（gaps=0、dups=0）。修正前後 jar 交錯 A/B（3 輪中位數）：

| 場景 | 修正前（`b022ab5f5b`） | 修正後（`8daa179a5a`） |
|---|---:|---:|
| 6p 1KB MB/s | 458（441–493） | **1056**（818–1123） |
| 6p 1KB CPU 秒/GB | 2.35 | 2.16 |
| 1p 100B MB/s | 538（457–649） | **749**（683–843） |
| 1p 100B CPU 秒/GB | 2.64 | 2.60 |

6p 的差距與 F3 的機制一致：修正前 credit 已滿的 partition 仍以 `maxBytes=0` 留在 request 裡，broker 對 request 中第一個非空 partition 照樣回一個 batch，response 被這些「強迫的」小 batch 佔滿、往返次數變多；修正後這些 partition 不在 request 裡。1p 100B 的差距沒有對應的機制解釋（單一 partition 不走 `maxBytes=0` 路徑），修正前這一輪的數字也低於附錄 C 的同場景（807–883），可能是時段飄移；只記錄為觀察，不列為修正的效益。

**尚未解決**：
1. F3 的真 broker 長時間重現（A 有 backlog、B 空、應用端停止消費、觀察記憶體）——admission 修正後預期不再增長，尚未量；上表的 6p A/B 只間接支持。
2. F8 的精確 ablation（同一套 admission、同 session 模式、記錄實際 outstanding bytes 與峰值記憶體、平衡變體順序、保存 jar hash）。
3. Commit → Heartbeat fatal error 順序在 timer 路徑上的整合測試（我方對 Codex 的審查 §3.2 也指出兩邊都缺）。
4. Codex 建議的 entry ownership / credit 守恆不變式（涵蓋 sink、current、setAside、error、丟棄路徑）尚未寫成 property test；目前只有 `SinkCollectorTest` 的個案。
5. 以真實 `runOnce` 的 fixture（Codex 的 `NetworkLoopFixture` 一類）取代 `ConsumerEventLoopTest` 的 mock manager。
6. 以「未消費 response 數」計的 fetch 深度上限（真正的深度 1 模式）：同時解決 F8 的控制變因與 `testAsyncConsumerFetchHonoursFetchSizeIfLargeRecordNotFirst` 這類依賴深度 1 的測試。

---

## 附錄 G：對 Codex 兩個問題的獨立審查摘要（2026-09-08）

完整報告：`/Users/unknowntpo/Documents/Codex/2026-09-07/kafka-next-poll-condition-validation/outputs/claude-review-of-deadline-timeout-and-polling.md`（fable reviewer，唯讀審查，baseline `820533b870`）。修法與回歸測試的 patch 在該 reviewer 的暫存目錄 `scratchpad/codex-review2/claude-review-fix-and-test.patch`。

**問題一（時間快照造成額外等待）：已確認，嚴重度低，值得修。** `ConsumerNetworkThread.runOnce` 在 `:214` 取快照、`:222-226` 所有 manager 共用、`:228` 原封傳給 `NetworkClientDelegate.poll`；`NetworkClientDelegate.java:161-168` 與 `NetworkClient.java:695-711` 只取 min，沒有任何一層扣除已耗時。Codex 的實驗忠實，無假陽性。各 manager 的 delay 起算點一致（`RequestState.java:159-162`、`HeartbeatRequestState.java:63-96`），統一扣除安全。實務影響小：只延後純 timer 驅動且無 I/O 的工作；同樣模式也在 `Sender.runOnce:343-345` 與 classic `ConsumerNetworkClient`，是 Kafka 既有慣例，**不能當「舊架構壞掉」的證據**。`maximumTimeToWait` 是同類型問題，目前無證據顯示獨立缺陷，建議另開小 PR。修法 3 行加單調 clamp；reviewer 在 `ConsumerNetworkThreadTest` 內寫了回歸案例（p ∈ {0, 1, 80, 100, 120} 加 event 對照）：baseline 4/24 失敗、套用修法 24/24 通過。

**問題二（每輪 poll 全部 manager 是否值得替換）：不值得為掃描成本替換。**
- A（遍歷 + guard）：合成 163–324 ns/loop，每 manager 5–8 ns；真實 BUSY 的 86/88 個背景 CPU 樣本沒有一個落在純迴圈。可忽略。
- B（manager 真實工作）：幾乎全是 `FetchRequestManager.poll` 準備 fetch 的 allocation，任何架構都要做。
- C（過度喚醒）：成本在 wakeup / event 往返，隨 poll 頻率成長，不在掃描；解法是改 app↔background 協定，與 scheduler 無關。這正是 §2.1 loop-only 變體要量的東西。
- D（scheduler 管理成本）：bitmap 版 BUSY 3.51× / 8.74× 慢於掃描，熱點 60–77% 在 timer TreeMap。
- 判斷：保留掃描、修等待條件、局部優化 fetch 準備。不否定事件驅動：v2 的數字方向值得驗證，但量的是 C 不是 A。

**邊界**：timeout 扣除、`maximumTimeToWait` 用新時間、background event 喚醒、fetch 準備的 allocation、減少 per-poll event 都可獨立 upstream，無需 KIP。KIP-1371 必須證明的是：高 poll 頻率下每 GB 的 wakeup / sys CPU 低於「trunk + 可攜改動」、D 的成本不高於掃描、行為等價（含附錄 F 的 F9 逾時反例）。

對本設計的含意：§1.5 第 4 列（時間只在阻塞前取樣一次）是真的但影響小、且可在舊架構 3 行修掉——它不是切換架構的理由；第 2 列（每次 poll 的固定成本）是 C 類，要用 §2.1 的 loop-only 變體單獨證明。

