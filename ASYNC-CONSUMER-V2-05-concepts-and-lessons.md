# ASYNC-CONSUMER-V2 05：概念清單與教訓（給未來回頭看的人）

> 目的：不論這條分支最後有沒有合併，把它裡面「值得留下的想法」與「踩過的坑」獨立記下來。每一條都寫清楚：它是什麼、解決什麼、在哪裡、證據是什麼、能不能搬到別處。日期 2026-09-08；程式在分支 `async-consumer-v2-review-fixes`（完整版）與 `async-consumer-v2-loop-only`（只有迴圈）。

## 1. 概念清單

### 1.1 執行模型

| 概念 | 是什麼 | 解決什麼 | 在哪裡 | 證據 | 可搬性 |
|---|---|---|---|---|---|
| **Per-task timer，無全域等待時間** | 每個 manager 自己排自己的下次時間；迴圈只看最早的 deadline；沒有 `maximumTimeToWait` 的全域 min | 舊迴圈「最沒耐心的 manager 綁架整條執行緒」（03 §1.5 第 3 列）與六張 busy-loop JIRA | `ManagerTask`、`LoopTimer` | `ConsumerEventLoopTest`；三方 A/B mpr50 CPU/GB −11% | 可搬：任何「多個元件各自回報下次時間」的 loop |
| **1 ms 下限 + 到期只重跑自己** | `LoopTimer.MIN_DELAY_MS`；timer 到期只執行那一個 task | 把「某個 manager 回 0」的傷害從整條執行緒縮成一個 task 每 1 ms 一次 | `LoopTimer.schedule` | `LoopTimerTest`；03 §1.6 第 1 列：有界化而非解決 | 可搬 |
| **事件觸發的輪詢（不是事件驅動的 manager）** | request 完成或 command 才做一次「全部 manager 各跑一次」；閒置時不跑 | 舊迴圈每輪都跑全部 | `ConsumerEventLoop.runDirtyWork`、`ManagerTask.run` | 反例：per-manager 喚醒讓 `testResetUsingDurationBasedAutoResetPolicy` 逾時（03 附錄 F 的 F9） | 概念可搬；細粒度需要宣告依賴 |
| **每 pass 去重** | 同一個 pass 裡 timer 與 dirty pass 只跑該 manager 一次（pass 序號） | Codex 審查 F9 量到的重複執行 | `ManagerTask.lastRunPass` | `dueTimerAndDirtyPassRunAManagerOnceInTheSamePass` | 可搬 |
| **失敗後必重排** | manager 的 `poll()` 拋例外時先以 100 ms 重排 timer 再回報；reaper 同樣 try/finally | timer 式排程的 liveness 漏洞（F5） | `ManagerTask.run`、`ConsumerEventLoop.reap` | `managerTimerIsRearmedAfterPollThrows` | 可搬：任何「先標記完成再執行」的 timer |
| **完成不等於進展** | 立即完成的 position 嘗試不得再觸發 manager pass；只有真的送出 request 的嘗試才觸發 | 不經 timer 的自我觸發空轉（F4） | `ConsumerEventLoop.maybeUpdateFetchPositions` 的 `asynchronous[]` 旗標 | `positionsAttemptThatCompletesWithoutProgressDoesNotRerunManagersOnItsOwn` | 可搬：所有「完成回呼設 dirty」的設計都要問這個問題 |
| **零 event 的 poll** | 應用端 `poll()` 只寫一個 volatile 序號；迴圈在自己的 pass 做簿記（reconcile 檢查、auto-commit timer、poll timer、position 初始化） | 每次 poll 2 個 event + 2 次 selector wakeup 的固定成本（03 §1.5 第 2 列） | `ConsumerEventLoop.onApplicationPoll` / `housekeepApplicationPoll` | 三方 A/B：loop-only 對 trunk mpr50 +17%、CPU/GB −11% | 可搬到舊迴圈（附錄 G 的「trunk + 可攜改動」） |
| **條件式喚醒** | `LoopSignal.wakeupIfParked`：只在迴圈真的 park 時才寫 pipe；Dekker 式 `prepareToPark` 防漏 | 應用端 4.9% 時間花在 pipe write | `LoopSignal` | `LoopSignalTest` | 可搬 |

### 1.2 資料路徑

| 概念 | 是什麼 | 解決什麼 | 在哪裡 | 證據 | 可搬性 |
|---|---|---|---|---|---|
| **Fetch 有唯一 owner，續發在 response 回呼裡** | response 一到，同一個 stack 內檢查 epoch、放進 sink、決定續發 | 續發要繞應用執行緒與一整輪迴圈（管線深度 1，三方都只用 ¼ CPU） | `FetchPipeline.onSuccess` → `issueFetches` | 02 §3.7 實驗 2.15×；三方 A/B loop-only → 完整版 2.4× | 機制可搬回舊架構（04 文件） |
| **Credit 模型與四條不變式** | I1 連續、I2 單 in-flight、I3 位元組額度、I4 epoch fencing | 預抓的正確性能被陳述與測試 | `FetchPipelineModel`、`PartitionFetchState` | `FetchPipelineModelTest` 含 property test | 模型可搬 |
| **Position epoch fencing** | seek / reset / assign 開新 epoch；舊 epoch 的 response、sink entry、credit 回傳全部作廢 | 預抓與 seek 的競態（02 的 hack 會停住） | `FetchPipelineModel.positionChanged`、`RecordSink.take` 的 epoch 檢查 | 真 broker `OffsetCheck` 的 seek 場景 | 可搬 |
| **錯誤與資料額度分開計** | 錯誤是 `pendingErrors` 計數與 size 0 的 error entry，不佔位元組額度 | 先前資料的 credit 歸還會解鎖未處理的錯誤（F6） | `FetchPipelineModel.errorReceived` / `errorConsumed`、`FetchedBatch.error` | `errorBlocksFetchingIndependentlyOfDataCredit` | 可搬 |
| **沒有 credit 就不進 request** | 不用 `maxBytes=0`；靠 session forget / re-add | broker 對 request 第一個非空 partition 不論 `maxBytes` 都回一個 batch（F3） | `FetchPipeline.addPartitionToRequest` | `FetchPipelineTest`；6p A/B 458 → 1056 | 這是 broker 語意，任何 client 都適用 |
| **Entry ownership 規則** | 離開 sink 的 batch 永遠在 `current`、`setAside` 之一，或已釋放 credit | 例外路徑丟掉 batch 與 credit（F1、F2） | `SinkCollector.collect` | `SinkCollectorTest` | 可搬 |
| **只有可交付的資料才算「有進展」** | paused partition 的 buffered 資料不算等待的釋放條件 | poll 對 paused 資料空轉（F7） | `SinkCollector.hasDeliverableData`、`RecordSink.hasReady(predicate)` | `onlyPausedDataIsNotDeliverable` | 可搬 |
| **狀態式 park + signal 計數** | 應用端先記 `signals()`、檢查條件、再 `await(condition)`；生產者先發佈再 signal | lost wakeup；permit 語意的假喚醒 | `Parker`、`RecordSink.publish` | 03 §1.6 問題 3 的說明 | 可搬 |
| **Per-record 工作留在應用執行緒** | 背景執行緒只做 epoch 檢查、放 sink、續發 | 背景執行緒的延遲（heartbeat）與使用者程式碼的契約 | 03 §6.0 | 附錄 D：應用執行緒只忙 24% | 設計原則 |

### 1.3 方法

| 概念 | 內容 | 證據 |
|---|---|---|
| **把可回移的優化與架構分開** | 能套回舊架構的（prefetch 機制、`RecordHeaders`、解碼路徑）不算切換理由；架構的貢獻要單獨量 | 04 文件；03 §2 的 2×2；§2.1 的 loop-only 變體 |
| **交錯 A/B 是唯一可信的跨變體比較** | 同一變體跨時段飄移 ±15%，Codex 容器把負載推到 150 時三個變體都掉到 1/3 | 03 附錄 B/C 註記、§2.1 |
| **一次只改一個變因的階梯** | trunk → loop-only → 完整版，每一階只多一件事 | §2.1：迴圈 +7% / +17% / +58%，fetch 路徑 2.4× |
| **對每個「結構性保證」逐條證明有界** | 「busy-loop 結構性消失」只對 timer 路徑成立；其他觸發要各自證明（F4 就是漏掉的一條） | 03 §1.5 第 5 列、附錄 F |
| **交叉審查** | 兩份獨立實作互審，重現測試不改對方斷言 | 附錄 F、附錄 G、Codex 工作區的兩份審查 |

## 2. 教訓（含負面）

1. **manager 之間的依賴藏在 future 鏈裡。** commit manager 的 OffsetFetch 完成會在 offsets manager 排入 ListOffsets；heartbeat 驅動 membership 再驅動 commit。沒有宣告就不能做針對性喚醒；per-manager dirty 在單元測試就壞了。要細粒度，先改契約。
2. **「完成」回呼設 dirty 是空轉的溫床。** 任何 future 完成就 dirty 的設計，都要問「這次完成有沒有改變狀態」。
3. **broker 的 `minOneMessage` 讓 `maxBytes=0` 不是閒置訊號。** 設計文件 §4.6 原本的假設錯了，是審查者從 broker 原始碼找出來的。
4. **重用低階 API 不會自動繼承高階 API 的政策。** 重用 `FetchCollector.fetchRecords` 沒有帶來 `collectFetch` 的延後錯誤契約（F1）。
5. **測試的 mock 會掩蓋時序類問題。** `MockClient` 不阻塞、`MockTime` 不前進，需要顯式觸發的東西（reaper、metadata 版本、重複 poll）都要在測試裡明列；真 broker 才暴露 incremental session 省略 partition、seek 進已抓範圍。
6. **量測要控負載。** 別的 agent 的容器在跑時的數字全部作廢；跑前看 `uptime`。
7. **2× 不是架構給的。** 迴圈本身在單 partition 大批次幾乎沒差，在高 poll 頻率與多 partition 才有；吞吐來自 fetch 路徑，而那部分可以回移。論證要據此收斂（03 §9）。
8. **時間快照問題是真的但很小，且是 Kafka 既有慣例**（`Sender`、classic consumer 都一樣），三行可修，不是換架構的理由（附錄 G）。
9. **KIP-1371 的四類問題與吞吐目標幾乎正交**（03 §1.6）：我們只有界化了第 1 類、部分解了第 3 類。

## 3. Request manager 要不要完全事件驅動？（判斷，2026-09-08）

**事件驅動的性質**（缺一不可，否則只是「事件觸發的輪詢」）：

1. 工作由攜帶身分的事件啟動：事件說明「誰、為什麼」，接收者不必重新掃描狀態找出要做什麼。
2. 每個元件只對自己訂閱的事件反應；沒有「順便問一下其他人」。
3. 沒有全域的「下次何時跑」彙總；每個元件自己的等待條件是宣告出來的（時間、某個 request 完成、某個狀態改變）。
4. 結果是明確的轉移：空結果要說「什麼輸入才能推進」（KIP-1371 問題 2）。
5. 等待帶版本：等待者記下它看過的狀態版本，效果的發佈順序保證它會被叫醒（KIP-1371 問題 3）。

目前的迴圈在分派層與 fetch 路徑滿足 1–3、5；六個 manager 只滿足「被事件觸發」，不滿足 1、2、4。

**真的有必要嗎？分三個目的看：**

| 目的 | 需要 manager 事件化嗎 | 理由 |
|---|---|---|
| 吞吐與 CPU | **不需要** | 附錄 G：掃描成本每 manager 5–8 ns，真實 BUSY 樣本沒有一個落在純迴圈；省下的是 event / wakeup 往返，迴圈已經做到 |
| 消除 busy-loop 類 bug | **timer 路徑不需要；完整消除需要** | 1 ms 下限已把傷害限制在單一 task；要讓 manager「不再算出 0」必須改結果語意（性質 4） |
| KIP-1371 的問題 1、2、4（可行性、空結果語意、生命週期） | **需要，且只能靠改契約** | 這些是 `RequestManager.poll()` 回傳型別與 manager 之間隱含依賴的問題，迴圈怎麼排都解不掉 |

**我的判斷**：不必「完全」事件驅動，也不該一次做。建議的中間形態是**宣告式的 pull**：`poll(now)` 保留為執行入口（manager 累積的邏輯不動），但回傳值多帶「我在等什麼」——時間、某個 request 的完成、某個狀態版本（Codex `NextPollCondition` 的 `Signal` 是這個方向的第一步）。迴圈據此做針對性喚醒，manager 的程式碼改動只在回傳值。這同時解掉問題 1、2，而不需要把六個 manager 改寫成 callback 式 reactor。順序：先 `OffsetsRequestManager` 與 `CommitRequestManager`（OffsetFetch → ListOffsets 那條鏈是最明確的依賴），用 `testResetUsingDurationBasedAutoResetPolicy` 當守門測試；heartbeat / membership 最後，因為 fatal error 的順序依賴還沒有整合測試。完全 callback 化的 reactor（效果暫存到輪尾）在目前的證據下沒有收益，且會抵銷 fetch 路徑的續發時機，不建議。

`ManagerTask` 就是這條路的接縫：一個 manager 改了回傳型別，它的 task 就能改成針對性喚醒，其他 manager 不受影響。

### 3.1 「標出在等什麼」不等於「據此做事」——避免重走 KIP-1371 PoC 的路

只把 `WAIT_FOR` 標出來、再蓋一層排程器決定何時 poll，manager 被 poll 後做的事完全沒變：這是更精準的輪詢時間表，不是事件驅動，而且排程器本身有成本（Codex 的 TreeSet / bitmap PoC 比掃描慢 3.5–8.7×，附錄 G）。「據此做事」有三層：

| 層次 | 意思 | 價值 | 本專案 |
|---|---|---|---|
| (a) 針對性喚醒 | 只跑條件被滿足的 manager | 省 A 類掃描成本，可忽略 | 未做，不值得 |
| (b) 增量執行 | 條件攜帶身分（哪個 request、哪個 partition），manager 只對它做 O(1) 的事，不重掃全部狀態 | 只在 manager 每次執行的工作量隨狀態大小成長時有價值 | **fetch 路徑做了**（`FetchPipeline` 的 per-partition 狀態與回呼內續發），這是 loop-only → 完整版 2.4× 的來源之一 |
| (c) 等待本身 | 迴圈只 park 在宣告條件的聯集上，沒有安全網 timer、沒有完成後跑全部 | 正確性：R1、R6 的機械保證 | 未做，用安全網與全部重跑當退路 |

六個協定 manager 中只有 `FetchRequestManager` 的工作量隨 partition 數成長且每個 response 都跑，值得做 (b)，已由 `FetchPipeline` 取代。其餘 manager 的狀態是幾個旗標與一兩個 pending request，每次 `poll()` 幾十 ns，(a)(b) 都沒有可量到的收益。所以對協定 manager，標記 `WAIT_FOR` 的唯一價值是**正確性**（R1、R2 可被機械檢查；配合排程器的版本驗證，可以讓「自我觸發」這一類 busy loop 在結構上不可能——但只有這一類，見 06 文件 R1 的分類表），它不帶來吞吐，也不需要新的排程資料結構。完全事件驅動（對每個 manager 做 (b)(c)）是把幾十 ns 的掃描換成幾百行的回呼結構；它唯一合理的動機是 R6、R9 的機械保證，而那可以用宣告依賴（文件與守門測試）達成，不必改寫 manager。
