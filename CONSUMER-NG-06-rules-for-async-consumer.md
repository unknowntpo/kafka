# CONSUMER-NG 06：從新引擎整理出、舊 `AsyncKafkaConsumer` 也能套用的規則（2026-09-10）

這份文件回答一個問題：我們在 consumer-ng（01–05）與前一條線（loop-only、S1–S8）踩過的坑，有哪些可以整理成**不換架構就能用在 trunk `AsyncKafkaConsumer` / `ConsumerNetworkThread` / 六個 `RequestManager`** 的規則。

起點是 KIP-1371（KAFKA-20995）列的問題，不是它的解法：那個解法（Consumer Reactor、`NextPollCondition`、單一 state owner）已經迭代過好幾版，過時了；但它指出的問題形狀還在，而且我們的引擎在整合測試裡又各踩了一遍。每條規則都附：問題、規則本身、在 trunk 怎麼落地（具體到 class / method）、用什麼測試釘住、我們的證據。

原則（沿用前一條線的決定）：**可攜的規則不是換架構的理由**。這裡的每一條都應該能以小 PR 進 trunk；換不換架構是另一個問題（03 §2 的量測才是那個問題的證據）。

## 1. 問題清單

KIP-1371 頁面與 JIRA 的四類，合併寫成一份：

| # | 問題形狀 | KIP-1371 的證據 |
|---|---|---|
| P1 | **deadline 不帶原因**：`maximumTimeToWait()` / `PollResult.timeUntilNextPollMs` 只說「多久後再問我」，不說是「現在就能做」「等時間」還是「等某個輸入」。timer 到 0 但 coordinator 未知或前一個請求在飛 → 兩條執行緒空轉 | KAFKA-20253（re-auth 失敗後高 CPU）、KAFKA-20426、KAFKA-20970 |
| P2 | **狀態改變跨 owner**：一個 manager 的完成改了另一個 manager 的狀態，依賴與順序隱含在 future 鏈裡；遲到的回應對「請求建立之後才發佈的狀態」動手 | KAFKA-17066、KAFKA-17674（positions 初始化跨執行緒、in-flight 範圍沒保留） |
| P3 | **app 狀態有多條交接路徑**：records、background event、operation future、position、相容狀態各走各的同步機制，觀察者可能先看到結果再看到它依賴的狀態 | PR 21476（fetch 資料先於 position 可見）、KAFKA-18641（position 前進與 auto-commit 快照競態） |
| P4 | **效果先於狀態發佈**：完成 / 通知 / 喚醒立刻觸發 app 執行，但對應的狀態與等待決定還沒發佈 → app 看到舊視角或重新進入同一個等待（ping-pong） | KAFKA-20854（空 fetch 結果觸發無進展的喚醒）、KAFKA-20397（metadata 錯誤發佈與等待進入競態） |
| P5 | **生命週期依賴分散**：coordinator 發現、commit、leave-group、shutdown 由不同元件各自啟停，沒有一份「還有什麼在飛」的最終視角 | KAFKA-18569、KAFKA-19357 |

我們這條線再加三類，KIP-1371 沒列但同樣是「分散的假設」：

| # | 問題形狀 | 我們的證據（05 §4–5） |
|---|---|---|
| P6 | **「有人會定期 poll 我」的隱含假設**：manager 或子系統靠「每輪全掃」才會被推進；一旦迴圈不再每輪全掃，它們就停 | positions 更新、coordinator 斷線偵測、pattern 訂閱重算、FindCoordinator 的送出時機，四個都在整合測試裡停過 |
| P7 | **資源沒有列釋放者**：接收 buffer、佇列裡的資料、in-flight 槽——只要一條路徑沒有明確的 owner 與釋放點，長跑必定停住或漏 | 非 fetch 回應的 buffer 從未回池（`fetch.max.bytes=10 KiB` 四個回應後整個 consumer 停）；fetch 續發佔滿單一 in-flight 槽讓 FindCoordinator / ListOffsets 餓死 30 秒 |
| P8 | **錯誤不屬於任何一次呼叫**：背景重試產生的錯誤排隊，下一次不相干的呼叫撿到 | `auto.offset.reset=none` + seek 後 poll 撿到舊的 `NoOffsetForPartitionException` |

## 2. 規則

### R1 每次執行結束要宣告「在等什麼」，排程器只在該輸入的版本嚴格前進時重跑（P1、P2）

**規則。** manager 的一次 `poll(now)` 結束時必須是三者之一：`WORK_NOW`（送出了請求或改了狀態）、`RETRY_AT(deadline)`（等時間）、`WAIT_FOR(input)`（等某個輸入）。排程器記住它宣告時看到的狀態版本，之後只在「自己的 timer 到期」「宣告的輸入到達且版本嚴格大於宣告時」「command」三者之一時再跑它。不允許「延遲 0 但什麼都沒做」。

**trunk 落地。** 不必一次改契約：`RequestManager.waitCondition()` 加一個預設回 `ANY_INPUT` 的方法（= 今天的行為，零風險），`ConsumerNetworkThread.runOnce` 對每個 manager 記 `(declaredCondition, declaredVersion)`，每輪只跑宣告允許的。`CoordinatorRequestManager`（FindCoordinator 在飛時 `OWN_COMPLETION`）與 `TopicMetadataRequestManager` 可以立刻宣告；Heartbeat / Commit / Offsets 因為互相依賴（R6）先留 `ANY_INPUT`。timer 給 1 ms 下限。

**測試。** 凍結時鐘下每個 manager 的執行次數上界（同一狀態不得跑第二次）；`KafkaConsumerTest.testResetUsingDurationBasedAutoResetPolicy` 是跨 manager 依賴的守門員。

**證據。** loop-only 線 S1；consumer-ng 的 `ManagerTask.wantsRun`。每個 pass 的 TRACE（哪個 manager 跑了、有沒有送東西）是找違反者最快的辦法。

### R2 完成不等於進展；同步完成的 future 永遠不觸發重跑（P1）

**規則。** 一個操作「完成」而沒有改變任何狀態，不是輸入。特別是回傳時已經完成的 future（沒排任何請求就完成）不可以觸發「再跑一次 manager」。

**trunk 落地。** `OffsetsRequestManager.updateFetchPositions` 在什麼都沒排時立即完成；app 側的 `CheckAndUpdatePositionsEvent` 完成後如果 positions 仍缺，不要在同一個 poll 迭代裡立刻再送一個，等下一個 poll 迭代（R9）或 metadata 變化。同理 `maximumTimeToWait()` 回 0 但送不出請求的情況一律換成 `RETRY_AT(retry.backoff.ms)`。

**測試。** 「positions 嘗試立即完成而沒有進展，不會自己重跑 manager」；用 pass / loop 計數斷言。

**證據。** 我們的引擎在 committed offset 卡在 `AWAIT_VALIDATION` 時每分鐘 57 萬個 pass，就是這條被違反。

### R3 每個 pass 發佈一份不可變的決定，先發佈再 signal；等待帶版本（P3、P4）

**規則。** 背景執行緒每輪結束時發佈一份不可變的決定：版本、下一個 deadline、positions 是否齊、reconciliation 序號、有無待交付的事件。所有跨執行緒的可見效果（records、event、future 完成）先發佈再 signal。app 執行緒的等待帶著它看到的版本，只在它可能等待的欄位改變時被叫醒；醒來後重新評估條件，不假設「被叫醒 = 條件成立」。

**trunk 落地。** `ConsumerNetworkThread.runOnce` 末尾組一個 `PassDecision`（volatile 引用）；`FetchBuffer.awaitNotEmpty` 與 `AsyncKafkaConsumer.poll` 的等待改成「等版本變」而不是「等 timeout 或被 wakeup」；`BackgroundEventHandler.add` 先入隊再 signal；`NetworkClientDelegate` 裡直接的 `wakeup` 拿掉，統一在 pass 末尾決定要不要叫醒。

**測試。** 「app 只在它可能等待的欄位改變時被叫醒」；`LoopSignal` 式的交錯案例（signal 發生在 prepare-to-park 之前 / 之後都不能丟失也不能空醒）。

**證據。** loop-only 的 R3 修正（只在 reconcile 進行中才因序號喚醒）把 idle ping-pong 拿掉；consumer-ng 閒置每分鐘 0.36 秒 CPU 對 trunk 1.96 秒。

### R4 錯誤屬於某一次呼叫；同一次呼叫最多送一次（P8、P4）

**規則。** 背景產生的錯誤必須知道它是替哪一次 app 呼叫（哪個 poll 迭代、哪個 `position()`）工作時發生的；同一次呼叫最多送一個；呼叫結束後產生的錯誤給下一次呼叫，但一旦狀態改變（seek、assign）就不再成立的錯誤要作廢。

**trunk 落地。** `OffsetsRequestManager.cachedUpdatePositionsException` 加上「呼叫序號」；`AsyncKafkaConsumer.updateFetchPositions` 每次呼叫遞增序號；`seek*` / `assign` 清掉快取的 positions 錯誤。metadata 錯誤（invalid topic 等）走 `ErrorEvent` 到 app（`notifyMetadataErrorsViaErrorQueue=true`），不要在背景執行緒丟出去被吞掉。

**測試。** `PlaintextConsumerFetchTest.testFetchInvalidOffset`（poll 丟 `NoOffsetForPartition` → seek → poll 必須丟 `OffsetOutOfRange`）；`PollTest.testNoOffsetForPartitionExceptionOnPollZero`（poll(0) 也要拿到）。

### R5 每個資源有唯一 owner，每條路徑列釋放者（P7、P3）

**規則。** 佇列裡的東西、接收 buffer、in-flight 槽、credit：離開一個 owner 時一定進入另一個 owner 或被釋放。導入任何池化 / 引用計數之前，先列一張「路徑 × 釋放者」表；表上有空格就不能合併。

**trunk 落地。** 今天 `ClientUtils.createNetworkClient` 寫死 `MemoryPool.NONE`，所以沒有這個問題；但只要有人替 consumer 接上接收池（KIP 討論過），就要同時在 `NetworkClient.handleCompletedReceives` 或 poll 之後把**非 fetch 回應**的 buffer 還回去，因為它們在 poll 內已經完整解析。`FetchBuffer` / `CompletedFetch` 的 drain 已有 owner，可直接對表。

**測試。** 把容量壓到極小（`fetch.max.bytes=10 KiB`）跑整套整合測試：任何漏釋放幾個回應內就會停住；`taskset -c 0` 單核最容易抓交接競態。

**證據。** 05 §4：非 fetch 回應的 buffer 漏了幾百 MB 才會被發現，測試把它提前到四個回應。

### R6 控制請求優先於資料請求（P7、P1）

**規則。** consumer 的連線每條只有一個 in-flight 槽（`max.in.flight.requests.per.connection=1`）。指定節點的控制請求（ListOffsets 到 leader）與不指定節點的請求（FindCoordinator、metadata 走 `leastLoadedNode`）都必須能拿到槽：待送佇列裡有等某節點的請求，那個節點這輪不發 fetch；有不指定節點的請求，這輪全部不發 fetch。

**trunk 落地。** 今天 trunk 的 fetch 要等 app poll 才續發，槽自然有空檔，所以問題被時序掩蓋；`FetchRequestManager.poll` 只要加一個「`NetworkClientDelegate.unsentRequests()` 裡有沒有等這個節點 / 不指定節點的請求」的檢查就能把它變成規則，而且任何加快 fetch 續發的改動（KAFKA-20854 那類、prefetch）都必須帶著這條。

**測試。** callback 裡呼叫 `beginningOffsets`（`PlaintextConsumerCallbackTest.*BeginningOffsets*`）與 coordinator 換節點時的 close（`ConsumerBounceTest.testAsyncClose`）：沒有這條就是 30 秒 request timeout。

### R7 「被定期 poll」的假設要明列成觸發表（P6、P2）

**規則。** 每個靠時間或靠別人推進的子系統，列出它的觸發來源；任何減少輪詢的改動（R1）先對表。至少這幾個：positions 更新（app 呼叫 / metadata 變化 / 前一次完成）、coordinator 連線是否還活著（每次 network poll 後檢查，斷線且在重連 backoff 中就標成 unknown，與 classic consumer `checkAndGetCoordinator` 相同）、pattern 訂閱重算（每次 poll 且 metadata 版本變了）、poll timer 刷新（app 在 poll 內時）、auto-commit timer。

**trunk 落地。** 大部分今天靠 `runOnce` 每輪全掃隱含保證；把它們寫成 `ConsumerNetworkThread` 裡一張表（哪個步驟、哪個條件），既是文件也是 R1 遷移的前置。coordinator 斷線偵測是 trunk 現在真的缺的一項：commit 會送到死節點拿 `RetriableCommitFailedException`，而不是等重新發現。

**測試。** `PlaintextConsumerCommitTest.testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose`（要 `CommitFailedException`，不是 retriable）、`testAsyncConsumerPositionAndCommit`。

### R8 生命週期只有一個排序者（P5）

**規則。** close 是一個在一個地方寫死的序列，而不是幾個 event 各自完成：auto-commit → 停止 commit / coordinator 查找 → app 執行緒跑 revoke（epoch > 0）或 lost callback → leave group → 等最後一個 async commit 並跑 callback → 網路層在 close timer 內把在途請求跑完（沒有在途就立刻結束）。`closed` 旗標在序列**結束**才設（callback 還會呼叫 consumer）；close 開始就丟掉 wakeup；給關閉期請求的時間是 `min(timeout, request.timeout.ms)`。fatal error 的讀取順序（Commit 讀、Heartbeat 清）也屬於這張序列。

**trunk 落地。** `AsyncKafkaConsumer.close` 已經大致是這個順序，但分成 `CommitOnCloseEvent` / `StopFindCoordinatorOnCloseEvent` / `LeaveGroupOnCloseEvent` 三個 event 加 `pollOnClose`；改成一個 `LifecycleSequencer`（loop-only 已做）讓順序只有一份。

**測試。** `PlaintextConsumerCloseTest`（預設 close ≥ `fetch.max.wait.ms`、指定 timeout 則忽略它）、`ConsumerBounceTest.testAsyncClose`（coordinator 不在時 close 要在 request timeout 內回來，manual assign 的 commit 要成功）、`PlaintextConsumerCallbackTest.*OnPartitionsRevoked`（callback 在 close 內跑且還能呼叫 consumer）。

**證據。** 我們的 close 曾經：閒置時等滿 30 秒（drain 迴圈先 poll 再檢查）、callback 沒跑、callback 裡 `assign` 拿到「already closed」、被上一個 `wakeup()` 打斷 auto-commit——每一個都是這張序列少一行。

### R9 app 執行緒的每個等待迭代都是一個輸入（P1、P4）

**規則。** 一次長 `poll()` 內部的每個等待迭代，都要讓背景執行緒把「只有 poll 路徑可以做的事」做一次：`maybeReconcile`、`onConsumerPoll`、auto-commit timer、heartbeat 的 poll timer、positions 請求。這樣一次 `poll(60 s)` 內就能完成 join → callback → fetch，而不是每個階段等下一次 `poll()` 呼叫。

**trunk 落地。** `AsyncKafkaConsumer.poll` 的 `do { ... } while (timer.notExpired())` 每圈送一次 `AsyncPollEvent`（trunk 已接近），關鍵是背景端把它當輸入而不是當 timer；positions 請求跟著每圈走（R2、R4）。

**測試。** loop-only 的 R11 回歸測試；`ConsumerBounceTest`、`PlaintextConsumerSubscriptionTest` 的 join-in-one-poll 案例。

### R10 安全網要明列、有界、可見，不能用來掩蓋（P1）

**規則。** 不要有「每 100 ms 醒來以防萬一」的 timer。允許的安全網只有：pass 失敗後的固定 backoff、`RETRY_AT` 的 1 ms 下限、close timer。每個都要記 metric（pass 速率、manager 跑了卻沒送東西的比率），讓 busy loop 在儀表上看得到。

**trunk 落地。** `ConsumerNetworkThread` 的 `MAX_POLL_TIMEOUT_MS`（100 ms）安全網拿掉之前，先加 `background-pass-rate` 與 `manager-runs-without-requests-rate` 兩個 metric（loop-only 已做），量一週再拿。

### R11 閒置只剩協定本身的週期（P1、P4）

**規則。** 沒有資料、沒有 app 呼叫時，背景執行緒的喚醒只能來自 heartbeat 間隔、`fetch.max.wait.ms` 的空回應、`metadata.max.age.ms`。量法固定：3–6 分鐘視窗的每分鐘 CPU 秒與每秒自願 context switch，扣掉 JVM 地板。

**trunk 落地。** 這是 R1–R3、R10 的驗收指標，不是另一個機制；trunk 今天每分鐘 1.96 秒、每秒 68 次喚醒，目標 ≤ 0.4 秒、≤ 15 次。

### R12 驗證方法本身也是規則

- 全套 consumer 整合測試（clients-integration-tests 的 12 個類別）是最便宜的回歸網；用 `-Dkafka.consumer.delegate=builtin`（或同型的切換）做 A/B，能立刻分辨「測試本身的 race」與「我們的 bug」。
- 診斷靠每個 pass 的 TRACE：跑了哪個 manager、有沒有送出、版本多少；一分鐘的 log 就能看出 busy loop 與餓死。
- 資源類 bug 用「把容量壓到極小」與「單核 `taskset`」逼出來；時序類 bug 用「broker 關掉 / coordinator 換節點」的既有測試。
- 效能只認穩態（暖機後 ΔCPU/ΔGB）與長視窗閒置；短跑數字被 JIT 灌水。

## 3. 對照：trunk 現況、我們的引擎、可攜性

| 規則 | trunk `AsyncKafkaConsumer` | consumer-ng | 進 trunk 要換架構嗎 |
|---|---|---|---|
| R1 宣告等待 + 版本重跑 | 否（每輪全掃 + `maximumTimeToWait`） | 是（`ManagerTask`） | 否：預設 `ANY_INPUT` 的方法 + `runOnce` 記版本，逐 manager 遷移 |
| R2 完成 ≠ 進展 | 每 poll 重送 event | 是 | 否 |
| R3 每 pass 一份決定、先發佈再 signal | 否（各來源各自 signal） | 是（`PassDecision`） | 否，但要動 `FetchBuffer` 的等待與 `NetworkClientDelegate` 的 wakeup |
| R4 錯誤屬於呼叫 | 部分（`cachedUpdatePositionsException` 無序號） | 是 | 否 |
| R5 資源守恆表 | 不適用（無池） | 是 | 否；導入池時必做 |
| R6 控制請求優先 | 靠時序 | 是 | 否；任何 prefetch 改動的前提 |
| R7 觸發表 | 隱含 | 是 | 否 |
| R8 單一生命週期排序者 | 三個 event + `pollOnClose` | 是 | 否 |
| R9 等待迭代是輸入 | 接近 | 是 | 否 |
| R10 有界可見的安全網 | 100 ms timer、無 metric | 是 | 否 |
| R11 閒置指標 | 1.96 s/min | 0.36 s/min | — |

「不換架構」的意思是：這些都能在 `ConsumerNetworkThread` + 六個 manager 的現有拓撲上做。它們做完之後，剩下的架構差異只有 fetch 路徑（03 的吞吐與 CPU/GB）與每 poll 的固定成本（event 往返），那才是 03 §2 要量的東西。

## 4. PR 檢查表

改 `AsyncKafkaConsumer` / `ConsumerNetworkThread` / 任何 `RequestManager` 的 PR 回答這幾題：

1. 這個 manager 這次執行結束時在等什麼？（R1）它有可能回 0 卻送不出請求嗎？（R2）
2. 新增的每個跨執行緒可見效果，發佈點在 signal 之前嗎？app 醒來後會重新評估條件嗎？（R3）
3. 新增的錯誤屬於哪一次 app 呼叫？狀態改變後它會作廢嗎？（R4）
4. 新增的佇列 / buffer / 槽，誰釋放？路徑表填滿了嗎？（R5）
5. 有沒有讓 fetch 更早續發？那 R6 加了嗎？
6. 有沒有減少任何輪詢？R7 的表對過了嗎？
7. 動到 close / fatal error 嗎？序列表更新了嗎？（R8）
8. 有沒有加 timer？它有下限、有 metric、有理由嗎？（R10）
9. 跑過 12 個 consumer 整合測試類別，並用 builtin 切換排除測試本身的 race 嗎？（R12）
