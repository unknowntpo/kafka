# ASYNC-CONSUMER-V2 06：背景執行緒與應用執行緒的一致性契約（規範草案）

> 目的：KIP-1371（KAFKA-20995）列的四類問題——沒有可行進展的緊急工作、空結果語意不明、發佈與等待的順序競態、生命週期依賴分散——都不是某個迴圈實作的 bug，而是「沒有規範」的結果。這份文件把它們變成**與實作無關**的契約：不論是 trunk 的 `ConsumerNetworkThread`、本專案的 `ConsumerEventLoop`、還是 KIP-1371 的 reactor，只要違反其中一條，就會重新製造出同一類 bug。每條規則寫：規則、為什麼、怎麼遵守（程式模式）、怎麼驗證、目前狀態。日期 2026-09-08。

## 0. 詞彙

- **owner**：某個可變狀態唯一被允許修改它的執行緒或元件。
- **effect（可見效果）**：應用執行緒看得到的東西：future 完成、背景事件入佇列、buffer 有資料、`SubscriptionState` 改變、喚醒。
- **decision（決定）**：背景執行緒對「接下來等什麼、等多久」的結論。
- **version（版本）**：單調遞增的整數，狀態每次改變就前進；等待者用它判斷自己看到的狀態是否已過時。

## 1. 規則

### R1 每個嘗試都要宣告它在等什麼（對應 KIP-1371 問題 1、2）

- **規則**：任何「可能推進狀態」的呼叫（`RequestManager.poll`、position 更新、fetch 建立）回傳的結果必須是三者之一：`WORK_NOW`（已送出 request 或已改變狀態）、`RETRY_AT(deadline)`（純時間）、`WAIT_FOR(input)`（某個 request 完成、某個狀態版本前進、某個外部事件）。**不允許**「延遲 0 但什麼都沒做」。
- **為什麼**：舊迴圈的六張 busy-loop JIRA 全部是「回 0 但推不了」；本專案的 F4 是同一件事換個地方。只要結果不表達可行性，排程器只能猜。
- **怎麼遵守**：`PollResult` 加上等待條件（Codex `NextPollCondition` 的方向）；在契約改好前，迴圈對「延遲 0」一律套 1 ms 下限，並把它當 `RETRY_AT`，不當 `WORK_NOW`。
- **怎麼驗證**：凍結時鐘、無 I/O、無 command 下連跑 N 個 pass，每個 manager 的執行次數必須有上界（`ConsumerEventLoopTest.idlePassDoesNotRerunManagers` 的形式），對每一個 manager 各寫一個。
- **目前狀態**：trunk 違反（回 0 即整輪重跑）；本專案迴圈只做到下限，manager 契約未改。

**Busy loop 的分類（哪些能「不可能」，哪些只能「有界」）**：

| 類型 | 例子 | trunk | 本專案迴圈 | 標記 + 排程器版本驗證 |
|---|---|---|---|---|
| 1. 自我觸發：元件說「現在跑我」但推不了 | manager 回 0 而 coordinator 不可用（六張 JIRA）；F4 | 整條執行緒空轉 | timer 路徑有界（1 ms，只跑自己）；完成回呼那條靠個案修 | **不可能**，但只在排程器驗證下：元件只能在「它要求的 timer 到期」「它宣告的輸入到達且版本嚴格大於它看過的」「command」三者之一時被重跑。純標記不夠：宣告已成立的條件、或回 `WORK_NOW` 卻沒送出東西，都會再空轉 |
| 2. 元件間循環 | commit 完成 → membership 反應 → 再 commit，淨狀態不變 | 可能 | 可能，「完成就跑全部」讓它以 RTT 速率轉 | 可能；每步都是真事件。只有跨元件的「淨狀態版本沒前進就不得再送」守恆檢查擋得住 |
| 3. 外部風暴 | broker 立刻回錯誤 | 有界於 manager 內的 backoff | 同左 | 同左；是 retry 策略問題 |
| 4. 應用端全速 `poll(0)` | 緊迴圈 poll | 每次 poll 2 個 event | **消除**（volatile 寫入） | 取決於 app↔background 協定 |
| 5. timer 回 0 | `timeUntilNextPollMs = 0` | 整輪重跑 | 有界（1 ms，只跑它） | 同左，除非 R1 禁止「延遲 0 但沒做事」並驗證 |

R1 的完整形式因此是：結果宣告等待條件，**且**排程器記住元件宣告時看到的版本，只在版本嚴格前進時重跑。只做前半是 KIP-1371 PoC 的狀態，不改變第 1 類的性質。

### R2 完成不等於進展（對應問題 1）

- **規則**：completion 回呼只能在「這次完成改變了狀態」時標記重跑；同步完成（呼叫當下就完成的 future）永遠不得觸發重跑。
- **為什麼**：F4：position 嘗試立即完成 → 設 dirty → 下一輪再嘗試 → 再完成，不經任何 timer 的空轉。
- **怎麼遵守**：註冊回呼後再設「非同步」旗標；回呼裡先檢查旗標（`ConsumerEventLoop.maybeUpdateFetchPositions` 的 `asynchronous[]`）；或更好：回呼只標記 `WAIT_FOR` 已滿足的那個等待者（R1）。
- **怎麼驗證**：`positionsAttemptThatCompletesWithoutProgressDoesNotRerunManagersOnItsOwn`。任何新加的 `whenComplete → dirty` 都要附這種測試。
- **目前狀態**：本專案已遵守；trunk 的 `AsyncPollEvent` 鏈在每次 poll 重做，靠應用端節奏限制。

### R3 先發佈、後 signal；等待帶版本；每個等待狀態列出喚醒來源（對應問題 3）

- **規則**：(a) 生產者先把狀態寫到共享結構，再 signal。(b) 等待者先讀版本（或 signal 計數）、再評估條件、只有版本沒變才 park，醒來後重新評估。(c) 每一個 `await` / `park` / condition wait 的呼叫點，必須在註解列出所有能叫醒它的來源，且每個來源都真的會 signal。
- **為什麼**：trunk 的 `BackgroundEventHandler.add` 不喚醒任何人，應用執行緒可能在事件已到之後才進入 100 ms 等待；02 §2.4。
- **怎麼遵守**：`Parker.await(condition, deadline)` + `signals()`；`LoopSignal.prepareToPark`（Dekker 式）。新的共享佇列一律包成「入佇列即 signal」（`SignallingQueue`）。
- **怎麼驗證**：兩執行緒交錯測試：生產者在等待者評估條件之後、park 之前發佈，等待者必須不 park 或立即醒（`LoopSignalTest`、`RecordSinkTest` 的形式）。Code review 檢查 (c) 的清單。
- **目前狀態**：本專案應用端與迴圈端都遵守，且自 2026-09-08 起 loop 以 `PassDecision` 在 app 可見欄位改變時喚醒（07 文件第 1 步）；trunk 的背景事件佇列違反 (a)(c)。

### R4 每個狀態有唯一 owner；跨執行緒只走列舉過的通道（對應問題 3、4）

- **規則**：每個可變狀態在 javadoc 標明 owner；非 owner 只能透過明列的通道（本專案：sink、credit、command、result；trunk：application event queue、background event queue、`FetchBuffer`）影響它。owner 執行緒的方法在進入時 `assert` 執行緒身分。
- **為什麼**：兩條執行緒都改 `SubscriptionState.position` 是 KAFKA-13563 一類 race 的根源；沒有 owner 就沒有辦法談順序。
- **怎麼遵守**：`ConsumerEventLoop` 的欄位分成 loop-only / app-only / volatile 三組並註明；`FetchPipeline` 只在 loop 執行緒呼叫；`SinkCollector` 只在應用執行緒。
- **怎麼驗證**：執行緒斷言在測試中開啟；SpotBugs 的非 volatile 共享欄位警告當錯誤處理。
- **目前狀態**：本專案大致遵守但沒有執行緒斷言；`SubscriptionState` 仍是兩邊都寫的共享物件（沿用 trunk）。

### R5 資源守恆：離開佇列的東西一定有 owner 或已釋放（對應問題 3 的資料面）

- **規則**：任何從共享結構取出的 entry（batch、credit、事件）在所有路徑上（含例外）都必須：被某個 owner 持有、或被明確釋放。不可有「取出後例外 → 消失」。
- **為什麼**：F1、F2：初始化例外讓 batch 與 credit 同時失去 owner，partition 從此無法 fetch。
- **怎麼遵守**：取出後立即指派到具名的持有變數（`current` / `setAside` / `initializing`），例外處理只在這些變數之間轉移或釋放。
- **怎麼驗證**：property test：隨機操作序列（publish / take / 例外 / seek）後，`published == held + released`。目前只有 `SinkCollectorTest` 的個案，守恆 property test 待補。
- **目前狀態**：本專案 sink 路徑遵守；trunk 的 `FetchBuffer` 由 `collectFetch` 的政策保證。

### R6 元件間的依賴要宣告，不能靠「順便被 poll 到」（對應問題 2、4）

- **規則**：若元件 A 的完成會讓元件 B 有工作（commit manager 的 OffsetFetch → offsets manager 的 ListOffsets；heartbeat → membership → commit），這個依賴必須表達為 B 的 `WAIT_FOR(A 的 request)` 或 A 明確通知 B；不得依賴「排程器反正會 poll 到 B」。在依賴宣告完成之前，排程器**必須**在任何完成後跑全部元件（這是安全的退路，不是最佳化）。
- **為什麼**：per-manager 喚醒在 `KafkaConsumerTest.testResetUsingDurationBasedAutoResetPolicy` 逾時（附錄 F 的 F9）；依賴藏在 future 鏈裡，換排程器就壞。
- **怎麼驗證**：每條已知的跨 manager 鏈一個凍結時鐘的單元測試（OffsetFetch → ListOffsets；heartbeat → reconcile → commit → ack）。這些測試同時是任何「針對性喚醒」改動的守門員。
- **目前狀態**：本專案採全部重跑的退路；依賴清單未宣告。

### R7 時間：決定用它被計算時的時間，阻塞前用新時間重算剩餘（對應問題 1 的時間面）

- **規則**：所有 deadline 以絕對時間存放；相對等待只在阻塞前一刻用新讀的時鐘計算，並對時間倒退 clamp。不得把一輪開頭的快照當成阻塞時的時間。
- **為什麼**：Codex 問題一（附錄 G）：處理耗時 80 ms 後仍等滿 100 ms；trunk、`Sender`、classic consumer 都有此慣例。
- **怎麼遵守**：`LoopTimer.timeToNextMs(now)` 在阻塞前以新 `now` 呼叫；trunk 的修法是 `NetworkClientDelegate.poll` 前扣除已耗時（3 行）。
- **怎麼驗證**：附錄 G reviewer 的回歸案例（處理耗時 p ∈ {0, 1, 80, 100, 120}，阻塞時間必須是 max(0, 100 − p)）。
- **目前狀態**：本專案遵守；trunk 待修（patch 已有）。

### R8 錯誤攜帶它所指的狀態版本，交付時檢查是否仍成立（對應問題 3）

- **規則**：由背景執行緒自主發起的工作所產生的錯誤（不是應用端某次呼叫直接要求的），發佈時要帶上它所指的條件；應用執行緒只在條件仍成立時才丟給使用者，否則丟棄並記錄。應用端直接要求的工作（`commitSync`、`position()`）的錯誤綁在該呼叫的 future 上，不走這條。
- **為什麼**：loop 自己的 position 嘗試失敗，`ErrorEvent` 可能在使用者 `seek()` 之後才被下一次 `poll()` 丟出（附錄 F 整合測試段）。
- **怎麼遵守**：`FetchPositionsErrorEvent` + 交付時 `hasAllFetchPositions()` 檢查。通用化：`ErrorEvent` 帶 `Predicate<State> stillRelevant` 或狀態版本。
- **怎麼驗證**：seek 後舊錯誤不得出現的單元測試（待補：目前靠整合測試 `testAsyncConsumerFetchInvalidOffset`）。
- **目前狀態**：本專案 `FetchPositionsErrorEvent` 帶 `stillRelevant` predicate（07 文件第 1 步）；其他背景錯誤（metadata error）仍無條件丟出。

### R9 生命週期轉移只有一個排序者（對應問題 4）

- **規則**：coordinator 發現、commit-on-close、leave-group、fetch session 關閉、執行緒停止的順序，由**一個**地方定義並執行；其他元件只提供「我要做什麼」，不自己決定「什麼時候」。fatal error 的「誰先讀、誰清」順序（commit manager 讀、heartbeat manager 清）必須寫成明確的轉移，不能靠 `entries()` 的順序。
- **為什麼**：兩份審查都指出 Commit → Heartbeat 的 fatal error 順序依賴在兩邊設計都存在且無測試。
- **怎麼驗證**：close 序列的整合測試（coordinator 不可用時 commit 仍完成：`ConsumerBounceTest.testAsyncClose`）；fatal error 重排測試（待補）。
- **目前狀態**：本專案沿用 trunk 的 close 序列，未集中。

### R10 安全網要明列、有界、會自我修復

- **規則**：每一個週期性的「以防萬一」timer（manager 1 s、reaper 100 ms）都要有註解說明它在防什麼；timer 任務拋例外必須重排；不得有「等別人剛好把我叫醒」的隱含依賴。
- **怎麼驗證**：`managerTimerIsRearmedAfterPollThrows`；每個安全網一個「拿掉它會壞什麼」的測試或說明。
- **目前狀態**：本專案遵守；trunk 靠每輪全跑，沒有安全網概念。

## 2. 三種實作對照

| 規則 | trunk `ConsumerNetworkThread` | 本專案 `ConsumerEventLoop` | KIP-1371 reactor（依 JIRA 描述） |
|---|---|---|---|
| R1 宣告等待 | 否 | 下限有界化，契約未改 | 是（結果三分） |
| R2 完成 ≠ 進展 | 每 poll 重做 | 是 | 是（狀態版本） |
| R3 發佈後 signal、版本等待 | 背景事件不喚醒 | 是 | 是（彙總決定先發佈） |
| R4 唯一 owner | 部分 | 部分（無斷言） | 是（state owner） |
| R5 資源守恆 | `collectFetch` 政策 | 個案測試，property 待補 | 未提及 |
| R6 依賴宣告 | 靠每輪全跑 | 靠完成後全跑 | 是（cross-manager observations ordered） |
| R7 時間 | 否（patch 已有） | 是 | 是 |
| R8 錯誤帶條件 | 否 | 部分 | 是 |
| R9 生命週期排序者 | 否 | 否 | 是 |
| R10 安全網 | 無 | 是 | 未提及 |

讀法：KIP-1371 的 reactor 是「用一個結構同時滿足 R1–R4、R6–R9」的實作；代價是效果要在輪尾批次釋放（03 §1.6 的說明）。本專案是「在不改 manager 契約下能滿足的最大集合」；剩下的 R1、R6、R9 只能靠改契約。trunk 的差距最大，但 R3、R7 是可以獨立小修的。

## 3. 給後續改動的檢查表（PR 模板）

改到背景執行緒、request manager、應用端等待、或任何跨執行緒佇列時，回答：

1. 我新增或修改的每個回傳「延遲」的地方，是 `WORK_NOW` / `RETRY_AT` / `WAIT_FOR` 哪一種？延遲 0 時是否真的做了事？（R1）
2. 我新增的每個 completion 回呼，在同步完成時會不會觸發重跑？（R2）
3. 我新增的每個等待點，列出了所有喚醒來源嗎？每個來源都是先發佈再 signal 嗎？（R3）
4. 我改的狀態的 owner 是誰？我在 owner 執行緒上嗎？（R4）
5. 我從佇列取出的東西，在例外路徑上誰持有？（R5）
6. 我的改動讓某個 manager 的完成影響到另一個 manager 了嗎？那條依賴寫在哪裡？（R6）
7. 我傳下去的時間是新讀的還是輪頭的快照？（R7）
8. 我發出的錯誤，使用者收到時條件還成立嗎？（R8）
9. 我碰到 close / leave / coordinator 順序了嗎？是在唯一的排序者裡改的嗎？（R9）
10. 我加的週期性 timer 在防什麼？它拋例外會重排嗎？（R10）

每一題「是」都要指到一個測試。
