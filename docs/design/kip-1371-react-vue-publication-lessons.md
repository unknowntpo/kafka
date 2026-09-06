# KIP-1371：React／Vue 對狀態發布與效果排序的啟發

日期：2026-09-06。討論基準：`codex/kip-1371-batched-decisions-poc`，
程式證據至 `24b3e3fa8c`，契約摘要至 `66991ce889`。

本文是設計討論筆記，不是已採納的 KIP 規範。框架行為以官方文件為依據；
Kafka 對應是類比推論，不能當作 POC 正確性或效能證據。

## 核心結論與目的

框架透過限制狀態存取與效果執行的位置，提供特定範圍的順序保證；
它們不會自動消除非同步操作之間的狀態相依。

KIP-1371 的目的仍是讓 Request Manager（RM）作者主要處理自己的 domain policy，
而不必在每條新行為中重新建立跨 manager 的排序與通知規則。
應先確認每種效果的接收者需要哪些狀態，再決定共同發布邊界，
不直接把所有 completion 分成「立即執行」或「全域延後」二選一。

## 競爭的 forces

| Force | 依據與狀態 | 取捨 |
| --- | --- | --- |
| RM 作者能局部推理 | 使用者明確提出的目的 | 共同保證越少，作者越可能需要自行重建跨 RM 順序 |
| 接收者不可觀察缺少前提的結果 | POC 的發布／等待測試支持特定路徑 | 需要明確定義前提，不等於所有狀態都要同步完成 |
| 操作完成應及時傳遞 | 設計需求；額外延遲尚未量測 | 統一延後效果會增加排程、保留與生命週期成本 |
| 控制遷移複雜度 | 使用者對完整模型的疑慮 | 局部例外過多，也可能讓規則難以維護 |
| 保留操作身分與授權 | captured scope／owner-version 的既有證據 | 批次排序不能取代 stale-response 驗證 |

問題是：如何讓每種效果只在其必要前提成立後對外可見，同時避免不必要的
全域等待？成功條件應由接收者可觀察的錯誤與測試定義，而不是抽象類別數量。

## Vue：狀態修改與觀察時機分開

Vue 3 的 reactive state 可立即修改，但 DOM 更新會批次排程。
因此「資料已改」不等於「DOM 已更新」；`nextTick()` 可等待該次 DOM 更新。
這不是等待所有 network request 或 Promise 完成。[官方說明](https://vuejs.org/guide/essentials/reactivity-fundamentals.html#dom-update-timing)

Watcher 是依狀態變更執行的回呼，執行時機有不同契約：

| 選項 | 觀察範圍與限制 |
| --- | --- |
| 預設 `pre` | 在 parent component 更新之後、owner component 的 DOM 更新之前執行 |
| `post` | 在 owner component 的 DOM 更新之後執行；不是整個應用程式所有工作都完成 |
| `sync` | 隨 reactive mutation 同步執行，不做批次合併；連續修改多個欄位時可能觀察中間狀態 |

Vue 並未要求所有回呼都等同一個全域屏障；需要更新後 DOM 的觀察者選擇
`post`，同步觀察者則承擔不同的排序與執行次數。[Watcher 時機](https://vuejs.org/guide/essentials/watchers.html#callback-flush-timing)

對於舊 request 晚到，watcher cleanup 提供失效時取消工作的接點，
仍由作者取消或忽略失效結果。`onWatcherCleanup` 在 Vue 3.5+ 可用，
必須於 watcher 同步執行期間登記，不能放在 `await` 之後。
這不自動建立 Kafka 所需的 owner-version fencing。
[Cleanup 契約](https://vuejs.org/guide/essentials/watchers.html#side-effect-cleanup)

## React：以純計算限制換取排程自由

React 區分 render（計算畫面）與 commit（套用 DOM）。每次 render 的 state
與 event handler 對應該次 snapshot，不是任意讀取持續變動的共享狀態。
[Render／Commit](https://react.dev/learn/render-and-commit)、
[State snapshot](https://react.dev/learn/state-as-a-snapshot)

這帶來幾項開發限制：

- Render 不得送出 request 或修改非局部狀態等外部效果。
- Props／state 必須視為不可變的 snapshot，透過指定入口要求更新。
- Render 必須可安全重新計算，不能依賴「只執行一次」。

框架因此能重新執行、暫停或捨棄尚未提交的計算，而不重複外部動作。
副作用仍可在 event handler 或 Effect 中執行；並非所有副作用都由框架統一延後。
[純度規則](https://react.dev/reference/rules/components-and-hooks-must-be-pure)

`useEffect` 用來在對應 commit 後與外部系統同步。作者仍須宣告依賴、
清理舊工作，並處理 response 亂序。它不是 exactly-once 或
「所有非同步工作完成」的保證；也不能把它一概描述為瀏覽器 paint 之後才執行。
[useEffect 契約](https://react.dev/reference/react/useEffect)

## Completion 的具體情境

此處 completion 指內部操作 future 被標記完成，不是 `runOnce()` 結束，
也不直接等於 application thread 執行使用者的 `OffsetCommitCallback`。

POC 測試 `testCompletionObserverIsNotABatchSnapshot` 控制同一次 I/O batch
內 commit 成功與 heartbeat coordinator invalidation 的順序：

1. 先處理 commit 成功，完成 commit future。
2. Future 的 inline dependent 立即執行，此時 coordinator 仍為已知。
3. 再處理 heartbeat response，coordinator 變為未知。

反轉 response 順序，dependent 看到的 coordinator 狀態也不同；兩種順序下
commit 都成功，batch 結束後 coordinator 都正確變為未知。
內部狀態讀取是測試觀察點，尚未證明 public API 有 bug。
[完整證據](kip-1371-async-poll-metadata-delivery.md#major-decision-exposed-what-does-a-completion-promise)

「Commit 成功」不會因之後發現 coordinator 失效而被推翻。
但若接收者需要的不只是該次 commit 結果，就必須另行定義其觀察前提。

## 原模型與 Approach 2：不能混淆的保證

原完整模型的目標，是把納入遷移的 application-visible effects 放到共同出口：
對應 state 與 `ReactorSchedule` 發布後，再釋放效果。
這不是對原 POC 所有相容路徑都已完成遷移的宣稱。

Approach 2 允許既有路徑的操作 future 在局部結果就緒後完成；
aggregate wait 的發布與舊等待失效通知另由共同機制處理。
它未提供 schedule-before-every-effect 的統一保證。
[目前契約與限制](kip-1371-approach2-contract-summary.md)

重要修正：原模型的 publication-before-effect 不自動等於
「同批所有 RM 的狀態都更新完成」。原討論亦包含跨 owner command 延至
下一輪套用的情況。發布屏障的內容與範圍必須明確定義，不能把它當成
整個 consumer 的全域交易。批次範圍更強的保證是另一項設計選擇。

## 可借鏡的結構與代價

以下是由案例抽出的候選結構，不是要求導入 Vue／React API：

| 結構 | 解決的問題 | 代價與新的問題 |
| --- | --- | --- |
| 依觀察需求提供效果出口 | 局部 completion 與依賴發布狀態的通知不必共用相同時機 | 必須分類效果、標示前提，避免任意選擇較弱出口 |
| 限制元件持有的能力 | 作者不能任意繞過指定發布／通知路徑 | 需涵蓋間接 helper 與既有相容路徑；不會自動補齊 domain predicate |
| 分離計算與正式接受動作 | 允許重算候選決策，不重複外部效果 | 需處理接受前狀態變化、資源保留、失效與失敗恢復 |
| 以操作身分處理失效結果 | 舊 response 不可改錯目前狀態 | 取消不代表遠端動作未發生；仍須 owner 驗證與明確終態 |

這些結構的組合順序是：先確認接收者依賴，再界定出口與能力，
最後以操作身分／版本處理延遲結果。排序與失效處理互補，不能互相取代。

不能把 RM 的 `poll()` 直接當作 React render：目前它會記錄 in-flight、
保留操作並建立 request，不是可隨意重跑的純計算。採用可重算模型需要
拆開候選計算與 request admission（正式接受這次嘗試、記錄保留狀態），
而且 admission 也不等於 request 已經送上網路。

前端 UI 排程也不是 Java 跨執行緒 happens-before 證明。
Kafka 仍須保留 queue、同步 buffer、volatile publication 等具體同步機制。

## 待評估的方案與下一步

| 方案 | 好處 | 尚需證明 |
| --- | --- | --- |
| 維持 Approach 2 的局部 completion＋共同等待通知 | 保留既有操作完成路徑，遷移較小 | 所有受影響接收者是否都能安全讀取與重新等待 |
| 對依賴特定發布的效果採選擇性屏障 | 可只延後必要效果，類似 Vue 明確指定觀察時機的啟發 | 效果分類是否完整、邊界能否由介面／測試約束 |
| 統一暫存納入範圍的 application effects | 共同順序較清楚 | 精確發布範圍、取消／關閉／重入行為，以及延遲與保留成本 |

目前建議先建立效果清單，不直接採納更強模型。每列至少記錄：
產生者、接收者、所需狀態、發布位置、釋放位置、失效條件及驗證案例。

優先涵蓋 commit completion、fetch data wakeup、metadata error notification、
aggregate-wait change notification。驗證應包含 response 順序互換、
先通知後等待／先等待後通知、late response，以及取消與關閉。

Commit 案例不需要全批延後，不能推論 fetch wakeup 或 error notification
也不需要發布前提。選擇性屏障目前只是候選方向，尚未獲得作者定案或
全路徑驗證；本筆記不改變現行程式、公開 KIP、benchmark 或遷移承諾。
