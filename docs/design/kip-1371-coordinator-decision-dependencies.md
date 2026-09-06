# KIP-1371：Coordinator 的決策依賴，不等於全域優先級

日期：2026-09-06。本地基準：`6f79ffb0af`，Java sources 同 `a191d43372`。
範圍為 Approach 2 的 coordinator、regular heartbeat、commit／offset-fetch 路徑。
本文依 pattern-language 區分需求、機制與待驗證推論，不修改 production 排程或公開 KIP。

## 結論與 Intent

Coordinator 提供 dependent request 的必要事實，但不是所有工作都應等待的
最高優先工作。需要的契約是「依據有效的 owner context 接受新 attempt」，
不是「反覆 poll coordinator，直到它完成才處理其他 RM」。

相依關係也不是單向樹：Heartbeat／Commit 讀取 coordinator，response 又可能
回報 coordinator 失效。應區分讀取、回報、owner 更新與新決策，而不是把
所有關係投影成一個 RM 排序或中央 dependency scheduler。

目的是讓作者知道自己需要哪些事實、可以回報哪些 observation；
不需要自行重建其他 RM 的 I/O 或 publication 順序。

## 決策依賴表

| 需要／回報的事實 | 使用者／來源 | 誰套用狀態 | 何時影響後續決策 | 不應推論的保證 |
| --- | --- | --- | --- | --- |
| Coordinator node 已知 | Heartbeat 建立 request；Commit poll 與 request construction | Coordinator RM 的 discovery response handler | 更新後的 manager pass 重新讀取；仍須各自滿足 membership、in-flight、backoff 等條件 | 已知 node 不代表可直接送出所有 request |
| Discovery 正在 in flight | Coordinator RM 自己 | Coordinator request state | 等 network completion；重複 poll 不產生 discovery | 依賴者等待 discovery，不代表阻塞 network I/O 或不相關工作 |
| Discovery 可在 backoff 後重試 | Coordinator RM 自己 | Coordinator request state | 時間到且沒有 in-flight attempt 才能建立 discovery | 高優先級不能跳過 backoff／重複保留 attempt |
| 捕捉的 coordinator version 已失效 | Heartbeat／Commit／OffsetFetch 的特定 error 或 disconnect | Coordinator RM 的 version-fenced 方法 | callback 當下驗證；只在 observedVersion 等於目前 version 時使 owner 未知 | 舊 observation 被忽略，不代表那次 request 的失敗也應忽略 |
| 當前 coordinator 已變未知 | 上一列更新的結果 | Coordinator RM | 尚未建立的 dependent attempt 在下一次 pass 被擋住；discovery 可依本地條件建立 | 已建立／送出的 attempt 不會被自動重寫或撤回 |
| 操作本身成功／失敗 | 該 attempt 的 response | 該操作 RM 的結果路徑 | Future 可在 callback 內完成；後續新操作可排隊 | 操作完成不承諾同批其他 callbacks 已更新完 state |
| Coordinator fatal error | Coordinator 發現，Commit 讀取，Heartbeat 亦讀取並清除 | 現行 fatal-error 儲存與既有接收路徑 | 影響 unsent operation failure 與 background error delivery | 這不是無副作用 snapshot；沒有在本次證明順序互換仍等價 |

Node 與 version 在目前單一 network-thread 路徑中由相鄰呼叫取得；
不能因此把這個實作描述成跨執行緒的 atomic snapshot API。
版本是 owner-local 身分，不是 group member epoch。

## 三個 dry runs

### 1. Coordinator 未知，discovery 已在 flight

1. Coordinator 不再建立第二個 discovery，回報等待 network completion。
2. Commit 因 coordinator 未知，不建立 dependent request。
3. Heartbeat 的既有 guard 也略過 request construction。
4. Loop 仍須驅動 I/O；discovery callback 更新 owner 後，依賴者才能重新判斷。

這裡「優先 poll coordinator」無法替代 I/O。也不能要求所有 managers
等到自己的所有 futures 完成，才容許其他可執行工作。
Heartbeat 仍有 legacy numeric wait／EMPTY 路徑；本筆記不宣稱它已全面使用 AwaitInput。

### 2. 同次 I/O 中，commit 成功與 heartbeat NOT_COORDINATOR

無論兩個 response 的順序，原 commit 的成功保持有效；若成功的 continuation
排入新的 commit，該 operation 在 callback 中只是排隊，不遞迴 poll／send。
Heartbeat 回報失效，由 coordinator 的方法即時驗證並套用。

當 delegate.poll 返回，Approach 2 在完成標記為真、仍 running、application queue
為空時，跑一次 post-I/O pass。此時只建立 discovery，新的 commit 等待 owner。
Discovery completion 回來後，下一個適用 pass 才建立 commit／heartbeat；
建立後仍待下一次 network poll 發送，不是在 callback 中直接送出。

因此關鍵是「整個已接收 completion batch 先處理，再做相依 admission」，
不是在 response callbacks 中將 coordinator 提升到最高執行優先級。
不符合額外 pass 條件時由下一輪 input／manager 邊界接手；沒有無界排空。

### 3. C7 的舊 response 晚於 coordinator 重新發現

當前 owner version 已更新，即使重新發現的是相同 node，舊 observation
也不可使目前 owner 失效。這需要 version fencing，不能靠 coordinator
在 manager list 排第一達成。另一方面，該次操作的真實結果仍由其 RM 處理；
owner 變更不會把已收到的有效成功改成失敗。

## Forces、候選結構與代價

| 張力 | 目前可用結構 | 代價／剩餘問題 |
| --- | --- | --- |
| 局部推理 vs 跨 owner 依賴 | 讀取 owner context、帶 captured version 回報 observation | Domain 仍須定義哪些錯誤要求 invalidation；窄介面尚未全面限制 raw 方法 |
| 新決策要新狀態 vs 既有操作要穩定 context | 完成批次後決策，保留已接受的 attempt | 不能讓舊 future observer 誤認自己看到 batch snapshot |
| 恢復要及時 vs 避免 busy poll | 輸入啟動與有限 retry；不等待不相關 futures | 依賴者重新啟動與 wait 通知須有 liveness 證據，不能只靠型別 |
| 共同規則 vs 框架成本 | 同一 loop 的有界 pass＋owner-local 檢查 | 額外 full pass 成本未量測；timer-driven owner changes 仍是另一個問題 |

Pattern 組合為：識別前置條件 → owner 驗證回報 → 在明確邊界重新決策 →
保留各操作自己的完成契約。每一步回應不同 force，不能彼此取代。

方案比較：

- **保留 RM 與現有有界 loop**：本次正常恢復已有 component 證據；保留為比較基準。
- **建立全域 priority／dependency scheduler**：目前沒有顯示必要性；只排序 RM
  不能表達晚到 observation、跨 phase 以及已接受操作，還會增加新的排程義務。
- **共同 protocol 決策入口**：可集中特定相依規則，但不是由 node 依賴本身就能推導。
  必須先找出窄介面無法清楚表達的共同 invariant，再決定合併範圍。

## 證據索引與限制

主要 production 接點：

- `CoordinatorRequestManager.nextPollCondition`、`onSuccessfulResponse`、
  `markCoordinatorUnknownIfCurrent`。
- `AbstractHeartbeatRequestManager.poll`、`makeHeartbeatRequest`、
  `onErrorResponse`、`maybePropagateCoordinatorFatalErrorEvent`。
- `CommitRequestManager.poll`、`buildRequestWithResponseHandling`、
  response handling 與 `maybeFailOnCoordinatorFatalError`。
- `ConsumerNetworkThread.runOnce` 的 completion-batch／post-I/O cutoff。

本次重跑既有測試，不新增 production 或新測試：

```sh
./gradlew :clients:test --rerun \
  --tests '*ConsumerBatchedDecisionTest.testCompletionObserverIsNotABatchSnapshot' \
  --tests '*ConsumerBatchedDecisionTest.testHeartbeatInvalidationPrecedesFollowupAdmissionInEitherResponseOrder' \
  --tests '*ConsumerBatchedDecisionTest.testReadyFollowupIsAdmittedWithoutWaitingForUnrelatedHeartbeat' \
  --tests '*ConsumerBatchedDecisionTest.testImmediateFollowupResponseDoesNotRecursivelyDrainAnotherBatch' \
  --tests '*ConsumerBatchedDecisionTest.testLateHeartbeatInvalidationCannotClearRediscoveredOwner' \
  --tests '*ConsumerAdmissionContractTest' \
  --tests '*CoordinatorRequestManagerTest' \
  --offline --max-workers=2 -PmaxParallelForks=1 -PmaxTestRetries=0 -PtestLoggingEvents=failed
```

JDK 17／Gradle 9.7.1：20 個案例、3 個 suites，零 failures／errors／skips，
retries 關閉。真實 manager／loop／delegate 與 MockClient 混合；membership
或 payload 部分為受控 seam。沒有真實 broker、全部 variants、效能、任意 RM
排序或 fatal-error fan-out 的完整證明。本次 focused report 已覆寫原報告位置。

## 下一個有界驗證

正常 coordinator recovery 不需要再引入優先級或先合併 RM。
一個更有辨識力的 interface probe 是 fatal-error delivery：
Commit 的非消耗讀取與 Heartbeat 的 get-and-clear 共用 owner state。
可先讓兩個合法接收者的處理順序互換，檢查待處理 operation 與 application
error 的終態是否保持必要語意。這是已存在的依賴面，不是假造任意旁路；
目前只是 code-inspection 發現，尚未聲稱存在 user-visible bug 或選定修法。

若證明消耗權限造成問題，先比較 owner 保留事實／接收者獨立 delivery 契約，
再考慮擴大 protocol 邊界。timeout／membership／commit 的政策問題仍依
[RM 邊界 dry run](kip-1371-rm-boundary-dry-run.md) 另行處理，不混入此探針。
