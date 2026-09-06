# KIP-1371：Actor、Future 與狀態所有權的啟發

日期：2026-09-06。程式基準：Approach 2，`a191d43372`。
本文是依 pattern-language 方法整理的設計筆記，不是已採納的 KIP 規範。
Java／Akka 行為以官方文件為依據；Kafka 對應是設計推論，不能視為新 POC 證據。

## 核心結論

Future 不是「沒有 mailbox 的 actor」。兩者處理不同層次：

- Future 表達某次操作的結果，以及依賴該結果的後續計算。
- Actor 封裝行為與狀態，透過訊息入口處理一連串工作。
- 一個 actor 可以同時管理多個尚未完成的操作；每個操作可以有自己的 future。

因此「等待什麼結果」與「誰在什麼邊界修改狀態」應分開設計。
加入 queue 本身不會替 future 補上 domain owner、合法訊息協定或終態規則。

## Context、Intent 與 Forces

目的是讓新增 RM 行為的作者主要理解 domain policy，不必重新推理每條
future continuation 的執行位置、跨 owner 授權與 application 通知順序。
目前 POC 已有單一 network loop、版本檢查、窄化的 Fetch 能力與操作結果契約；
尚未建立全域 effect barrier，也未全面禁止 callback 直接呼叫其他模組。

| Force | 證據／狀態 | 衝突與限制 |
| --- | --- | --- |
| 作者能局部推理 | 使用者明確目的；既有 extension probe 找到漏接例外 | 越多特殊 callback 路徑，越需要跨層知識 |
| 保護狀態修改權限 | Fetch capability 與 owner-version 的局部證據 | 限制存取不代表 domain 判斷一定正確 |
| 必要更新先於依賴它的新決策 | completion-batch POC 的特定路徑證據 | 不等於所有 timer transition 都已處理，亦不應等待所有 futures |
| 保留操作結果與既有 lifecycle | operation-result 測試與 commit scope characterization | 停止等待不等於取消遠端操作 |
| 減少框架與執行成本 | 使用者對完整模型的疑慮；效能未知 | mailbox／協調訊息增加保留、排程及關閉義務 |

問題不是「如何把 RM 改成 actor」，而是如何提供可重用的修改與觀察邊界，
同時避免把必要的同步決策拆成更多非同步協定。

## Future 與 Actor 的差別

此處 Future 以 Java `CompletableFuture` 為例，Actor 以 Akka Typed 的
逐訊息處理模型為例；不同語言、actor runtime 的重入與排序保證不能一概而論。

| 面向 | Future／CompletionStage | Actor |
| --- | --- | --- |
| 核心身分 | 某次結果／計算階段 | 可接收訊息的行為與狀態實體 |
| 生命週期 | 通常由 pending 進入成功、失敗或取消；chain 是其他 stages | 可依序處理多次訊息，直到停止；不一定長壽命 |
| 狀態隔離 | 保護 future 自身的完成競爭，不保護 callback 捕捉的 RM 狀態 | 在狀態未外洩、存取遵守 actor 邊界的前提下提供本地隔離 |
| 執行安排 | 取決於 continuation 與 executor；不自動建立 RM mailbox | runtime 將訊息交給 actor 的處理入口 |
| 多項非同步工作 | 相依 stages 可串接；獨立 futures 不形成 owner 的統一處理順序 | 完成結果可轉成訊息，在 owner 邊界處理；操作仍可能交錯 |
| 完成語意 | 表示由 API 定義的某個結果，不自動代表整個 consumer 已穩定 | 訊息送出不等於已處理；reply 承諾何種里程碑仍由協定定義 |

`CompletableFuture` 的一般完成競爭只有一方成功；非 Async continuation
可由完成 future 的執行緒執行，Async 版本依 executor 安排。
不應將一般單次結果契約延伸成「任意 mutable API 都被禁止」；例如它另有
`obtrudeValue`／`obtrudeException` 強制覆寫能力，POC 不將它們視為合法結果路徑。
[Java 官方契約](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/CompletableFuture.html)

Akka 的 actor 序列化的是訊息處理，不是跨 I/O 的整個業務操作。
Actor 發出 request 後可以處理其他訊息，原 response 後來才到；中間狀態可能已變。
Actor 也不等於一條專屬 thread。
[Actor 執行模型](https://doc.akka.io/libraries/akka-core/current/typed/guide/actors-intro.html)

## 兩者可以合作，而不是互相取代

概念流程：

```text
owner 收到 Start(operationId, scope)
  → 記錄操作 context，啟動非同步工作並取得 future
  → 返回，允許處理其他輸入

future 完成
  → 轉成 Finished(operationId, result) 訊息

owner 處理 Finished
  → 驗證操作是否仍有效
  → 更新自己的狀態
  → 依契約回覆結果或通知接收者
```

Akka `pipeToSelf` 是這種接合方式：把外部 CompletionStage 結果轉成訊息，
後續狀態操作在 owner 收到訊息時處理。`ask` 則可讓呼叫端以 future 等待 actor
回覆。兩者都需要明確的錯誤與生命週期規則，不是自動 exactly-once。
[Interaction patterns](https://doc.akka.io/libraries/akka-core/current/typed/interaction-patterns.html)

把所有 callback 指定到同一個 executor，可能提供序列執行，但仍不自動提供
actor 的封裝、受限引用、operation context 或跨 owner 協定。相反地，使用
actor framework 卻洩漏 mutable state，也會破壞原本期待的隔離。

## Kafka 的實際 callback 與 phase

在此 revision，正常 FindCoordinator response 路徑是：

```text
ConsumerNetworkThread.runOnce
  → NetworkClientDelegate.poll
  → NetworkClient.poll / completeResponses
  → FutureCompletionHandler.onComplete / future.complete
  → CoordinatorRequestManager 的 whenComplete / onResponse
  → 更新 coordinator
  ← delegate.poll 返回
  → 符合條件時執行一次 post-I/O manager pass
```

callback 已在 network thread 的 I/O 呼叫內執行。只為了回到同一條 thread
再加 mailbox，未必有收益；若需要延後到另一個決策階段，才需說明額外邊界的理由。
其他 future 不能一概放入 I/O phase：AsyncPoll 的 position future 若已完成，
註冊 continuation 時可在 input phase 執行；若稍後完成，則由完成路徑觸發。
例外清理／逾時路徑也不能直接等同正常 response 路徑。

「callback 不應任意改狀態」的精確含義是：必須有相應所有權與執行環境，
不是禁止 owner callback 更新自己的狀態；更不是宣稱目前所有 callbacks
都已具備這些限制。

## Pattern 組合及其 resulting context

| 可重用結構 | 解決的問題與 solution form | 代價／產生的下一個問題 |
| --- | --- | --- |
| 受限狀態入口 | 跨模組不傳遞任意修改能力，以窄介面請求 owner 處理 | 要定義合法操作；helper 不能重新暴露旁路 |
| 非同步結果回交 owner | 執行環境不確定時，結果先回到受控處理入口再修改 state | 增加交接；須辨識晚到、取消及重複結果 |
| 操作身分與版本檢查 | 回交結果帶 scope／attempt context，由 owner 驗證 | 版本新不等於仍有操作授權；必須保留 domain policy |
| 必要範圍的決策邊界 | 明確列出哪些 owner 更新先於哪些新 admission | 必須決定 batch cutoff，避免等待所有工作或無界排空 |
| 結果觀察與完成權限分離 | 呼叫端可終止自己的等待，不得改掉共用 owner result | 保留 observer 的成本、底層工作何時終止仍需定義 |

這是生成關係：隔離使跨 owner 溝通顯性化；非同步溝通產生延遲結果；
版本檢查處理部分延遲，但不建立共同決策順序；排序也不取代結果發布契約。
不能只採用第一個結構，就宣稱剩下的問題都由 actor 解決。

例如 Heartbeat 用 C7 發 request，Coordinator 已改成 C9，舊 response 才回交：
即使訊息逐一處理，Coordinator 仍須拒絕以 C7 的 observation 清除 C9。
多個 actor 的 mailbox 亦不自動形成共同 phase；Akka 的基本訊息排序保證
限定 sender／receiver 配對，不能當成全域 barrier。
[訊息排序](https://doc.akka.io/libraries/akka-core/current/general/message-delivery-reliability.html)

## Solution space 與決定

保留 Approach 2＋窄介面，是成本較小的比較基準；每 RM 一個 actor 可強化
本地入口，卻增加跨 RM 協定；一個 consumer actor 保留共同執行邊界，卻不
自動保護內部模組；直接借用 actor 的受限入口則不必先引入外部 runtime。
上述方案均未因本文而採納；沒有遠端部署、actor persistence 或 restart 需求證據。

優先檢查既有 RM 是否切開了必須共同決策的規則，再決定哪些地方需要訊息化。
詳見 [RM 邊界 dry run](kip-1371-rm-boundary-dry-run.md)。

`ConsumerReactor` 是使用者認可的目標 class 命名；目前程式仍是
`ConsumerNetworkThread`。整合時應承接既有 rename PR，不將 class rename
等同 runtime thread name 變更，也不把名稱當成新的同步保證。

相關本地證據：

- [操作結果契約重用](kip-1371-operation-result-contract-reuse.md)
- [Future continuation 漏接例外](kip-1371-rm-extension-contract-probe.md)
- [Fetch 能力與 observer 隔離](kip-1371-fetch-capability-poc.md)
- [React／Vue 發布啟發](kip-1371-react-vue-publication-lessons.md)

本次只記錄設計與來源，不新增 production adapter、mailbox 或 actor dependency。
