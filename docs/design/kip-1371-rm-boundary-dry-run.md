# KIP-1371：RM 邊界與共同決策入口 dry run

日期：2026-09-06。基準：Approach 2 `a191d43372`。
本次只比較設計與重跑既有測試，不修改 production、RM 順序、公開 KIP 或 benchmark。
方法為 pattern-language；候選邊界不是已採納的需求。

## 問題與初步結論

程式碼分工、狀態所有權、決策邊界不必一對一。
Heartbeat／Commit 可保留不同實作模組，同時由共同入口界定哪些更新先於新決策。
但把兩個模組放進同一個 class、actor 或 wrapper，不會自行建立這個順序。

本次情境已能區分「整理呼叫位置」與「改變行為」：現有順序會讓 commit
先捕捉 epoch 7；先執行 heartbeat timeout 路徑則使 commit 捕捉 default -1。
後者仍建立 commit，並不自動禁止它。因此不能只調換 RM 順序，就宣稱完成
「timeout 後不再允許新的 group commit」這項尚未採納的政策。

## Intent、Forces 與可否證條件

Intent：新增 group 行為時，作者不必靠掌握 reactor 的 RM list 順序，
才能保持必要的 membership／commit 規則。

| Force | 證據與狀態 | 衝突 |
| --- | --- | --- |
| 小模組可局部理解 | 使用者目的；現有 heartbeat／commit 實作分工 | 同一規則跨模組時，作者仍需理解相依關係 |
| 必要更新先於新 admission | 候選規則；尚未證明 timeout 必須優先 | 可能改變既有 commit context、重試與結果 |
| 已接受的 attempt 保留身分 | 既有 characterization 支持 | 不可因新 owner 狀態任意重建或丟棄 transport intent |
| 控制框架與遷移成本 | 使用者目的 | 擴大共同 owner 可能變成過大的 protocol 模組 |
| 保留 variant 與 lifecycle 行為 | 相容性要求；本次測試只涵蓋指定 regular 路徑 | 不能將 group-managed 規則套用所有 commit |

成功條件不是 class 數量下降，而是相同語意下，新增行為所需的外部順序知識
確實減少，且 retry、結果、資源保留和必要輸入的處理仍完整。
若只多一層入口，內部仍依賴相同隱含順序，不能宣稱改善了契約。

## 實際證據與範圍

以下檔案均指本文件標示 revision 的本地程式：

- `RequestManagers` regular list：coordinator → commit → heartbeat → membership。
- `AbstractHeartbeatRequestManager.poll`：先檢查 coordinator／skip 條件，
  再更新 poll timer；過期時呼叫 `transitionToSendingLeaveGroup(true)`，並建立 leave heartbeat。
- `AbstractMembershipManager.updateMemberEpoch`：變更 epoch，透過 listener
  通知 Commit RM；非正 epoch 通知為 empty。Heartbeat 產生後的 owner 路徑可進入 STALE。
- `CommitRequestManager.poll`／`tryAdmit`：沿用 coordinator、expiration、
  in-flight、backoff 等檢查後保留並建立 attempt；沒有因此新增 group-membership 授權政策。

既有 `ConsumerBatchedDecisionTest` 使用真實 manager／membership／delegate，
但以 MockTime、MockClient 和受控 joined-epoch 設定執行；不是完整 public-consumer
生命週期、真實 join 或 broker 接受性證明。
詳細原始證據見 [owner changes during manager polling](kip-1371-fetch-capability-poc.md#follow-up-owner-changes-during-manager-polling)。

## 相同情境下的兩種邊界

共同起點：coordinator 已知、member epoch 7、非 static membership，poll timer 已過期；
一個 explicit async commit operation 已排隊，但尚未建立 attempt。
「排隊」與「admission」不同；後者表示建立本次嘗試並記錄其 context／保留狀態，
仍不等於已送上網路或 broker 已處理。

### A：現有 RM 邊界（已執行）

1. Commit RM 先 poll，建立 epoch-7 attempt。
2. Heartbeat RM poll 偵測 expiry；membership 改為 leave epoch，通知 Commit RM。
3. Leave heartbeat 建立；此 fixture 的 membership 進入 STALE。
4. 已建立的 commit 維持 epoch 7，不追改內容。
5. MockClient 回覆成功時，該次 commit operation 仍成功。

結果：維持依序接受的 attempt context。它沒有承諾所有 timer-driven transition
優先於本次 pass 的任何 admission。額外 post-I/O pass 也不能回頭改掉 pre-I/O attempt。

### B：共同 group-protocol 入口（候選，未實作）

此入口不必吸收所有 state；可要求 owner 先套用明確列出的 transition，
再呼叫各模組的 admission。內部模組數量可保持不變。

| B 的具體內容 | 會得到什麼 | 證據／限制 |
| --- | --- | --- |
| Wrapper 仍依序呼叫 commit.poll、heartbeat.poll | 與 A 相同；隱含順序仍存在 | 只是結構推論，不是新的 POC 成果 |
| 改呼叫 heartbeat.poll、commit.poll | 在既有比較測試中，commit 使用 -1，仍產生 request | 已有真實 manager 對照；不是「只處理 state transition」 |
| 明確先處理必要 transition，再按選定政策接受新 request | 可把所選優先規則放在一個入口 | 尚未實作；必須先選定待處理 commit 的行為 |

第三種不能直接以 heartbeat.poll 充當純 transition：它同時建立 leave request、
重設 request state，且還有 coordinator unknown 的早退條件。
若抽出 prepare／admit 階段，必須保留或明確重新定義這些前提與效果，
不能假定它們可任意重跑或都應提前。

這是本次比較的可觀察差異：B 若維持語意，暫無新的順序保證；B 若要提供
timeout 優先保證，就必須選擇新政策，而不是只重新命名或合併 manager。

## 必須保留的反例

- **已建立 attempt**：後來 timeout 不能自動推翻已收到的成功，也不能直接刪除
  已保留的 request。若要取消，需另一項完整政策。
- **Retry**：既有測試中 sync commit 遇到 coordinator loading，可在 backoff 後以
  -1 重試；最新 epoch 並不證明仍有操作授權。Async、periodic、before-rebalance
  的結果不同，不能共用一個「timeout 全部失敗」捷徑。
- **Coordinator unknown**：Heartbeat 現行早退發生在 poll timer 更新之前。
  抽出全域 timer phase 可能連這個情境一起改變，不在兩個已知 coordinator 對照內。
- **Static／manual assignment／share／Streams／close**：沒有從本次 regular fixture
  得出相同規則；不能因 coordinator 或 epoch 值相似就視為相同操作。

## Pattern 判讀與方案取捨

候選結構是「將必須共同維持的規則放入一個明確決策邊界」，而不是「合併所有 RM」。
它回應局部推理與一致性之間的衝突；代價是更大的 protocol 責任範圍、
更多 variant 分支，以及必須明確處理已接受操作。

其 resulting context 仍需要 owner 授權、attempt scope、結果觀察契約，
並不消除 application thread 的同步發布問題。
若最終只有少數相依關係，一條窄化 owner 介面可能比擴大共同邊界更合適；
若每次新增行為都重建相同協調，共同入口才有更強的理由。

Actor-per-RM 是另一方案，但 mailbox 不會替我們決定 timeout 與 commit 的優先權。
參見 [Actor／Future 啟發](kip-1371-actor-future-lessons.md)。

## Public 契約初查與剩餘的不確定性

同 revision 的 `KafkaConsumer` failure-detection Javadoc 說明：長時間未 poll
會主動離組，並可能出現 commit failure，目的是限制 group 成員的提交資格。
`commitSync` Javadoc 說明失去 assignment／不再屬於 group 時的不可重試失敗，
也區分 manual assignment；manual assignment 不使用 consumer group management。

這些既有文字提供安全性約束，不能把「讓失效的 group operation 無條件繼續」
當成任意可選行為。但它們未定義本例的精確本地線性化點：timer 到期、
owner 套用離組轉換、request admission、broker 處理之間，哪些先後會使
先前排隊的操作失去資格。故不能由 Javadoc 直接推導 heartbeat 必須排在 commit 前。

已核對的是本地 public API 說明，尚未完成該 consumer protocol 的 broker
epoch 驗證與真實 consumer path 證據。下一步應先補這個窄範圍證據，
而不是要求作者憑偏好決定合法／非法的 commit。

## 決策點與下一個最小實驗

本次不以相同內容的新 wrapper 充當 POC，也不重做已存在的順序測試。
在更動 production 前需要回答：

> 對仍有效、已排隊但尚未建立 attempt 的 group-managed commit，
> 若 poll timeout 在本次決策點已到期，應保留既有接受行為，還是優先套用
> membership transition？若優先套用，該 operation 應失敗、等待何種輸入，或仍可接受？

Public 契約的初查如上；需再核對 protocol 與真實路徑，才由作者選定仍有自由度的部分。
不能僅靠本測試的 epoch -1 或注入成功，判斷正確的 broker 語意。
規則選定後，以相同案例為基準，先寫可區分規則的測試，再做最小 transition／admission
入口實驗；不需要同步引入 actor runtime、全域事件圖或新的 effect queue。

## 本次驗證

在 `a191d43372` 的未變更 Java sources 上，重跑既有 15 個案例，
零 failures／errors／skips，retries 關閉。這是本次 focused run，
不是重跑先前全部 1,535 個案例，也不是 B 的新 production 驗證。

```sh
./gradlew :clients:test --rerun \
  --tests '*ConsumerBatchedDecisionTest.testLeaveTransitionDoesNotRewriteAnAdmittedCommit' \
  --tests '*ConsumerBatchedDecisionTest.testCommitFailureAfterPollTimeout' \
  --tests '*ConsumerBatchedDecisionTest.testCommitAdmissionReservesOnceAndRechecksRetryState' \
  --offline --max-workers=2 -PmaxParallelForks=1 \
  -PmaxTestRetries=0 -PtestLoggingEvents=failed
```

本地使用 JDK 17、cached Gradle 9.7.1，沒有下載或啟動 broker。
XML／HTML 位於 `clients/build/test-results/test`／`clients/build/reports/tests/test`，
本次執行已覆寫該位置的先前測試報告。
