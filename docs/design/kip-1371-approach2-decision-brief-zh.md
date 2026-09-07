# Approach 2：契約與決策摘要

2026-09-06；程式基準 `df756cee981273c758e45ea58efffb120c73f9c1`。
狀態：供設計討論，非正式 KIP 更新或 production-ready 宣告。

## 目標與判斷標準

讓 Request Manager 保留 domain policy，由共享迴圈與狹窄介面明確承接必要的跨元件規則；不預設所有 callback 都必須集中執行。

優先順序是 **安全性 → 有限時間內的進展 → 相容性、重複消費量與成本**。安全性前提是 application 處理完每次 `poll()` 返回的資料，再呼叫下一次 `poll()` 或 close。內部 position 前進、records 返回、application 處理完成、broker commit 成功是不同時點。

## 已驗證的保護與已知缺口

| 契約 | 執行者與目前證據 | 限制 |
| --- | --- | --- |
| 本 POC 的 per-poll 初次捕捉完成後，才開始本次 collection | application 等待既有 per-poll checkpoint；事件處理器先捕捉再完成 checkpoint。延遲捕捉、wakeup、error、timeout、interrupt 有元件測試 | 等的是捕捉，不是 broker ack；可能延後 buffered collection，效能未驗證 |
| commit 結果不等於舊 assignment 仍可套用 | Commit RM 管理結果；Membership RM 以既有離組／rejoin guard 判斷後續 reconciliation。成功／失敗四種組合有測試 | continuation 被擋住，不代表網路 commit 被取消 |
| retry 也需要安全捕捉，這是尚未預設修復的缺口 | 真實 membership、`poll()`、collector 與 `runOnce()` 配合模擬 transport，重現 retry 在 collection 中途讀取 position 10；對照組讀到 0 | 尚無真實 broker 持久化與 crash/restart 證據 |

上述保護不構成全域「所有 manager 都完成後才允許任何 effect」保證。一般路徑仍使用 `ConsumerNetworkThread`，callback 預設 inline；owner 的 domain 檢查不能由統一 phase 取代。

## Retry snapshot：尚未定案的契約

候選要求：retry 不得採用仍在 collection 中、尚未符合 auto-commit 使用契約的進度；仍須有明確的重試期限及最終結果。

| 方案 | 安全性／進展 | 代價與狀態 |
| --- | --- | --- |
| 既有 callback 重讀當下 position | 可持續 retry，但已重現提前捕捉 | 目前預設；不能以 offset 最新作為安全證明 |
| 下一個安全 `poll` 點再捕捉 | 可避免在 collection 中途讀取 | application 遲遲不再 poll 時，可能延後 rebalance 或耗盡期限；未實作 |
| 保留本次操作首次安全 snapshot | 實驗中 retry 仍送出 0，不需額外 application poll；backoff、錯誤與期限有測試 | **已實作但預設關閉**；可能增加重複消費，不再每次重讀最新 position |
| 另行維護最新安全 offset view | 可望兼顧更新與背景進展 | 必須定義發布、seek、assignment 與生命週期；未實作，不能只取「最近一次返回 records」的位置 |

目前的 `enableRetainedRebalanceRetrySnapshot()` 只供測試開啟，保留的是**首次** snapshot，不是持續更新的安全 view。建議保留此最小候選與預設行為做後續對照，但尚未選定正式 retry 語意。

## 不新增什麼，以及還缺什麼

暫不新增全域 queue、dependency graph 或通用 snapshot 失效機制。訂閱模式不能直接切成非空手動 assignment；`seek` 也不承諾取消已送出的 commit。不能只因舊 snapshot 存在，就推導出必須全面取消舊操作。

仍須確認：實際 ownership 改變後舊 commit 的 scope、合法 seek 後 retry 的相容性，以及真實 broker 下的 committed offsets／重啟重讀結果。Member identity 更新本身不是 scope 正確性的證明。

## 下一步與驗收界線

1. 先確認是否接受「同一次 rebalance commit 保留首次安全 offsets」的新鮮度／重複消費取捨；否則比較下一安全點或安全 view，不默默切換方案。
2. 以同資料、同交錯比較預設與候選：阻擋 collection 返回、觸發 retry、讀回 broker committed offsets，再做 crash/restart；另測正常完成與到期。控制時序只屬測試設施。
3. 回到原 KIP 的完整 issue inventory，依實際 baseline、路徑與 variants 逐項驗收；不再僅靠增加本議題的單元案例推進結論。

測試通過只支持其命名範圍：snapshot 實驗的 800 個測試與後續 315 個 lifecycle 測試來自不同 revision／選集，不相加，也不代表所有原 KIP issues 已解決。

證據：[snapshot 與 lifecycle audit](kip-1371-auto-commit-snapshot-audit.md)；[完整 issue inventory](kip-1371-issue-coverage.md)；[較早的整體契約評估](kip-1371-approach2-contract-summary.md)。
