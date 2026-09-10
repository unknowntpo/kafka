# A4：app 內部等待期間的 auto-commit

工作分支：`codex/next-poll-condition-trunk`，worktree：`next-poll-condition-trunk`。

## 本輪改變

CommitRequestManager 在 app 已登記內部等待、尚未收到通知且等待尚未到期時，直接由 BG 處理真正的 auto-commit deadline。deadline 透過 `PollResult.timeUntilNextPollMs` 傳回 network loop；這條已接線的 commit 路徑不再以 `maximumTimeToWait` 要求 app 定期醒來。既有 AsyncPollEvent／assignment／rebalance 的提交入口保留。

這不是完整移除 `maximumTimeToWait`。Fetch 的持續需求與完整通知、其他 Future 等待、共用 API 的 Share consumer 相容性仍需完成。Commit RM 目前也仍是 scheduler 的 legacy manager，未宣稱所有 manager 輪詢已移除。

## Offset 安全邊界

不在每次 public poll 複製全部 offsets。只在 commit 到期且沒有上一筆 in-flight commit 時：

1. 讀取有效的 app 等待 epoch（一次實際等待的識別碼）。
2. 呼叫同步的 `SubscriptionState.allConsumed()` 取得獨立 map。
3. 再確認等待仍有效且 epoch 相同，才建立 commit request。

如果 app 在讀取期間醒來、收到工作通知、開始另一次等待或等待到期，丟棄快照。如果 app 在第二次檢查後才醒來，快照已經完成，內容仍是它原先等待時的位置；之後的消耗不會改動這份 map。讀取的是 consumer position，沿用原本 seek／reset 的 offset 語義，並非新增「每個 offset 都必須對應已回傳 record」的限制。

ApplicationPollWait 的鎖只保護等待狀態；不在該鎖內讀 offsets、建立 request 或執行 callback。SubscriptionState 使用自己的同步機制。這避免擴大共享鎖範圍及與 FetchBuffer 的鎖形成反向取得順序。

## 排程與通知

App 真正進入 FetchBuffer 等待時，先發布等待狀態，再呼叫 network wakeup 一次。因為這次等待可能新增更早的 auto-commit deadline，BG 必須重新計算當下的 network poll timeout。已有保留通知、無需實際等待的情況不會觸發此通知；auto-commit 未啟用的 factory 不安裝這個 listener。

- 到期且 app 仍在等待：提交快照，沿用原 interval／retry backoff。
- 上一筆 commit in-flight：不重送、不安排假的短期限；response 由 network loop 處理。
- coordinator 未知：不新增 commit；既有 coordinator 發現流程恢復後，重新檢查。
- 空 offsets：不建立 request，timer 重設 interval。
- close：沿用關閉流程，不新增背景週期提交。

完成後若有 interceptor 或 user callback，既有 callback queue 先入列，再 `FetchBuffer.wakeup()`；app 醒來後透過 `executeCallbacks()` 執行。沒有 interceptor 就不為空工作喚醒 app。通知也會撤銷內部等待的活性保護，避免 callback 卡住時繼續被視為健康。

這只補既有 commit callback queue 的通知，未擴展先前暫停的跨 RM queue 設計。

## 驗證與限制

本輪 311 個測試全部通過（7 個 XML suite，0 failure／error／skip）；Checkstyle main/test、Spotless 與 `git diff --check` 通過。新增 8 個測試。

驗證檔案（大型封存檔保留於原工作區） 包含完整 log、XML、統計與相關測試原始碼。[本輪 production 差異](a4-background-auto-commit.patch) 以此次改動前的髒工作樹為基準，不是相對 trunk 的完整 patch。

新增受控測試涵蓋 deadline 前後、實際 request offset、in-flight 去重與回覆後恢復、app 活躍／已通知／等待過期不讀 offsets、讀取期間換等待 epoch 丟棄快照、coordinator／retry backoff、空 offsets／close、callback 發布先於通知，以及真實 Condition 等待登記先於 BG 通知。

時間由 MockTime 控制，Commit RM、Kafka Timer、SubscriptionState 與 request builder 使用實作；broker response、coordinator、部分 callback invoker 使用 mock。FetchBuffer 的等待登記測試使用真實 thread／Condition。這不等於 broker 端到端測試或 performance benchmark。本輪未使用 homelab，沒有宣稱效能提升。

成本是每次實際停等的一次 network wakeup，加上 commit 到期才做的 O(partitions) offsets 快照。舊 app 短等待仍在時，可能額外放大前者；移除短等待後需重新量測 CPU／wakeup 次數，不能以本輪正確性測試推論效能。
