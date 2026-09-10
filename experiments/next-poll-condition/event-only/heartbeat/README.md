# Heartbeat 事件排程接線驗證

## 結論

在隔離編譯副本中，已把實際 ConsumerHeartbeatRequestManager／AbstractHeartbeatRequestManager 接入既有 event-only scheduler。Coordinator discovery 會喚起 heartbeat，無須每輪呼叫 heartbeat.poll。Production source 未改動，沒有提交、重寫 Git 歷史或啟動 broker。

本次是有邊界的正確性實驗，尚不是完整 consumer migration 或效能證明。

## 通知與等待契約

| 來源 | 接線 | 驗證 |
|---|---|---|
| coordinator 回覆／狀態變更 | 沿用前輪 stateChanged generation condition | 找到 coordinator 後執行真實 heartbeat request 建構 |
| heartbeat RPC 完成／失敗 | 在原 callback 處理狀態之後，以 finally 發布 heartbeatInput | 成功／TimeoutException 回覆後重新計算 interval 或 retry |
| membership state／epoch | ConsumerHeartbeatRequestManager 建構時註冊 MemberStateListener | 測試替身捕捉 listener，驅動 skip → active |
| application resetPollTimer | 原 resetPollTimer 更新後發布 heartbeatInput | 舊 deadline 取消，新 max.poll.interval deadline 生效 |
| heartbeat interval／retry／max poll | 登記 scheduler deadline | 不靠逐一讀取 maximumTimeToWait |
| close | 保留 pollOnClose 建構 leave request，scheduler.close 取消等待 | 直接 pollOnClose 與 scheduler close 後通知測試 |

每次執行前取得 coordinator／heartbeat input 的 generation，再讀取／推進狀態，以涵蓋執行期間的通知。當 heartbeat request 還在飛行中，不把 heartbeat interval 當成喚醒原因；仍保留必要的 max.poll.interval deadline。正在離開 group 時不重複登記已失效的 poll timeout。

候選保留原 poll 業務邏輯於 pollState，外層轉換為明確的 input 或 deadline condition。membership state 變更由既有 listener 傳入；沒有增加新 thread 或 executor。原 maximumTimeToWait 方法仍保留在副本裡，實验的嚴格 scheduler 不呼叫它；不能據此刪除 production 的 application 等待契約。

## 本次通過的測試

1. Coordinator 未知時，1,000 次 scheduler drain 沒有再次執行 heartbeat；coordinator completion 後產生實際 ConsumerGroupHeartbeat request。
2. Request 尚未完成，即使超過 heartbeat interval，也不再次 poll heartbeat；完成後遵守既有最短 backoff。
3. 成功回覆後，interval 前不執行，到期產生下一個 request；失敗後 retry deadline 前不執行，到期重送。
4. application poll reset 取消舊 max.poll.interval deadline；新期限到達時，即使 request 還在飛行中仍產生 leave heartbeat。
5. membership listener 通知能讓 skipped heartbeat 再次執行。
6. 直接 pollOnClose 保留 leave request；scheduler close 後再收到 listener 通知，不恢復 manager 執行。

Runner 輸出四組 PASS。第一次測試假設「逾期 heartbeat 收到成功回覆後立即重送」失敗；查核 RequestState.onSuccessfulAttempt 確認它仍重設最短 backoff，因此修正測試以保留現有語意，沒有為了通過測試移除 backoff。

## 測試邊界

- 真實 source：coordinator（前輪 notification 副本）、ConsumerHeartbeatRequestManager、AbstractHeartbeatRequestManager、HeartbeatRequestState、MemberStateListener；使用既有 RequestState 和 scheduler 編譯產物。
- 測試替身：ConsumerMembershipManager、heartbeat payload state、BackgroundEventHandler 與部分 config。Request builder／response callback／request timing 是真實程式，payload 內容來自測試設定。
- 使用 scheduler 直接 drain 及手動 request completion，沒有真實網路、broker、OS wakeup 或 ConsumerNetworkThread application event routing。
- Membership listener 是測試直接觸發；未跑真實 membership/rebalance 狀態機。pollOnClose 的單元驗證不等於整個 consumer close 已驗證。
- Application resetPollTimer 是直接呼叫；AsyncPollEvent 的完整產生／派送／阻塞喚醒尚未驗證，因此不能宣稱可移除 production maximumTimeToWait。
- 尚未覆蓋所有 fatal/fenced/reconciliation/REMAIN_IN_GROUP 情境；shared abstract 類別也影響 share heartbeat，不能把本候選直接投入 production。
- 沒有 CPU benchmark、完整 Gradle 或 Checkstyle 驗證；沒有新的效能數字。

## 下一個實作關卡

先把真實 ConsumerMembershipManager 的狀態變化與 AsyncPollEvent 路徑納入整合測試，覆蓋 join/rebalance/fenced/unsubscribe/close 與 coordinator fatal errors，驗證沒有漏通知或重複 leave。完成後再用相同輸入與 request 輸出對照掃描 baseline，量測稀疏、全忙與 timer 負載的 CPU、allocation、事件到派送延遲。

## 重跑與證據

在 next-poll-condition-trunk 內、保留既有 isolated harness 產物時：

```sh
/opt/homebrew/bin/python3 experiments/next-poll-condition/event-only/coordinator/validate.py
/opt/homebrew/bin/python3 experiments/next-poll-condition/event-only/heartbeat/validate.py
```

Runner 產生並編譯隔離副本，不覆寫 production source。證據包包含 runner、probe、候選 patch、最終 log 與來源摘要；依賴既有 classpath／cached jars，不是獨立可攜的建置包。
