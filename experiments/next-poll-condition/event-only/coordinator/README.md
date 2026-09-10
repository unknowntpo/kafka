# Coordinator 事件派送驗證

結論：Request Manager 可以保留 RPC 邏輯歸屬，透過事件決定何時再次執行。本次驗證了 coordinator 回覆直接通知自己及依賴者，不必逐一詢問其他 managers。

## 實驗變更

在隔離編譯的 CoordinatorRequestManager 副本增加 package-private `stateChanged()`，回傳既有 inputChanged signal 的 generation 快照。實際 production Java 檔案沒有改動。使用既有嚴格事件 scheduler 與 subscription reuse 候選；不另建排程器。

測試依賴者先取得 condition，再讀取 coordinator 狀態；request 的實際 completion handler 更新 coordinator 後 publish，scheduler 將兩個訂閱者標為 ready，下輪才執行。外部 invalidation 透過 application queue + network wakeup 送入。

## 執行結果

| Manager 數量 | 初始化後等待輪數 | 等待期間額外 manager.poll | 一次成功回覆後執行的 managers | 不相關 managers 額外執行 |
|---|---:|---:|---:|---:|
| 8 | 1,000 | 0 | 2 | 0 |
| 32 | 1,000 | 0 | 2 | 0 |
| 128 | 1,000 | 0 | 2 | 0 |

另外通過：
- invalidation 同時通知 coordinator 與依賴者，產生一次重新查找 request。
- RPC timeout 後，在 backoff 到期前不再次 poll coordinator，到期後發出重試。
- 通知先於 scheduler 訂閱仍不遺失；下一輪重新檢查狀態。
- callback 不遞迴執行 manager。
- 不相關 manager 的 maximumTimeToWait 設為直接失敗；本次事件 loop 沒有呼叫它。

原始 DISPATCH 列依序為：manager 數、等待輪數、等待期間額外 polls、成功回覆後相關 polls、不相關 polls、invalidation 後累計 requests。所有數字皆先經斷言核對。

## 限制與下一步

這是呼叫次數及正確性驗證，不是 CPU benchmark。使用實際 coordinator 邏輯、request completion handler 與改寫的 ConsumerNetworkThread loop，但 transport 不阻塞、不連 broker；依賴者為合成 manager，尚未替換 heartbeat/commit。沒有證明 OS 層喚醒延遲或整體 consumer 正確性，也沒有執行完整 Gradle checks。

沒有逐一呼叫 manager 不等於排程成本與數量完全無關：現有 BitSet 和 timer 容器仍有處理成本，需另外量測。

下一步選一個真實依賴者（建議 heartbeat），盤點它在 coordinator 未知時的所有等待來源，再把 coordinator 通知接入。必須保留 heartbeat/retry timer、應用程式 poll 與 max.poll.interval 的存活語意。此 stateChanged 是候選契約，並未證明所有狀態變更都已涵蓋：例如 fatal error 被清除的通知需求、backoff 期間 close 的處理需另行驗證。

接線完成後，以相同輸入、相同 request 輸出比較掃描與事件版本，分別量稀疏、全忙、timer 負載；報 CPU、配置量與事件到派送延遲，不能只用少呼叫幾次就推論效能提升。

## 重跑

在保有既有 isolated harness 編譯產物的 next-poll-condition-trunk repository 內，執行：

```sh
/opt/homebrew/bin/python3 experiments/next-poll-condition/event-only/coordinator/validate.py
```

此腳本讀取既有 reuse-classpath.json，單獨編譯 coordinator 副本與 probe，以單 worker、128 MB JVM 執行。證據包包含 probe、runner、候選契約 patch 與本次執行 log；不是完整自帶依賴的建置包。
