# 七個 managers：TreeSet 與直接掃描的局部 JMH 結果

**直接掃描不是普遍較便宜。** 四種局部負載中，只有七個 deadline 每輪全部到期時較省 CPU；其餘三種負載均較耗 CPU。這是目前實作的對照，不代表其他掃描設計。

CPU 為整個測試 JVM 每次 scheduler pass 的奈秒成本，由同一 iteration 的 CPU rate 與 JMH pass rate 推導。表格是三個獨立 fork 平均值的中位數；百分比依同輪配對計算。

| 負載 | Tree CPU ns/pass | Scan CPU ns/pass | Scan 相對 Tree 的 CPU 變化（三輪） |
|---|---:|---:|---|
| 未到期，反覆檢查 | 12.1 | 56.4 | +359.6% / +378.6% / +384.7% |
| 七個 deadline 每輪到期 | 523.2 | 386.8 | -6.8% / -26.5% / -26.1% |
| 每輪一個 signal 提前取消 | 121.0 | 137.3 | +15.2% / +13.4% / +16.2% |
| 不同 deadline、signal、legacy 混合 | 119.2 | 189.3 | +58.8% / +59.3% / +51.2% |

## 為什麼會出現取捨

直接掃描移除了 TreeSet 的插入、移除與節點配置；但每次檢查會遍歷 entriesByOrder，而且本輪保留原有 expire() 呼叫位置。全體頻繁到期時，省掉反覆修改樹的成本可能值得；只有少量 deadline 改變時，掃描工作反而增加。這是結合程式路徑與量測的解釋，尚未用 CPU profile 分解各段成本。

IDLE 沒有推進邏輯時間，也沒有真正等待 socket；它測的是重複查找成本，**不能把 CPU ns/pass 比例當成真實閒置 consumer 的 CPU/s 比例**。DUE 是壓力負載，不是七個實際 Kafka managers 每毫秒都執行的宣稱。

一般 consumer group 的七個 managers，目前 Coordinator、Heartbeat、Offsets、Fetch 的公開 poll 回傳 condition；Commit、Membership、TopicMetadata 仍回傳 legacy 結果。Condition 還可能只有 signal，所以實際 timer 數量會隨狀態變動，不能把七個 managers 當成恆定七個 timer。局部 MIXED 只有一個 legacy manager，是結構性測試；真實七個 managers 的組合由 broker benchmark 驗證。

## 配置與變異

- IDLE 兩版均接近零 bytes/pass。
- DUE：Tree 約 640 bytes/pass；Scan 三個 forks 約 360、584、584。CPU 改善方向一致，但配置平台與改善幅度有變異，不宣稱固定節省 280 bytes/pass。
- SIGNAL：Tree 約 152；Scan 約 112、112、192 bytes/pass，不能宣稱每輪配置都改善。
- MIXED：Tree 約 134、Scan 約 97 bytes/pass；配置較少，但 CPU 明顯更高。

## 驗證與界線

兩版各通過相同 342 個相關測試，包括四個新增 deadline 邊界案例；四種負載各 100,000 次操作的順序與等待 checksum 一致，主機上也重新核對。Checkstyle／Spotless 通過。SpotBugs 兩版都報相同四項警告，因此不能稱全部靜態檢查通過。

每版／情境三個獨立 JVM forks，交錯 T/S、S/T、T/S；每 fork 三次 2 秒暖機、三次 3 秒正式測量。JMH 1.37、七個 RequestManager 介面的輕量實作、真實 scheduler，沒有 mocks／broker。固定 0.5 CPU、512 MiB、zero swap、CPU 2；CPU 包含 GC/JIT 成本。操作單位是 pass，不是 records。

Live branch 的 deadline 實作未變；對照保留在隔離工作目錄。真實 consumer 的正式吞吐比較另行執行，不能從本表推論其收益。

證據：[原始 JSON 與逐輪結果](deadline-scan-pilot/summary.json)、[改動與測試 patch](deadline-scan/scheduler-with-tests.patch)、[JMH workload](deadline-scan/DeadlineIndexBenchmark.java)、[jar 差異與 SHA](deadline-scan/inputs.json)、[SpotBugs 差異](deadline-scan/spotbugs-comparison.json)、[實驗流程](deadline-scan/protocol.md)。

初次 JMH discovery 失敗發生於量測前，已排除且清理。有效試跑的 broker、資料與服務亦已清理，見 [cleanup](deadline-scan-pilot/cleanup.log)。
