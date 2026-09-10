# Deadline TreeSet 與直接掃描：驗證結論

**保留目前 TreeSet，不採用本次直接掃描版。** 七個 managers 並不代表掃描一定較省 CPU：只有 deadline 全部頻繁到期的局部情境受益；其他三種局部情境較差，真實消費也未展示可重現收益。

這是固定原型改動集合、固定資源與 workload 的採用判斷，並非證明所有線性掃描都較慢。每輪只掃一次、快取最早 deadline 等設計不在本次範圍。

## 改了什麼

基準是先前封存的完整原型改動集合，包含相同 no-spin 修正。Scan 只將 RequestManagerScheduler 的 TreeSet 改成遍歷 entriesByOrder 中有效的 Waiting deadline；保留原有 expire() 呼叫位置、ready／legacy、固定 batch、signal 訂閱與 Waiting 重用。

兩個 client jar 的位元差異只在 RequestManagerScheduler 及其兩個內部類別；其他 class 完全相同。Live branch 未套用此實驗改動。實際普通 consumer group 有七個 managers，其中四個公開 poll 回傳 condition、三個仍為 legacy；condition 不一定帶 deadline，因此實際 timer 數量並非恆定七個。

## 七個 managers 的局部結果

每版／情境三個獨立 JVM forks。以下是 Scan 相對 Tree 的每次 scheduler 操作 CPU 成本變化範圍：

| 情境 | CPU 成本變化 |
|---|---:|
| Deadline 尚未到期，反覆檢查 | +360%～+385% |
| 七個 deadline 每輪全部到期 | -7%～-27% |
| 每輪一個 signal 提前取消 deadline | +13%～+16% |
| Deadline、signal 與 legacy 混合 | +51%～+59% |

反覆檢查的絕對成本約 Tree 12 ns/pass、Scan 56 ns/pass；不能把倍率當成真實 consumer 閒置 CPU/s 的倍率。混合情境配置量下降約 134→97 bytes/pass，但 CPU 仍上升，說明配置較少不保證較快。[完整局部方法、變異與結果](deadline-scan-component.md)。

## 真實 consumer：五輪交錯比較

每輪使用兩個新 JVM，順序 T/S、S/T、T/S、S/T、T/S。每 fork 暖機 60 秒，再正式量測 50 秒；試跑未混入正式樣本。

| 輪次 | Scan 吞吐變化 | Scan 每筆 CPU 成本變化 |
|---|---:|---:|
| 1 | -8.07% | +6.64% |
| 2 | +5.88% | -3.99% |
| 3 | +0.28% | +1.79% |
| 4 | -3.21% | +7.39% |
| 5 | -6.44% | +5.06% |

五個 fork 平均值的中位數：Tree 約 152.4 萬 records/s、309.4 CPU ms/百萬筆；Scan 約 145.9 萬 records/s、323.9 CPU ms/百萬筆。這些中位數不是上表配對百分比的計算基準。

配對幾何平均為吞吐 -2.44%、CPU/record +3.29%。探索性的配對 log-t 95% 區間分別為 -9.06%～+4.66%、-2.42%～+9.33%；假設五輪 log ratio 為獨立常態樣本，樣本很少，兩個區間均包含零。不能宣稱已證明固定幅度的回退或統計等效。

暖機加長後仍有波動：個別 fork 的後兩次相對前兩次吞吐變化約 -15.3%～+10.7%，CPU/record 約 -5.4%～+18.1%。未拆分 JIT、GC、共享主機與排程因素，沒有事後只挑穩定或有利的 iteration。結果足以支持「沒有證據值得換成這版掃描」，不支持精確的普遍回歸幅度。

## 範圍與驗證

- 真實單 member consumer group、單 partition、五百萬筆既有 backlog、每筆 128 bytes，消費至尾端後 seek(0) 繼續。每筆驗證 offset／內容序號，包含驗證與 seek 成本。
- JMH 1.37 控制暖機、量測與 forks。每 invocation 1000 筆，JMH 操作數與已驗證 record 數核對一致。GC profiler 為 bytes/record；process CPU 包含 application、network、GC、JIT 與 JMH。
- Intel N150、Java 17.0.20；client CPU 2、0.5 CPU quota、512 MiB、zero swap，fork 內核對限額、祖先 cgroup 與 OOM。Broker CPU 0/1、producer CPU 3；producer 在量測前退出。CPU 綁定不是獨占保留。
- 兩版各 342 個相關測試通過，含四個新增 deadline 案例；四種負載各 100,000 次操作的順序／等待 checksum 相同。兩個有效 benchmark 階段均先通過真實 consume、idle、unavailable 檢查。
- Checkstyle／Spotless 通過；JMH annotation processor 編譯與 discovery 通過。整個 JMH module 編譯曾因既有 CPU profiler 的 raw Result 警告搭配 Werror 失敗，因此本輪使用先前的 scoped harness 編譯方式，不能稱完整 module build 通過。
- SpotBugs 在兩版均報相同四項警告，未新增差異；本次沒有修正這些警告，也沒有把靜態檢查標示為全通過。
- 正式容量比較僅 consume；不宣稱真實 idle CPU、尾延遲、功耗或多 partition 的結果。

初次 discovery 失敗未產生量測樣本，已排除。所有本輪 remote broker、資料、Java processes 與 run directory 已清理；保留原始日誌、JSON、來源、patch 與 frozen jars。

## 證據

- [實作與新增測試 patch](deadline-scan/scheduler-with-tests.patch)、[client jar 差異與 SHA](deadline-scan/inputs.json)
- [局部與暖機試跑 JSON](deadline-scan-pilot/summary.json)
- [正式全部 iteration 與配對結果](deadline-scan-formal/summary.json)、[獨立檢查、區間與波動](deadline-scan-formal/validation.json)
- [正式執行設定與命令](deadline-scan-formal/results/manifest.json)、[實驗流程](deadline-scan/protocol.md)
- [Tree 測試](deadline-scan/tree-tests.json)、[Scan 測試](deadline-scan/scan-tests.json)、[既有 SpotBugs 警告對照](deadline-scan/spotbugs-comparison.json)
- 清理：[discovery failure](deadline-scan-pilot-discovery-failure/cleanup.log)、[有效試跑](deadline-scan-pilot/cleanup.log)、[正式測量](deadline-scan-formal/cleanup.log)
