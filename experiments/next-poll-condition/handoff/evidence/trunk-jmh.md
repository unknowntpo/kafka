# JMH：trunk、no-spin 與完整 NextPollCondition 原型

本輪不支持為了效能採用完整 NextPollCondition 原型。相對同輪 trunk，完整原型的三輪正常吞吐變化為 **-16.7% / -2.7% / -14.8%**，每百萬筆 CPU 成本變化為 **+20.6% / +5.4% / +22.5%**。no-spin 的主要收益在 coordinator 不可用的空轉場景，三輪 CPU 變化為 **-93.0% / -93.5% / -93.4%**。

no-spin 的正常吞吐仍有不利樣本；本輪不能視為一般效能無回退的驗收。

這是固定版本、單一硬體與 workload 的診斷結果，不是所有部署的效能保證。部分 iteration 仍有變化，不能宣稱已確立完全穩態、統計等效或普遍回歸幅度。

## 三組使用相同基準

固定 trunk commit：`820533b870106cc0e0ac60e2076b8644d68bd85f`。三組都完整編譯自相同 source 基準，client jars 在量測前封存並驗證 SHA。

- **trunk**：未修改的 archived trunk source。
- **nospin**：只套用 AbstractHeartbeatRequestManager 的零等待修正；jar 的位元內容差異僅該 class 與其內部 class。
- **condition**：目前完整原型，加上與 nospin 相同的等待政策，包含 Waiting 重用、通知與 application-wait 改動。這組結果不能單獨歸因於 scheduler。

既有工作目錄未被本輪改寫。[三組 artifact 與 class 差異](trunk-threeway/inputs.json)、[來源及測試紀錄](trunk-threeway/build-provenance.json)。

## JMH 結果

表格為 **三個獨立 fork 平均值的中位數**。每 fork 有三次正式 iteration；配對百分比依同輪計算，所以不一定等於表格中位數直接相除。

CPU ms/s 表示每秒消耗的 CPU 毫秒；1000 ms/s 等於持續使用一顆核心。BUSY 的 CPU/Mrecord 為每百萬筆的 JVM CPU 毫秒。消費情境的 JMH operation 已正規化為一筆 record。

| 情境 | 版本 | Process CPU ms/s | Records/s | CPU ms/Mrecord | 配置 bytes/record |
|---|---|---:|---:|---:|---:|
| unavailable | trunk | 498.87 | — | — | — |
| unavailable | nospin | 33.23 | — | — | — |
| unavailable | condition | 36.24 | — | — | — |
| idle | trunk | 31.93 | — | — | — |
| idle | nospin | 32.27 | — | — | — |
| idle | condition | 36.94 | — | — | — |
| consume | trunk | 484.90 | 1,373,505 | 359.4 | 554.8 |
| consume | nospin | 488.47 | 1,258,804 | 416.1 | 556.5 |
| consume | condition | 488.51 | 1,168,587 | 438.1 | 567.8 |

`consume` 是持續重讀既有 backlog；`idle` 是已入組但無新資料；`unavailable` 是 coordinator 不可用。後兩者的空 poll 次數不是資料吞吐量，因此不放入 records/s 欄位。

### 所有配對變化

| 情境 | 比較 | 指標 | Round 1 / 2 / 3 |
|---|---|---|---|
| unavailable | nospin / trunk | process_cpu_ms_per_s | -93.0% / -93.5% / -93.4% |
| unavailable | condition / trunk | process_cpu_ms_per_s | -92.8% / -92.5% / -92.7% |
| unavailable | condition / nospin | process_cpu_ms_per_s | +2.9% / +14.3% / +9.0% |
| idle | nospin / trunk | process_cpu_ms_per_s | -4.0% / -1.0% / +2.1% |
| idle | condition / trunk | process_cpu_ms_per_s | +10.7% / +13.6% / +16.8% |
| idle | condition / nospin | process_cpu_ms_per_s | +15.3% / +14.8% / +14.5% |
| consume | nospin / trunk | records_per_s | -10.3% / +19.8% / -17.7% |
| consume | nospin / trunk | process_cpu_ms_per_million_records | +15.8% / -14.3% / +19.0% |
| consume | nospin / trunk | allocated_bytes_per_record | +0.4% / -9.2% / +10.0% |
| consume | condition / trunk | records_per_s | -16.7% / -2.7% / -14.8% |
| consume | condition / trunk | process_cpu_ms_per_million_records | +20.6% / +5.4% / +22.5% |
| consume | condition / trunk | allocated_bytes_per_record | +2.4% / -2.8% / +2.2% |
| consume | condition / nospin | records_per_s | -7.2% / -18.7% / +3.4% |
| consume | condition / nospin | process_cpu_ms_per_million_records | +4.2% / +23.1% / +3.0% |
| consume | condition / nospin | allocated_bytes_per_record | +2.0% / +7.1% / -7.1% |

## 成熟框架與量測界線

使用 [OpenJDK JMH](https://github.com/openjdk/jmh) **1.37** 的 annotation processor 產生 benchmark harness，由 JMH 控制暖機、正式 iterations、fork 與結果輸出。Benchmark 放在 Kafka 的 jmh-benchmarks module。為讓三個 frozen client jar 使用同一份 harness，採用 scoped annotation-processor 編譯與獨立 classpath，沒有把某一版 client 嵌入共用 shadow jar。

每個情境、每版共有三個獨立 JVM forks；輪替順序為 T/N/C、N/C/T、C/T/N。每個 fork：3 × 5 秒暖機、3 × 10 秒正式量測、單一 benchmark worker。共 27 個正式 forks、81 次正式 iterations。九個較短的 JMH smoke checks 先通過，未混入正式統計。

正常消費每次方法 invocation 處理並驗證恰好 1000 筆，使用 `@OperationsPerInvocation(1000)`，JMH 主結果因此是 records/s。每次 iteration 另核對 JMH 計數與實際驗證筆數完全一致。每筆驗證 partition、offset、128-byte payload 與內嵌序號；回傳 checksum 交由 JMH 消費，迴圈與驗證成本包含在結果中。

使用 JMH **內建 GC profiler**。消費情境的 `gc.alloc.rate.norm` 是 bytes/record，包含 benchmark 與 client 的配置流量，不是 retained heap。閒置與 unavailable 的 operation 是一次 poll，不能將其 B/op 誤寫成 B/record。

Process CPU 透過 JMH InternalProfiler 介面補充。JMH iteration lifecycle 中擷取 JVM process CPU 與 cgroup 計數，排除 trial 的 consumer 建立與 close；CPU 包含 application、background、GC、JIT 與該 fork 的 JMH 成本。CPU/record 使用同一 iteration envelope 的已驗證操作數。Cgroup CPU 另包含同 cgroup 的 JMH launcher，原始指標均保留。這個小型 profiler 不取代 JMH 的量測迴圈或統計。

同版跨 fork 與部分 iteration 仍有明顯變化，尤其正常吞吐與閒置 CPU。暖機設定不能視為完全穩態的證明；GC/JIT、共享主機與排程影響尚未分離。不把九個同版 iterations 當成九個獨立 forks，也不從三個 forks 推論統計等效。

## 真實 workload 與資源限制

- 真實 KafkaConsumer，consumer group protocol，單 member、單 partition，auto commit enabled。獨立 Kafka 4.1.0 broker；三個版本使用相同 client 設定。
- 預載五百萬筆、每筆 128 bytes。消費至尾端後 seek(0) 續讀，seek 與應用端驗證成本包含在 throughput。這是反覆讀取有限 backlog，不是多 broker／多 partition 的完整容量測試。
- IDLE fork 先 subscribe、完成 assignment，再 seek 到 log end，由 JMH 暖機與測量空 poll；沒有混入先前 native 測試的「先跑忙碌再閒置」歷史。
- UNAVAILABLE 使用本輪 bind 但未 listen 的 loopback port；每次 iteration 檢查 coordinator absent、heartbeat interval=0、無 in-flight heartbeat。這不是 pending-heartbeat-response 的測試。
- Native Intel N150，Java 17.0.20。Client affinity CPU 2、0.5 CPU quota、512 MiB memory、zero swap；fork heap 64/256 MiB，ActiveProcessorCount=1。JMH launcher heap 32/64 MiB，與 fork 共用該 cgroup。
- Broker CPU 0/1、producer CPU 3；producer 在正式測量前退出。每個 JMH fork 內驗證 limits 與祖先 cgroup，記錄 OOM 及 throttling。所有完成測量的 OOM 檢查通過。
- 本輪未測稀疏輸入尾延遲、member churn、rebalance 或多 broker 故障恢復，不是完整 consumer 替換驗收。
- 核心綁定不是獨占保留。主機前後 load、CPU pressure 與環境紀錄保留；未變更共享主機 governor 或其他服務。

## 功耗

**未量到瓦數或焦耳。** 主機有 RAPL energy counters，但 energy_uj 僅 root 可讀，非互動 sudo 需要密碼。沒有修改權限或主機設定。空轉 CPU 的降低可支持減少無效運算，不能換算成整機節電百分比；正常 consume 的 CPU/s 也必須搭配 CPU/record 判讀。

## 驗證、重現與清理

no-spin 版通過 251 個相關 regression tests，完整 candidate 通過 338 個；兩者 Checkstyle／Spotless 通過。新增 JMH 程式亦通過 Kafka 的 Checkstyle／Spotless、annotation-processor 編譯與 benchmark discovery，再通過九項真實 broker／unavailable JMH smoke checks。Optional SpotBugs 未執行。

- [JMH module patch（benchmark、CPU profiler 與 scoped import rules）](trunk-jmh/benchmark.patch)
- [JMH style 檢查](trunk-jmh/style.log)
- [每個 fork 的設定、輸出檔與執行命令](trunk-jmh/results/manifest.json)
- [全部原始 iteration 分數、fork summaries 與配對差異](trunk-jmh/summary.json)
- [Remote 驗證過的完整輸入 SHA](trunk-jmh/results/inputs.json)
- [主機量測前紀錄](trunk-jmh/results/host-before.txt)、[量測後紀錄](trunk-jmh/results/host-after.txt)
- [本輪 broker/data/process 清理驗證](trunk-jmh/cleanup.log)
- [先前 native cross-check 與試跑排除說明](trunk-threeway.md)

本輪自建 broker 已停止，生成的 backlog/data 與 remote run directory 已移除，確認沒有殘留本輪 Java processes。保留 compact logs、JMH JSON、source、patches 與 frozen client jars；未刪除共用 broker runtime libraries。
