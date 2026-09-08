# ASYNC-CONSUMER-V2 04：可回移的優化（不屬於架構切換的論點）

> 本文件收納 v2 過程中做過、但**舊架構（`AsyncKafkaConsumer` / `ConsumerNetworkThread`，部分連 `ClassicKafkaConsumer`）也能套用**的優化。它們刻意不放在 03 設計文件裡：03 只講新架構本身改了什麼、帶來什麼。這裡的每一項都應該獨立提交，讓所有 consumer 受益；它們的增益**不能**用來說服 reviewer 切換架構。
> 承接 `01-problem-analysis`、`02-deadline-model-and-baseline`；架構本身的貢獻量測見 03 文件 §2。

## 1. 分類

| 改動 | 在哪裡 | 能否套回舊架構 | 分類 |
|---|---|---|---|
| `RecordHeaders` 延遲配置（M0） | `common.header.internals` | **完全可以**，共用程式碼，舊架構已自動受益 | 獨立改善，非切換理由 |
| per-record 解碼瘦身（M2）：key/value 各一個 view、每 batch 一次 leader epoch / timestamp type、list 預配置、延遲 `abortedProducerIds` | `DefaultRecord`、`CompletedFetch` | **完全可以**，`CompletedFetch` 是 async 與 classic 共用的 | 獨立改善，非切換理由（附錄 C 的 +9–11% 不能算給 v2） |
| 無鎖交接（`RecordSink` 取代 `FetchBuffer` 的鎖） | 新類別 | 可以：把 `FetchBuffer` 換成無鎖佇列是局部改動；且已澄清**沒有量到鎖競爭**（03 文件附錄 B/C 的量測註記） | 非切換理由 |
| Credit 驅動的 prefetch（response 到達即續發） | `FetchPipeline` | **機制可以**：02 文件 §3.7 的 100 行實驗就是在舊架構的 `AbstractFetch` 裡做的，100B 場景 2.15×、classic 1.52×。要做成正式版需要：繞過 `pendingFetchRequestFuture` gate 與 buffered-node 排除、seek / rebalance 的 position fencing、credit 歸還通道、fetch session 下的續發——這些在舊架構裡會變成又一組跨執行緒 event 與 flag，但**做得到** | 吞吐 2× 的主要來源，**但不是架構獨有**；切換理由只能算「在新架構裡它是資料模型的一部分，而不是另一組 event」 |
| 穩態 `poll()` 零 event、零 selector wakeup | `PipelinedKafkaConsumer.poll` + `Parker` | **困難**：舊迴圈需要每次 poll 送 event 才知道應用端活著（poll timer 重設、fetch gate、manager 重跑），這是 deadline 模型的結構性需求；社群已把每次 poll 的 event 從 3 個減到 1–2 個（`AsyncPollEvent`），再減會撞到模型 | 架構獨有 |
| 每個 manager 自己的 timer、1 ms 下限、到期只重跑該 manager | `ConsumerEventLoop` + `ManagerTask` | **不行**：這就是把 deadline-driven 換掉 | 架構獨有；解決的是 busy-loop 那一類 bug（§1.5 第 5 列）與閒置節奏 |
| response 直接分派給 owner，不等下一輪 | `ConsumerEventLoop` | **部分**：舊架構的 response 回呼也能直接做事（實驗 hack 就是這樣續發的），但每個 response 仍會觸發一整輪 `runOnce()` | 架構獨有的是「每個 pass 只跑髒的東西」 |

結論：吞吐 2× 主要來自 prefetch，而 prefetch 的機制可以回移。架構本身帶來的是每次 poll 的固定成本歸零與 busy-loop 類 bug 的結構性消失，量化在 03 文件 §2。

---

## 2. Fetch 深度（prefetch）：續發不等應用端

### 2.1 機制

現行 `AbstractFetch` 每個 node 一個 in-flight、partition 有 buffered 資料就整個 node 停止 fetch、續發要等應用執行緒消費後送 `CreateFetchRequestsEvent`（01 文件 B 類）。改法是：response 一到，就用該 response 的結尾 offset 續發下一個 fetch，允許每個 partition 有 `prefetchFactor × max.partition.fetch.bytes` 的未消費資料在途。02 文件 §3.7 用 100 行的實驗 hack 在舊架構的 `AbstractFetch` 裡做到這件事：100B 場景 356 → 766 MB/s（2.15×），classic 431 → 654（1.52×），CPU 秒/GB 不變。

正式回移到舊架構需要補上：繞過 `pendingFetchRequestFuture` gate 與 buffered-node 排除、seek / rebalance 後的 position fencing（實驗 hack 沒有，`seekToBeginning` 後偶爾停住）、credit 歸還通道、fetch session 下的續發（實驗用 sessionless full fetch 避開）。在 v2 裡這些是 `FetchPipelineModel` 的四條不變式（03 文件 §4.1）；在舊架構裡會是另一組跨執行緒 event 與 flag。

### 2.2 上限如何推導（不新增公開設定）

- 每 partition：`prefetchLimitBytes = prefetchFactor × max.partition.fetch.bytes`，`prefetchFactor` 預設 2，走 `ConsumerConfig.defineInternal("internal.fetch.prefetch.factor")`（與 `internal.throw.on.fetch.stable.offset.unsupported` 同一種內部設定機制），供調校與測試，不進公開文件。
- 每 node：一個 in-flight，response ≤ `fetch.max.bytes`（既有）。
- 總記憶體上界：`assigned × (prefetchFactor × max.partition.fetch.bytes) + nodes × fetch.max.bytes`。現行設計的上界是 `assigned × max.partition.fetch.bytes + nodes × fetch.max.bytes`，所以預設值下最多多一倍 buffered 資料，這是「2 倍吞吐」的直接代價，寫進文件。
- 02 文件顯示 depth 2 與 4 / 8 沒有差別，所以預設 2 已足夠。

### 2.3 量測

02 文件 §3.7 的實驗（舊架構 + hack，depth 0 / 2 / 4 / 8）與 03 文件 §2 的 2×2（舊架構 ± prefetch、新架構 ± prefetch）。摘要（100B，3 輪中位數，`ASYNC-CONSUMER-V2-bench/results-arch-2x2.csv`）：

| 場景 | 舊架構 | 舊架構 + prefetch | 新架構，factor=1 | 新架構，factor=2 |
|---|---:|---:|---:|---:|
| 預設 MB/s | 444 | 849（1.91×） | 484 | 1039 |
| `max.poll.records=50` MB/s | 337 | 519（1.54×） | 447 | 877 |

預設 poll 頻率下，prefetch 單獨就給舊架構 1.91×；高 poll 頻率下它在舊架構只到 1.54×，因為每次 poll 的固定成本把省下的時間吃掉——那一段是架構的事（03 文件 §2）。

---

## 3. `RecordHeaders` 延遲配置（M0）

`common.header.internals.RecordHeaders` 改為第一次 `add` 才建內部 list；無 header 的 record 只配一個小物件。`Headers` 的可變語意不變，producer 也受益。02 文件 profile 上限 1.3%。

---

## 4. Per-record 解碼路徑（M2）

### 4.1 Receive buffer 走 `MemoryPool`（M2）

`Selector` 已支援 `MemoryPool`；consumer 目前在 `ClientUtils.createNetworkClient` 拿到 `MemoryPool.NONE`。改為建立 `SimpleMemoryPool(size = nodes × fetch.max.bytes × 2, maxSingle = fetch.max.bytes)`，並讓 `FetchPipeline` 對每個 response buffer 做 ref-count：每個 publish 到 sink 的 batch 持有一個引用，應用端消費完（或因 epoch 丟棄）時 `release`；全部歸零時 `pool.release(buffer)`。這是 `Selector` / `NetworkReceive` 既有的抽象，不是新機制；producer 端也可受益。效果：去掉每 response 的 1–50 MB 配置與歸零（02 文件 `bzero` / `HeapByteBuffer.<init>`），並讓「部分消費 pin 住整包」有明確的回收點。

### 4.2 `RecordDecoder`（M2）

把 `CompletedFetch.fetchRecords` / `parseRecord` 的邏輯搬到一個沒有共享狀態的 `RecordDecoder`（每個應用執行緒一個實例；為 M3 的多執行緒解碼預留），輸入 `FetchedBatch` 輸出 `List<ConsumerRecord<K,V>>`。修正 02 文件量到的熱點，全部是通用改法：

| 熱點 | 改法 | 通用性 |
|---|---|---|
| `Buffer.<init>` ×4/筆 | 解析時直接產生交給 deserializer 的那一個 view，不再 `readBytes` slice 後又 `duplicate()` | 只影響 consumer 內部解析器 |
| `maybeLeaderEpoch` `Optional`/筆 | 每個 batch 算一次，同一個不可變 `Optional` 實例給整個 batch 的 record | 無 API 影響 |
| `RecordHeaders` + `ArrayList`/筆 | `RecordHeaders` 改為**延遲配置**內部 list（第一次 `add` 才建）；無 header 的 record 只配一個小物件 | `common` 的通用改進，producer 也受益；`Headers` 可變語意不變 |
| `Utils.toArray`/筆（byte[] deserializer） | 不改 `Deserializer` 契約；但 `ByteArrayDeserializer` 本身覆寫 `ByteBuffer` 版本時仍需拷貝（使用者擁有 `byte[]`）。保留，列為已知成本 | — |
| `ArrayList` 預設容量 | 以 batch 的 record 數預先配置 | — |
| CRC 每 batch 全掃 | 保留（正確性），M3 才考慮搬到 loop 或 worker | — |

### 4.3 超出之後（M3）

02 文件顯示 depth ≥ 2 後卡在單連線 I/O 路徑（背景執行緒一半時間在 `read` / `kqueue`）。可行且通用的方向，留到 M3 依數據決定：
- `DecodeStage` 介面：CRC + 解壓在 worker pool 上做（per-partition 序號保序），應用端只做反序列化；壓縮 topic 與重量級 deserializer（Avro / Protobuf）受益最大。
- 對同一 broker 開多條連線（`NetworkClient` 以 node id 為 key，需要在 `FetchPipeline` 層做 connection 分派）。這改變 broker 端資源用量，需要 KIP，故只列不做。

### 4.4 M2 結果與一項設計修正

**設計修正：§4.1 的 receive buffer 重用不可行。** Consumer 交給使用者的 record bytes 是 response buffer 的 `ByteBuffer` slice（`ByteBufferDeserializer` 直接回傳原 buffer；第三方 deserializer 的 `ByteBuffer` overload 也可能保留它），API 契約允許使用者無限期持有。重用 buffer 會在使用者手上的資料底下覆寫記憶體。Kafka 自己的 `SimpleMemoryPool` 只做「預算」不重用，正是這個原因。所以每個 response 一次新配置（含歸零）是這個契約的代價，不是實作缺陷；能做的只有用預算型 pool 給記憶體上界（robustness，非吞吐），列為之後的選配。§8 M2 的「背景執行緒 alloc/GB −50%」退出條件因此撤回。

**M2 實際做的**（commit 見 git log「M2: cheaper per-record decode path」）：`Record.keyView()/valueView()` 非複製視圖（`DefaultRecord` 每次還原原始 position/limit，因為 key/value 是 batch 的 slice、capacity 超出欄位範圍）；`CompletedFetch` 每 batch 算一次 leader epoch 與 timestamp type、先取大小再反序列化、預配置 list、`abortedProducerIds` 延遲配置。每筆紀錄少 2 個 `ByteBuffer` 與 1 個 `Optional`；classic 與 v2 共用 `CompletedFetch`，兩者都受益。

**量測方法的修正**：同一個變體在同一台機器上不同時段的 100B 吞吐可以從 630 擺到 1050 MB/s（gradle 跑了幾小時後的熱節流恢復期）。跨時段比較不可信，只能用**交錯 A/B**（M1 jar、M2 jar 逐次交替，同一時段內比）。`ASYNC-CONSUMER-V2-bench/results-m2-ab.csv`：

| 100B，交錯 4 次取中位數 | M1 | M2 |
|---|---:|---:|
| 預設 MB/s | 807 | 883（+9%） |
| `max.poll.records=50` MB/s | 791 | 880（+11%） |
| 預設 CPU 秒/GB | 2.46 | 2.36（−4%） |
| `max.poll.records=50` CPU 秒/GB | 2.73 | 2.52（−8%） |
| 1KB lz4 MB/s / CPU 秒/GB | 556 / 1.51 | 588 / 1.50 |

Profile：`Buffer.<init>`（8.9%）與 `maybeLeaderEpoch`（8.1%）從應用執行緒熱點消失；剩下的是 `Utils.toArray`（`ByteArrayDeserializer` 的必要拷貝）、`DefaultRecord.readFrom`（varint 解析）、`HeapByteBuffer.get`，以及 `ArrayList.<init>` 這種配置慢路徑的歸屬點。§8 M2「應用執行緒 per-record CPU −20%」以整體 CPU 秒/GB 看只達到 −4～−8%，未達標；再往下要動的是 `DefaultRecord.readFrom` 的解析方式與反序列化介面（M3 的 `DecodeStage`）。

**測試**：`DefaultRecordTest`（含新案例：視圖每次還原、不配置、`key()` 不受視圖消費影響）、`DefaultRecordBatchTest`、`MemoryRecordsTest`、`CompletedFetchTest`、`FetchCollectorTest`、`FetcherTest`、`FetchRequestManagerTest`、`KafkaConsumerTest`、`ConsumerEventLoopTest` 共 1139 案例通過；checkstyle / spotbugs / spotless 通過。

**下一步選項**：M3（`DecodeStage`：CRC + 解壓在 worker 上做，壓縮 / 重 deserializer 場景）、或先做記憶體上界的預算型 pool。（M4 Streams 遷移已於附錄 E 取消。）

**一個量測上的澄清**：`time-between-network-thread-poll-avg` 在 100B 場景約 0.05 ms（loop 每秒約 2 萬個 pass），起初以為是每次 app poll 觸發一輪；把「新的 poll」從 `hasPendingWork` 移除後數字不變，代表 pass 頻率是 I/O 驅動的：一個 1 MB response 分多個 TCP segment 到達，`Selector` 每次可讀就醒一次，舊迴圈也是同一個數量級（02 文件 3.4 節 0.05–0.12 ms）。所以 loop 的**每個 pass 必須便宜**（目前閒置 pass 只有：空 command queue、metadata 版本比較、timer 檢查、`poll(0)`），這比減少 pass 次數更重要；M2 的 `MemoryPool` 會再減少每個 pass 的 buffer 配置。

---

---

## 5. M3（worker 上做 CRC + 解壓）：不做，數據不支持

M3（`DecodeStage`：CRC + 解壓搬到 worker）只在應用執行緒是瓶頸時有效。用 M2 的 jar 量 lz4 場景：

| 量測 | 1KB lz4 | 1KB 未壓縮 |
|---|---:|---:|
| 應用執行緒 CPU 佔用（profile） | 24%（其中 21% 是 `LZ4_decompress_safe`） | – |
| 背景執行緒 CPU 佔用 | 29% | – |
| broker JVM CPU（8 核） | 27–35% | 52–60% |
| `fetch-latency-avg`（1 MB fetch） | **1.9 ms** | 0.98 ms |
| `fetch-size-avg` | 0.98 MB | 1.05 MB |
| 吞吐 | 460–590 MB/s | 960–1220 MB/s |
| classic 同場景 | 380 MB/s | ~1095 MB/s |

三方都沒飽和；單 partition 每次只能有一個 in-flight fetch（fetch 以 offset 定址，不知道上一個 response 的結尾就無法發下一個），所以吞吐上界 ≈ fetch 大小 / fetch 延遲 = 1 MB / 1.9 ms ≈ 530 MB/s，與實測一致。lz4 topic 的 fetch 延遲是未壓縮的兩倍，classic 也有同樣的 2.5× 差距，所以是 broker / 儲存端（本機 lz4 topic 可能部分不在 page cache），不是 consumer。

結論：在這台機器上 M3 不會提高吞吐。單 partition 的 latency-bound 場景剩下的槓桿是 `max.partition.fetch.bytes`（減少往返次數，設定而非程式碼）與 broker 端。M3 保留為「重量級 deserializer（Avro / Protobuf）且應用執行緒飽和」場景的選配，需要先量到應用執行緒飽和才做。

下一步原本改做 M4（Streams 遷移），後於附錄 E 取消。

---

---

## 6. v2 的總數字（含 prefetch 與 M2；只供參考，不是切換論點）

環境同 02 文件（同機單節點 KRaft、loopback、資料在 page cache）；`ConsumerPerformance` 的 `fetch.MB.sec`，3 次取中位數；CPU 秒/GB 以 `/usr/bin/time -l` 的 user+sys 計。原始數據在 `ASYNC-CONSUMER-V2-bench/results-m1.csv`。

| 場景 | baseline async（02 文件） | classic | **v2 M1** | v2 / baseline | CPU 秒/GB（v2 / classic / baseline） |
|---|---:|---:|---:|---:|---|
| 1p 100B | 356 | 466 | **829** | **2.33×** | 2.26 / 2.27 / 2.42 |
| 1p 100B `max.poll.records=50` | 320 | 444 | **906** | **2.83×** | 2.56 / 2.48 / 3.57 |
| 1p 100B `internal.fetch.prefetch.factor=1` | – | – | 472 | ≈ classic | 2.23 |
| 1p 1KB | 1034 | 1095 | 1219 | 1.18× | 1.20 / 1.05 / 1.09 |
| 1p 1KB lz4 | 417 | 432 | 556 | 1.33× | 1.54 / 1.52 / 1.56 |
| 6p 1KB（同一 broker） | 764 | 934 | 1278 | 1.67× | 1.92 / 1.88 |

- §8 的 M1 退出條件：100B ≥ 2.0×（達成 2.33×）、CPU 秒/GB ≤ 現行（達成）、`max.poll.records=50` 的 CPU/GB ≤ classic（2.56 vs 2.48，差 3%，見下）。
- `prefetch.factor=1` 退回 classic 水準，證明增益來自 credit 管線而非其他改動。
- 正確性：`OffsetCheck`（每 partition offset 連續、無重複、消費完後 `seekToBeginning` 能重新讀到 offset 0）在 1p / 6p、`max.poll.records` 500 / 50、lz4 全部通過。
- facade 測試 `KafkaConsumerTest` 225/225；`AsyncKafkaConsumerTest`、`ShareConsumerImplTest` 等舊類別測試不受影響。

- `prefetch.factor=1` 退回 classic 水準，證明增益來自 prefetch 深度。
- 架構單獨的貢獻請看 03 文件 §2 的 2×2。
