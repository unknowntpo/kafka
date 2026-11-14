# FETCH-PIPELINE-07：KIP 素材（insight 清單）

用途：寫 KIP 時的素材庫。每一條都標明可以放進 KIP 的哪一節、證據在哪份文件、信心程度。
環境：除非另外註明，數字都來自 morefine（N150，4 核，broker 與 client 同機，資料在 page cache）。
標記：【已量測】有實驗數據。【推論】由數據推出，但沒有直接實驗。【想法】還沒有任何驗證。

## A. 可以放進 Motivation 的 insight

### A1. trunk 的背壓是「有沒有」，不是「有多少」【已量測】

- trunk 的規則：一個 partition 有未取完的資料，同一個 broker 的所有 partition 都不 fetch。每個 node 同時只有一個 request。
- 結果：application 處理期間，網路和 broker 是閒的。
- 換成每個 node 一份 bytes credit 並重用 receive buffer 之後，吞吐量多 33% 到 52%。
- 證據：05 文件「一個 partition 何時可以 fetch」的調查；06 文件 §1 的 trunk 與本分支兩欄。

### A2. trunk 的 consumer 在 1 KB 訊息上輸 librdkafka 約兩成【已量測】

- 6 partition × 1 KB：trunk 1099，librdkafka 最佳設定 1415（MB/s）。
- 1 partition × 1 KB：trunk 1120，librdkafka 1530。
- 本分支把差距補回來：1508 與 1490。
- 小訊息相反：Java 遠快於 librdkafka 的單筆 API（848 對 125）。
- 限制：只有一台機器、單一 broker、librdkafka 的批次 API 沒有量。KIP 裡要寫明這些限制。
- 證據：06 文件 §1。

### A3. 靠調 config 才快的 client 不可靠【已量測】

- librdkafka 在 6 partition × 1 KB 上，預設值與最佳設定差 13%。
- librdkafka 預設的 partition 層級流量控制加固定 backoff，會讓它自己空等。100 B 訊息時它的兩條 thread 只用了 53% 和 65%。
- 對 KIP 的意義：新設計不加 config，大小從現有 config 推導，行為由執行期量測決定。
- 證據：06 文件 §2 的 G6、G7。

### A4. 小訊息的成本是配置量，不是 CPU【已量測】

- 100 B 訊息：讀 11.4 GB 的 payload，JVM 配置 47.1 GB 的物件，是 payload 的 4.1 倍。
- 約四成是中間垃圾：每筆一個 `HeapByteBuffer` slice（14%）、`DefaultRecord`（14%）、空的 `RecordHeaders` 與 `ArrayList`（12%）、每筆一個 `Optional`（4%）、`ArrayList` 擴容（3.5%）。
- 只拿掉 slice、`Optional`、擴容三項：配置少 17%，吞吐量從 855 升到 985 MB/s。
- 這不需要改 public API。
- 證據：06 文件 §2a。原型在 `FETCH-PIPELINE-06-lean-decode-prototype.patch`。

### A5. receive buffer 每次重新配置是 background thread 的大宗成本【已量測】

- 重用前，background thread 有 33% 的 CPU 花在把新 buffer 清零和 JDK 內部的複製。
- 重用後命中率 99.6%（6 partition）與 98.9%（1 partition）。
- 證據：03 文件；04 文件的 PT1 證據欄。

## B. 可以放進 Design 與 Rejected Alternatives 的 insight

### B1. 瓶頸會隨 payload 移動【已量測】

- 1 KB 訊息：跑滿的是 background thread（網路讀取）。
- 100 B 訊息：跑滿的是 application thread（decode）。
- 意義：只最佳化其中一條 thread 的設計，只對一部分 workload 有效。
- 證據：06 文件 §2 的 G1。

### B2. 平行化在記憶體頻寬受限的機器上是純成本【已量測】

- 兩個 consumer 併行讀 1 KB：總吞吐量等於一個 consumer，每 GB 的 CPU 多 60%。
- decode 搬到 worker thread：1 個 worker 多 6%，3 個 worker 少 2%，每 GB 的 CPU 多 35% 到 50%。
- 根因是 A4：每秒能寫進記憶體的位元組數有上限，換 thread 做不會少寫。
- Rejected Alternative 的寫法：「固定數量的 decode thread」被否決，因為它在小機器上只增加成本。
- 證據：06 文件 §2 的 G2 到 G4、§2a。

### B3. 平行度必須由執行期量測決定【推論】

- 由 B2 推出：同一個改動在 4 核機器是成本。大機器的結果預期不同，但還沒有數據。
- 設計方向：預設單線。用爬坡法嘗試加一個 worker 或一條 fetch 通道，量到增益才保留。
- 還缺的證據：大機器上的數據。計畫見 06 文件 §5。

### B4. 先減少配置，再談多線程【已量測加推論】

- 已量測：減少配置有 15% 增益，加 worker 沒有（A4、B2）。
- 推論：正確的順序是先把每筆 record 的配置降到最低，再評估平行化。順序反過來會把記憶體問題誤判成 CPU 問題。

### B5. 每個 partition 的 fetch 大小不是瓶頸【已量測】

- `max.partition.fetch.bytes` 從 1 MB 改成 8 MB 與 50 MB，吞吐量都在雜訊範圍內。
- Rejected Alternative 的寫法：「依 partition 數動態放大每個 partition 的 fetch 大小」被否決，因為量不到增益。
- 證據：06 文件 §2 的 G5。

### B6. `max.poll.records` 不是瓶頸【已量測】

- 從 500 改成 50000，吞吐量沒有變好。
- 意義：application thread 和 background thread 之間每次 `poll()` 的交接成本，在這個 workload 下不是限制。
- 證據：06 文件 §2a 第 3 點。

### B7. 背壓有兩個位置：fetch 的准入，和 buffer 的取出順序【推論】

- `FetchBuffer` 是先進先出。即使 fetch 對每個 partition 公平，冷 partition 的資料仍然排在熱 partition 後面。
- 只改 fetch 粒度不一定改善延遲。
- 還缺的證據：05 文件的實驗 E3（熱冷 partition 偏斜）。

### B8. partition 層級 credit 在單線 application 下可能不需要【推論】

- application 是單一 thread 時，所有 partition 共用一個接收端。背壓粒度只影響延遲公平性與記憶體上限，不影響總吞吐量。
- application 多線程成立後，各 partition 速度不同，partition 層級 credit 才變成必要配套。
- HTTP/2 的兩層流量控制（connection 與 stream）是對應的類比。

### B9. credit 的範圍與 buffer pool 的範圍不一致【推論】

- credit 是每個 node 一份 `fetch.max.bytes`。記憶體上限是 node 數 × `fetch.max.bytes`。
- `CachingMemoryPool` 的上限是全域 2 × `fetch.max.bytes`。
- 多 node 時命中率預期下降，還沒有量。
- KIP 至少要寫出新舊記憶體上限的公式。trunk 是 partition 數 × `max.partition.fetch.bytes`。

## C. 來自 OpenTelemetry Collector batch processor 棄用文章的 insight

來源：https://ganhua.wang/batch-processor-opentelemetry-collector 。文章內容沒有對照 OpenTelemetry 原始碼驗證。

### C1. 「還沒完成就回報成功」是舊設計的核心病【想法】

- consumer 的對應：auto-commit 在 `poll()` 回傳時就把上一批視為已處理。使用者把 record 丟進自己的 thread pool 時，會提交還沒處理完的 offset。
- 這是 application 多線程 KIP 的 Motivation 素材。

### C2. 完成訊號應該是獨立的物件【想法】

- 現況：buffer 歸還、credit 歸還、offset 前進，各用各的方式判斷「處理完」。buffer 歸還掛在 `FetchMetricsAggregator` 上。
- 方向：一個完成訊號，三件事都訂閱它。
- 可能的收穫：buffer 的生命週期與 application 可見的生命週期對齊後，可以拿掉只允許內建 deserializer 重用 buffer 的白名單。這需要改 API 契約。

### C3. 換了位置，同一個容量數字的意義會變【已量測（文章）加推論（我們）】

- 文章的例子：batch 與 queue 換位置後，同一個 `queue_size` 的實際容量掉了約 99%。
- 我們做了同類的事：上限從每個 partition 一份改成每個 node 一份。見 B9。

### C4. 狀態按分片切開，執行資源全域共用且有上限【想法】

- 文章的 `workerPool` 用 token 限制所有分片共用的並行數。
- 對應到我們：若要做 decode worker 或 fetch 通道，應該是全域共用的池，不是每個 partition 一條 thread。

### C5. consumer 天生免疫的兩件事【推論】

- 持久化在 batch 之前：broker 的 log 就是持久 queue，consumer 的 buffer 都可以丟。
- 背壓不需要選項：pull 模型不 fetch 就是背壓，不需要「滿了要丟還是要擋」的 config。

## D. 方法上的 insight

- **先量再設計。** 今天三個看起來合理的假設都被實驗否定：加大 partition fetch 大小、decode 平行化、加大 `max.poll.records`。
- **找不變量。** decode 平行化的根因是靠「不同 thread 數下 young GC 次數相同」找到的。
- **每條 thread 的 CPU 要搭配 wall-clock profile 看。** CPU 沒跑滿只代表它在等，不代表它有餘裕。
- **量測腳本要驗證。** 今天有兩次腳本錯誤產生了錯的數字：函式被重複載入而同時跑了兩個 consumer；視窗長度算錯而高估 28%。逐秒序列是最可靠的對照。
- **對手要給最佳設定。** 只比 librdkafka 的預設值會高估我們的優勢。

## E. 還沒有證據的缺口

| 缺口 | 影響哪一條 | 需要什麼 |
|---|---|---|
| 大機器上平行化有沒有增益 | B3 | opensource4you 的 `kafka-e2e`，新的 ducktape 測試 |
| 多 broker 時單一 background thread 的表現 | A2、B9 | 多 broker 環境 |
| 真實壓縮比下的表現 | A2、B1 | 可壓縮的 payload 產生器 |
| librdkafka 批次 API 的吞吐量 | A2 | `rd_kafka_consume_batch_queue` 的測試程式 |
| 拿掉 `DefaultRecord` 與空 `RecordHeaders` 的增益 | A4 | 完整的 cursor 式 decode 原型 |
| 熱冷 partition 偏斜時的延遲 | B7、B8 | 05 文件的實驗 E3 |
