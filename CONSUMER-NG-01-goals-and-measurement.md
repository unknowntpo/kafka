# CONSUMER-NG 01：目標、約束與量測方法

2026-09-09 起的新主線。前一條線（`async-consumer-v2-loop-only`，文件 `ASYNC-CONSUMER-V2-*`）證明了「只換背景迴圈」在吞吐、CPU、閒置功耗上都與 trunk 打平，價值只剩結構保證，那條線交給 Astra 以 KIP 的形式繼續。這條線沒有那些限制。

## 1. 目標

在不同資源等級下，把 Kafka Java consumer 的**效能與功耗**做到與最強的非 Java 實作同一等級或更好。

| 等級 | 代表機器 | 關注點 |
|---|---|---|
| T1：1–2 核、小記憶體（edge / sidecar / 容器配額 0.5–1 CPU） | morefine 限制 `taskset` 1 核、`-Xmx256m` | 每 GB 的 CPU 秒、閒置時的喚醒次數與 CPU、記憶體上限下不退化也不 OOM |
| T2：4 核（一般服務） | morefine（Intel N150，4 核，31 GB） | 吞吐與 CPU/GB 並重 |
| T3：8 核以上 | M1 Pro 筆電（8 核） | 單 consumer 吞吐上限、多 partition 擴展 |

「贏」的定義是同一份工作負載、同一台機器、同一個 broker 上，三個指標都不輸：

1. 吞吐：MB/s 與 records/s（1 partition 與 6 partition；100 B 與 1 KB records）。
2. CPU 效率：process 的 user+sys 秒 / 消費 GB。
3. 功耗代理：閒置（有 assignment、topic 無新資料）60–180 秒的 CPU 秒與 voluntary context switch 次數；trunk 目前約 0.75% 核心。
4. **記憶體有界**：負載上升（partition 變多、broker 突然吐出大量資料、應用處理變慢）時，consumer 的記憶體用量必須由設定決定的上限管住，不能因為堆積而 OOM。判準：在小 heap（例如 `-Xmx256m`）與多 partition（64、256）下，突發流量時 RSS 與 heap 佔用有可預期的上限，吞吐降級而不是崩潰；trunk 的 `fetch.max.bytes` / `max.partition.fetch.bytes` 只管單一回應，不管在途與已緩衝的總量。

「有界」的實作原則：向 broker 要資料之前先看還有多少額度（credit / admission），額度由使用者可設定的總緩衝上限與應用的消費速度決定；已收到但尚未交付的資料算在額度裡；額度用完就不發 fetch，讓 broker 端等待而不是 client 端堆積。這與功耗目標一致：不做無謂的 fetch。

## 2. 約束（只有這些）

- **Public interface 不變**：`org.apache.kafka.clients.consumer.Consumer<K,V>` 的方法與語意、`ConsumerConfig` 的設定與預設值、`ConsumerRebalanceListener` 回呼契約、`Deserializer` 契約（含 KIP-863 的 `ByteBuffer` 多載）、既有 metric 名稱。
- **重用 RPC 層**：`NetworkClient`、`Selector`、`AbstractRequest/Response` 與 protocol message、`Metadata`。這些不重寫。
- **Java 21+**：新實作放在自己的 gradle 模組，`--release 21`；Vector API（incubator）與 FFM 只在量到收益時才用。
- 只從 trunk 分叉；不用犧牲通用性的 hack；量測在安靜機器上做；push 前要授權。

不再守的舊限制：不動 Streams / share consumer、保留 `RequestManager` 邏輯、只改 deadline-driven 部分。

## 3. 對照組

| 實作 | 語言 | 工具 | 備註 |
|---|---|---|---|
| trunk `KafkaConsumer`（classic 與 consumer protocol） | Java | `kafka-consumer-perf-test` | 起點 |
| librdkafka | C | `examples/rdkafka_performance -C` | morefine 上以 mklove 從原始碼建（無 root；關 zlib/zstd/ssl/sasl，本量測不用壓縮） |
| franz-go | Go | `examples/bench` | Go 1.25 tarball 裝在家目錄 |

三者跑同一個 broker（trunk 發行包，單節點，`~/kafka-bench`）、同一批 topic（`big100b` 1p × 40M × 100 B；`t6p` 6p × 3M × 1 KB；`idle1p` 空 topic；記憶體 cell 另建 `t64p` / `t256p`，並以慢速消費者模擬應用處理變慢）。每個 cell 交錯跑 3 輪取中位數；每執行緒 CPU 用 `/proc/<pid>/task/*/stat`（既有 `thread-cpu.sh`），context switch 用 `/proc/<pid>/status`。

## 4. 方法：先量、再假設、再做

順序固定：**先建立三方基準線**（第 3 節），再用 profile（async-profiler 或 `perf`）找出 Java 實作與最強實作之間每 GB 的 CPU 差在哪一層，才決定做什麼。不先設計架構。

前一條線已知的事實，作為初始假設而非結論：

- 完整版（`FetchPipeline` 深度 + credit admission + 無鎖交接 `RecordSink`）在 1p 100 B 上對 trunk +2.6×；這條路徑在 4 核 morefine 上是 +47–69%。表示 fetch 續發與交接是第一層瓶頸。
- 背景執行緒的 CPU 由 socket 讀取主導；app thread 由 record 解碼與複製主導。零拷貝（直接在 socket 讀進的 buffer 上迭代 record、`ByteBuffer` 反序列化）是第二層。
- 閒置成本兩邊都是每 500 ms 一次空 fetch 回應 + timer；要更低必須讓 broker 的 `fetch.max.wait.ms` 與 client 的喚醒一致，且沒有其他週期性 timer。
- T1 等級可能根本不該有背景執行緒：app thread 在 `poll()` 內直接驅動 `NetworkClient`，只在 poll 間隔超過 heartbeat 間隔時才需要別的執行緒保活。

## 5. 產出物與判準

- `CONSUMER-NG-02-baseline.md`：三方基準線與 profile，指出每 GB 的 CPU 去了哪裡。
- 之後每一個改動都要附「在哪個等級、哪個指標、贏多少、代價是什麼」；不能在三個等級都不退化的改動不進主線。
- 正確性門檻與前一條線相同：`consumer_test.py` 系統測試全綠（Jenkins `kafka-e2e`，JDK 25）、單元測試、`clean build -x test` gate。前一條線的兩個教訓（R11 停擺、R3 ping-pong）都要有對應的閒置與長 poll 測試。
