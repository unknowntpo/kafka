# FETCH-PIPELINE-06：對 librdkafka 的基準、擴展實驗與方案

日期：2026-11-14。環境：morefine（N150，4 核，31 GB），broker 與 client 同機，資料全在 page cache。
librdkafka 版本 v2.6.1，從原始碼編譯，用它內附的 `rdkafka_performance -G`（high-level consumer，一次取一筆）。
Java 用 `kafka-consumer-perf-test`，`group.protocol=consumer`。數字是暖機後各次的中位數，單位 MB/s。

## 0. 結論

1. 本分支在這台機器上已經不輸 librdkafka。四種 payload 中三種領先，一種持平。trunk 在 1 KB 的兩種 payload 上輸 librdkafka 約 22% 到 27%。
2. 這台機器已經到頂。兩個 consumer 併行的總吞吐量等於單一 consumer。decode 平行化原型沒有增益，CPU 成本多 35% 到 50%。
3. 所以「再大幅成長」無法在 morefine 上證明或否定。需要有餘裕的機器。opensource4you 的 `kafka-e2e` runner 是可行的環境，但需要你授權才能觸發。
4. 同一個設計不可能同時適合 4 核和 128 核。這是「自己找甜蜜點」的直接證據：平行度必須是執行期量出來的，不是 config。

## 1. 對 librdkafka 的基準

| payload | trunk | 本分支 | librdkafka 預設 | librdkafka 最佳設定 | 本分支對 librdkafka 最佳 |
|---|---|---|---|---|---|
| 6 partition × 1 KB | 1099 | 1508 | 1255 | 1415 | +7% |
| 1 partition × 1 KB | 1120 | 1490 | 1534 | 1530 | −3% |
| 6 partition × 100 B | 574 | 848 | 108 | 125 | 6.8 倍 |
| 6 partition × 1 KB，lz4 | 880 | 1336 | 1050 | 1044 | +28% |

備註：

- 「最佳設定」是 `queued.min.messages=10000000`、`fetch.queue.backoff.ms=0`，以及只關 backoff 兩組中較好的一組。也試過 `check.crcs=true` 和加大 `fetch.message.max.bytes`，沒有更好。
- lz4 topic 的 payload 是隨機位元組，壓不下去。這一列量到的是解壓流程的成本，不是真實壓縮比下的表現。
- 這個 librdkafka 沒有編 zstd 和 zlib，所以沒有比 zstd。
- 100 B 的差距來自 librdkafka 每筆訊息的固定成本。它不論訊息大小都是約每秒 110 萬到 150 萬筆，兩條 thread 都接近跑滿。它的批次取出 API 可能比較快，這次沒有量。
- 1 partition × 1 KB 兩邊的差距在雜訊範圍內。

## 2. 收集到的 Force（都是【觀察】）

- **G1 瓶頸隨 payload 移動。** 穩態下各 thread 的 CPU（單位：核）：

  | payload | MB/s | background thread | application thread | broker | 全機忙碌核數 |
  |---|---|---|---|---|---|
  | 6 partition × 1 KB | 1474 | 1.08 | 0.79 | 0.65 | 2.55 / 4 |
  | 6 partition × 100 B | 約 850 到 1030 | 0.74 | 約 1.0 以上 | 0.39 | 2.36 / 4 |

  1 KB 時跑滿的是 background thread。100 B 時跑滿的是 application thread。
- **G2 這台機器在 1 KB 時到頂。** 兩個 consumer 各讀同一個 topic，總和是 1406 到 1489 MB/s，等於單一 consumer。每 GB 的 CPU 從 1.15 秒升到 1.85 秒。CPU 還有空核，所以限制是記憶體頻寬，跟 03 文件對 N150 的結論一致。
- **G3 100 B 時多一個 consumer 只多 18%。** 總和約 1000 MB/s，每 GB 的 CPU 從 2.1 秒升到 3.3 秒。
- **G4 decode 平行化在這台機器沒有用。** 原型把整個 `CompletedFetch` 的 decode 移到 worker thread。

  | payload | 0 個 worker | 1 個 | 2 個 | 3 個 |
  |---|---|---|---|---|
  | 6 partition × 100 B | 836 | 878 | 820 | 813 |
  | 6 partition × 1 KB | 約 1530 | 1427 | 1462 | 1313 |
  | 6 partition × 1 KB，lz4（後兩輪） | 約 1330 | 1335 | 1328 | 1263 |

  100 B 配 1 個 worker 多 5%，每 GB 的 CPU 多 35%。其餘都持平或變差。
- **G5 每個 partition 的 fetch 大小不影響吞吐量。** 1 MB、8 MB、50 MB 的差距都在雜訊內。「單 partition 被來回次數限制」的假設被否定。
- **G6 librdkafka 的預設流量控制會讓它自己等待。** 預設設定下，100 B 時它的兩條 thread 只用了 53% 和 65%。這是 partition 層級背壓加上固定 backoff 的代價，是 05 文件 S1 的反面教材。
- **G7 靠調 config 才快的系統不可靠。** librdkafka 在 6 partition × 1 KB 上，預設和最佳設定差 13%。使用者不會知道要調哪個。

## 2a. 為什麼 decode 平行化沒有用（2026-11-14 追加）

背景：100 B 小訊息時，application thread 的 CPU 是跑滿的。直覺是把 decode 搬到別的 thread 就會變快。實測只多 6%，3 個 worker 還變慢。

查證過程，每一步都是【觀察】：

1. **不是 GC 暫停。** young GC 每次 1 到 5 毫秒，4 秒內 11 次。
2. **不是 fetch 回應太小。** 每個 partition 的 fetch 大小從 1 MB 改 8 MB，沒有變化。
3. **不是 `poll()` 每次回太少筆。** `max.poll.records` 從 500 改 50000，沒有變化。
4. **worker 在等工作。** wall-clock profile 顯示 3 個 worker 時，decode thread 有 72% 的時間閒著。1 個 worker 時它 88% 的時間在跑。
5. **不變量是配置量。** 0 個和 3 個 worker 的 young GC 次數相同，吞吐量也相同。
6. **配置量是 payload 的 4.1 倍。** allocation profile：讀 11.4 GB 的 payload，配置了 47.1 GB 的物件。

   | 配置來源 | 佔比 | 是否必要 |
   |---|---|---|
   | value 的 `byte[]`（`Utils.toArray`） | 31% | 必要。使用者拿到的就是它 |
   | `ConsumerRecord` | 18% | 必要 |
   | `HeapByteBuffer`（`Utils.readBytes` 每筆切一個 slice） | 14% | 中間垃圾 |
   | `DefaultRecord` | 14% | 中間垃圾 |
   | `RecordHeaders` 與它內部的 `ArrayList` | 12% | 沒有 header 時是垃圾 |
   | 每筆一個 `Optional`（leader epoch） | 4% | 垃圾。同一個 batch 的值都一樣 |
   | `ArrayList` 擴容的 `Object[]` | 3.5% | 垃圾 |

結論：這台機器受記憶體頻寬限制。每秒能寫進記憶體的位元組數是固定的。decode 由哪條 thread 做，要寫的位元組數都一樣，所以加 thread 沒有用。要變快，只能少寫。

### 驗證：減少配置的原型

原型做三件事：value 不切 slice，直接從 batch buffer 複製成 `byte[]`；leader epoch 的 `Optional` 每個 batch 只建一次；結果的 `ArrayList` 預先給容量。`DefaultRecord` 和 `RecordHeaders` 沒動。

| 設定 | MB/s（穩態） | 整段的 young GC 次數 |
|---|---|---|
| 關 | 855 | 46 |
| 開 | 985 | 38 |
| 開，加 1 個 decode worker | 980 | 38 |
| 開，加 2 個 decode worker | 978 | 38 |

配置少 17%，吞吐量多 15%。吞吐量跟著配置量走，假設成立。加 worker 仍然沒有用。
原型存成 `FETCH-PIPELINE-06-lean-decode-prototype.patch`，包含 decode worker 原型。它只處理沒有 key、沒有 header、value 用 `ByteArrayDeserializer` 的 record，不能直接 commit。

### 這對方案的意義

- 小訊息的下一步是「少配置」，不是「多 thread」。還沒動的 `DefaultRecord`（14%）和空的 `RecordHeaders`（12%）是下一批目標。
- 這不需要改 public API。它是 `/Users/unknowntpo/repo/unknowntpo/kafka/KAFKA-21152-work/` 裡 `CompletedFetch` cursor 分層想法的具體理由。
- 大機器上的結論可能不同。記憶體頻寬夠時，瓶頸才會回到 CPU，那時 thread 才有用。這仍然需要大機器驗證。

## 3. 這些 Force 對 05 文件的修正

- 小訊息的吞吐量上限在 morefine 上由配置量決定，見 §2a。
- 05 的 F16（多線程的價值取決於 decode 與處理的佔比）要加一個條件：機器要有記憶體頻寬的餘裕。morefine 沒有。
- 05 的 M1（內部多線程 decode）不能預設開啟。在 morefine 上它是純成本。
- 05 的 S1（partition 層級 credit）如果要做，不能用固定 backoff。credit 一歸還就要能立刻 fetch。

## 4. 方案

目標分兩層。第一層已經完成並有數據。第二層是設計，還沒有可信的數據。

### 第一層（已完成）：單線管線做到機器上限

本分支現有的三件事：移除 trunk 的三個 fetch 條件、每個 node 一份 bytes credit、receive buffer 重用。
相對 trunk 的增益是 33% 到 52%，並且不輸 librdkafka。這一層不需要任何 config。

### 第二層（設計）：自己找甜蜜點的平行度

**原則**：平行度是執行期的量測結果，不是設定。預設是單線。只有量到增益才加，量不到就退回。

- **兩個可以加的方向**，對應 G1 的兩種瓶頸：
  - decode worker：application thread 跑滿時（小訊息、壓縮）有用。
  - fetch 通道：background thread 跑滿時（大訊息）有用。做法是對同一個 broker 開多條連線，各由一條 thread 讀。這是 librdkafka「每個 broker 一條 thread」的延伸，但通道數不綁定 broker 數。
- **控制方式**：爬坡。每隔一段固定時間比較「加一個單位之前」和「之後」的吞吐量。增益超過門檻就保留，否則退回，並拉長下次嘗試的間隔。
- **為什麼這樣設計**：G4 顯示同一個改動在 4 核機器是純成本。128 核機器的結果一定不同。固定的 thread 數對其中一邊是錯的。
- **不加 config**：門檻與間隔是內部常數。上限由 `Runtime.availableProcessors()` 推導。
- **代價**：爬坡會在嘗試期間多花 CPU。吞吐量受 application 處理速度限制時，爬坡不該啟動。判斷條件是 application 是否在 `poll()` 內等資料。

### 第三層（需要 KIP）：application 多線程

沿用 05 文件的 M2 和 M3。前提是第二層在大機器上證明平行度有價值。完成訊號和 partition 層級 credit 是它的配套。

## 5. 驗證計畫：opensource4you 的 `kafka-e2e`

已查證（唯讀）：

- `kafka-e2e.yml` 可以指定 `account=unknowntpo` 和任意 commit。我有 `opensource4you/clip` 的 push 權限，所以可以觸發。
- 最近一次 run 的 runner 回報 64 GB 記憶體。核心數沒有在 log 裡，你說是 128 核。
- 現成的 `tests/kafkatest/benchmarks/core/benchmark_test.py` 的 `test_consumer_throughput` 只讀 1000 萬筆 × 100 B，約 1 GB。以目前的速度約 1 秒讀完，量到的是 JIT 暖機，不能用。

需要做的事：

1. 在本分支新增一個 ducktape 測試，資料量至少 20 GB，payload 涵蓋 100 B、1 KB、壓縮，broker 數 3。
2. 把 decode worker 原型（`FETCH-PIPELINE-06-parallel-decode-prototype.patch`）改成由測試參數控制 worker 數。
3. 推到 origin，觸發 `kafka-e2e`，對照組是 base commit `e90d6f42c2`。

限制：

- ducktape 的 broker 和 client 都是同一台機器上的 container，走 docker 網路。絕對數字不能跟 morefine 比，只能比同一次 run 內的各組。
- 那邊沒有 librdkafka。對 librdkafka 的比較只能留在 morefine。
- runner 是社群共用資源。每一次觸發都需要你明確授權。

## 6. 還沒回答的問題

- 多 broker 時 Java 單一 background thread 對 librdkafka「每個 broker 一條 thread」的差距。morefine 無法量。
- 真實壓縮比下的表現。這次的壓縮 payload 壓不下去。
- librdkafka 的批次取出 API 的吞吐量。
- 05 文件的停滯歸因量測機制還沒實作。這次用每條 thread 的 CPU 取代了它的一部分功能。

## 7. 原始資料

morefine 的 `~/kafka-bench/`：`lang-exp.log`、`matrix-exp.log`、`tuned-exp.log`、`next-exp.log`、`sys-exp.log`、`dec-exp.log`。
librdkafka 的原始碼與編譯結果留在 `~/kafka-bench/rdk/`（200 MB），供之後重跑。測試 topic 已全部刪除。
