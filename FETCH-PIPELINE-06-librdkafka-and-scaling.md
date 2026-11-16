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

## 5a. `kafka-e2e` 的結果（2026-11-15）

測試：`tests/kafkatest/benchmarks/core/fetch_pipeline_benchmark_test.py`。3 個 broker、12 partition、資料在 page cache。所有 container 在同一台主機。數字是 3 次的中位數，所有 consumer 加總（MB/s）。consumer 的 heap 是工具預設的 512 MB。

### consumer 數量（`test_consumer_scaling`）

| consumer 數 | 100 B 對照組 | 100 B 本分支 | 1 KB 對照組 | 1 KB 本分支 |
|---|---|---|---|---|
| 1 | 512 | 550 | 722 | 841 |
| 2 | 441 | 442 | 728 | 769 |
| 4 | 417 | 445 | 706 | 812 |
| 8 | 417 | 412 | 723 | 707 |

- 對照組：trunk `e90d6f42c2`，run 43658324808，`github-runner-2`（32 核、62 GB）。
- 本分支：`92a7780ce5`，run 43660360495，`jenkins-worker-2`（48 核、157 GB）。
- 兩欄跑在不同機器上，不能互相比較。`kafka-e2e` 不能指定 runner。
- 同一設定 3 次之間的差距到 20%。

【觀察】兩台機器、兩個 revision 上，加 consumer 總吞吐量都不增加。這與 morefine 的 G2 相同，但這裡有 32 與 48 核。

【推論，未驗證】瓶頸是共用的東西：主機的記憶體頻寬、Docker bridge 網路，或 broker。這個環境量不出平行化的增益，也量不出 revision 之間的差距。下一步見 `FETCH-PIPELINE-09-gcp-bench.md`。

### decode 原型（`test_decode_prototype`）

`fp-bench-proto` `92b1f3e88e`，run 43662251844，`github-runner-2`。同一次 run、同一台機器、同一份資料，所以三個數字可以互相比較。只有 100 B、單一 consumer。

| 開關 | 3 次（MB/s） | 中位數 | 對 base |
|---|---|---|---|
| base | 476、511、570 | 511 | |
| `-Dfp.lean=true` | 636、663、645 | 645 | +26% |
| `-Dfp.decode.threads=2` | 659、575、720 | 659 | +29% |

- 【觀察】減少配置（lean）在這台機器上有 26% 增益，而且 3 次的最小值高於 base 的最大值。morefine 是 15%。
- 【觀察】2 條 decode thread 的中位數高於 base，但 3 次的範圍（575 到 720）與 base（476 到 570）幾乎相接。需要更多次數才能下結論。morefine 上 decode thread 沒有增益。
- 測試在 `-Dfp.decode.threads=4` 的第 2 次失敗：`java.lang.OutOfMemoryError: Java heap space`。原因是 512 MB 的 heap 放不下 4 條 thread 預先 decode 的資料。1 KB 的組合都沒有跑到。
- 修正：測試改成所有組合固定 `-Xms2g -Xmx2g`。修正後還沒有重跑。
- 注意：512 MB 的 heap 讓 GC 成為被量測的一部分。上面所有數字都在這個條件下。morefine 的量測用的 heap 大小要另外核對，才能跨環境比較。

## 5b. GCP：broker 與 consumer 分開（2026-11-15，階段一）

腳本：`FETCH-PIPELINE-09-gcp.sh`、`FETCH-PIPELINE-09-run.sh`。原始結果：`FETCH-PIPELINE-09-results-gcp-run1/`。

- client：`c4-standard-8`（8 vCPU、30 GB、Xeon Platinum 8581C）。broker：`n2-highmem-4`（4 vCPU、32 GB），單一 KRaft broker。zone `us-central1-a`。
- 12 partition。100 B × 10 GB、1 KB × 20 GB，資料在 broker 的 page cache。consumer heap 2 GB。每個組合 3 次，60 次全部成功。
- broker 到 client 的頻寬：`iperf3` 量到 9.3 Gbit/s，約 1160 MB/s。
- MB/s 是 `kafka-consumer-perf-test` 的整體數字，包含加入 group 的時間，所以低於 broker 端看到的峰值。

### 吞吐量（3 次的中位數，所有 consumer 加總，MB/s）

| 組合 | 100 B | 1 KB | broker 峰值送出（100 B / 1 KB） |
|---|---|---|---|
| trunk，1 個 consumer | 396 | 626 | 489 / 722 |
| 本分支，1 個 | 660 | 888 | 881 / 970 |
| trunk，2 個 | 552 | 962 | 746 / 1057 |
| 本分支，2 個 | 887 | 1013 | 1032 / 1092 |
| trunk，4 個 | 837 | 1043 | 1033 / 1133 |
| 本分支，4 個 | 958 | 1094 | 1143 / 1125 |

原型（`fp-bench-proto`，1 個 consumer）：

| 開關 | 100 B | 1 KB |
|---|---|---|
| base | 681 | 851 |
| `-Dfp.lean=true` | 672 | 799 |
| `-Dfp.decode.threads=2` | 704 | 814 |
| lean 加 2 條 decode thread | 728 | 846 |

### 觀察

1. 【觀察】單一 consumer：本分支比 trunk 快 67%（100 B）與 42%（1 KB）。broker 的 CPU 峰值在 7% 到 11%，網路峰值低於上限。這兩個數字是 consumer 的差距。
2. 【觀察】trunk 加 consumer 會擴展：100 B 是 396 → 552 → 837。這與 `kafka-e2e` 的平坦曲線相反，所以那個平坦是共置造成的。
3. 【觀察】2 個與 4 個 consumer 的組合撞到網路上限。broker 送出的峰值是 1030 到 1143 MB/s，上限約 1160 MB/s。這些組合的數字不是 consumer 的上限。本分支「4 個只比 1 個多 45%」不能解讀成擴展性差。
4. 【觀察】100 B 時 CPU 沒有在等記憶體。instructions per cycle 是 2.4 到 2.6，`topdown-be-bound` 約 28% 到 30%。「小訊息受記憶體頻寬限制」在這台機器上不成立。morefine 上是否成立，還沒有硬體計數器的證據。
5. 【觀察】lean 原型在這裡沒有增益（672 對 681，在雜訊內）。morefine 是 +15%，`kafka-e2e`（512 MB heap）是 +26%。decode thread 也在雜訊內（+3%）。
6. 【推論】第 4、5 點互相一致：這台機器記憶體快、heap 夠大，配置量不是瓶頸。減少配置的增益取決於機器與 heap 大小，不是普遍成立的。
7. 1 KB 的 instructions per cycle 是 1.2 到 1.45，L1 cache miss 是 100 B 的 3 倍。1 KB 的成本是複製資料。

### 限制

- 一次執行、一對機器。3 次之間的差距最大 20%（每個組合的第 1 次常偏低）。
- `perf stat` 包住整個 JVM process，含 JIT 與 GC thread，而且只量每個組合的第 1 次。
- `mem_load_retired.l3_miss` 讀到 0，判定為這台 VM 不支援，沒有採用。
- 多 consumer 的上限需要更高的頻寬才量得到。需要先把配額提高，broker 改用 8 vCPU 以上的機型。

## 5c. GCP：網路不再是上限（2026-11-15，階段二）

環境：client `c4-standard-8`（8 vCPU，Xeon 8581C），broker `n2-standard-16`，`us-central1-a`。`iperf3` 量到 31.3 Gbit/s（約 3900 MB/s）。consumer heap 2 GB（另有標示的除外）。12 partition，單一 broker。63 次全部成功。VM 存在時間 03:28 到 04:08 UTC。

「N 個 consumer」是 N 個獨立的 `kafka-consumer-perf-test` process，各自一個 group，各自讀完 12 個 partition。

### 吞吐量（3 次的中位數，所有 consumer 加總，MB/s）

| consumer 數 | 100 B trunk | 100 B 分支 | 1 KB trunk | 1 KB 分支 |
|---|---|---|---|---|
| 1 | 445 | 892（+100%） | 736 | 1021（+39%） |
| 2 | 812 | 1585（+95%） | 1371 | 1932（+41%） |
| 4 | 1355 | 2348（+73%） | 2447 | 3219（+32%） |
| 8 | 2063 | 2591（+26%） | 3297 | 3455（+5%） |

原型（`fp-bench-proto`，100 B，1 個 consumer）：

| 組合 | MB/s |
|---|---|
| base，heap 2 GB | 890 |
| lean（`-Dfp.lean=true`），heap 2 GB | 926 |
| base，heap 512 MB | 892 |
| lean，heap 512 MB | 920 |
| `-Dfp.decode.threads=2`，heap 2 GB | 877 |

### 每個 thread 的 CPU（第 2 次重複，4 秒的 `top -H` 取樣，單次）

| 組合 | application thread | consumer network thread | 其他 |
|---|---|---|---|
| 100 B trunk，1 個 | 44% | 29% | |
| 100 B 分支，1 個 | 100% | 41% | |
| 100 B 原型 lean | 84% | 37% | |
| 100 B 原型 decode2 | 55% | 36% | 兩個 decode thread 各 39% |
| 1 KB trunk，1 個 | 19% | 30% | |
| 1 KB 分支，1 個 | 28% | 28% | |

### 觀察

1. 網路不是上限時，獨立 consumer 的總吞吐量會隨數量增加。1 到 4 個：100 B 分支 2.6 倍，1 KB 分支 3.2 倍。這回答了 07 文件的缺口 B3，也表示 `kafka-e2e` 的平坦曲線來自共置與網路。
2. 8 個 consumer 的數字不是 consumer 的上限。client 的 CPU 閒置只剩 0.1%。1 KB 的 broker 傳送量尖峰是 3866 MB/s，等於網路上限。
3. 100 B、分支、1 個 consumer：application thread 是 100%。瓶頸是 application thread 上的 decode。
4. 100 B、trunk、1 個 consumer：兩個 thread 都沒有滿（44% 與 29%）。這符合「application thread 的 `poll()` 才觸發下一個 fetch」造成兩個 thread 互相等待。分支的 +100% 主要來自拿掉這個等待。這是從數據推論，沒有用 profiler 驗證。
5. 1 KB、1 個 consumer：兩個 thread 都只有約 30%，吞吐量卻停在約 1000 MB/s。client CPU、broker CPU（最低閒置 32%，而且那是 8 個 consumer 時）、網路都沒有滿。推論：上限是單一連線上「一次只有一個 fetch 在途」的來回週期。§5d 的本機實驗修正了這個推論：上限是單一連線的位元組速率，不是每次 fetch 的固定成本。
6. decode 平行化沒有增益的原因符合第 5 點：`decode.threads=2` 把 application thread 降到 55%，吞吐量卻還是 877 MB/s，接近 1 KB 的單連線上限。瓶頸從 decode 移到了 fetch 的來回週期。
7. lean 在這台機器上沒有可分辨的增益（+4%，在各次之間的變動範圍內），heap 512 MB 與 2 GB 都一樣。lean 的增益與 heap 大小無關這一點成立；增益本身在這台機器上不存在。

### 對設計的含意

- 單一 consumer 要超過約 1000 MB/s，需要多條連線或一條連線上多個在途 fetch。只把 decode 平行化不夠。
- 分片（每個分片自己的連線加自己的 decode）同時解決第 3 點與第 5 點。獨立 consumer 的 2.6 到 3.2 倍是它的參考上限。

### 限制

- 單一 broker。多 broker 時每個 broker 各有一條連線，第 5 點的上限會不同。
- thread CPU 是單次 4 秒取樣，不是整段平均。
- client 只有 8 vCPU（4 個實體核心加 hyper-threading）。C4 配額無法提高。
- 第 4、5、6 點的因果是推論。

## 5d. 本機：單一連線的上限是位元組速率（2026-11-15）

目的：驗證 §5c 第 5 點的推論。免費，在 Mac 上用 Docker container 執行。腳本：`FETCH-PIPELINE-10-fetch-cycle.sh`。

設定：1 個 consumer、12 個 partition、1 KB、6 GB。比較 1 個 broker 與 3 個 broker（每個 broker 是 4 個 partition 的 leader）。另外用 `kafka-consumer-perf-test` 的 `--fetch-size` 改變每次 fetch 的大小。

「連線使用率」的算法：`fetch-total` × `fetch-latency-avg` ÷ 連線數 ÷ fetch 時間。1.0 表示連線上永遠有一個 fetch 在途。

### 結果

每次 fetch 的大小（本分支、1 個 broker、同一輪、3 次的中位數）：

| `--fetch-size` | 回應大小 | `fetch-latency-avg` | 每秒 fetch 數 | fetch MB/s | 連線使用率 |
|---|---|---|---|---|---|
| 256 KB | 3.1 MB | 3.0 ms | 315 | 922 | 0.95 |
| 1 MB（預設） | 12.4 MB | 11.7 ms | 82 | 957 | 0.96 |
| 4 MB | 49.6 MB | 41.3 ms | 23 | 1086 | 0.96 |

1 個 broker 對 3 個 broker（本分支、預設 fetch 大小、MB/s 的中位數）：

| 輪 | 1 個 broker | 3 個 broker |
|---|---|---|
| 第 1 輪（各 9 次） | 643 | 893 |
| 第 2 輪（9 次） | 648 | 沒有跑 |
| 第 3 輪（各 3 次） | 906 | 806 |

### 觀察

1. 本分支的連線使用率是 0.94 到 0.97。連線上幾乎永遠有一個 fetch 在途。trunk 是 0.77 到 0.88。
2. 回應大小變成 16 倍，`fetch-latency-avg` 也變成約 14 倍，吞吐量只多 18%。每次 fetch 的固定成本很小。上限是單一連線每秒能送多少位元組。
3. 1 個 broker 對 3 個 broker 沒有結論。第 1 輪是 +39%，第 3 輪是 −11%。各次之間的差距（例如 782 到 1669）大於兩組的差距。
4. consumer network thread 的 CPU：1 個 broker 是 65% 到 80%，3 個 broker 多數是 87% 到 98%。這和 GCP 不同（GCP 是 28%）。本機的瓶頸比較接近 network thread 本身。

### 含意

- §5c 第 5 點要改寫：限制不是「一次一個 fetch」造成的來回等待，而是單一連線的位元組速率。把 fetch 做成管線化（一條連線多個在途 fetch）不會有幫助；多條連線才有機會。
- GCP 上約 1000 MB/s 等於 8 Gbit/s。可能的原因有三個，還沒有分辨：GCP 對單一 TCP 連線的頻寬上限；`kafka-consumer-perf-test` 把 `receive.buffer.bytes` 固定成 2 MB；broker 對單一連線的傳送速度。
- 要分辨，需要在 GCP 量單一連線的 `iperf3`（之前量的 31 Gbit/s 是 8 條連線的總和），以及改變 `--socket-buffer-size`。結果在 §5e：原因是 broker 的 `socket.send.buffer.bytes` 預設值。

### 限制

- 這台 Mac 同時有其他工作（load average 約 8，8 核），broker 與 consumer 搶同一組 CPU。絕對值不能和 GCP 比較。
- 前兩輪的 `--fetch-size` 變化沒有生效：`kafka-consumer-perf-test` 會用 `--fetch-size` 蓋掉 `max.partition.fetch.bytes`。那兩輪的三個本分支組合都當成預設大小的重複。
- thread CPU 是單次取樣；第 3 輪的 1 個 broker 跑得太快，取樣落在結束之後，沒有 `consumer_backgr` 的數字。

## 5e. GCP：上限來自 broker 的傳送緩衝（2026-11-15，階段三）

目的：找出 §5c 第 5 點與 §5d 留下的問題的原因。腳本：`FETCH-PIPELINE-11-single-connection.sh`。機器與階段二相同（client `c4-standard-8`，broker `n2-standard-16`，`us-central1-a`）。1 個 consumer、12 個 partition、1 KB、20 GB、每個組合 3 次。

### 網路（`iperf3`，broker 傳給 client，8 秒）

| 連線數 | Gbit/s |
|---|---|
| 1 | 29.4、31.6（兩次） |
| 2 | 31.6 |
| 4 | 31.5 |
| 8 | 31.7 |
| 1，視窗限制在 200 KB（`-w 200K`） | 11.3 |

網路對單一 TCP 連線沒有上限。把視窗限制在 200 KB 時是 11.3 Gbit/s（約 1400 MB/s）。

### 吞吐量（MB/s，3 次的中位數）

broker 的 `socket.send.buffer.bytes` 預設是 102400（100 KB）。`-1` 表示交給 kernel 自動調整。

| broker `socket.send.buffer.bytes` | client `receive.buffer.bytes` | trunk | 本分支 |
|---|---|---|---|
| 102400（預設） | 2 MB（工具預設） | 734 | 948 |
| 102400（預設） | `-1` | 706 | 968 |
| `-1` | 2 MB | 1228 | 1836 |
| `-1` | `-1` | 1205 | 1681 |

其他組合（本分支，broker `-1`）：`--fetch-size` 4 MB 是 1667；把 client 的 `net.core.rmem_max` 提高到 64 MB 之後，2 MB 是 2039，32 MB 是 2177。這些都在各次之間的變動範圍內（1541 到 2287）。

### kernel 對 fetch 連線的統計（broker 端的 `ss -ti`）

| broker 設定 | 傳送緩衝（`tb`） | `delivery_rate` | 限制 |
|---|---|---|---|
| 預設 | 204800 bytes | 10.2 Gbit/s | `sndbuf_limited` |
| `-1` | 4194304 bytes | 26.7 Gbit/s | `rwnd_limited` 26.7% |

`sndbuf_limited` 表示連線在等傳送緩衝有空間。`rwnd_limited` 表示連線在等接收端讀走資料。

### 觀察

1. 原因是 broker 的 `socket.send.buffer.bytes` 預設值。改成 `-1` 之後，本分支 +94%（948 到 1836），trunk +67%（734 到 1228）。
2. client 的 `receive.buffer.bytes` 沒有可分辨的影響。這台 VM 的 `net.core.rmem_max` 是 4 MB，2 MB 的設定沒有被截斷。
3. broker 改成 `-1` 之後，限制移到接收端：連線有 26.7% 的時間在等 client 讀取。這時 application thread 與 consumer network thread 各約 55% 到 65%（只有兩次取樣）。
4. broker 改成 `-1` 之後，本分支比 trunk 快約 50%（1836 對 1228）。預設值之下是 +29%。broker 的限制拿掉之後，client 端的改進比較看得出來。
5. §5c 的階段二數字都是在 broker 預設值之下量的。1 KB 的 1 個到 4 個 consumer 那幾列受這個限制影響。

### 含意

- 這是 broker 端的設定，不是 consumer 的程式碼。使用者今天就可以設定 `socket.send.buffer.bytes=-1`。
- 改預設值需要 KIP。要先確認：延遲較高的網路上結果如何（視窗固定時，吞吐量與來回延遲成反比，所以 100 KB 在跨 zone 時影響更大）；每條連線多用的記憶體；producer 方向的 `socket.receive.buffer.bytes` 是否有同樣的問題。
- consumer 端「多條連線」的設計仍然有用，但優先順序在這個設定之後。

### 社群裡的先前討論（2026-11-15 查詢）

查了 ASF Jira（專案 KAFKA）、apache/kafka 的 pull request、網頁搜尋。

- 沒有找到提議修改 broker `socket.send.buffer.bytes` 或 `socket.receive.buffer.bytes` 預設值的 issue、KIP 或 pull request。
- [KAFKA-16496](https://issues.apache.org/jira/browse/KAFKA-16496)（Open，2024-04-09，沒有 assignee，沒有 pull request）：提議把 client 的 `receive.buffer.bytes` 預設值從 64 KB 改成 `-1`。理由相同：固定的值讓 Linux 不能自動調整視窗。回報者來自 Netflix，並連到 Linux netdev 郵件列表的討論。最後一則留言是 2024-06-28。
- [KAFKA-724](https://issues.apache.org/jira/browse/KAFKA-724)、[KAFKA-3840](https://issues.apache.org/jira/browse/KAFKA-3840)：讓 broker 與 client 接受 `-1`。只加了選項，沒有改預設值。
- [KAFKA-3463](https://issues.apache.org/jira/browse/KAFKA-3463)（2016）：把 consumer 的接收緩衝預設值改成 64 KB。

原始碼裡相關的預設值（trunk `e90d6f42c2`）：

| 設定 | 預設值 |
|---|---|
| broker `socket.send.buffer.bytes`、`socket.receive.buffer.bytes` | 100 KB |
| broker `replica.socket.receive.buffer.bytes` | 64 KB |
| consumer `receive.buffer.bytes` | 64 KB |
| producer `send.buffer.bytes` | 128 KB |

兩個要注意的地方：

- `config/server.properties`、`config/broker.properties`、`docker/server.properties` 這些範例檔明確寫了 `socket.send.buffer.bytes=102400`。只改程式碼裡的預設值，不會影響照範例檔部署的使用者。
- 我們的量測用 `kafka-consumer-perf-test`，它把 consumer 的 `receive.buffer.bytes` 設成 2 MB。一般應用程式拿到的是 64 KB。64 KB 對吞吐量的影響沒有量。

### 限制

- 單一 broker、同一個 zone，來回延遲約 0.1 到 0.3 ms。
- 每個組合只有 3 次，broker 改成 `-1` 之後各次之間的變動變大（1541 到 2287）。
- thread CPU 的取樣在執行後第 8 秒。broker 改成 `-1` 之後每次只跑約 10 秒，多數取樣落在結束之後。
- 沒有分析這次的 broker CPU。
- 沒有量 100 B，也沒有量多個 consumer。

## 5f. GCP：consumer 的接收緩衝預設值是第二個上限（2026-11-15，階段四）

問題：一般應用程式不會設定 `receive.buffer.bytes`，拿到的是 consumer 預設值 64 KB。§5e 的 client 都用 `kafka-consumer-perf-test` 的預設值 2 MB，所以沒有量到這個情況。

設定與 §5e 相同（broker `n2-standard-16`、client `c4-standard-8`、同一個 zone、12 個 partition、1 KB record、20 GB、一個 consumer）。腳本：`FETCH-PIPELINE-11-single-connection.sh` 的 `rcv` 與 `broker-buffer` 子命令。

### 吞吐量（MB/s，3 次的中位數）

| broker `socket.send.buffer.bytes` | consumer `receive.buffer.bytes` | branch | trunk | branch 對 trunk |
|---|---|---|---|---|
| 102400（預設） | 65536（預設） | 531 | 457 | +16% |
| 102400（預設） | 2 MB | 1035 | 734 | +41% |
| 102400（預設） | `-1` | 1034 | 765 | +35% |
| `-1` | 65536（預設） | 504 | 449 | +12% |
| `-1` | 2 MB | 1959 | 1059 | +85% |
| `-1` | `-1` | 2089 | 1163 | +80% |

各次的範圍：`-1`/65536 的 branch 是 372 到 551；`-1`/2 MB 的 branch 是 1281 到 2118；其他組合的範圍在中位數的 ±10% 內。

### 觀察

1. **兩邊都用預設值時，一個 consumer 只有約 450 到 530 MB/s。** 這是沒有調整過的部署實際拿到的數字。
2. **只改 broker 沒有用。** consumer 留在 64 KB 時，broker 改成 `-1` 的結果不變（531 對 504）。
3. **只改 consumer 只拿到一半。** broker 留在預設值時，consumer 改成 2 MB 或 `-1` 會停在約 1000 MB/s（§5e 的上限）。
4. **兩邊都改成 `-1`：trunk 457 → 1163（2.5 倍），branch 531 → 2089（3.9 倍）。**
5. **64 KB 時 consumer 的 thread 沒有吃滿。** 第 8 秒的取樣：background thread 約 20% CPU，應用程式 thread 約 14%。`fetch-latency-avg` 是 22 ms（2 MB 時是 5 ms），每次 fetch 的大小相同（12.3 MB）。時間花在等資料從網路進來。
6. **kernel 的數字吻合。** client 端的 `ss`：64 KB 時 `rb131072`（kernel 把要求的值加倍）、`rcv_ssthresh` 約 79 KB；`-1` 時 `rb33554432`、`rcv_ssthresh` 約 20 MB。
7. **緩衝越小，branch 對 trunk 的差距越小。** 兩邊預設值時是 +16%，兩邊 `-1` 時是 +80%。連線被緩衝限制時，client 端的改進顯示不出來。

### 含意

- KAFKA-16496（把 client 的 `receive.buffer.bytes` 預設值改成 `-1`）單獨做，在這個環境只能從約 500 到約 1000 MB/s。broker 的 `socket.send.buffer.bytes` 要一起改。
- 這支持把 broker 與 client 兩邊的預設值放進同一個 KIP。
- FetchPipeline 的效益要在緩衝不是限制的環境才量得到。之前所有用 `kafka-consumer-perf-test` 的數字都是 2 MB 的接收緩衝，不是應用程式的預設值。

### 限制

- 來回延遲約 0.1 ms、31 Gbit/s 的連結。延遲較高的網路上，固定的小緩衝影響會更大，但沒有量。
- 每個組合 3 次。一個 consumer、一個 broker、1 KB record、無壓縮。
- 沒有量 producer 方向（broker 的 `socket.receive.buffer.bytes` 與 producer 的 `send.buffer.bytes`）。
- 沒有量 follower 抓取（`replica.socket.receive.buffer.bytes`，預設 64 KB）。

## 5g. GCP：緩衝放開之後的上限與多 consumer 擴展（2026-11-16，階段五）

問題：§5e 與 §5f 之前的多 consumer 與 100 B 數字，都是在 socket 緩衝的限制下量的。緩衝放開之後，一個 consumer 被什麼限制？多個 consumer 能擴展到哪裡？

設定：機器與 §5e 相同。broker `socket.send.buffer.bytes=-1`，consumer `--socket-buffer-size=-1`。1 KB 用 40 GB 資料，100 B 用 20 GB 資料，都放得進 broker 的 page cache。每個 consumer 是獨立的 process 與獨立的 group，各自讀完整個 topic。每種資料寫完後先讀一次不記錄。腳本：`FETCH-PIPELINE-09-run.sh` 的 `FP_PLAN=phase5`。

### 吞吐量（MB/s，所有 consumer 的總和，3 次的中位數）

| record | consumer 數 | trunk | 本分支 | 本分支對 trunk |
|---|---|---|---|---|
| 1 KB | 1 | 1077 | 1607 | +49% |
| 1 KB | 2 | 1850 | 3082 | +67% |
| 1 KB | 4 | 2996 | 3417 | +14% |
| 100 B | 1 | 601 | 861 | +43% |
| 100 B | 2 | 972 | 1775 | +83% |
| 100 B | 4 | 1575 | 2561 | +63% |

本分支 1 KB、一個 consumer 的三次是 1164、1607、1943，差距很大。第一次掛著 `perf stat`。其他組合的三次都在中位數的 ±10% 內。

連結的上限約 3900 MB/s（`iperf3` 量到 31.6 Gbit/s）。本分支 1 KB、4 個 consumer 時，broker 網卡的送出量是 3300 到 3860 MB/s（`sar`），已在連結上限。

### 和階段二（§5c，broker 傳送緩衝是預設值）比較，本分支

| record | consumer 數 | 階段二 | 這次 |
|---|---|---|---|
| 1 KB | 1 | 1021 | 1607 |
| 1 KB | 2 | 1932 | 3082 |
| 1 KB | 4 | 3219 | 3417 |
| 100 B | 1 | 892 | 861 |
| 100 B | 2 | 1585 | 1775 |
| 100 B | 4 | 2348 | 2561 |

1 KB 的數字變了。100 B、一個 consumer 的數字沒有變：它本來就低於約 1000 MB/s 的緩衝上限，所以沒有被緩衝限制。

### consumer 的 thread CPU（整個執行期間每 2 秒取樣，平均值）

| 組合 | 應用程式 thread | background thread |
|---|---|---|
| 1 KB，trunk，1 個 consumer | 約 31% | 約 46% |
| 1 KB，本分支，1 個 consumer（1943 MB/s 那次） | 57% | 53% |
| 100 B，trunk，1 個 consumer | 約 59% | 約 40% |
| 100 B，本分支，1 個 consumer | 80% 到 96% | 37% 到 52% |
| 100 B，本分支，2 與 4 個 consumer | 87% 到 96% | 38% 到 46% |

### broker 端 kernel 對 fetch 連線的統計（`ss -ti`，每 5 秒）

`busy` 是連線有資料要送的累計時間。`sndbuf_limited` 與 `rwnd_limited` 是其中等傳送緩衝、等接收端的時間。

| 組合 | `busy` 佔經過時間 | `sndbuf_limited` | `rwnd_limited` |
|---|---|---|---|
| 1 KB，trunk，1 個 consumer | 約 70% | 低於 2% | 0 |
| 1 KB，本分支，1 個 consumer | 約 100% | 低於 1% | 0 |
| 100 B，本分支，1 個 consumer | 約 60% | 低於 3% | 低於 1% |

broker 沒有任何 thread 超過 15% CPU。

### 觀察

1. **100 B 時，本分支的上限是應用程式 thread。** 它在 80% 到 96%。record 的解碼在這條 thread 上（`poll()` 裡）。連線只有約 60% 的時間有資料要送，表示 broker 在等 consumer。
2. **1 KB 時，本分支的上限在 TCP 連線本身，不在 consumer 的 thread。** 三次執行裡，連線都是 100% 的時間有資料要送，而且不是在等緩衝或接收端。consumer 的兩條 thread 都在 57% 以下。
   同一次執行內，每 5 秒送出的量在 2.7 GB 到 12 GB 之間擺動。kernel 量到的來回時間（`rtt`）在 0.5 ms 與 3 到 6 ms 之間跳動，來回時間高的時候送得慢。重送的量很小（40 GB 裡約 1 MB）。
   來回時間變長的原因沒有量到。可能是 client 的 kernel 處理不過來（一條連線的接收由一個 CPU 處理），也可能是雲端的虛擬網路。三次的差距（1164、1607、1943）來自這個擺動。
3. **trunk 讓連線閒置。** 1 KB 時連線只有約 70% 的時間有資料要送，兩條 thread 也都沒有吃滿。這和 §5c 的「連線使用率」一致。
4. **多個 consumer 可以擴展，直到連結上限。** 1 KB 時本分支 2 個 consumer 就到 3082 MB/s。
5. **100 B 時 2 個 consumer 幾乎是 2 倍**（861 → 1775），4 個是 3.0 倍。client 只有 8 個 vCPU，4 個 consumer 各有一條接近吃滿的 thread，再加上 background thread 與 JIT。

### 含意

- 小 record 的下一步在應用程式 thread：減少每筆 record 的解碼與配置成本，或把解碼分到別的 thread。
- §5c 的兩個原型值得在緩衝 `-1` 下重量。當時 lean 是 926 MB/s、`decode.threads=2` 是 877 MB/s，基準是 890。基準沒有被緩衝限制，但任何改進都會立刻碰到約 950 到 1000 MB/s 的緩衝上限。§5c 第 6 點把 877 解釋成「瓶頸移到 fetch 的來回週期」，現在看來更可能是碰到緩衝上限。這是推論，還沒有量。
- 大 record 的下一步不在 consumer 的 thread。一條連線能送多少由 TCP 連線決定，要更高的吞吐量要用更多連線。這是 share-nothing consumer 那邊的題目。這也是推論：昨天同樣設定、20 GB 資料量到 2089 MB/s（§5f），今天中位數是 1607，兩個數字都是實測。

### 限制

- 沒有記錄 client 整機的 CPU 與網路（`mpstat`、`sar`），腳本漏了這一步。所以第 2 點的 kernel 處理只是推測，第 5 點也不能確定 4 個 consumer 時 client 的 CPU 是否用完。
- 每個組合 3 次。
- trunk 100 B、一個 consumer 從階段二的 445 變成 601，原因不明。
- 資料必須放得進 page cache，所以最快的組合每次只跑約 20 秒。
- 一個 broker、同一個 zone、無壓縮。

## 6. 還沒回答的問題

- 多 broker 時 Java 單一 background thread 對 librdkafka「每個 broker 一條 thread」的差距。morefine 無法量。
- 真實壓縮比下的表現。這次的壓縮 payload 壓不下去。
- librdkafka 的批次取出 API 的吞吐量。
- 05 文件的停滯歸因量測機制還沒實作。這次用每條 thread 的 CPU 取代了它的一部分功能。

## 7. 原始資料

morefine 的 `~/kafka-bench/`：`lang-exp.log`、`matrix-exp.log`、`tuned-exp.log`、`next-exp.log`、`sys-exp.log`、`dec-exp.log`。
librdkafka 的原始碼與編譯結果留在 `~/kafka-bench/rdk/`（200 MB），供之後重跑。測試 topic 已全部刪除。

本機 fetch 週期實驗（§5d）：`FETCH-PIPELINE-10-results-local/fetch-cycle-results.tgz`（三輪的原始輸出與 `summarize.py`）。

GCP 階段三（§5e）：`FETCH-PIPELINE-11-results-gcp/`（client 與 broker 的原始輸出、`summarize.py`）。

GCP 階段四（§5f）：`FETCH-PIPELINE-12-results-gcp/client-results.tgz`（`progress.log` 有每次的吞吐量；每次執行另有 `.out`、`.ss`、`.threads`）。

GCP 階段五（§5g）：`FETCH-PIPELINE-13-results-gcp/`（`client/results.tgz`、`broker/results.tgz`、`summarize.py`）。
