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
5. 1 KB、1 個 consumer：兩個 thread 都只有約 30%，吞吐量卻停在約 1000 MB/s。client CPU、broker CPU（最低閒置 32%，而且那是 8 個 consumer 時）、網路都沒有滿。推論：上限是單一連線上「一次只有一個 fetch 在途」的來回週期。沒有驗證。
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

## 6. 還沒回答的問題

- 多 broker 時 Java 單一 background thread 對 librdkafka「每個 broker 一條 thread」的差距。morefine 無法量。
- 真實壓縮比下的表現。這次的壓縮 payload 壓不下去。
- librdkafka 的批次取出 API 的吞吐量。
- 05 文件的停滯歸因量測機制還沒實作。這次用每條 thread 的 CPU 取代了它的一部分功能。

## 7. 原始資料

morefine 的 `~/kafka-bench/`：`lang-exp.log`、`matrix-exp.log`、`tuned-exp.log`、`next-exp.log`、`sys-exp.log`、`dec-exp.log`。
librdkafka 的原始碼與編譯結果留在 `~/kafka-bench/rdk/`（200 MB），供之後重跑。測試 topic 已全部刪除。
