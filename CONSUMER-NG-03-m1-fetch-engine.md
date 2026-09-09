# CONSUMER-NG 03：M1 fetch engine 第一版的結果（2026-09-09）

M1 只做 02 §4 的兩個槓桿 (a)(b)，範圍刻意小：手動 assign、沒有 group / commit / metrics / rebalance。目的是量「fetch 路徑重做之後，Java 的 bytes 與 per-record 成本能到哪裡」。程式碼在 `clients/consumer-ng`（Java 21 模組）：

- `DirectBufferPool`：接在 `Selector` 的 `MemoryPool` 掛鉤上的有界 direct buffer 池。socket 直接讀進 direct buffer（免 JDK 暫存複製），buffer 重用不清零（免 memset），池滿時 `tryAllocate` 回 null，network 層停止讀該連線，資料留在 broker（記憶體上限即 backpressure）。
- `FetchEngine`：一條 I/O 執行緒，重用 `NetworkClient` / `Selector` / `FetchSessionHandler` / `FetchRequest`；每個 broker 一個 fetch 在飛，前一個回來就發下一個，只要「在途 + 已排隊未交付」的 bytes 低於 credit。閒置時自己沒有任何 timer，唯一週期是 broker 的 `fetch.max.wait.ms`。
- `FetchSegment` + `RecordReader`：回應裡每個 partition 的 records 以 segment 交給 app thread，仍指向接收 buffer；app thread 原地迭代 batch / record，交付完釋放；同一回應的 segment 共用一個 refcount，最後一個釋放時 buffer 回池。
- `bench/ConsumeBench`：印出與 `kafka-consumer-perf-test --show-detailed-stats` 相同格式的逐秒統計，`steady.py` 直接可用；`--loops` 讓穩態視窗不受 topic 大小限制。

## 1. 結果（morefine，穩態，CRC 檢查開啟）

「bytes」= `ByteArrayDeserializer`（每筆 value 複製一次，與 trunk 的 perf 工具相同）；「ByteBuffer」= KIP-863 的 `ByteBufferDeserializer`（零複製）。對照組數字來自 02。

### 6 partition × 1 KB（bytes 路徑）

| | T2（4 核）MB/s | T2 CPU s/GB | T1（1 核）MB/s | T1 CPU s/GB |
|---|---:|---:|---:|---:|
| trunk consumer | 1,078 | 1.18 | 920 | 1.07 |
| librdkafka | 1,295 | 1.32 | 1,218 | 0.88 |
| franz-go | ~1,000 | 1.32 | 1,024 | 0.91 |
| **M1 bytes** | **1,502** | **1.08** | **1,236** | **0.81** |
| **M1 ByteBuffer** | **1,770** | **0.84** | **1,385** | **0.68** |
| M1 bytes，heap 256 MB | 1,491 | 1.11 | — | — |

### 1 partition × 100 B（per-record 路徑）

| | T2 MB/s | T2 rec/s | T2 CPU s/GB |
|---|---:|---:|---:|
| trunk consumer | 496 | 5.2M | 2.74 |
| franz-go | 216 | 2.2M | 7.35 |
| librdkafka | 114 | 1.1M | 9.96 |
| **M1 bytes** | **788** | **8.3M** | **2.22** |
| **M1 ByteBuffer** | **972** | **10.2M** | **1.96** |

### 記憶體

12 GB 的 6p 消費、heap 256 MB：RSS 326 MB（heap 256 + direct 池 47 MB + JVM），穩態吞吐與 CPU/GB 與 2 GB heap 相同。池的實際配置量由 credit（預設 64 MB）與 `fetch.max.bytes` 決定，不隨 partition 數或 broker 吐資料的速度增長，這就是 01 §1 目標 4 要的「有界」。

### 閒置

（穩態視窗：第 10–70 秒；見 §3 的量測修正）

| | CPU 秒 / 分鐘 | 自願 context switch / 秒 | RSS |
|---|---:|---:|---:|
| 空 JVM 地板（`Thread.sleep`） | 0.02 | 30 | 96 MB |
| trunk consumer | 1.96 | 68 | 243 MB |
| **M1** | **0.82** | **38** | 165 MB |
| librdkafka / franz-go（02，整段 60 秒含啟動） | ≤ 0.14 | 13–22 | 24 MB / 14 MB |

扣掉 JVM 地板，trunk 每分鐘 1.94 秒、每秒醒 38 次；M1 每分鐘 0.80 秒、每秒醒 8 次。M1 的 8 次/秒來自：broker 每 500 ms 一次的空 fetch 回應（2 次）、pool 釋放時的自我喚醒（2 次）、app thread 的 1 秒輪詢（1 次）與其餘 JVM 雜訊；CPU 仍比 librdkafka 高約 6 倍，下一步先 profile 閒置期的 I/O thread。

## 2. 讀法

- **在 4 核與 1 核上，M1 的吞吐與 CPU/GB 都已超過 librdkafka 與 franz-go**，而且是在 CRC 檢查開啟、value 仍複製一次的設定下；零複製再加 15–25%。這回答了 01 的問題：Java 沒有輸的理由，輸的是舊的 fetch 路徑（heap 接收 buffer 的清零與二次複製、fetch 續發等 app poll、每筆 record 的物件與複製），不是語言。
- **單核的收益比四核大**（CPU/GB −24% 對 −8%）：因為省掉的是複製與 memset 這種純 CPU 工作，核心越少越明顯。
- **小 record 也贏**：per-record 路徑沒有特別優化（仍走 `DefaultRecordBatch` 的迭代與 `ConsumerRecord` 建構），增益來自 fetch 續發不再等 poll 與交接變輕。這條路徑還有 02 §3 列的物件成本可以拿。
- **JIT 暖機仍在**：whole-run 對穩態的差距（1.30 對 1.08 s/GB）就是每個 process 10–17 秒 CPU 的暖機。這是下一個目標，與 fetch 路徑無關。

## 3. 過程中修掉的東西與量測修正

- **接收 buffer 的 refcount 競態**（單核才穩定重現，CRC 檢查抓到 `CorruptRecordException`）：I/O thread 把回應的第一個 partition 做成 segment 放進佇列後，app thread 立刻消費並釋放，refcount 歸零、buffer 回池被下一個回應覆寫，而同一回應的其他 partition 還沒做成 segment。修法：owner 建立時就持有一個「建立者引用」，全部 segment 建完才放掉（`Owner.creationDone`）；回歸測試 `receiveBufferIsFreedOnlyAfterCreationIsDoneAndEverySegmentIsReleased`。教訓：**單核（`taskset -c 0`）是找交接競態最好的環境**，四核上跑一天也不一定碰到。
- **閒置量測要扣掉啟動**：先前「閒置 60 秒」的 syscall 數裡，主執行緒每秒 137 次 `read` 其實是啟動期讀 jar 做 class loading（`strace -y` 看到 fd 是 `kafka-clients.jar`、`log4j-core.jar`、`jackson`），不是閒置行為。閒置改用第 10–70 秒的增量（`idle-steady.sh`）。
- `assign()` 必須同步建立佇列並告訴 `SubscriptionState`（`ConsumerMetadata` 靠它決定 metadata request 要問哪些 topic）；不然 metadata 永遠不含該 topic。
- I/O thread 在 pool 釋放時對自己 `wakeup()`（每個空回應一次 `write` + 一次多餘的 `epoll_wait` 返回），待修：只有跨執行緒釋放且引擎因 credit 停下時才需要喚醒。

## 4. 下一步（依 02 §4 的缺口）

1. **閒置**：拿到 §1 的閒置穩態數字後，把 I/O thread 的自我喚醒與 app thread 的 1 秒輪詢去掉（app thread 只在 `onData` 或 timeout 醒）。
2. **暖機**：量 JDK 25 AOT cache（`-XX:AOTCache`）對 time-to-steady-state 的效果；這是 T1 短命工作的主要成本。
3. **per-record 物件**：每筆 6–8 個物件裡，`Optional`、空 `RecordHeaders`、兩個 slice 可以減少；量 1p 100 B。
4. **功能面**：group membership / commit / rebalance 要在同一個 I/O 執行緒上重做（不能回到 `RequestManager` 的輪詢模型，那是前一條線證明過的平局）；這是 M2 的範圍，做完才能跑 `consumer_test.py`。
5. **T3**：每個 broker 一條執行緒的 shared-nothing 版本，在 8 核筆電上量線性度。
