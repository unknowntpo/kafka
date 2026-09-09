# CONSUMER-NG 02：三方基準線與 profile（2026-09-09）

機器：homelab `morefine`（Intel N150 4 核、31 GB、Ubuntu 24.04、JDK 17 跑 trunk 發行包 `820533b870`）。broker 單節點在同一台機器。對照組：librdkafka（`rdkafka_performance -G`，master，mklove 建，無壓縮/SSL）、franz-go（`examples/bench -consume -group`，Go 1.25）。原始數據與腳本在 `CONSUMER-NG-bench/`。

## 1. 第一個發現是量測方法：短跑的 Java 數字被 JIT 灌水

固定筆數的短跑（3–10 秒）裡，C2 編譯執行緒佔了 **54%** 的 CPU 樣本（`profiles/t6p-cpu.collapsed.gz`）。用 `steady.py`（對齊 `kafka-consumer-perf-test --show-detailed-stats` 的逐秒統計與 `/proc/<pid>/stat`，取暖機 8–10 秒之後的 ΔCPU/ΔGB）之後：

| cell | whole-run CPU s/GB | 穩態 CPU s/GB | 暖機期燒掉的 CPU |
|---|---:|---:|---:|
| T2 6p 1 KB（12 GB） | 2.29 | **1.18** | 16.5 s |
| T2 1p 100 B（4 GB） | 5.96 | **2.74** | 13.6 s |
| T1 6p 1 KB | 1.69 | **1.07** | 9.7 s |
| T1 1p 100 B | 4.63 | **2.96** | 7.8 s |

結論：第 2 節的 fixed-count 表只能拿來看對手；Java 一律引用穩態欄。**暖機本身是一個缺口**（每個 process 8–17 秒 CPU），對短命的 T1 工作是主要成本。

## 2. 基準線

fixed-count / fixed-duration 三輪中位數（`baseline-T2.csv`、`baseline-T1.csv`；Java 欄位改用穩態值）：

### T2（4 核）

| 實作 | 6p 1 KB MB/s | 6p CPU s/GB | 1p 100 B MB/s | 1p CPU s/GB | 閒置 60 s CPU | 閒置自願 ctx switch | RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| Java consumer（穩態） | 1,078 | 1.18 | 496 | 2.74 | 5.9 s | 8,356 | 1.4–1.8 GB（`-Xms2G`；256 MB heap 時穩態不變） |
| Java classic（穩態） | 1,091 | 1.04 | — | — | — | — | — |
| librdkafka | **1,295** | 1.32 | 114 | 9.96 | **0.14 s** | **791** | 24–277 MB |
| franz-go | 768 | 1.32 | 216 | 7.35 | **0.07 s** | 1,355 | 24–32 MB |

### T1（`taskset -c 0`，1 核）

| 實作 | 6p 1 KB MB/s | 6p CPU s/GB | 1p 100 B MB/s | 1p CPU s/GB | 閒置 60 s CPU | 閒置自願 ctx switch |
|---|---:|---:|---:|---:|---:|---:|
| Java consumer（穩態） | 920 | 1.07 | 335 | 2.96 | 4.7 s | 10,134 |
| Java classic（穩態） | 885 | 1.12 | — | — | — | — |
| librdkafka | **1,218** | **0.88** | 202 | 5.33 | 0.14 s | 835 |
| franz-go | 1,024 | 0.91 | 250 | 3.78 | 0.05 s | 757 |

讀法：

- **小 record（每筆固定成本）Java 大幅領先**：1p 100 B 的吞吐是對手的 1.3–4 倍、CPU/GB 是對手的 1/1.3–1/3.6。JVM 的物件配置與 JIT 內聯在這種負載上是優勢，不是包袱。
- **bytes 路徑（1 KB）Java 穩態落後 17–25% 吞吐**；CPU/GB 在 4 核與 C/Go 同級（1.18 vs 1.32），單核落後 20%（1.07 vs 0.88）。
- **閒置差一個數量級**：Java 每秒醒約 140–170 次、60 秒燒 4.7–5.9 秒 CPU（含 JVM 啟動約 1–2 秒）；對手每秒醒 13–22 次、燒 0.05–0.14 秒。
- **記憶體**：RSS 差距主要是 heap 設定（本測試 `-Xms2G`）。256 MB heap 的穩態 CPU/GB 與 2 GB 幾乎相同，所以「有界」是可達的，但 JVM 的 RSS 下限仍會是幾十 MB 級而非 24 MB。
- librdkafka 的 1p 100 B 在單核（202）比 4 核（114）快：它的 broker thread 與 app thread 跨核來回是瓶頸。設計 shared-nothing 時要避免同樣的陷阱。

## 3. profile：Java 的 CPU 去了哪裡（itimer，1 ms；`analyze-collapsed.py`）

以 6p 1 KB（bytes 路徑）為主，1p 100 B（per-record 路徑）補充：

**背景執行緒（`consumer_background_thread`，佔非 JIT 樣本約 60%）**

| 成本 | 佔背景執行緒 | 說明 |
|---|---:|---|
| `libc read`（kernel → user 複製） | 34% | socket 讀取本身，C/Go 也付 |
| **`memset`** | **15%** | 每個 fetch 回應 `ByteBuffer.allocate` 一塊新 heap buffer 要清零 |
| **arraycopy + `Unsafe.copyMemory`** | **10%** | JDK 對 heap buffer 的 socket 讀取要先進暫存 direct buffer 再複製一次 |
| metrics / `Sensor.record` | 4–7% | 每個回應與每筆 record 都碰 sensor（鎖） |
| `epoll_wait` | 2–6% | 小回應時喚醒頻繁 |
| FetchResponse 解析與 CompletedFetch 建立 | 7–11% | |

**app thread（`main`）**

| 成本 | 6p 1 KB | 1p 100 B | 說明 |
|---|---:|---:|---|
| `Utils.toArray` + arraycopy（value 複製成 `byte[]`） | 28% | 11% | `ByteArrayDeserializer` 路徑；KIP-863 的 `ByteBuffer` 多載可免 |
| ConsumerRecord 建構（`Optional`、`RecordHeaders`、兩個 `ByteBuffer` slice、`ConsumerRecord`） | 53%（含上列） | 70%（含上列） | 每筆 6–8 個物件；`maybeLeaderEpoch` self 21% 是內聯歸因 |
| metrics | 8% | 6% | |
| GC 執行緒 | 1% | 1% | 2 GB heap 下微不足道；小 heap 需再量 |

## 4. 缺口與對應的槓桿（都是假設，每一項要有自己的量測才算數）

| 缺口（穩態） | 目標 | 槓桿 | 預期 |
|---|---|---|---|
| 閒置每秒醒 140–170 次、5 s/60 s | ≤ 每秒 5 次、≤ 0.3 s/60 s | 唯一的週期性來源只剩 `fetch.max.wait.ms` 的空回應與 heartbeat；app thread 只在有資料或有事件時醒；沒有 100 ms 的安全網 timer | 10× |
| bytes 路徑吞吐 −17–25%，單核 CPU/GB −20% | ≥ librdkafka | (a) fetch 續發不等 app poll（前一條線量到 +47–69% 的 pipeline）；(b) 池化的 direct 接收 buffer：免 memset、免 JDK 暫存複製（背景執行緒 −25%）；(c) 在接收 buffer 上直接迭代 record，不複製 | 吞吐 +30–50%，bg CPU −25% |
| 每筆 record 的物件與複製 | 再降 30% | `ByteBuffer` 反序列化路徑、無 header 時的共享空 headers（要在 API 語意內）、metrics 每批一次不每筆一次 | app CPU −30% |
| 暖機 8–17 s CPU / process | 接近 0 | JDK 25 的 AOT cache（JEP 483/515）、減少 megamorphic call site 讓 C2 更快收斂；量 time-to-steady-state | T1 短命工作的主要收益 |
| RSS 由 heap 設定決定 | 可預期、有上限 | 接收 buffer off-heap 池 + credit 總量上限；heap 只放 record 物件 | RSS = 設定值 + 幾十 MB |
| T3（8 核以上）單 consumer 擴展 | 線性到連線數 | 每個 broker 連線一條執行緒（shared-nothing）、每 partition SPSC 佇列、app thread 只合併 | 待量 |

不做的：改 `ConsumerRecord` 的公開形狀、要求使用者的 `Deserializer` thread-safe（提供 opt-in）。

## 5. 下一步

1. 用 `t6p12` 與 `steady.py` 把 T2/T1 的 Java 穩態再各跑 2 輪確認區間，並補 256 MB heap 的 RSS。
2. 對 (a)+(b)（pipeline + direct 接收池）做第一個可量的原型，只量 6p 1 KB 穩態與閒置。
3. 閒置：先寫出 60 秒內每一次喚醒的來源清單（strace `-e epoll_wait,futex` 計數），設計時逐一消掉。
