# FETCH-PIPELINE 03：第一輪量測（2026-09-18，morefine）

base = `upstream/trunk` `e90d6f42c2`，fp = 本 branch `31ccd975d0`（三道 gate 拆除、credit 1×、`sendPrefetches` 刪除）。兩者只差 `kafka-clients` jar；同一個 broker（同機，4 核）、同一份資料、`group.protocol=consumer`、`kafka-consumer-perf-test`、`ByteArrayDeserializer`、CRC 開啟、無壓縮。數字是 `steady.py` 的穩態視窗（warmup 之後的增量），不是整段平均。7 輪交錯（每輪內 base/fp 順序對調），跑前確認機器閒置（load 0.01）。

## 1. 吞吐與 CPU/GB（中位數，括號內是 min–max）

| 工作負載 | base MB/s | fp MB/s | 倍率 | base 秒/GB | fp 秒/GB | CPU/GB |
|---|---:|---:|---:|---:|---:|---:|
| 1p × 100 B（n=7） | 513.3（506–522） | 801.2（778–809） | **1.56×** | 2.622 | 2.752 | +5.0% |
| 1p × 100 B，`max.poll.records=50`（n=7） | 356.7（352–368） | 527.9（525–533） | **1.48×** | 4.819 | 3.739 | **−22.4%** |
| 6p × 1 KB（n=14） | 1,069.0（1,050–1,082） | 1,184.5（1,156–1,206） | 1.11× | 1.221（1.13–1.32） | 1.474（1.37–1.55） | **+20.7%** |

base 自身的離散度是 3.0–4.7%，三個吞吐訊號都遠在包絡之外。base 的 1p×100B（513）與 6p×1KB（1,069）跟 consumer-ng 文件記的 trunk（496、1,078）對得上，環境一致。

**6p × 1 KB 的 CPU/GB 退步是真的**：n=14，base 最大值 1.322 < fp 最小值 1.368，區間不重疊。

## 2. per-thread CPU（整段，含約 2–3 秒 JVM 啟動，所以核心占比略為低估）

| | wall | 背景執行緒 | app 執行緒 | 總 CPU |
|---|---:|---:|---:|---:|
| 6p×1KB base | 17.0 s | 9.95 s（0.59 核） | 5.17 s（0.30 核） | 23.9 s |
| 6p×1KB fp | 15.7 s | 10.97 s（0.70 核） | 6.70 s（0.43 核） | 27.4 s |
| 1p×100B base | 25.9 s | 11.59 s（0.45 核） | 13.86 s（0.54 核） | 36.6 s |
| 1p×100B fp | 18.1 s | 10.96 s（0.61 核） | 14.03 s（**0.78 核**） | 38.5 s |

讀法：

- **6p×1KB：兩條執行緒都離飽和很遠**（app 0.43 核、背景 0.70 核）。瓶頸不在 consumer 的執行緒，在同機的 broker 與 loopback。這個工作負載上任何「加執行緒」的設計都沒有東西可以解放。
- **6p×1KB 的 CPU 退步兩條執行緒都有份**：背景 +10%、app **+30%**、JIT +1 秒。app 執行緒也變貴，指向**鎖競爭**而不是單純多做事：gate 1 拆掉後 `prepareFetchRequests` 每個 pass 都跑，`FetchBuffer.bufferedBytesByPartition()` 每次上 `FetchBuffer` 的鎖重建 map（跟 app 執行緒的 peek/poll 搶同一把鎖），`bufferedBytesByNode` 對每個有 buffer 的 partition 各做一次 `SubscriptionState` monitor 內的查詢（跟 app 執行緒的 position 更新搶）。成本 ∝ partition 數 × pass 頻率，所以 1p 看不到、6p 高吞吐時浮現。**待修**：bytes 帳改成增量維護，不要每 pass 在鎖內重建。**（這個假說已被 §4 推翻：profile 顯示 `bufferedBytesByPartition` 只佔背景執行緒 0.4%，多出的 CPU 在 deserializer 的 memcpy。）**
- **1p×100B：fp 的 app 執行緒到 0.78 核**（扣掉啟動大約 0.85–0.9），接近飽和。小 record 的瓶頸已經從「等 fetch」移到「app 執行緒的 per-record 成本」。這是 01 的 L2（per-batch 物件共用）該接手的地方；它只有一個 partition，任何 partition 級的平行化都幫不了它。

## 3. 過程中修掉的量測問題（留給下一次）

- 資料被 retention 清空（broker 跑了 9 天，`log.retention.hours=168`）：症狀是 consumer 卡到 timeout、0 筆、lag=0。先查 `kafka-get-offsets --time -2` 與 `-1` 是否相等。
- 資料集超過 page cache（30 GB vs 25 GB）會變成量 NVMe：base 的 6p×1KB 掉到 387 MB/s，`/proc/diskstats` 顯示 232 MB/s 來自磁碟。資料集總和必須塞得進 cache，跑前 `cat` 一次預熱。
- 資料集太小則沒有穩態視窗：3.7 GB 在 500 MB/s 下 8 秒跑完。1p×100B 用 100M 筆（9.3 GB）、6p×1KB 用 12M 筆（11.7 GB），warmup 8 s / 6 s。
- 跑完刪 topic（`big100b`、`t6p12`）與 consumer group，還原 dist 的 clients jar。

---

以下 §4–§8 是第二輪。共同條件：morefine（Intel N150，4 顆 E-core，6 MB L3），broker 同機，topic `t6p12` = 6 partition × 12M 筆 × 1 KB，page cache 預熱。base = `upstream/trunk` `e90d6f42c2`；fp = 本 branch 加 buffer 重用之前的版本。CPU/GB = 穩態每消費 1 GB 用掉的 CPU 秒數。除非另註，都是 3 次的中位數。

## 4. 發現 A：fp 的 CPU/GB 退步是平台效應，不是程式碼問題

### 4.1 credit 大小實驗

| `fetch.max.bytes` | base MB/s | base CPU/GB | fp MB/s | fp CPU/GB | CPU/GB 差 |
|---|---:|---:|---:|---:|---:|
| 50 MB | 1,076 | 1.228 | 1,156 | 1.595 | +30% |
| 8 MB | 1,071 | 1.215 | 1,188 | 1.561 | +28% |
| 2 MB | 977 | 1.390 | 953 | 1.562 | +12% |

- fp 的 CPU/GB 不管 credit 多大都約 1.56。
- 2 MB 的差距變小，只是因為 base 自己變差。
- 所以兩個原本的假說都不成立：「工作集被擠出 cache」與「每個 pass 的固定開銷」。

### 4.2 async-profiler（itimer，per thread）

- fp 在 app 執行緒多出的 CPU，全部是 deserializer 把 record value 複製成 `byte[]` 的 memcpy：1,092 → 1,807 個樣本（+65%）。
- 沒有任何新的程式碼路徑有可見成本。
- `FetchBuffer.bufferedBytesByPartition` 只佔背景執行緒 0.4%。

### 4.3 Apple M1 Pro 交叉驗證（本機 broker，同一工作負載，3 對，中位數）

| | MB/s | CPU/GB |
|---|---:|---:|
| base | 498 | 0.834 |
| fp | 628 | 0.801 |
| 差 | **+26%** | **−4%** |

在記憶體頻寬充足的機器上，退步消失。

**解讀**：在 N150 上，fp 讓兩條執行緒的大量記憶體複製同時發生，它們搶記憶體頻寬與 L3。多出來的 CPU 秒是 stall cycle，不是多做的事。

**決定**：credit 維持 `1 × fetch.max.bytes`。

## 5. 發現 B：consumer 的 CPU 花在哪（fp，1 KB record，每 GB）

| 執行緒 | 秒/GB | 組成 |
|---|---:|---|
| 背景 | 0.59 | `read()` syscall 36%；每個回應配一塊新接收 buffer 的 memset 19%；JDK 從暫存 direct buffer 複製到 heap 接收 buffer 14%；其餘（回應解析、metrics、selector）約 27% |
| app | 0.27 | value 複製成 `byte[]` 約 46%（`byte[]` deserializer 本來就要做）；每筆 record 的 `maybeLeaderEpoch` / `Optional` 約 11%（歸因不確定）；每筆 record 的配置 |

- 在這台機器上，背景執行緒把單一 consumer 限制在約 1.7 GB/s。
- 背景執行緒的 33%（memset + 暫存 buffer 複製）是可以省掉的複製與清零。這是 §7 的依據。

## 6. 發現 C：decode 分片的門檻實驗（condition-1）

condition-1：app 執行緒 ≥ 0.9 核，且背景執行緒 < 0.7 核。意思是 app 執行緒飽和、背景還有餘裕，這時把 decode 分給多條執行緒才有東西可以解放。

以下是第 2 輪的數字。第 1 輪 fp 的數據被 page cache 回寫干擾（剛 produce 完 30 GB）。核心數 = CPU 秒 ÷ 含啟動的 wall，所以穩態值略高。

| 工作負載 | fp MB/s | fp app 核 | fp 背景核 | base MB/s | base app 核 | base 背景核 | 過門檻？ |
|---|---:|---:|---:|---:|---:|---:|---|
| 6p × 100 B | 611 | 0.77 | 0.61 | 442 | 0.53 | 0.44 | 否 |
| 6p lz4（1 KB） | 925 | 0.77 | 0.68 | 721 | 0.49 | 0.42 | 否 |
| 6p zstd（1 KB） | 719 | 0.87 | 0.37 | 603 | 0.69 | 0.25 | **是** |

第 1 輪 zstd 對得上：fp 697 MB/s、app 0.88、背景 0.40；base 601、app 0.69、背景 0.25。

- 只有 zstd 過門檻（0.87 是含啟動的值）。扣掉啟動之後，app 執行緒在穩態實際上約 1 核，瓶頸是解壓縮。
- decode 分片只會幫到重度壓縮的工作負載，所以維持延後（02 §4.7）。

## 7. 發現 D：接收 buffer 重用（`MemoryPool`）

### 7.1 原型（同一個 jar，用 system property 切模式；none = fp 對照組）

| 模式 | MB/s | CPU/GB | 背景 秒/GB | app 秒/GB |
|---|---:|---:|---:|---:|
| none | 1,230 | 1.43 | 0.74 | 0.48 |
| heap 重用 | 1,583 | 1.20 | 0.56 | 0.45 |
| direct 重用 | 1,588 | 1.19 | 0.51 | 0.45 |

- 吞吐 +29%，CPU/GB −16%。
- 背景執行緒 −25%（heap）/ −31%（direct），跟 §5 profile 預測的 33% 對得上。
- direct 沒有吞吐優勢，卻多了 native 記憶體、`Cleaner`、以 direct buffer 解析的風險。所以選 heap。

### 7.2 最終版本（3 對交錯，中位數）

| | MB/s | CPU/GB |
|---|---:|---:|
| base | 1,086 | 1.30 |
| fp | 1,212 | 1.475 |
| **final（fp + 重用）** | **1,613** | **1.10** |
| final vs base | **+49%** | **−15%** |

### 7.3 正確性

- M1 上的 smoke test：三種模式都拿回全部 2,000,000 筆，CRC 檢查開啟。
- 新增測試：`CachingMemoryPoolTest`、`FetchMetricsAggregatorTest`、`ConsumerUtilsTest`、`CompletedFetchTest`（buffer 被清掉之後，record 與例外都不再引用它）、`NetworkClientTest`（只有被要求時，回應才把接收 buffer 還給 pool）。
- clients / network / memory 共 4,808 個 unit test 通過。唯一例外是 `SslTransportLayerTest.testSelectorPollReadSize`，它在這台 Mac 的 base commit 上以同樣方式失敗（環境問題）。

設計決定見 01 §5a。

## 8. 結論

- **6p × 1 KB 最終版本對 base：吞吐 +49%（1,086 → 1,613 MB/s），CPU/GB −15%（1.30 → 1.10）。** §1 的 CPU/GB 退步不再存在。
- fp 單獨的 CPU/GB 退步是 N150 記憶體頻寬的平台效應（§4）。在 M1 Pro 上 fp 本身就是 +26% 吞吐、−4% CPU/GB。
- buffer 重用的收益來自背景執行緒，符合 profile 的預測（§5、§7）。
- decode 分片只對 zstd 這類重度壓縮有意義，維持延後（§6）。
- 未量的：app 執行緒上 per-batch 快取 leader epoch `Optional`（§5 的 11%）。
