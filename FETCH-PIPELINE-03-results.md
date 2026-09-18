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
- **6p×1KB 的 CPU 退步兩條執行緒都有份**：背景 +10%、app **+30%**、JIT +1 秒。app 執行緒也變貴，指向**鎖競爭**而不是單純多做事：gate 1 拆掉後 `prepareFetchRequests` 每個 pass 都跑，`FetchBuffer.bufferedBytesByPartition()` 每次上 `FetchBuffer` 的鎖重建 map（跟 app 執行緒的 peek/poll 搶同一把鎖），`bufferedBytesByNode` 對每個有 buffer 的 partition 各做一次 `SubscriptionState` monitor 內的查詢（跟 app 執行緒的 position 更新搶）。成本 ∝ partition 數 × pass 頻率，所以 1p 看不到、6p 高吞吐時浮現。**待修**：bytes 帳改成增量維護，不要每 pass 在鎖內重建。
- **1p×100B：fp 的 app 執行緒到 0.78 核**（扣掉啟動大約 0.85–0.9），接近飽和。小 record 的瓶頸已經從「等 fetch」移到「app 執行緒的 per-record 成本」。這是 01 的 L2（per-batch 物件共用）該接手的地方；它只有一個 partition，任何 partition 級的平行化都幫不了它。

## 3. 過程中修掉的量測問題（留給下一次）

- 資料被 retention 清空（broker 跑了 9 天，`log.retention.hours=168`）：症狀是 consumer 卡到 timeout、0 筆、lag=0。先查 `kafka-get-offsets --time -2` 與 `-1` 是否相等。
- 資料集超過 page cache（30 GB vs 25 GB）會變成量 NVMe：base 的 6p×1KB 掉到 387 MB/s，`/proc/diskstats` 顯示 232 MB/s 來自磁碟。資料集總和必須塞得進 cache，跑前 `cat` 一次預熱。
- 資料集太小則沒有穩態視窗：3.7 GB 在 500 MB/s 下 8 秒跑完。1p×100B 用 100M 筆（9.3 GB）、6p×1KB 用 12M 筆（11.7 GB），warmup 8 s / 6 s。
- 跑完刪 topic（`big100b`、`t6p12`）與 consumer group，還原 dist 的 clients jar。
