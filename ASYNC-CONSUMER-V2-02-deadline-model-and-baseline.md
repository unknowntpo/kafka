# Deadline-driven 推進模型的瓶頸、baseline 量測、公開 issue 調查

- 承接 `ASYNC-CONSUMER-V2-01-problem-analysis.md`；基準 commit `820533b870`
- 本文三件事：(1) 回答「`ConsumerNetworkThread` 的 deadline-driven 推進模型會造成哪些瓶頸」；(2) 本機 baseline 量測；(3) 公開 JIRA / GitHub / 郵件列表中效能相關 issue 的整理與方向判斷
- 量測環境：macOS（Darwin 25.3）、8 核、32 GB、JDK 21.0.1、單節點 KRaft broker 與 consumer 同機、PLAINTEXT loopback、資料在 page cache。數字只適合比較相對關係，不代表絕對效能

---

## 1. Deadline-driven 推進模型是什麼

`ConsumerNetworkThread.runOnce()`（`ConsumerNetworkThread.java:210-242`）每輪固定做：

1. drain 應用執行緒送來的 event 並逐一處理
2. 取一次 `currentTimeMs`
3. 對每個 `RequestManager` 呼叫 `poll(currentTimeMs)`，每個回傳 `PollResult{timeUntilNextPollMs, unsentRequests}`；取所有 `timeUntilNextPollMs` 的最小值當 `pollWaitTimeMs`（上限 5000 ms）
4. `NetworkClientDelegate.poll(pollWaitTimeMs)` → `select()` 阻塞，直到有 I/O、被 `wakeup()`、或超時
5. 再對每個 manager 呼叫 `maximumTimeToWait(currentTimeMs)`，取最小值存進 `cachedMaximumTimeToWait`，給應用執行緒當它下次可以阻塞多久的上限
6. reaper 檢查所有追蹤中 event 的 deadline，過期的用 `TimeoutException` 完成

也就是：**沒有任何一個元件「被通知後直接推進」。每個元件都在每一輪被問一次「你現在要做什麼？下次什麼時候再問你？」**，而整條執行緒的節奏 = 所有回答裡最小的那個時間 + 任何一次外部喚醒。Event 的完成也不是「事件驅動」而是「deadline 驅動」：event 帶著 `deadlineMs`，背景執行緒在某一輪處理到它，或 reaper 在某一輪發現它過期。

## 2. 這個模型造成的瓶頸

### 2.1 推進粒度 = 一整輪迴圈，而一輪的成本是 O(managers + tracked events)，與「誰需要推進」無關

一個 fetch response 到達後，下一個 fetch request 不會在同一輪送出：`handleFetchSuccess` 在 `NetworkClient.poll` 內同步執行（把 `CompletedFetch` 放進 `FetchBuffer`、`signalAll` 喚醒應用執行緒），但 `FetchRequestManager.poll` 只在**下一輪**才會建請求，而且還要先由應用執行緒設下 `pendingFetchRequestFuture` 這個 gate（第一份文件 B3）。所以單一 partition 的每個 fetch 之間的氣泡是：

```
response 到達 → (本輪結束: maximumTimeToWait ×N、reaper ×2 list) → 應用執行緒被 signal 喚醒（排程延遲）
→ collectFetch（解壓/解析/反序列化整批）→ sendPrefetches 送 CreateFetchRequestsEvent + selector wakeup
→ 背景執行緒下一輪: drain event → poll ×N managers → prepareFetchRequests（掃 SubscriptionState / FetchBuffer）→ send
→ RTT → 下一個 response
```

其中「應用執行緒處理整批」這一段是串在管線裡的，因為 gate 要等它。管線深度 1 加上這個氣泡，就是第 3 節量到的「fetch-rate ≈ 900/s、每個 1 MB」這種鎖步行為。

### 2.2 迴圈節奏被最沒耐心的 manager 綁架，而且是全域的

`pollWaitTimeMs = min(所有 manager 的 timeUntilNextPollMs)`，`cachedMaximumTimeToWait = min(所有 manager 的 maximumTimeToWait)`。任何一個 manager 回 0，整條執行緒進入 `poll(0)`，其他所有 manager 也跟著被每輪重問一次。穩態下 `FetchRequestManager.maximumTimeToWait` 在沒有 in-flight fetch 時回 `retryBackoffMs`（100 ms，`FetchRequestManager.java:83-85`），所以應用執行緒與背景執行緒都被綁在「至少每 100 ms 醒一次」。

這個設計是公開 issue 裡最大的一群 bug 的共同根因：KAFKA-20426、20540、20970、21010、21031、19804 全都是「某個 manager 在某個狀態下回 0 → busy loop」，其中三個至今仍 open（見第 4 節）。這不是個別 bug，是模型的性質：**模型沒有「誰喚醒我、為什麼」的資訊，每次醒來都重新評估全部**，任何一個 manager 算錯 wait time 就是 100% CPU。

### 2.3 時間只在阻塞前取樣一次，deadline 判斷用的是舊時間

`currentTimeMs` 在 `select()` 之前取（:214），阻塞最多 5 s 後，步驟 5 的 `maximumTimeToWait`、步驟 6 的 reaper、`maybeFailOnMetadataError` 用的都是阻塞前的時間。deadline 的判斷精度因此是「一輪迴圈」，不是毫秒；而 `cachedMaximumTimeToWait` 給應用執行緒的又是**上一輪**算出來的值，天生過時。

### 2.4 應用執行緒的喚醒與 event 完成脫鉤

`AsyncPollEvent` 完成只設 volatile flag（`AsyncPollEvent.java:121-133`），`BackgroundEventHandler.add` 不喚醒任何人。應用執行緒唯一的喚醒來源是 `FetchBuffer` 的 condition，其餘（rebalance 回呼、錯誤、metadata 更新）都要等它自己因為 100 ms 上限醒來再輪詢。反過來，`FetchBuffer.wakeup()` 又是 permit 語意（第一份文件 C4），任何一次 fetch 完成都放行一次應用執行緒，即使沒資料。結果是兩種浪費並存：該醒的時候沒人叫（延遲以 100 ms 計），不該醒的時候被叫醒（空轉一整輪 poll 邏輯）。

### 2.5 背景執行緒的工作量隨應用端 poll 頻率成長，而不是隨資料量成長

每次 `poll()` 送 1–2 個 event，每個 event 一次 selector wakeup，每次 wakeup 一整輪 `runOnce()`。所以背景執行緒的迴圈次數 ≈ 2 × 應用端 poll 頻率 + response 數。當應用端用小 `max.poll.records`（poll 頻率高）時，背景執行緒每秒要跑上萬輪，每輪十幾個配置、兩次 manager 遍歷、三個 sensor。第 3 節的 metrics 直接量到這個關係：`max.poll.records=50` 時應用端 7.4k poll/s，背景執行緒 13k 輪/s。KAFKA-18376（`max.poll.records=5` 時 async 50% CPU vs classic 10%）與 KAFKA-18139（async 的 `NetworkClient.poll` 次數是 classic 兩倍）就是這個現象。

### 2.6 所有關注點在同一條執行緒上序列化，沒有優先權

一個大 fetch response 的 `handleFetchSuccess`（每 partition 一個 `CompletedFetch` + 拿鎖 + `signalAll`）、heartbeat、commit、metadata 更新、rebalance reconcile 全部排在同一輪裡。fetch 資料量大時 heartbeat 處理延後；rebalance reconcile 時 fetch 延後。模型沒有辦法表達「這件事比較急」。

### 2.7 小結：deadline-driven 對吞吐的實際傷害

| 影響 | 機制 | 量化（第 3 節） |
|---|---|---|
| 管線氣泡 | fetch 的續發要繞過應用執行緒 + 一整輪迴圈 | 100B：三方都只用 22–29% CPU；只把續發改成 response 到達即發，吞吐 356 → 766 MB/s（2.15×，3.7 節） |
| 固定成本隨 poll 頻率成長 | 每 poll 2 event + 2 wakeup + 1 輪 | mpr50：背景 13k 輪/s；CPU/GB 比 classic 多 37%，sys 多 79%；wakeup 的 pipe write 佔應用執行緒 4.9% |
| 100 ms 節奏 | `maximumTimeToWait` 最小值 | 閒置時兩條執行緒各每 100 ms 醒一次 |
| busy loop 風險 | 任一 manager 回 0 | 六張 JIRA，三張 open |

結論：deadline-driven 模型是**polling scheduler**，不是 reactor。它的正確性依賴每個 manager 都把 wait time 算對，它的效率隨應用端行為劣化，而且它把 fetch 的續發放在應用執行緒的關鍵路徑上。新設計應改成**資料驅動 + 事件驅動**：fetch 由「buffer 目標深度」自主續發（不經應用執行緒）、每個來源（socket、timer、event）直接分派到對應處理器、應用執行緒只在有結果時被喚醒。

---

## 3. Baseline 量測

### 3.1 方法

- 工具：`bin/kafka-consumer-perf-test.sh`（`ConsumerPerformance`），比較欄位用 `fetch.MB.sec`（扣掉 join/rebalance 時間），因為 broker 的 `group.initial.rebalance.delay.ms=3000` 只影響 classic 的 join，會扭曲總 MB/s
- 兩種 protocol：`group.protocol=classic` 與 `consumer`，其餘設定相同（`receive.buffer.bytes=2MB` 為工具預設）
- 資料：ProducerPerformance 產生，random payload（不可壓縮，lz4 場景仍有解壓成本）

### 3.2 第一輪：短 run（3M × 1KB / 5M × 100B，3 次取中位數）

| 場景 | classic MB/s | consumer MB/s | consumer / classic |
|---|---:|---:|---:|
| 1p 1KB | 906 | 869 | 0.96 |
| 1p 1KB lz4 | 416 | 520 | **1.25** |
| 1p 100B | 380 | 321 | 0.84 |
| 1p 1KB `max.poll.records=50` | 995 | 742 | 0.75 |
| 1p 1KB `max.partition.fetch.bytes=256K` | 1049 | 903 | 0.86 |
| 6p 1KB（同一 broker） | 901 | 764 | 0.85 |

觀察：
- lz4 是唯一 consumer 贏的場景，但**這是短 run 的假象**：3.5 節的長 run 顯示兩者持平，差異來自 JIT 暖機期間 I/O 卸載的效果。
- 其餘場景 consumer 慢 4–25%，且 poll 頻率越高（mpr50、100B）差距越大，對應 2.5 節。
- 6 partition 同 broker 比 1 partition 沒有更快：B2 的 node 層級排除讓多 partition 無法增加管線深度。

### 3.3 為什麼 1KB 場景兩者都卡在約 900 MB/s

- 兩個 consumer 同時跑（不同 group）各得 620 MB/s，合計 1.24 GB/s → broker 端不是單一 consumer 的上限。
- `max.partition.fetch.bytes` 從 1 MB 改 8 MB，吞吐不變（classic 819–857、consumer 791–860）→ 每次 fetch 的位元組上限不是瓶頸。
- 短 run 的 profile（async-profiler，cpu，1 ms）：consumer 1KB 下背景執行緒 36% 樣本、應用執行緒 19%，兩者都不飽和；背景執行緒的樣本 30% 在 `kevent`、25% 在 `read`、4% 在 `bzero`（每個 response 一個新 heap buffer 的歸零）；應用執行緒有 `__psynch_cvwait`（在等 `FetchBuffer`）。
- 因此 1KB 未壓縮在本機是**單一 TCP 連線的 I/O 路徑**（syscall 密度 + broker 單連線送出 + buffer 歸零）在限制，classic 與 consumer 共用這段程式碼，所以一樣。這個場景不適合當「2 倍」的分母；適合的是 CPU-bound 場景（小記錄、壓縮、大量 partition），以及「每 GB 消耗的 CPU 秒數」這種效率指標。

### 3.4 Metrics 佐證 deadline 模型（`--print-metrics`，consumer protocol，1p 1KB）

| 指標 | 預設 | `max.poll.records=50` |
|---|---:|---:|
| `time-between-poll-avg`（應用端）| 0.95 ms | 0.135 ms（≈ 7.4k poll/s） |
| `time-between-network-thread-poll-avg` | 0.121 ms（≈ 8k 輪/s） | 0.077 ms（≈ 13k 輪/s） |
| `select-rate` | 1.9k/s | 2.9k/s |
| `fetch-size-avg` | 1.05 MB | 1.05 MB |
| `records-per-request-avg` | 1012 | 1012 |
| `fetch-latency-avg` | 2.3 ms | 2.2 ms |
| `poll-idle-ratio-avg` | 0.31 | 0.05 |

每個 fetch（1 MB）伴隨約 10 輪背景迴圈與 2–3 次 `select`；應用端 poll 頻率提高 7 倍時背景迴圈提高 1.6 倍、select 提高 1.5 倍，但資料量完全相同。（註：rate 類 metrics 是 30 s 視窗平均，run 只有數秒，絕對值偏低；相對關係有效。）

### 3.5 第二輪：長 run 與 CPU 效率（40M × 100B、8M × 1KB、8M × 1KB lz4；3 次取中位數）

以 `/usr/bin/time -l` 量整個 consumer JVM 的 user+sys，除以消費的 GB，得到不受 I/O 上限影響的效率指標。

| 場景 | protocol | MB/s | msg/s | CPU 秒/GB | user/GB | sys/GB |
|---|---|---:|---:|---:|---:|---:|
| 1p 100B | classic | 431 | 4.52M | 2.31 | 1.85 | 0.46 |
| 1p 100B | consumer | 477 | 5.00M | 2.42 | 1.88 | 0.55 |
| 1p 100B `max.poll.records=50` | classic | 398 | 4.17M | 2.61 | 2.09 | 0.52 |
| 1p 100B `max.poll.records=50` | consumer | 320 | 3.35M | **3.57** | 2.64 | 0.93 |
| 1p 1KB | classic | 1095 | 1.12M | 1.05 | 0.80 | 0.25 |
| 1p 1KB | consumer | 1034 | 1.06M | 1.09 | 0.80 | 0.28 |
| 1p 1KB lz4 | classic | 432 | 0.44M | 1.52 | 1.08 | 0.44 |
| 1p 1KB lz4 | consumer | 417 | 0.43M | 1.56 | 1.10 | 0.46 |

- 長 run 下 consumer 與 classic 的 CPU/GB 幾乎相同（差 3–5%），符合「per-record 程式碼同一份」。
- 差距只在 poll 頻率高的 `max.poll.records=50`：consumer 多 37% CPU/GB，其中 sys 多 79%（selector wakeup syscall）。這就是 2.5 節。
- 短 run 與長 run 的 lz4 結論相反（短 run consumer +25%，長 run 持平）：短 run 的差異來自 JIT 暖機期間 I/O 卸載的效果，不是穩態。**第一輪的 lz4 結論撤回。**

### 3.6 沒有任何一方飽和：lockstep 的直接證據

長 run（100B、consumer protocol、477 MB/s）期間的 CPU 佔用：

| 角色 | CPU 佔用 | 主要在做什麼 |
|---|---:|---|
| consumer 應用執行緒 | ~22% | `Buffer.<init>` 11.6%（每筆 4 個 slice/duplicate）、`maybeLeaderEpoch` 8.1%、`IOUtil.write1` 4.9%（**selector wakeup 的 pipe 寫入**）、`Utils.toArray` 4.3%、`parseRecord` 4.3%、`DefaultRecord.readFrom` 3.6%、`ConsumerRecord.<init>` 2%、`RecordHeaders.<init>` 1.3% |
| consumer 背景執行緒 | ~27% | `KQueue.poll` 43%、`SocketDispatcher.read0` 21%、`HeapByteBuffer.<init>` 2.4%（每個 response 新 buffer） |
| broker（整個 JVM，8 核） | ~25%（約 2 核） | 單連線 sendfile |
| classic 應用執行緒 | ~29% | `KQueue.poll` 14% + `read0` 14% + 與上面相同的 parse profile |

三方都在等彼此：這是 2.1 節描述的序列化環路（request → broker → read → parse → 下一個 request）。每個 1 MB fetch 的 cycle ≈ 2.2 ms，其中 `fetch-latency-avg` ≈ 1.7–2.3 ms（等 broker + 網路 + read），應用端解析只佔 ~0.5 ms，但因為下一個 fetch 要等應用端消費完才發，這些時間是**相加**而不是**重疊**。

### 3.7 決定性實驗：prefetch-ahead 管線（分支 `async-consumer-v2-exp-prefetch` @ `eb83eb21b1`）

在 `AbstractFetch` 加一個實驗開關 `-Dkafka.exp.prefetch.depth=N`（預設 0 = 原行為）：response 一到，背景執行緒立刻用該 response 的結尾 offset 續發下一個 fetch，不等應用執行緒消費；每個 partition 最多 1 個 in-flight、最多 N 個未消費的 fetch 在 buffer；用 sessionless full fetch 避開 fetch session 的 epoch 序列限制。改動約 100 行，只為驗證假設，不是設計。

| topic | protocol | depth 0 | depth 2 | depth 4 | depth 8 | 倍率（depth 2 / 0） |
|---|---|---:|---:|---:|---:|---:|
| 100B | consumer | 356 MB/s（3.7M msg/s） | **766 MB/s（8.0M msg/s）** | 795 | 769 | **2.15×** |
| 100B | classic | 431 | – | 654 | – | 1.52× |
| 1KB | consumer | 765 | 763 | 871 | 869 | ~1.0–1.1× |

（每格 2 次取中位數；CPU 秒/GB 在 depth 0 與 depth 2 之間**不變**：100B consumer 皆為 2.7 左右，只是 wall time 減半。）

正確性驗證（`OffsetCheck`，8M × 1KB，depth 4）：consumer / classic × `max.poll.records` 500 / 50，四組皆 count = 8,000,000、gaps = 0、duplicates = 0。已知缺陷：`seekToBeginning` 後 consumer + `max.poll.records=50` 有時 10 秒內讀不到資料（3 次中 2 次），因為 hack 沒有在 position 改變時讓 prefetch 狀態失效；正式設計必須用 position epoch fencing 處理 seek / reset / assignment 變更。

depth 2 之後的 profile（100B、687 MB/s，含 profiler 開銷）：應用執行緒 58%、背景執行緒 50%，depth 4/8 沒有再提升 → 新的天花板是**單一 TCP 連線的 I/O 路徑**（背景執行緒一半時間在 `read0` + `KQueue.poll`；1KB 場景的 ~900 MB/s 也是同一道牆）。要再往上要靠減少 syscall 次數（大 read、direct buffer、buffer 重用）與多連線，屬於設計文件的範圍。

---

## 4. 公開 issue 調查（JIRA / GitHub / 郵件列表 / 部落格）

來源：Apache JIRA REST、apache/kafka PR、lists.apache.org、公開部落格。截至 2026-09-07，已發佈最新版為 4.3.1；「fix 4.4.0」代表已進 trunk 但未發佈。

### 4.1 反覆被點名的元件（依 issue 數）

1. **`maximumTimeToWait()` / poll timeout 推導**（heartbeat、commit、fetch manager）：任一 manager 回 0 → 兩條執行緒 `poll(0)` 自旋。KAFKA-20426（fixed 4.3.0）、20540（**open**，Streams HB）、20970（fixed 4.4.0）、21010（**patch available**）、21031（**open**，HB in-flight 時 timer 過期）、19804（**open**，初始 HB interval 為 0）。KIP-909 非同步 DNS（4.4）拉長了「coordinator 未知」的窗口，把大部分曝露出來。
2. **`FetchBuffer` 喚醒與 inflightPoll 生命週期**：空 response 不喚醒導致阻塞整個 timeout（KAFKA-19259，fixed 4.2.0）；broker 上沒有 pending fetch 導致低流量 topic 延遲 80 倍（KAFKA-20780：classic 3.2 fetch/s、28 ms；async 0.47 fetch/s、2300 ms，fixed 4.4.0）；修 20780 的 PR 22979 讓 CPU 翻倍（KAFKA-20904，Confluent 內部定期測試），再由 KAFKA-20854 修回（無條件 `FetchBuffer.wakeup()` 繞過 backoff）；生命週期債務 KAFKA-20844（**open**，「分散在 5 個方法、futures + flags，已證明容易出錯」）、20534（**open**）、20397（**open**，PR 21991）；fetch session 被過早驅逐 KAFKA-17182（Blocker，fixed 4.0/4.1，背景 buffering 與 request 建構的競態）。
3. **每 poll 的 event loop 開銷**：KAFKA-18376（`max.poll.records=5` 時 async >50% CPU vs classic 10%，fixed 4.2.0 by non-blocking `AsyncPollEvent`）、19295（每 event 產 UUID）、19297（熱路徑用 Streams API）、19296（`Selector.wakeup()` 次數比 classic 多 1–2 個數量級，**Won't Do**）、19665（`pollForRecords` 等 future，**Won't Fix**）、18139（**open**：100M records 下 async `NetworkClient.poll` 約 800k 次 vs classic 400k，CPU 多 >10%，延遲 12 ms vs 10.5 ms）。
4. **Reconciliation 與應用執行緒收集的交互**：KAFKA-20332（正確性修正，重新引入等待 → CPU 上升）→ 20535（fixed 4.3.1：不在 RECONCILING 時仍等 `reconciliationCheckFuture`；PR 22199 的數字：50k rec/s、100 B 下 CPU 225.7m → 325.7m → 248.4m，classic 18% vs async 25%）。
5. **Commit manager**：KAFKA-20765（STALE_MEMBER_EPOCH 重試同步自旋 → StackOverflow，Streams 執行緒被卡到 `default.api.timeout.ms`）。
6. **佇列無界**：KAFKA-15173（**open** since 2023，application/background queue 無上限，「可能 OOM」）。users@ 有一則 share consumer 連錯 port 後 `consumer_background_thread` 高 CPU + 記憶體緩慢成長的回報（2026-01，kafka-clients 4.1.1）。

### 4.2 症狀群

- **busy loop / 高 CPU**：啟動、閒置、設定錯誤、auth 失敗、小 `max.poll.records`——兩條執行緒都會燒。這是最大的一群。
- **喚醒延遲**：poll 阻塞滿 timeout、fetch 之間有空隙、低流量 topic 延遲爆炸。
- **吞吐倒退 vs classic**：公開資料幾乎都是用 CPU 與延遲表述，**沒有任何公開的 MB/s 對比**；KAFKA-16110（公佈效能測試結果）自 2024 年 open 至今內容為空；ducktape 的 throughput benchmark 直到 4.5（KAFKA-20978）才加入 consumer protocol。
- **記憶體**：只有無界佇列與上述 users@ 回報，沒有確認的 leak ticket。
- **Streams**：因為 streams rebalance protocol 用 `AsyncKafkaConsumer`，20765、20540、20860/20861（assignment 損壞 → 每次 heartbeat 都 NPE crash loop）、20939 + PR 23364（Streams/Connect 被迫把 `bootstrap.resolve.timeout.ms` 設 0）都是 Streams 可見的。

### 4.3 部落格 / 廠商

只有 Instaclustr 的 rebalance benchmark（10 consumer、1000 partition：classic 103 s → 新協定 5 s）與 Confluent 的定性描述；**沒有任何廠商或獨立第三方發表過 Java async consumer vs classic 的穩態吞吐 / CPU 對比**。

### 4.4 這些 issue 告訴我們該往哪走

1. **公開世界沒有人在量吞吐**。所有回報都是 CPU 與延遲；Kafka 社群自己的效能基準到 4.5 才涵蓋新協定。這意味 (a) 我們的 baseline 工具本身就有價值，(b) 「2 倍」必須自己定義並公開量法。
2. **社群花了兩年在修 deadline 模型的症狀**（六張 busy-loop、四張喚醒延遲、三張 lifecycle 債務），而且修一個常常弄壞另一個（20780 → 20904 → 20854；20332 → 20535）。這強烈支持第 2 節的判斷：問題在模型，不在個別 manager。新設計不該再有「每個 manager 回報 wait time」這種全域最小值機制。
3. **`FetchBuffer` 與 inflightPoll 生命週期是第二大熱區**，且 KAFKA-20844 的作者自己承認它「已證明容易出錯」。新設計的 fetch 續發應由背景端依 buffer 深度自主決定，應用執行緒不參與。
4. **per-poll 固定成本已被社群拿掉了最容易的部分**（UUID、Streams API、blocking future），剩下的（event、wakeup、reaper、metrics）是結構性的，社群選擇 Won't Do（19296）。要再降只能換結構。
5. **沒有人動 per-record 路徑**。所有公開 issue 都在 event loop 與 wakeup 上，`CompletedFetch` / 解壓 / 反序列化 / buffer 配置這條線零 issue、零 benchmark。這是 2 倍最大的空間，也是最沒有競爭的方向。

---

## 5. 結論與下一步

### 5.1 結論

1. **現行 async consumer 的吞吐上限來自序列化的 fetch 環路，不是 CPU。** 100B 場景三方都只用四分之一的 CPU；只把「response 到達即續發」這一件事做對，單 consumer 吞吐 2.15×，CPU/GB 不變。這一項獨立於其他所有優化，且已用 100 行 hack 證明。
2. **deadline-driven 模型是這個環路的結構性原因**：fetch 續發被綁在應用執行緒的 event 上、迴圈節奏被全域最小 wait time 綁架、喚醒與完成脫鉤。公開 issue 兩年來反覆修的都是它的症狀（第 4 節）。
3. **第二道牆是單連線 I/O 路徑**（~800–900 MB/s on this box），classic 與 consumer 共用；第三道是 per-record 成本（應用執行緒 ~30% 在可避免的配置與 syscall 上）。這兩道決定 2× 之後還能走多遠。
4. **「2 倍」的定義**：以 CPU-bound / latency-bound 場景（小記錄、多 partition、壓縮）的穩態 MB/s 與 CPU 秒/GB 為準，1KB 未壓縮單 partition 在本機不是有效的分母。

### 5.2 下一步

1. 寫設計文件 `ASYNC-CONSUMER-V2-03-design.md`：資料驅動的 fetch 續發（buffer 目標深度、position epoch fencing）、事件驅動的背景執行緒（取代全域 wait-time 最小值）、少 syscall 的 I/O 路徑（大 read、buffer 重用）、per-record 路徑瘦身（去掉 4 個 slice、`Optional`、`RecordHeaders`、強制 `byte[]` 拷貝），並明確列出 F1/F2 相容邊界。
2. 把 `bench/`（broker 設定、`bench.sh`、`matrix.sh`、`exp*.sh`、`profile.sh`、`OffsetCheck.java`）從 scratchpad 移進 repo 成為可重跑的 benchmark 工具。
3. 補 `CompletedFetch.fetchRecords` → `ConsumerRecord` 的 JMH。
