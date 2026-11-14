# FETCH-PIPELINE-05：partition 層級背壓——問題釐清與實驗設計

狀態：只有問題分析與實驗設計。沒有實作，沒有量測結果。
方法：pattern language（Context → Forces → Problem Space → Solution Space）。本文停在 Problem Space，Solution Space 只列候選，由實驗結果決定。

## 0. 結論

目標是提升吞吐量，所以 application 側也要能多線程處理。這會改變 public API，需要 KIP。現有架構不是限制條件。
在「application 單一 thread」的前提下，背壓粒度只影響延遲公平性和記憶體上限，不影響總吞吐量。
這個前提拿掉之後，各 partition 的處理速度會不同，partition 層級背壓就從「可能不需要」變成「多線程的必要配套」。
所以要量的問題多了一個：application 多線程能拿到多少吞吐量，以及它需要 consumer 提供什麼。
所以先做量測機制「停滯歸因」（定義見 §5），再分兩階段做實驗。第一階段用各種形狀的 payload 收集 Force，不預設問題。第二階段才檢驗問題。§8 的決策表把結果對應到做法。

## 1. Context 與 Intent

- 對象：`group.protocol=consumer` 的 async consumer，分支 `kip-fetch-pipeline`。
- 現況（trunk）：一個 partition 有未取完的資料時，同一個 broker 的所有 partition 都不 fetch。
- 現況（本分支）：改成每個 node 一份 bytes credit，大小等於 `fetch.max.bytes`。判斷在 `AbstractFetch.prepareFetchRequests`。
- Intent：
  1. application 處理多快，consumer 就供應多快。
  2. 一個 partition 不因為別的 partition 而等待。
  3. consumer 的記憶體有上限，而且上限可以用現有 config 算出來。
- 非目標：新增 config。
- 方向修正（2026-11-14）：變更 public API 和打破現有 thread 模型都在範圍內。多線程 decode 和 application 多線程處理都是候選。

## 2. 資料流

一段資料從 broker 到 application 會經過七站。「背壓」只作用在 A 站。

| 站 | 位置 | 資料在等什麼 | 現有觀測 |
|---|---|---|---|
| A | broker log，還沒被要求 | node 沒有 in-flight request，而且 node 有 credit | 無 |
| B | broker 處理 request | `fetch.min.bytes` 或 `fetch.max.wait.ms` | `fetch-latency`，只到 node 層級 |
| C | broker 組 response | `fetch.max.bytes` 依 request 內的 partition 順序填滿；每個 partition 最多 `max.partition.fetch.bytes` | 無 |
| D | background thread 讀進 receive buffer | CPU 與記憶體頻寬 | 03 文件的 profile |
| E | `FetchBuffer` 內停留 | application 取走排在前面的資料（先進先出） | 無 |
| F | application thread decode | 解壓、CRC、deserialize | 03 文件的 profile |
| G | application 處理 | 兩次 `poll()` 之間的使用者程式 | `time-between-poll`、`poll-idle-ratio` |

A、B、E 三站的等待，現有 metrics 分不出來。這是要先做量測機制的原因。

## 3. Forces

標記：【觀察】有程式碼或量測依據。【假設】還沒有證據。

- F1【觀察】credit 以 node 為單位。node 上所有可 fetch 的 partition 的 buffered bytes 加總，達到 `fetch.max.bytes` 就整個 node 停止。
- F2【觀察】每個 node 同時只有一個 in-flight fetch request。
- F3【觀察】被 `pause()` 的 partition，它的 buffered bytes 不計入 credit。它不會擋住別人，但也不在記憶體上限內。
- F4【觀察】fetch session 是增量的。request 不帶某個 partition，broker 就把它移出 session。trunk 的註解說嚴重時整個 session 會被淘汰。
- F5【觀察，C 站行為需在實驗中確認】broker 依 request 內的順序填 response，填到 `fetch.max.bytes` 為止。consumer 每次取完資料後把該 partition 移到順序最後。這已經是一個 partition 之間的公平機制，位置在 broker 端。
- F6【觀察】「bytes」在各站意義不同。credit 算壓縮後的 bytes。application 看到的是筆數（`max.poll.records`）。
- F7【觀察，現況而非限制】application 是單一 thread，依序處理所有 partition。`FetchBuffer` 是先進先出。`KafkaConsumer` 的 javadoc 明文規定不可多線程存取。這一條是設計要推翻的對象。
- F12【觀察】使用者今天要多線程，只有兩條路。第一條是開 N 個 consumer，代價是 N 條連線、N 個 background thread、N 個 fetch session。第二條是自己把 record 丟進 thread pool，這時 auto-commit 會在 record 處理完之前就提交 offset。
- F13【觀察】Kafka 的順序保證是 partition 內有序。partition 之間沒有順序保證。所以以 partition 為單位平行處理不違反任何現有保證。
- F14【觀察】offset commit 是每個 partition 一個位置。各 partition 可以獨立前進。
- F15【觀察】rebalance callback、`max.poll.interval.ms` 的存活判定、`pause()`、`seek()` 都假設只有一個 application thread。多線程後每一個都要重新定義。
- F16【假設】decode（F 站）加 application 處理（G 站）佔牆鐘時間的比例，決定多線程的吞吐量上限。03 文件量到 decode 是 0.27 秒每 GB，background thread 是 0.59 秒每 GB。application 不做事時，瓶頸可能已經在 background thread。
- F17【假設】多線程後，各 partition 的處理速度不同。慢的 partition 會佔住 node credit，拖住同一個 node 上快的 partition。
- F8【假設】偏斜時（同一個 broker 上一個熱 partition 加多個冷 partition），熱 partition 的資料佔滿 node credit，冷 partition 的延遲上升。
- F9【假設】多 node 時，記憶體上限是 node 數 × `fetch.max.bytes`。
- F10【觀察】使用者規則：不加 config，不做只對特定情境有效的最佳化。
- F11【觀察】沒有任何現有 metric 能回答「這個 partition 現在卡在哪一站」。

### 互相衝突的 Forces

- **F4 對 partition 粒度。** 要單獨停掉一個 partition，最直接的做法是 request 不帶它，這會讓 fetch session 反覆增刪。
- **F7 對 F8。** 即使 A 站對每個 partition 公平，冷 partition 的資料到了 E 站還是排在熱 partition 的資料後面。只改 fetch 的粒度，不一定能改善延遲。背壓有兩個位置：A 站的准入，和 E 站的取出順序。
- **F12 對 F15。** 使用者需要多線程，但整個 consumer 的生命週期語意都建立在單一 thread 上。
- **F16 對多線程的價值。** 如果瓶頸在 background thread，application 多線程不會提升吞吐量，要先平行化 D 站。
- **F17 對 F1。** 多線程成立後，node 層級 credit 就會讓快慢 partition 互相拖累。HTTP/2 的類比這時才完全成立。
- **F1 對 F9。** node 層級的 credit 讓每個 node 都能滿載，但總量隨 node 數放大。

### 一個類比

HTTP/2 的流量控制有兩層視窗：connection 層級和 stream 層級。Kafka consumer 對應到 node 和 partition，目前只有 node 那一層。HTTP/2 需要 stream 層級，是因為各 stream 的接收端速度不同。consumer 的所有 partition 共用一個接收端（F7）。所以這個類比是否成立，正是實驗要回答的問題。

## 4. Problem Space

每個問題都寫成可以被否定的形式。

- **P0（虛無假設）**：時間都花在 D、F、G 站。背壓粒度不影響任何可觀察的結果。
- **P1 延遲不公平**：偏斜時，冷 partition 的端到端延遲明顯高於沒有熱 partition 時。
  - P1a：延遲來自 A 站（等 credit 或等 in-flight）。
  - P1b：延遲來自 E 站（排在熱 partition 資料後面）。
- **P2 吞吐損失**：application 在 `poll()` 內等資料，同時 broker 上有資料可拿。
- **P5 吞吐量上限在哪一站**：application 不做事時，上限在 D 站還是 F 站。application 有處理成本時，上限在 G 站。這決定平行化要從哪一站開始。
- **P6 多 consumer 的代價**：開 N 個 consumer 得到的吞吐量，和它多付出的 CPU、連線、記憶體。這是單一 consumer 多線程方案必須勝過的基準。
- **P3 記憶體放大**：buffered bytes 的高峰隨 node 數線性成長。
- **P4 停滯資料佔用資源**：被 pause 的 partition 影響其他 partition 的吞吐，或它保留的記憶體沒有上限。

## 5. 量測機制：停滯歸因

**定義**：每個 partition 在任何時刻恰好處於一個狀態。在狀態轉換點記錄時間，累加每個狀態的總時間。所有狀態的時間加總必須等於牆鐘時間，這是量測機制的自我檢核。

### partition 的狀態

| 狀態 | 對應的站 | 轉換點（都是既有程式碼位置） |
|---|---|---|
| `NOT_FETCHABLE` | — | `SubscriptionState.isFetchable` 為 false |
| `WAIT_BACKOFF` | A | `prepareFetchRequests` 的 `isUnavailable` 分支 |
| `WAIT_INFLIGHT` | A | `nodesWithPendingFetchRequests` 分支 |
| `WAIT_CREDIT` | A | credit 用完的分支 |
| `IN_FLIGHT` | B、C、D | request 送出到 `handleFetchSuccess` |
| `BUFFERED` | E | `CompletedFetch` 進入 `FetchBuffer` 到被取完 |

每個狀態再依「broker 上是否還有資料」分成兩類。判斷依據是上一個 response 的 high watermark 減掉 fetch offset。落後為零時的等待不算停滯。

### application thread 的時間

牆鐘時間分成三份：

- 在 `poll()` 內等 `FetchBuffer`（application 沒資料可處理）。
- 在 `poll()` 內 decode（F 站）。
- 在 `poll()` 之外（G 站）。

### 每筆 record 的延遲

producer 寫入的 `CreateTime` 到 `poll()` 回傳的時間差，依 partition 統計 p50 和 p99。producer 和 consumer 在同一台機器上，時鐘一致。

### 三個導出指標

| 指標 | 回答的問題 |
|---|---|
| application 等資料而且落後大於零的時間比例 | P2 |
| 冷 partition 在 `WAIT_CREDIT`、`WAIT_INFLIGHT`、`BUFFERED` 各佔多少，以及它的 p99 延遲 | P1a、P1b |
| 全部 buffered bytes 的高峰 | P3、P4 |

### 實作方式

- 先做成不 commit 的量測用 jar，跟 04 文件量命中率時的做法相同。
- 是否變成正式 metric，等實驗證明哪幾個數字有用之後再決定。
- 成本：每輪 `prepareFetchRequests` 對每個 partition 多寫一個 long。E0 實驗驗證這個成本。

## 6. 實驗設計

環境：morefine（N150，4 核）。跑之前確認機器閒置。跑完刪除 topic 與資料。
每個設定跑 3 次取中位數。每個實驗都比較 trunk base 與本分支。

實驗分兩個階段。第一階段收集 Force，不預設任何問題。第二階段才檢驗 §4 的問題。
§3 的 Force 清單和 §4 的問題清單都是暫定的。第一階段的結果會改寫它們。

### 第一階段：收集 Force

目的：對各種形狀的 workload，量出時間花在 §2 的哪一站。這一階段沒有「通過」或「失敗」。產出是一張「workload 形狀 × 各站時間比例」的表，以及更新後的 Force 清單。

| 編號 | 變動的維度 | 取值 |
|---|---|---|
| E0 | 無（驗證量測機制） | 6 partition × 1 KB，application 不做事。吞吐量與未量測時相差小於 2%；各狀態時間加總等於牆鐘時間 |
| E1a | record 大小 | 100 B、1 KB、100 KB、1 MB |
| E1b | 壓縮 | none、lz4、zstd |
| E1c | batch 大小 | producer 的 `batch.size` 16 KB、1 MB |
| E1d | partition 數 | 1、6、100、1000 |
| E1e | 流量形狀 | 持續積壓、穩定低流量、突發（靜止後一次寫入大量資料） |
| E2 | application 每筆 record 的處理時間 | 0、1 µs、100 µs |
| E6 | consumer 數量（今天唯一的多線程做法） | 1、2、4 個 consumer，各自 `assign()` 不重疊的 partition，各自一個 thread。application 處理時間取 0 和 100 µs |

做法：

- 每次只變動一個維度，其他維度固定在 E0 的值。不做全組合。
- 每個設定記錄三組數字：partition 各狀態的時間比例、application thread 三份時間的比例、buffered bytes 高峰。
- 一個設定如果出現新的主導站，就把它寫成一條新的【觀察】Force。
- 一條【假設】Force 如果在所有設定都看不到效果，就標記為「未觀察到」，不刪除。

E6 回答 P5 和 P6。它量出「多線程」在現有架構下的吞吐量上限和代價，不需要寫任何新程式碼。
E6 的 morefine 只有 4 核，broker 也在同一台。consumer 數量到 4 時數字會受 broker 搶 CPU 影響，要同時記錄 broker 的 CPU。

第一階段結束時要能回答兩件事。第一件：application 多線程的吞吐量上限是多少，瓶頸在哪一站。第二件：哪些 workload 形狀會讓 A 站或 E 站成為主導。如果沒有任何形狀讓 A、E 站主導，§4 的 P1、P2 就沒有立足點，第二階段可以縮小。

### 第二階段：檢驗問題

第一階段的結果決定這裡要跑哪些。下表是目前依 §4 預先排定的。

| 編號 | 檢驗 | 設定 | 判讀 |
|---|---|---|---|
| E3 | P1a、P1b | 1 個熱 partition（持續滿速寫入）加 5 個冷 partition（每秒 10 筆），同一個 broker。對照組只有冷 partition。application 快、慢各一組 | 冷 partition 的 p99 延遲與對照組的比值，以及多出來的時間落在哪個狀態 |
| E4 | P3 | 同一台機器跑 3 個 broker process，12 partition，application 慢 | buffered bytes 高峰對 3 × `fetch.max.bytes`；`CachingMemoryPool` 命中率 |
| E5 | P4 | 6 partition 都有積壓，pause 其中 1 個 | 其他 5 個的吞吐量變化；被 pause 的 partition 保留的 bytes |

備註：

- E4 的三個 broker 共用 4 核，吞吐量數字不可信。E4 只看行為（bytes 高峰、命中率）。
- E3 同時確認 F5：看 broker 是否依順序輪流填 response。
- E1a 的 1 MB record 需要調高 broker 的 `message.max.bytes`。

## 7. Solution Space（候選，尚未選擇）

| 編號 | 做法 | 對應的問題 | 主要代價 |
|---|---|---|---|
| S0 | 維持 node 層級 credit | P0 成立時 | 無 |
| S1 | node credit 平分給該 node 上可 fetch 的 partition。超過份額的 partition 在 request 內調低它的 max bytes，不把它移出 request | P1a | 要驗證調低 max bytes 不會造成 fetch session 增刪（F4） |
| S2 | `FetchBuffer` 取出順序從先進先出改成在 partition 之間輪流 | P1b | 改變 record 回傳順序的可預期性；partition 內順序不變 |
| S3 | consumer 全域一份 bytes 預算，各 node 向它借 | P3 | node 之間要有公平規則；單 node 吞吐可能下降 |
| S4 | client 不做事，依賴 broker 的順序輪轉（F5） | E3 顯示冷 partition 沒有被餓到 | 無 |

S1、S2、S3 互相獨立，可以只做其中一個。

### application 多線程的候選（需要 KIP）

| 編號 | 做法 | 主要代價 |
|---|---|---|
| M0 | 維持現況，使用者開 N 個 consumer | F12 的全部代價 |
| M1 | consumer 內部多線程 decode，`poll()` 仍由單一 thread 呼叫 | 不改 API，但 G 站仍是單線程 |
| M2 | 每個 partition 一個可獨立 `poll()` 的 handle，不同 thread 各拿各的 partition | 新 API；rebalance 時 handle 失效的語意；F15 全部要重新定義 |
| M3 | consumer 持有 worker pool，使用者註冊每個 partition 的處理函式，consumer 推送 | 從 pull 變 push；處理函式的錯誤與重試語意；接近 share consumer 的模型 |

M2 和 M3 都需要「完成訊號」：一批 record 處理完，才釋放 receive buffer、歸還 credit、前進可提交的 offset。這是 OpenTelemetry 文章的 `Done` 回呼對應到 consumer 的形狀。
M2 和 M3 成立後，S1（partition 層級 credit）就是必要配套，不再是可選項。

候選 Pattern 有兩個形狀已經出現，但要等實驗後才寫成 pattern card：
- 「停滯歸因」：每個單位恰在一個狀態，累加狀態時間。
- 「兩層 credit」：共用連線一層，獨立流一層。

## 8. 決策表

| 實驗結果 | 做法 |
|---|---|
| E6 顯示多 consumer 的吞吐量接近線性成長，而且 application 處理時間大於零時差距更大 | 多線程有價值。進入 M1、M2、M3 的設計，並把 S1 列為配套 |
| E6 顯示吞吐量不隨 consumer 數量成長，瓶頸在 broker 或網路 | client 側多線程沒有空間。記錄原因，不做 |
| application 不做事時瓶頸在 D 站 | 先平行化 background thread 的讀取，再談 application 多線程 |
| 第一階段顯示時間都在 D、F、G；E3 冷 partition p99 小於對照組 2 倍 | S0。背壓不是下一步，回到 decode 或 application 側 |
| E3 冷 partition p99 大於對照組 2 倍，多出的時間在 `WAIT_CREDIT` 或 `WAIT_INFLIGHT` | S1 |
| E3 多出的時間在 `BUFFERED` | S2 |
| E4 高峰接近 3 × `fetch.max.bytes`，而且命中率明顯低於單 node | S3 |
| E2 顯示 application 等資料且落後大於零的比例高 | 問題在 F2（每 node 一個 in-flight），另開分析 |
| E5 其他 partition 吞吐下降 | 先當 bug 處理 |

「2 倍」是暫定門檻。E3 對照組的變異量出來後再調整。

## 9. 下一步

1. 實作 §5 的量測用 jar。
2. 跑 E0，確認量測機制的成本與自我檢核。
3. 跑 E6。它不需要量測用 jar 以外的新程式碼，而且直接回答多線程值不值得做。
4. 跑第一階段的 E1a 到 E1e 和 E2，產出「workload 形狀 × 各站時間比例」的表。
5. 依結果改寫 §3 的 Force 清單和 §4 的問題清單。
6. 依改寫後的問題清單，決定第二階段跑哪些實驗。
