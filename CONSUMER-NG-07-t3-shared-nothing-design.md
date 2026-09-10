# CONSUMER-NG 07：T3 shared-nothing（每個 broker 一條 fetch 執行緒）設計與量測門檻（2026-09-10）

01 §1 的第三個等級 T3 是「多核機器上一個 consumer 吃多個 broker」。這份文件先回答**什麼時候值得**，再給設計，最後是實作前必須先量到的門檻。效率優先：多一條執行緒就多一份喚醒與交接成本，只有單條 I/O 執行緒被量到飽和時才開第二條。

## 1. 現在的上限在哪裡

03 / 05 的量測都是 4 核 morefine 上 broker 與 consumer 同機。M2b 的 profile（6p 1 KB，1,458 MB/s）：itimer 樣本換算 consumer 全程 1.58 核，其中 I/O 執行緒 0.75 核、app 執行緒 0.57 核、JIT 0.17 核。也就是說 **I/O 執行緒沒有飽和，瓶頸是同機的 broker 與 loopback**；在這台機器上再加 fetch 執行緒量不到差別。

單條 I/O 執行緒的真實上限要在「consumer 獨佔一台機器、多個 broker 在別的機器、網路不是瓶頸」的環境才看得到。用 03 的數字外推：1 核跑 1,164 MB/s 時 I/O 執行緒約占 0.5 核（1p 100 B 的 profile 是 80% 在 `libc read`），單條執行緒大約在 2–2.5 GB/s 的 socket 讀取附近飽和，相當於兩張 10 GbE。**在那之前，T3 沒有收益，只有成本。**

所以 T3 的門檻是量測，不是功能：先做 §4 的兩台機器實驗，量到單執行緒飽和點，再決定要不要做 §2。

## 2. 設計（在門檻達到時）

### 2.1 分成兩個平面

- **控制平面**：現在的 `ConsumerEngine` I/O 執行緒不變——命令、housekeeping、六個 manager、`PassDecision`、coordinator / heartbeat / commit / metadata 的連線。所有 `SubscriptionState` 的**寫入**（assignment、position 初始化、reset、validation）留在這裡。
- **資料平面**：每個 broker 一條 `FetchWorker` 執行緒，擁有自己的 `NetworkClient` + `Selector`（只連那一個 broker）、自己的 `FetchSessionHandler`、自己的 in-flight 狀態與 credit 份額。它只做三件事：發 fetch、收回應、把 segment 放進 partition 佇列。

控制平面與 worker 之間只有兩種訊息：`assign(partition, offset, leaderEpoch)` / `revoke(partition)` / `seek(partition, offset)`（控制 → worker，MPSC 佇列 + wakeup）和 `error(partition, Errors)`（worker → 控制，走既有的 `backgroundEventHandler` 或一個 MPSC 佇列）。worker **不讀** `SubscriptionState`：它從 assign 訊息拿到起始 offset，之後自己維護 `nextFetchOffset`；position 仍由 app 執行緒在交付時寫回 `SubscriptionState`（與現在相同）。這樣 `SubscriptionState` 的 `synchronized` 不會變成多執行緒的熱點。

### 2.2 partition 佇列變成 SPSC

現在每個 partition 一個 `ConcurrentLinkedQueue`（多生產者）。T3 之後每個 partition 只有一個生產者（它 leader 所在的 worker）和一個消費者（app 執行緒），可以換成 SPSC ring（固定容量 = credit / 典型回應大小，滿了就是 backpressure）。leader 搬家時舊 worker 先 `revoke`（停止生產、丟掉在飛的結果），控制平面清佇列，再對新 worker `assign`——佇列在任一時刻仍只有一個生產者。

### 2.3 記憶體與 credit

- `DirectBufferPool` 共用（已是 lock-free 的 per-class deque + 原子計數），容量不變：有界記憶體的上限與 worker 數無關。
- credit 從「全域一份」變成「每個 worker 一份 = 總額 / worker 數，另加一個全域的溢出份額」：一個 broker 很慢時不會把別人的額度吃光，也不會讓總量超過上限。`FetchSegment.Owner` 的 refcount 已經是原子操作，釋放時只需要叫醒**那個** worker（現在是叫醒唯一的 I/O 執行緒）。

### 2.4 交付順序與 app 執行緒

`RecordReader` 已經是「每個 partition 一個 cursor、輪流交付」；多個 worker 只是讓佇列同時被填。metrics 的 lag/lead 仍在交付時記；`records-fetched` 的 per-fetch 聚合由 `Owner` 在 worker 所屬的回應上做（不跨執行緒）。

### 2.5 錯誤與 leader 變化

worker 收到 `NOT_LEADER` / `FENCED_LEADER_EPOCH` / `UNKNOWN_TOPIC_ID`：停該 partition、送 `error` 給控制平面、控制平面 `metadata.requestUpdate` 並在下一次 metadata 版本變化時重新分派（§2.2 的 revoke/assign）。`OFFSET_OUT_OF_RANGE` 同現在：控制平面決定 reset 或丟 `OffsetOutOfRangeException` 給 app。worker 的連線斷掉：它自己重連（`NetworkClient` 既有的 backoff），控制平面不用知道，除非 metadata 說 leader 換了。

### 2.6 執行緒數：一條規則，不加 config

依 01 §2.1 與「參數越少越好」：不加 `fetch.threads`。規則是**預設一條 worker（= 現在的行為，控制與資料同一條執行緒）；只有當一條 worker 連續 N 個 pass 都在飽和狀態（`network.poll` 回來時 socket 仍有未讀資料，且 credit 未用完）才開第二條，上限 = min(broker 數, 可用核心數 / 2)**。這是自適應，不是設定；門檻與 N 由 §4 的量測決定。如果 §4 量到「單執行緒飽和點高於任何合理的 NIC」，就不做這一節，文件留下結論。

## 3. 不做的事

- 不做「每個 partition 一條執行緒」：交接成本與 partition 數成正比，違反效率優先。
- 不把 app 側的反序列化搬到 worker：`Deserializer` 不要求 thread-safe，而且 03 量到 app 側成本在 record 解析與 `ConsumerRecord` 建構，那是 per-record 物件的問題（03 §4.3），不是執行緒數的問題。
- 不把控制平面拆開：manager 之間的依賴（06 R6）讓它們必須在同一條執行緒。

## 4. 實作前的量測門檻

需要兩台機器：consumer 獨佔一台（8 核筆電或 morefine），broker 3 台或同機 3 個 broker 在另一台，網路 ≥ 10 GbE 或至少 2.5 GbE（目前 morefine 只能經 cloudflared 連到，LAN 常不通，所以這一步**還做不了**）。

1. 單條 I/O 執行緒的飽和曲線：topic 18 partition 跨 3 broker，1 KB record，逐步加 partition 數，記 MB/s、I/O 執行緒 CPU 占比、`network.poll` 回來時的未讀比例。飽和點 = I/O 執行緒 CPU ≥ 90% 且 MB/s 不再隨 partition 數上升。
2. 同一曲線下 librdkafka（單執行緒模型）與 franz-go（每 broker 一條 goroutine）的數字，看它們在哪裡飽和。
3. 只有當飽和點低於網路上限，才實作 §2，並用同一條曲線驗收：每加一條 worker 的邊際吞吐 ≥ 70% 線性，CPU/GB 不高於單執行緒的 110%，閒置喚醒不隨 worker 數增加（閒置時 worker 沒有 timer）。
