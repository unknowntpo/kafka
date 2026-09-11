# FETCH-PIPELINE 02：fetch 資料平面與 per-broker 分片

接 01。這份回答兩個問題：fetch 要怎麼脫離 app 執行緒與背景事件迴圈（**這一版要做**），以及每個 broker 一條執行緒的 share-nothing 要怎麼做、什麼時候值得做（**設計留著，這一版不做**，理由在 §1）。

---

## 1. 先修正一個前提：share-nothing 不是它快的原因

「每個 broker 各自一條執行緒，share-nothing 平行化 fetch，所以它快」——這件事我們**設計過（07）但沒有實作過**，而且現有證據指向它不是吞吐增益的來源。三條證據：

**1.1 快的那個引擎是單一 I/O 執行緒。** `FetchPipeline` 的 javadoc 與 CONSUMER-NG-03 §1 都寫明「一條 I/O 執行緒，重用 `NetworkClient` / `Selector` / `FetchSessionHandler`；**每個 broker 一個 fetch 在飛**，前一個回來就發下一個」。1,502 MB/s（6p 1KB、4 核）是單執行緒量到的。程式碼裡 `NodeState` 是 per-broker 的**狀態分片**，但所有分片跑在同一條執行緒上。

**1.2 franz-go 才是「每個 broker 一條 goroutine」，而它比我們的單執行緒慢。** T2 約 1,000 MB/s vs 我們 1,502；T1 1,024 vs 1,236（03 §1）。這條是提示不是證明——Go 與 Java 的差異是混淆因子——但它至少說明 per-broker 執行緒不是吞吐的必要條件。

**1.3 直接 profile：I/O 執行緒根本沒有飽和。** 07 §1 記的 M2b profile（6p 1KB、1,458 MB/s、4 核 morefine）：consumer 全程 1.58 核，其中 **I/O 執行緒 0.75 核**、app 執行緒 0.57 核、JIT 0.17 核。瓶頸是同機的 broker 與 loopback。**沒有被擋住的執行緒可以解放**，多開一條只會多一份喚醒與交接成本。

**1.4 門檻至今沒過。** 07 §4 已經把 T3 定成量測門檻而不是功能：需要 consumer 獨佔一台、broker 在另一台、網路 ≥10GbE。morefine 目前只能經 cloudflared 連到、LAN 常不通，所以這個實驗還做不了。用 03 的 T1 數字外推：單一 I/O 執行緒在**1 核**上就做到 1,236 MB/s（1p 100B 的 profile 有 80% 在 `libc read`），飽和點大約在 2–2.5 GB/s 的 socket 讀取，相當於兩張 10 GbE。**低於這個的部署，per-broker 執行緒只有成本。**

所以：分片的設計我在 §4 寫完整，但這一版 KIP 不含它。把它塞進 KIP 會讓 reviewer 要求證明線性度，而我們拿不出那條曲線。

---

## 2. 你記得的「快」是解耦，不是並行

真正被量到的是兩件解耦：

- **fetch 的續發不再等 app 執行緒。** trunk 是 `FetchRequestManager.java:155-157`：沒有 `pendingFetchRequestFuture` 就 `return PollResult.EMPTY`，而那個 future 只有 app 執行緒送的 `CreateFetchRequestsEvent` 會設。回應到達本身不會觸發下一個 fetch。
- **fetch 不再是被迴圈輪詢的 `RequestManager`。** 回應在背景執行緒完成時，同一條執行緒立刻發下一個，只要 credit 允許。

這是 01 的 L1，1p100B 量到 1.91×，而且**在舊架構上就量到了**。並行化是在這之上的可選延伸。

順帶對照：01 §1.2 的「只有事件迴圈」實驗與 trunk 打平（1p100B 392 vs 404、6p1KB 635 vs 627）。迴圈換掉不會讓 fetch 變快；把 fetch 從迴圈的輪詢模型裡拿出來才會。

---

## 3. 這一版要做的：fetch 成為獨立的資料平面（仍是單執行緒）

目標是讓 fetch 的三個時機都不經過 app 執行緒、也不經過 manager 輪詢：**發（admission）、收（re-issue）、交付（delivery）**。

### 3.1 三條執行緒的分工

| | 誰跑 | 做什麼 |
|---|---|---|
| 控制平面 | `ConsumerNetworkThread`（不變） | membership、heartbeat、commit、metadata、ListOffsets、position 的初始化與 validation |
| 資料平面 | 同一條背景執行緒（這一版），未來可搬走 | 依 credit 發 fetch、回應到達就續發、把 segment 放進 per-partition 佇列 |
| app 執行緒 | 使用者的 `poll()` | 從佇列取 segment、解析 record、推進 position、釋放 segment 還 credit |

**這一版刻意讓資料平面仍在背景執行緒上**，但把它從「被 `RequestManagers.entries()` 輪詢的 `RequestManager`」改成「每個 pass 直接呼叫、由回應驅動」的元件。這樣 §4 的搬移後來只是換執行緒，不是換設計。

### 3.2 app 執行緒與資料平面之間只有三個介面

照 `FetchPipeline` 現在的樣子，app 執行緒只碰三件事，全部是 thread-safe 的：

- `queue(tp).peek() / poll()`：per-partition 的 `ConcurrentLinkedQueue`（每個 partition 只有一個生產者 = 它 leader 的分片，一個消費者 = app 執行緒，所以之後可以換成 SPSC ring）
- `released(segment, n)`：`queuedBytes.addAndGet(-size)` + refcount 釋放
- `starved()`：資料平面因為沒 credit 停下來了，釋放的一方要負責叫醒它

**`poll()` 不再需要為了「請下一個 fetch」而喚醒背景執行緒。** 這是 KAFKA-20854 那個 ping-pong 形狀消失的地方，也是 §3 的主要收益。

### 3.3 credit admission（01 §4 的機制，這裡補分片相關的部分）

```
可以再發一個 fetch  ⟺  在途 bytes + 已排隊未交付 bytes < credit
```

`inFlightBytes` 現在是資料平面私有的 `long`（單執行緒），`queuedBytes` 是 `AtomicLong`（app 執行緒會減）。分片之後 credit 要切成「每個分片一份 + 一個全域溢出份額」，理由在 §4.3。**現在就用「分片持有自己的在途帳、共享一個原子的排隊帳」這個形狀寫**，之後不用改。

### 3.4 這一版要拿掉的東西：`nodeFree`

`FetchPipeline.sendFetches(now, nodeFree)` 有一個 `Predicate<Node>`，用來避免 fetch 把 broker 的唯一 in-flight 槽佔滿而餓死控制請求。那條規則在 consumer-ng 是必要的（引擎刻意每個 broker 只有一個 fetch 在飛），而且它**過度寬鬆**：任何沒有指定 node 的待送請求（FindCoordinator、metadata）會讓**所有** broker 都停止 fetch。

trunk 不需要它：`ConsumerUtils.java:77` 的 `CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION = 100`，控制請求不會被一個 fetch 擋住。**移植時直接刪掉這個參數**，不要把它一起帶進 trunk。

### 3.5 trunk 的耦合圖（實地讀過，這是實作的依據）

**已經是兩條執行緒了，切法是「階段」而不是「連線」。** 背景執行緒建請求 + 處理回應（`ConsumerNetworkThread.java:222-228`）；app 執行緒排空 buffer、反序列化、推進 position（`AsyncKafkaConsumer.java:1983/2033/2075`、`FetchCollector.java:185`）。所以資料平面要做的不是「新增一條執行緒」，而是**把已經存在的那條縫改成由回應驅動**。

**position 的讀寫本來就跨執行緒：**
- 讀（背景，建 fetch 請求時）：`AbstractFetch.java:554` `subscriptions.position(tp)`，用在 `:479` 的 `position.offset` 與 `:482` 的 epoch。
- 寫（app，交付之後）：`FetchCollector.java:185` `subscriptions.position(tp, nextPosition)`。

兩者靠三樣東西串起來：`SubscriptionState` 的 monitor、`AsyncKafkaConsumer.java:2068-2080`（app 執行緒在背景的 validate-positions 階段完成前拒絕收集，註解明講是為了避免兩條執行緒同時寫 position）、以及 `FetchCollector.java:168` 的 `nextInLineFetch.nextFetchOffset() == position.offset` 過期守衛。

**`SubscriptionState` 是 thread-safe 的，但是粗粒度的單一 monitor**（`SubscriptionState.java:75` 明寫 thread-safe，幾乎每個 public method 都 `synchronized` 在實例上）。所以 §4.2 的問題**不是正確性，是競爭**：多個分片會在同一把鎖上排隊。

**`FetchBuffer` 的交接**：一個 `ReentrantLock` + 一個 `Condition`（`FetchBuffer.java:54-55`），app 執行緒阻塞在 `AsyncKafkaConsumer.java:2024` 的 `fetchBuffer.awaitWakeup(pollTimer)`。生產者是 `AbstractFetch.java:225`。另外有第三個寫入者：`Consumer.wakeup()` 會從任意執行緒呼叫 `fetchBuffer.wakeup()`（`WakeupTrigger.java:96-100`）。

**`NetworkClient` 與 `Selector` 都明寫 not thread-safe**（`NetworkClient.java:85`、`Selector.java:86`），而且整個 consumer 只有一個，在背景執行緒上延遲建立（`NetworkClientDelegate.java:488-503` 的 `CachedSupplier`，由 `ConsumerNetworkThread.initializeResources()` 呼叫）。coordinator、commit、heartbeat、offsets、fetch、metadata 全部共用它（`RequestManagers.java:94-104`）。這直接決定 §4.4：分片必須有自己的 client，於是有自己的連線。

**metadata 查 leader 是安全的**：`Metadata.fetch()`（`Metadata.java:130-132`）與 `topicIds()` 讀的是 `volatile MetadataSnapshot`，`Cluster` 不可變，所以是 lock-free 讀。但 `Metadata.currentLeader(tp)`（`:296`）是 `synchronized`，而那把鎖在每次 metadata 回應套用時也被背景執行緒持有。分片應該走 `fetch()` 的快照路徑，避免 `currentLeader`。

**metrics 本來就跨執行緒**：`Sensor.record` 是 thread-safe 的（`Sensor.java:226-236`，雙層 `synchronized`），但 **`FetchMetricsAggregator` 不是**（`FetchMetricsAggregator.java:31-56`，plain `HashSet` / `HashMap`、`record()` 沒有 `synchronized`），而它被一個 fetch 回應的所有 partition 共用（`AbstractFetch.java:178`）。consumer-ng 的 `FetchSegment.Owner` 做的是同一件事，但用原子 refcount。**移植時用 `Owner` 取代 `FetchMetricsAggregator` 的角色。**

### 3.6 L1 在 trunk 上的具體改動清單

1. **給每個 partition 一個私有的 prefetch cursor**（consumer-ng 的 `PartitionQueue.nextFetchOffset`），從 position 起算。建 fetch 請求時讀 cursor，**不讀** `subscriptions.position(tp)`（取代 `AbstractFetch.java:554`）。這是預抓的前提：position 是「已交付到哪」，cursor 是「已請求到哪」，兩者必須分開。
2. **拿掉 C1b 與 C1c**：`AbstractFetch.java:343-350` 的 `isNotBuffered` 與 `:466-471` 的 `bufferedNodes` 一起刪掉（連同 `bufferedNodes(...)` 這個 method，`:641-654`），改成 credit admission（在途 bytes + 已 buffer 未交付 bytes < credit）。有了第 1 項的 cursor，每個可 fetch 的 partition 永遠在每個 fetch 請求裡，所以 C1c 要防的 fetch-session eviction 不會發生。`FetchBuffer` 今天不記 bytes，要加。
3. **把 C1a 改成回應驅動**：`FetchRequestManager.java:155-157` 的 gate 移除，改由回應處理完成後在同一條背景執行緒上續發。
4. **fencing**：seek / reset / 撤銷指派要讓 cursor 與已 buffer 的資料一起作廢。`FetchCollector.java:168` 的等式守衛在「連續 offset、依序交付」的前提下仍然成立（第二份 `CompletedFetch` 的起始 offset 等於第一份的結束），但 seek 之後不成立，所以 cursor 要帶 position epoch。**這是 L1 唯一的正確性風險。**
5. **不要動 `AsyncKafkaConsumer.java:2068-2080` 的 validate-positions 屏障**：它擋的是 position 的雙寫，跟預抓無關，拿掉會引入 §3.5 說的那個 race。

---

## 4. per-broker 分片（設計留存，門檻見 §4.5）

### 4.1 現在的程式碼已經是分片形狀的

`FetchPipeline` 裡每個 broker 已經有 `NodeState`：自己的 `FetchSessionHandler`、`inFlight`、`inFlightOffsets`、`pendingResponse`、`pendingPayload`。分片要做的是把 `NodeState` 升級成「自己的 `NetworkClient` + `Selector` + 執行緒」。剩下真正共享的只有五樣東西，逐一處理：

| 共享的東西 | 現況 | 分片後 |
|---|---|---|
| `queues`（per-partition） | `ConcurrentHashMap`，每個 partition 一個 `ConcurrentLinkedQueue` | 每個 partition 只有一個生產者（它 leader 的分片），天然 SPSC |
| `DirectBufferPool` | lock-free per-class deque + 原子計數 | 直接共用；**有界記憶體的上限與分片數無關**，這是它相對於 per-thread pool 的優點 |
| `queuedBytes` / credit | 一個 `AtomicLong` + 一個私有 long | §4.3 |
| `SubscriptionState` | 分片會讀 `isFetchable` / `position`，會寫 high watermark / logStartOffset / lastStableOffset / offset reset | §4.2 — **最大的風險** |
| `FetchMetricsManager` sensors | `Sensor.record` 每次取時間並**上鎖**（05 §M2b profile：它本身佔 3.0% 樣本） | §4.4 |

### 4.2 `SubscriptionState`：不要讓它變成多執行緒熱點（是競爭問題，不是安全問題——見 §3.5）

07 §2.1 的答案是**分片不讀 `SubscriptionState`**：它從控制平面的 `assign(partition, offset, leaderEpoch)` 訊息拿到起始 offset，之後自己維護 `nextFetchOffset`；position 仍由 app 執行緒在交付時寫回。控制平面與分片之間只有 MPSC 佇列 + wakeup，訊息是 `assign` / `revoke` / `seek`（控制→分片）與 `error(partition, Errors)`（分片→控制）。

但現在的 `queuePartitionData` 會寫三個 log offset（`tryUpdatingHighWatermark` 等，給 lag/lead metrics 與 `currentLag()` 用）。分片之後這三個寫入要嘛搬到 app 執行緒的交付點（它本來就在寫 position），要嘛跟著 segment 一起傳。**傾向搬到交付點**：那裡已經持有 `SubscriptionState`，而且 lag 在交付時記才是使用者看到的語意。

### 4.3 credit 切分

從「全域一份」變成「每個分片 = 總額 / 分片數，另加一個全域溢出份額」。理由：一個 broker 很慢時不會把別人的額度吃光，也不會讓總量超過上限。`FetchSegment.Owner` 的 refcount 已經是原子操作；釋放時只需要叫醒**那個**分片（現在是叫醒唯一的 I/O 執行緒）。

### 4.4 兩個 07 沒寫到的成本

**(a) 連線數。** 07 §2.1 讓每個分片有自己的 `NetworkClient`，但沒討論這會讓同一個 consumer 對同一個 broker 開**兩條**連線（控制平面一條、分片一條）。broker 端有 `max.connections.per.ip`，大叢集的維運者會在 review 時問這件事。

實際的穩態沒有那麼糟：控制平面只固定跟 **coordinator** 講話（heartbeat / commit），metadata 與 ListOffsets 走 least-loaded node 且不頻繁，其餘連線會被 `connections.max.idle.ms` 收掉。所以穩態多的是「對 coordinator broker 多一條連線」。**但這必須寫進 KIP 明說**，不能讓 reviewer 自己發現。

替代方案是 librdkafka / franz-go 的做法：**用 broker 當分片鍵，而不是用功能**——所有寄給 broker B 的請求（包含 B 剛好是 coordinator 時的 heartbeat / commit）都跑在 B 的執行緒上。這樣連線數不變，是真正的 share-nothing。代價是整個 consumer 的請求路由都要重寫，而且違反 07 §3 的「不把控制平面拆開」（manager 之間的依賴讓它們必須同執行緒）。**這是另一個 KIP 的規模，不是這條線。**

**(b) `Sensor` 上鎖。** fetch metrics 的 `Sensor.record` 每次取一次時間並上鎖，05 已量到它佔 3.0% 樣本（那是單執行緒、無競爭的情況）。多個分片同時記 `bytes-fetched` / `records-fetched` 會在同一個 sensor 上競爭。要嘛 per-shard sensor 再聚合，要嘛把 per-fetch 的聚合留在 `Owner`（它本來就不跨執行緒）。**分片實作前必須先量這一項**，否則可能出現「加了執行緒，吞吐沒動，CPU/GB 變差」。

### 4.5 執行緒數：一條規則，不加 config

依「參數越少越好」：不加 `fetch.threads`。預設一條（= §3 的行為）；只有當一條分片連續 N 個 pass 都在飽和狀態（`network.poll` 回來時 socket 仍有未讀資料，且 credit 未用完）才開第二條，上限 = min(broker 數, 可用核心數 / 2)。門檻與 N 由 §4.6 的量測決定。

### 4.6 實作前的量測門檻（沿用 07 §4，加一項）

1. **單執行緒飽和曲線**：consumer 獨佔一台、3 個 broker 在另一台、≥2.5 GbE；18 partition、1KB record，逐步加 partition，記 MB/s、I/O 執行緒 CPU 占比、`network.poll` 回來時的未讀比例。飽和 = I/O 執行緒 CPU ≥ 90% 且 MB/s 不再上升。
2. 同曲線下的 librdkafka 與 franz-go，看它們在哪裡飽和。
3. **（新增）sensor 競爭的隔離量測**：在單執行緒上把 fetch metrics 全關，量吞吐與 CPU/GB 的上界；那是分片能拿到的最好情況。
4. 只有當飽和點低於網路上限才實作，驗收：每加一條分片的邊際吞吐 ≥ 70% 線性、CPU/GB ≤ 單執行緒的 110%、閒置喚醒不隨分片數增加。

---

## 5. 不做的事

- **每個 partition 一條執行緒**：交接成本與 partition 數成正比。
- **把反序列化搬到分片**：`Deserializer` 不要求 thread-safe；而且 03 量到 app 側成本在 per-record 物件（01 的 L2），不是執行緒數。
- **把控制平面拆成多執行緒**：manager 之間有 read-then-clear 的共享狀態，順序是契約（見 06 R6）。
- **現在就做 §4**：門檻沒過，見 §1。

---

## 6. 待拍板

1. §3 的範圍（fetch 成為由回應驅動的獨立資料平面、仍單執行緒）就是 01 的 L1，同意的話我照這個開工。
2. §4 要不要留在 KIP 文件裡當「未來工作」，還是完全移出、只留在我們的工作文件？我傾向**留一段「未來工作」並附門檻**，因為 reviewer 一定會問「為什麼不多執行緒」，把答案先寫好比被問到再補好。
3. §4.2 的三個 log offset 寫入搬到 app 執行緒交付點——這會改變 `currentLag()` 的更新時機（從「回應到達」變成「交付」）。這算不算行為變更要寫進 KIP？我認為要。
