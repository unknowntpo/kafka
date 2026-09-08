# AsyncKafkaConsumer 現況問題分析（v2 重寫前置文件）

- 基準：apache/kafka `trunk` @ `820533b870`（worktree `async-consumer-v2`，與 `upstream/trunk` 無分歧）
- 目標：在**公開 API（`KafkaConsumer` / `Consumer<K,V>`）完全不變**的前提下，重寫一個吞吐量至少為現行 `AsyncKafkaConsumer` 兩倍的實作
- 本文範圍：只回答「現在的架構有什麼問題」。設計方案另文處理
- 檔案路徑省略前綴 `clients/src/main/java/org/apache/kafka/clients/`，行號以基準 commit 為準

---

## 0. 一句話結論

`AsyncKafkaConsumer` 把 **I/O 搬到了背景執行緒，但沒有把「每筆紀錄的成本」搬走**：CRC、解壓縮、record 解析、反序列化、`ConsumerRecord` 配置全部仍在應用執行緒上，和 `ClassicKafkaConsumer` 一模一樣。它在這之上又加了一層每次 `poll()` 都要付的跨執行緒交握（2 個 event、2 次 selector wakeup、1 組 condition wait/signal、十幾個短命物件、十幾次 `synchronized`）。同時 fetch 管線深度被硬限制為「每個 broker 同時只有一個 in-flight 請求，而且該 broker 上任何 partition 還有未消費資料時完全不再發 fetch」，使抓取與消費實質上是序列化的。

因此：**單靠修 event loop 或減少配置，不可能得到 2 倍。** 兩倍必須來自 (1) 把 per-record 工作移出應用執行緒並平行化，(2) 讓 fetch 管線真正多深度地跑在消費前面，(3) 減少 payload 拷貝次數。第 1 節先建立這個瓶頸模型，第 2 節逐項列問題，第 3 節說明哪些問題決定上限、哪些只是雜訊。

---

## 1. 吞吐量瓶頸模型

現行架構有兩條執行緒：

```
 應用執行緒 (poll)                                 背景執行緒 (ConsumerNetworkThread)
 ─────────────────────────────                     ─────────────────────────────────────
 poll(Duration)                                    runOnce():
   ├─ checkInflightPoll                              ├─ drain applicationEventQueue
   │    └─ new AsyncPollEvent ──── queue+wakeup ───► │    └─ process(AsyncPollEvent)
   │                                                 │         ├─ reconcile / autocommit timer
   ├─ processBackgroundEvents ◄── queue ──────────── │         ├─ updateFetchPositions
   ├─ pollForFetches                                 │         └─ createFetchRequests()  (只設 gate)
   │    ├─ collectFetch()  ← 空                      ├─ for rm in managers: rm.poll()
   │    ├─ fetchBuffer.awaitWakeup(≤100ms) ◄─signal─ │    └─ FetchRequestManager.pollInternal
   │    └─ collectFetch()                            │         ├─ prepareFetchRequests (掃 SubscriptionState / FetchBuffer)
   │         └─ FetchCollector.collectFetch          │         └─ send FetchRequest
   │              └─ CompletedFetch.fetchRecords     ├─ NetworkClientDelegate.poll → select()
   │                   ├─ CRC32C 全掃                │    └─ handleFetchSuccess
   │                   ├─ 解壓縮                     │         ├─ FetchResponse.parse (zero-copy slice)
   │                   ├─ DefaultRecord.readFrom     │         └─ new CompletedFetch → fetchBuffer.add ─┐
   │                   ├─ Deserializer ×2            └─ for rm in managers: rm.maximumTimeToWait()      │
   │                   └─ new ConsumerRecord                                                            │
   └─ sendPrefetches → new CreateFetchRequestsEvent ── queue+wakeup ──►                                 │
                                                                        (signalAll 喚醒應用執行緒) ◄────┘
```

穩態吞吐量 = min( **網路管線供給速率**, **應用執行緒的 per-record 處理速率** )。

- 應用執行緒的 per-record 成本（解壓、解析、反序列化、配置）在 async 和 classic 是**同一份程式碼、同一條執行緒**（`CompletedFetch.java:187-339`，由 `AsyncKafkaConsumer.java:2042-2083` 呼叫）。這是單一 partition 高吞吐時的硬上限，async 版本並沒有降低它。
- 網路管線供給速率被 `AbstractFetch.prepareFetchRequests`（`AbstractFetch.java:420-497`）限制成「每 node 一個 in-flight、有 buffered 資料的 node 不發 fetch」，所以背景執行緒大部分時間在等應用執行緒把 buffer 吃完，而不是在填 buffer。
- 背景執行緒真正做的 per-byte 工作只有 socket read 與 protocol framing；它的 CPU 幾乎是閒置的。

換言之，現行架構有兩顆核心可用，但 per-record 的重活集中在一顆上，另一顆在等。要達到 2 倍，重寫必須改變這個分工，而不是把同一個分工做得更省。

---

## 2. 問題清單

分為六類：A 資料路徑（per-record / per-byte）、B fetch 管線深度（per-fetch）、C `poll()` 固定成本（per-poll）、D 共享狀態鎖、E 背景執行緒固定成本、F 結構與耦合。每項附影響與證據。

### A. 資料路徑：per-record / per-byte 成本

**A1. 解壓縮、CRC、record 解析、反序列化全部在應用執行緒。**
`FetchCollector.collectFetch` → `CompletedFetch.fetchRecords` → `nextFetchedRecord`（`CompletedFetch.java:187-244`，`streamingIterator` 在 :226）→ `parseRecord`（:312-339）。背景執行緒在 `AbstractFetch.handleFetchSuccess`（`AbstractFetch.java:151-249`）只做 `FetchResponse.responseData()` 與 `new CompletedFetch`，從不碰 record bytes。
影響：這是吞吐上限所在；async 對 per-record 成本的改善為零。

**A2. 每筆紀錄 9–12 個物件配置（未計使用者 deserializer 自身配置）。**
key/value 各一個 slice（`Utils.readBytes`）、`DefaultRecord`、key/value 各一個 `duplicate()`（`DefaultRecord.java:147,157`）、`RecordHeaders` + 內部 `ArrayList`（**即使沒有 header 也無條件配置**，`CompletedFetch.java:319`、`RecordHeaders.java:38-46`）、`Optional<Integer>` leader epoch（**每筆重算**，雖然它是 batch 常數，`CompletedFetch.java:284,352-354`）、`ConsumerRecord`（內含 `Optional.empty()` deliveryCount）。結果 `ArrayList` 用預設容量 10 而非 `maxRecords`（:269）。
影響：GC 壓力與 cache miss 直接吃掉 per-record 處理速率。

**A3. Payload 拷貝次數過多，且由 deserializer 決定。**
`parseRecord` 呼叫 `Deserializer.deserialize(topic, headers, ByteBuffer)`；其 default 實作是 `Utils.toNullableArray` 強制拷貝成 `byte[]`（`Deserializer.java:115-117`）。只有內建的 `String/Integer/Long/ByteBuffer…Deserializer` 覆寫了 `ByteBuffer` 版本；`ByteArrayDeserializer`、`BytesDeserializer`、`ListDeserializer` 與**幾乎所有第三方 deserializer（Avro / Protobuf / JSON）**都會被強制拷貝。
拷貝次數（plaintext）：未壓縮 + `ByteArrayDeserializer` = 2 次；**壓縮 + `ByteArrayDeserializer` = 4 次**（socket→payload、解壓→`ChunkedBytesStream` 中間陣列、`Utils.readFully`→每筆一個 `ByteBuffer.allocate`（`DefaultRecord.java:286`）、`toNullableArray`→`byte[]`）。SSL 再加 2 次。
影響：壓縮 topic 的 per-byte 成本被拷貝主導。

**A4. `check.crcs=true`（預設）對每個 batch 做一次完整的 CRC32C 掃描。**
`CompletedFetch.maybeEnsureValid`（:158-167）→ `DefaultRecordBatch.ensureValid`（`DefaultRecordBatch.java:150-158,395-401`），範圍是 attributes 到 batch 結尾，含仍壓縮的 payload。這是解壓與解析之外對每個 byte 的**額外一整遍**讀取，在應用執行緒上。
影響：per-byte 成本 +1 pass；且此 pass 天然可平行、可搬到背景執行緒。

**A5. 每個 fetch response 配置一個全新、大小等於整個 response 的 heap `ByteBuffer`，無 pool、無上限。**
Consumer 的 `Selector` 建構時硬寫 `MemoryPool.NONE`（`Selector.java:213`）與 `NetworkReceive.UNLIMITED`；`MemoryPool.NONE.tryAllocate` 就是 `ByteBuffer.allocate(size)`（`MemoryPool.java:28-31`）。預設 `fetch.max.bytes=50MB`，因此每個 response 可能是一次 50 MB 的 young-gen 配置。
所有 partition 的 `MemoryRecords` 都是這個 buffer 的 slice（zero-copy，`ByteBufferAccessor.java:74-81`），所以**只要 `FetchBuffer` 裡有任何一個 `CompletedFetch` 尚未消費完，整個 response buffer 都不能回收**（`max.poll.records` 造成的部分消費特別容易觸發）。
影響：大量 humongous allocation、GC 停頓、記憶體佔用不可預測。

**A6. 每個壓縮 batch 都重新建構 `Compression` 物件與串流。**
`DefaultRecordBatch.recordInputStream`（:273-277）每個 batch 呼叫 `Compression.of(type).build()`（配置 Builder + Compression），再包 codec stream + `ChunkedBytesStream`。`BufferSupplier` 只回收中間陣列（`AbstractFetch.java:100`，單執行緒 `HashMap`）。
影響：小 batch 場景下固定成本明顯；也阻礙跨執行緒解壓（`BufferSupplier` 非 thread-safe）。

**A7. 每個 partition 每個 response 建兩個 batch iterator。**
`CompletedFetch` 建構子建一個（:98），`FetchCollector.handleInitializeSuccess`（`FetchCollector.java:268`）為了測 `hasNext()` 再建一個就丟掉。另外 `CompletedFetch` 無條件配置 `abortedProducerIds` `HashSet`（:101），即使 `read_uncommitted`。

### B. Fetch 管線深度

**B1. 每個 broker 同時最多一個 in-flight fetch。**
`nodesWithPendingFetchRequests.contains(node.id())` → skip（`AbstractFetch.java:462-464`），而 `NetworkClient` 明明用 `max.in.flight=100` 建立（`ConsumerUtils.java:77`）。
影響：單一 partition 的管線深度就是 1；RTT 與 broker 端 `fetch.max.wait.ms` 直接串進吞吐公式。

**B2. 有 buffered 資料的 partition 不再 fetch，而且擴大到「該 node 上所有 partition 都不 fetch」。**
`fetchablePartitions(buffered)` 排除 buffered partition（:343-350）；`bufferedNodes`（:445, :465-470, :641-654）再排除整個 node。註解說原因是避免 fetch session 把 buffered partition 踢出 session cache。
影響：抓取與消費在 node 粒度上序列化：應用執行緒吃完 buffer → 才允許發下一個 fetch → 等 RTT → 才有新資料。這是 2 倍目標最直接的結構性障礙之一。

**B3. Fetch 只由應用執行緒觸發，背景執行緒從不自主預取。**
`FetchRequestManager.pollInternal` 以 `pendingFetchRequestFuture == null` 直接回 `EMPTY`（`FetchRequestManager.java:155-158`）；只有 `CreateFetchRequestsEvent` 或 `AsyncPollEvent` 的續段會設定它（`ApplicationEventProcessor.java:244-247, 763`）。
影響：應用執行緒不呼叫 `poll()` 的期間（正在處理上一批），背景執行緒不會主動把 buffer 填滿到某個目標深度；「prefetch」只是 `poll()` 回傳前多送一個 event（`AsyncKafkaConsumer.java:965, 2120-2127`）。

**B4. `FetchBuffer` 沒有任何 byte 或深度上限，也沒有「目標深度」概念。**
`FetchBuffer.java:50-67` 是無界 `ConcurrentLinkedQueue`。實際上限完全是 B1/B2 的副作用（每 partition ≤1 個 `CompletedFetch`、每 node ≤1 個 in-flight）。
影響：無法用「保持 N MB 在途」這種正常的 flow-control 設計來換吞吐。

**B5. `max.poll.records` 造成部分消費時，連鎖停擺。**
`FetchCollector.collectFetch` 只在 `isExhausted()` 才 `drain()`（`FetchCollector.java:191-193`）。未 drain 的 `CompletedFetch`：留在 `bufferedPartitions()` 裡（B2 生效，整個 node 停 fetch）、pin 住整個 response buffer（A5）、pin 住解壓串流與 `BufferSupplier` 的緩衝區、metrics 延後。
影響：`max.poll.records` 小於 batch 大小的常見設定下，吞吐被綁在應用端的 `poll()` 節奏上。

### C. `poll()` 的固定成本與跨執行緒交握（每次呼叫，與紀錄數無關）

**C1. 每次回傳資料的 `poll()` 產生 2 個 event、2 次 selector wakeup。**
`AsyncPollEvent`（`AsyncKafkaConsumer.java:1005-1008`）與 `CreateFetchRequestsEvent`（:2122），各自經 `ApplicationEventHandler.add`（`ApplicationEventHandler.java:96-105`）→ `LinkedBlockingQueue.add`（node 配置 + putLock）→ `networkThread.wakeup()` → `nioSelector.wakeup()`（eventfd/pipe 寫入 + JDK monitor）。`CreateFetchRequestsEvent` 還是 `CompletableEvent`，被 reaper 追蹤（`ConsumerNetworkThread.java:258-259`）。
Classic 的對應成本：一次 `client.poll()`，零 event。

**C2. 重複工作：`collectFetch()` 每次空 poll 執行兩次，`hasAllFetchPositions()` 每次 poll 執行兩次。**
`collectFetch` 在 `pollForFetches` 前後各一次（:1983, :2033），每次配置 `Fetch`（2 個 `HashMap`）+ `ArrayDeque`（`FetchCollector.java:92-93`）。`hasAllFetchPositions` 在 :1998 與 `PositionsValidator.java:149` 各一次，都是 `synchronized` 全掃 assignment。

**C3. `FetchBuffer` 是單一 `ReentrantLock` + 單一 `Condition`，`ConcurrentLinkedQueue` 形同虛設。**
所有存取都先拿鎖（`FetchBuffer.java:74-150, 212-273`）。`add()` 走 `addAll(List.of(cf))`（:98-100）：**每個 partition** 配置一個單元素 list、拿鎖、`signalAll()`。應用執行緒在 `collectFetch` 迴圈裡每個 partition 拿鎖 3–4 次（`nextInLineFetch` / `peek` / `setNextInLineFetch` / `poll`）。`bufferedPartitions()` 在鎖內配置 `HashSet` 並走遍整個 queue（:244-259），而它被兩條執行緒各自每輪呼叫（`AbstractFetch.java:429`、`AsyncKafkaConsumer.java:2005`）。

**C4. `awaitWakeup` 是 permit 語意而非狀態檢查，會產生空喚醒；而等待上限被鎖死在 100 ms。**
`awaitWakeup`（:165-194）以 `wokenup.compareAndSet(true,false)` 判斷，任何一次 `wakeup()`（每個 fetch 完成、失敗、無事可 fetch 都會呼叫，`AbstractFetch.java:297`、`FetchRequestManager.java:169`）都會放行應用執行緒一次，即使沒有資料，導致外層 `do/while` 多跑一整輪（含新 event、新 wakeup）。
`FetchRequestManager.maximumTimeToWait` 在沒有 in-flight fetch 時回傳 `retryBackoffMs`（100 ms，`FetchRequestManager.java:83-85`），使 `cachedMaximumTimeToWait` 上限為 100 ms → 應用執行緒每 100 ms 至少醒一次；背景執行緒也是。

**C5. `WakeupTrigger` 每次 poll 三次 CAS 迴圈與兩組 lambda 配置。**
`maybeTriggerWakeup`（`WakeupTrigger.java:149-164`）、`setFetchAction`（:96-113）、`clearTask`，各是 `AtomicReference.getAndUpdate`；前兩者配置 `AtomicBoolean` + 捕獲式 lambda（+ `FetchAction`）。Classic 只是一個 `AtomicBoolean` 讀取。

**C6. `processBackgroundEvents` 在佇列為空時仍做全套工作。**
無條件配置 `AtomicReference`（:2371）、`ArrayList`（`BackgroundEventHandler.java:66`）、`drainTo` 拿 takeLock、`recordBackgroundEventQueueSize(0)` sensor、`backgroundEventReaper.reap()` 全掃 + `time.milliseconds()`（:2405）。

**C7. Metrics 在熱路徑上每次 poll 拿 5 次以上 `Sensor` 鎖，且 per-partition lag/lead 每次重建 `SensorBuilder`。**
`recordPollStart/End`、`recordApplicationEventQueueSize` ×2、`recordBackgroundEventQueueSize`，各是兩層 `synchronized` + `time.milliseconds()`。`recordPartitionLag/Lead`（`FetchMetricsManager.java:138-166`）每個 partition 每次 poll 做 `tp + ".records-lag"` 字串串接、`SensorBuilder` 配置、`ConcurrentHashMap` 查找。都沒有 `shouldRecord()` 守衛。

**C8. 例外當控制流程。**
reconciliation gate 以 `catch (TimeoutException)` 回傳空 fetch（:2056-2058）；reaper 對每個過期 event `String.format` 建 `TimeoutException`（`CompletableEventReaper.java:105`）。

**C9. 每次 poll 至少 2 個 `Timer`、12–16 次 `time.milliseconds()`、2–4 個 `Optional`（`AsyncPollEvent.error()`）、2 次 `Map.copyOf`（`Fetch.java:115` + `ConsumerRecords.java:78`）。**

小計（1 partition、穩態）：每次 `poll()` 約 20–30 個短命物件、3–4 次跨執行緒交握、7–11 次 `FetchBuffer` 鎖、10–12 次 `SubscriptionState` monitor、2 次 `Metadata` monitor、8–9 次 CAS。這些在大 batch 時被攤薄，但在低延遲 / 小 batch / 多 partition 場景是主要成本。

### D. 共享狀態的鎖設計

**D1. `SubscriptionState` 是單一粗粒度 monitor，約 80 個 `synchronized` 方法，兩條執行緒都在熱路徑上頻繁進出。**
底層 `PartitionStates` 是 `LinkedHashMap`，非 thread-safe。應用執行緒每次 poll 進出 ~10–12 次；`FetchCollector.fetchRecords` 每個回傳 partition 6 次（`isAssigned`、`isFetchable`、`position` ×2、`partitionLag`、`partitionLead`）；`initialize` 每個 partition 最多 4 次（HW / LSO / logStart / preferredReplica）。背景執行緒 `prepareFetchRequests` 每次 1 + P + 2B + 2U 次，其中 `fetchablePartitions` 內部對每個 partition 重入 `isFetchableAndSubscribed`（`SubscriptionState.java:537`）。`numAssignedPartitions()` 是 `synchronized` 卻只讀 volatile int（:503）。
影響：在多 partition、高 poll 頻率下，兩條執行緒在這個 monitor 上互相排隊；也讓「把 record 處理平行化」無從下手，因為 position 更新綁在同一把鎖上。

**D2. `Metadata` monitor 在熱路徑上。**
`updateVersion()`（`Metadata.java:712`）每次 poll 至少一次（`PositionsValidator.java:144`，應用執行緒）+ 一次（背景 `updateFetchPositions`）；`maybeThrowAnyException()`（:610）**每個背景迴圈無條件一次**（`NetworkClientDelegate.java:169`）。`ConsumerMetadata.retainTopic` 在持有 `Metadata` monitor 時再進 `SubscriptionState` monitor（鎖巢狀）。

**D3. Position 更新與 fetch 決策共用同一把鎖，導致 `drain()` 順序耦合。**
`FetchCollector.java:189-193` 刻意在更新 position 後才 `drain()`，以確保背景執行緒看到新 position 再看到 `isConsumed`。這種跨執行緒的隱式順序約束是可平行化的阻礙。

### E. 背景執行緒每輪固定成本（即使無事可做）

**E1. 每輪固定配置十餘個物件、遍歷 managers 兩次。**
`processApplicationEvents` 無條件 `new LinkedList`（`ConsumerNetworkThread.java:248`）；`entries()` 迭代兩次（:222, :232）；`OffsetsRequestManager.poll` **無條件** `new ArrayList` + `PollResult` + `unmodifiableList`（`OffsetsRequestManager.java:169-174`）；heartbeat manager 每輪 2–3 個 `Optional` + 1 個 `PollResult`（`AbstractHeartbeatRequestManager.java:164-200, 254-271`）；`CommitRequestManager.maximumTimeToWait` 用 `Optional.map` 裝箱兩次 `long`（`CommitRequestManager.java:223-225`）；`TopicMetadataRequestManager.poll` 一個 `ArrayList`；`FetchRequestManager.poll` 每次建 3 個 method-reference lambda（`FetchRequestManager.java:119-123`）；reaper 的 `uncompletedEvents()` 與 `maybeFailOnMetadataError` 各一個 `ArrayList`（:240-241, :442）；`NetworkClient.poll` 一個 responses `ArrayList`；`trySend` / `checkDisconnects` 各一個 iterator。約 3 次 `currentTimeMillis` + 3 次 `nanoTime`，3 個 sensor。

**E2. `currentTimeMs` 在阻塞 `select()` 之前取樣，之後最多 5 秒過時。**
`ConsumerNetworkThread.java:214` 取樣，:228 阻塞（上限 `MAX_POLL_TIMEOUT_MS = 5000`），:232-241 的 `maximumTimeToWait`、reaper、metadata error 檢查全部用舊時間。

**E3. Event 完成不會喚醒應用執行緒；`AsyncPollEvent` 靠 volatile 輪詢。**
`AsyncPollEvent` 沒有主結果 future，`completeSuccessfully()` 只設 volatile（`AsyncPollEvent.java:121-133`），應用執行緒在 `checkInflightPoll` 輪詢。`BackgroundEventHandler.add` 也不喚醒（`BackgroundEventHandler.java:53-58`）。應用執行緒唯一的喚醒來源是 `FetchBuffer`；其他情況都靠 100 ms 上限（C4）。
影響：rebalance、錯誤傳遞等事件的延遲以 100 ms 為單位；也解釋了為什麼很多路徑需要 `processBackgroundEvents(future, timer, …)` 這種 100 ms 切片的忙等迴圈（:2456-2489）。

**E4. `FetchResponse.responseData()` 每次呼叫重建 `LinkedHashMap` 與每 partition 一個 `TopicPartition`（`FetchResponse.java:97-112`）；`handleFetchSuccess` 再配置 `HashSet` + `FetchMetricsAggregator`（`AbstractFetch.java:177-178`）。**

### F. 結構與耦合（影響重寫的可行邊界）

**F1. `ShareConsumerImpl` 與 Kafka Streams 深度依賴這套 internals。**
`ShareConsumerImpl` 使用 `ApplicationEventHandler`（內部 `new ConsumerNetworkThread`）、`RequestManagers`（share 版 overload，`RequestManagers.java:340`）、`NetworkClientDelegate`、`ApplicationEventProcessor`、`BackgroundEventHandler`、`CompletableEventReaper`、`AsyncConsumerMetrics`、`WakeupTrigger`、`SubscriptionState`，甚至直接引用 `AsyncKafkaConsumer.CompletableEventReaperFactory`（`ShareConsumerImpl.java:240`）。它不用 `FetchBuffer` / `FetchCollector` / `FetchRequestManager`（有自己的 `ShareFetchBuffer` 等）。
Streams：`StreamThread.java:592` 直接 `new AsyncKafkaConsumer<>(…, streamsRebalanceData)` 繞過 `KafkaConsumer`；:1203 直接 cast 呼叫 `subscribe(Collection, StreamsRebalanceListener)`；`ConsumerWrapper.java:48-52` 的 SPI 型別固定為 `AsyncKafkaConsumer`。`StreamsMembershipManager` 不繼承 `AbstractMembershipManager`，`ApplicationEventProcessor` 用 `MembershipManagerShim` 硬接（:862-870）。
含意：替換 `ConsumerNetworkThread` / `RequestManagers` / `ApplicationEventHandler` 會同時波及 share consumer 與 Streams；只替換 `FetchBuffer` / `FetchCollector` / `FetchRequestManager` / `CompletedFetch` 這一層對 share consumer 是安全的。`AsyncKafkaConsumer` 這個類名與 `subscribe(Collection, StreamsRebalanceListener)` 必須保留（或提供同名 shim）。

**F2. 測試表面綁在內部建構子上。**
`AsyncKafkaConsumerTest`（112 個測試）大量使用 `AsyncKafkaConsumer.java:609` 那個注入 22 個協作者的建構子；`KafkaConsumerTest`（135 個，`@EnumSource(GroupProtocol)`）透過 `ConsumerDelegateCreator.java:73` 的注入路徑；`ConsumerDelegateCreator.java:66-70` 的例外包裝契約也必須保留。整合測試：`clients-integration-tests` 約 400 個 `@ClusterTest`、`core` 的 Scala 測試 172 處 `groupProtocol` 參數化。
含意：重寫時 `ClassicKafkaConsumer` / `KafkaConsumer` / `ConsumerDelegateCreator` 不動，可維持 facade 層測試；`AsyncKafkaConsumerTest` 這種白盒測試會需要重寫。

**F3. 沒有任何 benchmark 覆蓋消費端熱路徑。**
`jmh-benchmarks` 只有 `SubscriptionStateBenchmark` 與 `RecordBatchIterationBenchmark`（只到 `DefaultRecord.readFrom`，不含 `CompletedFetch` / deserializer / `ConsumerRecord`）；ducktape 的 `test_consumer_throughput` 沒有 `group.protocol` 參數化；`ConsumerPerformance` 工具可注入 consumer factory（`ConsumerPerformance.java:62`），可以直接拿來量 baseline。
含意：任何「2 倍」宣稱都需要先補基準測量工具。

---

## 3. 對「2 倍」目標的含意：哪些問題決定上限

把上面的問題依「對穩態吞吐的影響」排序：

| 等級 | 問題 | 為什麼 |
|---|---|---|
| **決定上限** | A1 per-record 工作全在應用執行緒 | 單一執行緒的解壓 + 解析 + 反序列化速率就是天花板；不搬走，其餘優化最多省一二成 |
| **決定上限** | B1 + B2 + B3 每 node 一個 in-flight、buffered 即停、不自主預取 | 管線深度 1 讓 RTT 與 broker 等待時間直接串進吞吐；多 partition 同 node 時更糟 |
| **決定上限** | A3 + A5 拷貝次數與無 pool 的整包配置 | 壓縮場景 4 次拷貝 + 50 MB humongous allocation 讓 per-byte 成本與 GC 主導 |
| 顯著 | A2 / A4 / A6 / A7 per-record 與 per-batch 配置、CRC 額外一遍 | 決定「搬走之後」每顆核心能跑多快 |
| 顯著 | D1 / D2 粗粒度 monitor | 決定平行化後鎖競爭是否吃掉收益；也是設計 position/assignment 狀態時要優先解決的 |
| 中等 | B4 / B5 無 flow-control 概念、部分消費連鎖停擺 | 影響 `max.poll.records` 與大 batch 場景的穩定性 |
| 雜訊（但累積可觀） | C1–C9、E1–E4 per-poll / per-iteration 固定成本 | 只在小 batch、低延遲、高 poll 頻率或大量 partition 時浮現；大 batch 場景下被攤薄 |
| 邊界條件 | F1–F3 | 不影響效能，但決定重寫可以動哪些檔案、需要補哪些測試與 benchmark |

結論：**新設計的核心必須是「多深度預取 + 把 per-record 工作移出應用執行緒並平行化 + 少拷貝的資料路徑」**，其餘是配套。單純把 event loop 改成更精簡的 reactor、或把 `Optional` 與 lambda 清掉，不會接近 2 倍。

---

## 4. 本文未做、下一步要做的事

1. **建立 baseline 量測**：用 `ConsumerPerformance`（可注入 factory）+ 單節點 KRaft broker（或 `KafkaClusterTestKit` in-JVM），量 `group.protocol=classic` 與 `consumer` 在（a）1 partition 未壓縮、（b）1 partition lz4/zstd、（c）多 partition 同 broker、（d）小 `max.poll.records` 四種場景的 MB/s 與 records/s，並用 async-profiler 拿 flamegraph 確認第 1 節的瓶頸模型。這些數字定義「2 倍」的分母。
2. 補一個覆蓋 `CompletedFetch.fetchRecords` → `ConsumerRecord` 的 JMH，作為 per-record 路徑的微觀基準。
3. 寫設計文件（`ASYNC-CONSUMER-V2-02-design.md`）：以第 3 節的排序決定架構，並明確列出 F1/F2 的相容邊界。
