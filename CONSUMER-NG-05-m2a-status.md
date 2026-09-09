# CONSUMER-NG 05：M2a 狀態（2026-09-09）

M2a 把 04 §2 的引擎做出來並跑通 group 路徑。程式碼：`ConsumerEngine`（I/O 執行緒：命令佇列、每個 poll 迭代一次的 housekeeping、`ManagerTask` 排程、`FetchPipeline`、`PassDecision`）、`NgKafkaConsumer`（`Consumer<K,V>` 的 M2a 子集）、`RecordReader`（position 以 `SubscriptionState` 為準）、`EngineNetwork`（與 `ClientUtils.createNetworkClient` 相同的接線，只換成 direct buffer 池）。

## 1. 重用了什麼，clients 模組改了什麼

重用（未改邏輯）：`CoordinatorRequestManager`、`CommitRequestManager`、`ConsumerMembershipManager`、`ConsumerHeartbeatRequestManager`、`OffsetsRequestManager`、`PositionsValidator`、`SubscriptionState`、`ConsumerMetadata`、`NetworkClientDelegate`、`BackgroundEventHandler` 與其 event、`ConsumerRebalanceListenerInvoker`、`OffsetCommitCallbackInvoker`、`FetchSessionHandler`、`NetworkClient` / `Selector`。

clients 模組的小改動（都是可見性或一個小掛鉤，對既有實作無行為影響）：

| 改動 | 為什麼 |
|---|---|
| `ClientResponse.payload()`：回應攜帶它被讀進的 buffer | 池化接收 buffer 的呼叫者需要知道回應的物件指向哪塊記憶體 |
| `UnsentRequest.whenComplete` public | 排程器在別的 package 掛請求完成回呼 |
| `RequestManager.waitCondition()` 預設方法 + `WaitCondition` | manager 宣告它在等什麼（前一條線的 S1） |
| `SubscriptionState.isFetchable` / `assignmentId` public、`FetchPosition.offsetEpoch` / `currentLeader` public | fetch 路徑在別的 package |
| `ConsumerRebalanceListenerInvoker`、`OffsetCommitCallbackInvoker` 的建構子 public | app 側重用 |

## 2. 煙霧與穩態（morefine）

- assign 路徑：3M × 1 KB 3.35 秒。group 路徑：KIP-848 join → 消費 3M → `close()` auto-commit 六個 partition 到 log end（lag 0）→ 離群後 `kafka-consumer-groups --describe` 無活動成員。
- 穩態（`steady.py`，6p 1 KB，CRC 開啟，`byte[]` 複製）：

| | MB/s | CPU s/GB | 對照 |
|---|---:|---:|---|
| assign，T2（4 核） | 1,498 | 1.13 | M1 1,502 · 1.08；trunk 1,078 · 1.18；librdkafka 1,295 · 1.32 |
| **group（subscribe + heartbeat + auto-commit），T2** | **1,525** | **1.16** | 加上 group 協定沒有代價 |
| assign，T1（1 核） | 1,203 | 0.84 | M1 1,236 · 0.81；librdkafka 1,218 · 0.88 |

## 3. 還沒做的（M2b / M2c）

- `Consumer` 方法：pattern 訂閱（3 個多載）、`partitionsFor` / `listTopics`、`offsetsForTimes` / `beginningOffsets` / `endOffsets`、`currentLag`、`enforceRebalance`、metric 註冊、`clientInstanceId`；目前丟 `UnsupportedOperationException`。
- 沒有 consumer 層級的 metrics（`records-lag`、`fetch-*`、`commit-*` 等公開名稱）；效能數字裡因此少了 trunk 付的 5–8% metrics 成本，之後要補回並重量。
- ducktape 還沒跑（見 §4）。
- ducktape 的 ducker 映像是 JDK 17：要跑 system test，容器要用 JDK 21 映像（`ducker-ak` 有 `--jdk-version`）。
- 閒置與 T3 的量測要在 group 路徑上重做（heartbeat 加入後閒置喚醒會變）。

## 4. 接進 `KafkaConsumer` 與整合測試（同日補）

`ConsumerDelegateCreator` 在 clients 模組（Java 11），不能直接依賴 Java 21 的 ng 模組，所以用 `ServiceLoader` SPI：clients 定義 `ConsumerDelegateFactory`，ng 模組提供 `NgConsumerDelegateFactory`；jar 在 classpath 且 JVM ≥ 21 時 `group.protocol=consumer` 就走新實作，`-Dkafka.consumer.delegate=builtin` 強制走舊的（Gradle 測試可用 `-Pkafka.consumer.delegate=builtin` 轉發，方便 A/B）。`clients-integration-tests` 在 JDK 21 上把 ng 模組加進 test runtime classpath，所以既有的 consumer 整合測試（嵌入式 KRaft）直接就是新實作的回歸測試。

跑 `PlaintextConsumerCommitTest`（25 個 case）抓到四個問題，都是接線或排程層的，不是協定邏輯：

| 症狀 | 根因 | 修法 |
|---|---|---|
| 第二個 consumer `assign` 同一個 partition 後永遠拿不到 records；I/O thread 每分鐘 57 萬個 pass（busy loop） | 兩件事：(1) 引擎給 manager 的 `ApiVersions` 與 `NetworkClient` 的不是同一個實例，`OffsetsRequestManager` 看不到 broker 的版本，OffsetsForLeaderEpoch 永遠不送，committed offset 停在 AWAIT_VALIDATION；(2) `updateFetchPositions` 立即完成（沒有排任何請求）時引擎仍把 version +1 並重跑 manager，下一個 pass 又來一次 | (1) 一個 `ApiVersions` 共用；(2) 只有 future 未完成（真的排了請求）才重跑 manager，立即完成就等下一個輸入（poll 迭代、metadata 變化、timer） |
| `commitAsync` 之後立刻 `commitSync` / `close`，callback 沒被執行 | app 側沒等最後一個 async commit 完成就跑 callback | 記住最後一個 async commit 的 future，`commitSync` 與 `close` 先等它（有 timeout）再跑 callback |
| 所有 broker 關掉後 `commitAsync` + `close(500 ms)`，期望 `CommitFailedException`（coordinator 未知且正在關閉），拿到 `RetriableCommitFailedException` | coordinator 的連線斷了但引擎仍把它當已知，commit 直接送去死節點；trunk 之所以過是因為它在 `poll()` 才更新 positions，那次 OffsetFetch 失敗順便把 coordinator 標成未知（時序巧合） | 每次 `network.poll` 後檢查 coordinator 的連線：斷線且在重連 backoff 中就標成未知（與 classic consumer `checkAndGetCoordinator` 同一條規則），請求等重新發現而不是打死節點 |

接著跑 Assign / Close / Fetch / Poll 四個類別，又抓到：

| 症狀 | 根因 | 修法 |
|---|---|---|
| `fetch.max.bytes=10 KiB` 的測試全部卡死：consumer 收到幾個回應之後就再也收不到任何東西（連 FindCoordinator 回應都沒有） | **非 fetch 回應的接收 buffer 從來沒有還給 pool**。`Selector` 把每個回應都讀進 pool 的 buffer；fetch 回應由 `FetchSegment.Owner` 的 refcount 釋放，Metadata / FindCoordinator / Heartbeat / OffsetFetch… 的 buffer 沒人釋放。容量 300 MB 時要幾千個回應才會滿（heartbeat 每 3 秒一個 → 幾小時），10 KiB 的設定讓容量只有 60 KiB，四個回應就滿。這是長時間執行必定發生的記憶體問題，測試把它提前逼出來 | 每次 `network.poll` 之後掃 `Selector.completedReceives()`：不是 fetch 回應認領的 buffer 就還給 pool（回應物件在 poll 內已經完整解析，buffer 不再被引用）。另外 pool 容量下限 256 KiB（`DirectBufferPoolTest` 有回歸測試） |
| `auto.offset.reset=none` + seek 到不存在的 offset，期望 `OffsetOutOfRangeException`，拿到 `NoOffsetForPartitionException` | 引擎在背景每個 pass 都重試 positions，每次失敗都丟一個 `ErrorEvent`；seek 之後的 poll 撿到之前排隊的舊錯誤 | positions 只在 app 的 `poll()` / `position()` 期間更新（與舊實作相同：錯誤屬於觸發它的那次呼叫；閒置時不在背景重試），同一次呼叫最多送一個錯誤；OFFSET_OUT_OF_RANGE 沒有 reset policy 時把 `OffsetOutOfRangeException` 送給 app，並且該 partition 在 app 下一次 poll 前不再 fetch（否則 I/O thread 會對 broker 狂打） |
| 預設 `close()` 要 ≥ `fetch.max.wait.ms`（在飛的 fetch 要等它回來） | 引擎關閉時只 poll 一次就關網路 | `close(timeout)`：像舊的 network thread 一樣，在 close timer 內 poll 到沒有在途請求 |
| `records-lag` / `records-lead` metric 不存在 | M2a 沒接 fetch metrics | 重用 `FetchMetricsManager`：latency 在回應時記、bytes/records 在整個回應被 app 消費完時記（與舊的 per-fetch 聚合同形）、lag/lead 在每次交付後記、HW / LSO / log start offset 在回應時寫進 `SubscriptionState` |

**與舊實作刻意不同的一點**：`testFetchHonours(FetchSize|MaxPartitionFetchBytes)IfLargeRecordNotFirst` 期望第一次 `poll()` 只拿到小的那筆 record（大的那筆在第二個 fetch 才回來）。舊實作對同一個 partition 不會有第二個 buffered 回應（要 app 消費完才發下一個 fetch）；新引擎在 credit 內預先 fetch，所以第一次 poll 前兩個回應都到了，一次交付兩筆。這是 1 partition 吞吐量的來源之一（03 §1 的 1p 100 B），不是 `Consumer` 介面的語意，保留預取，這兩個 case 記為已知差異。

教訓：**manager 假設「有人會定期 poll 我」的地方，換成事件驅動的引擎後要一個個找出來**（positions、coordinator 連線）；**M1 的「有界記憶體」在每條路徑都要有釋放者**，這次靠把容量壓到極小的測試抓到；找的方法是整合測試 + I/O thread 的 pass 追蹤（TRACE 記每個 pass 跑了哪個 manager、有沒有送出東西）。

整合測試結果（JDK 21，本機）：

| 類別 | 結果 |
|---|---|
| `PlaintextConsumerCommitTest` | 25/25 |
