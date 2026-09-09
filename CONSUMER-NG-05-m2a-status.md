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
- 沒接進 `KafkaConsumer`：`ConsumerDelegateCreator` 在 clients 模組（Java 11），不能直接依賴 Java 21 的 ng 模組；計畫用 `ServiceLoader` SPI（clients 定義 `ConsumerDelegateFactory`，ng 模組提供實作，jar 在 classpath 且 JVM ≥ 21 時被選中）。接上後先跑 `clients-integration-tests` 的 consumer 測試（嵌入式 KRaft，本機 JDK 21 可跑），再上 ducktape。
- ducktape 的 ducker 映像是 JDK 17：要跑 system test，容器要用 JDK 21 映像（`ducker-ak` 有 `--jdk-version`）。
- 閒置與 T3 的量測要在 group 路徑上重做（heartbeat 加入後閒置喚醒會變）。
