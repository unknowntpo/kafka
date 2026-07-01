# Kafka 通訊版本選擇 Blog Notes

## 寫作目標

先寫成 blog post，再從 blog 中整理精華成 slide。

暫定主題：

```text
Kafka 溝通時的版本選擇：
為什麼要用這個設計、使用者看到版本錯誤時可以怎麼處理、發展至今的版本截斷
```

寫法參考既有文章：

- `Kafka 4.2.0 KIP-1034：內建 DLQ，終結手動錯誤處理`
- 文章先界定問題，再拆 workflow / mechanism / evidence / failure mode，最後收斂成操作含義。

## 目前核心判斷

Kafka 的版本問題不能只看 `Kafka 4.1.0` 這種 release version。

至少要拆成三種：

1. **Kafka release version**
   - 例如 `4.1.0`。
   - 代表 binary / distribution 的版本。

2. **Wire protocol API version**
   - 例如 `Produce v13`, `Fetch v18`, `DescribeClientQuotas v1`。
   - 每個 API 有自己的 version timeline。
   - 每次 request 都會選一個雙方都支援的 schema version。

3. **metadata.version / feature version**
   - Cluster-wide capability boundary。
   - 跟 metadata log、feature finalization、upgrade / downgrade 邊界有關。
   - 不等於單次 request 使用的 wire protocol version。

Eric presentation 主線：

```text
先建立全貌，再深挖。

release version
  -> 決定 node binary 支援能力上限

metadata.version / feature version
  -> controller finalized 的 cluster-wide contract
  -> broker-broker / broker-controller internal behavior 會依它決定能用哪些 metadata records、features、internal RPC versions

wire protocol API version
  -> 單次 request/response 使用哪個 schema
  -> client-broker 常透過 ApiVersions negotiation 選 latest usable version
```

要避免的錯誤主線：

```text
不要把所有通訊都講成 client-style ApiVersions negotiation。
broker-broker replication path 會受 finalized metadata.version 約束，例如 replica Fetch version 可由 metadataVersion.fetchRequestVersion 決定。
```

## 已修正的重點

不要把 mixed-version broker communication 只歸因於 rolling upgrade。

更精準說法：

```text
真實維運中可能會出現不同版本 broker 同時存在。
rolling upgrade 是最常見、官方文件最明確描述的場景，但不是唯一場景。
```

可能來源包含：

- rolling upgrade
- rolling downgrade
- staged rollout / canary broker
- 部分節點因維護窗口、硬體、OS、JDK 或風險控管延後升級
- binary 已升級，但 cluster feature / metadata.version 尚未 finalize

重要限制：

```text
Kafka 支援 mixed versions，主要是為了安全升級、降級與相容性。
這不代表鼓勵長期任意混跑不同 broker 版本。
```

## 第一層問題：為什麼需要通訊版本選擇？

因為通訊雙方不能假設彼此的 binary、API schema 與 feature boundary 完全相同。

典型情境：

```text
old client  -> new broker
new client  -> old broker
new broker  -> old broker
broker      -> controller
admin tool  -> broker
```

如果直接使用「本地最新版 API schema」送 request，可能發生：

- 對方完全不支援該 API。
- 對方支援該 API，但不支援該 version。
- request 裡包含某個舊 version 無法表示的必要欄位。
- 雙方對同一段 bytes 的 encode / decode 理解不同。

因此 Kafka 需要在通訊時選出：

```text
chosen version = max(intersection(local allowed range, remote supported range))
```

## 先聚焦哪條通訊線

先從 **client -> broker** 開始。

原因：

- evidence 最清楚。
- 可以用 `kafka-broker-api-versions.sh` 看到實際版本範圍。
- source code 有明確路徑：`NetworkClient` 與 `NodeApiVersions`。
- 這條線最容易解釋 `UnsupportedVersionException`。

暫時不要混在一起講：

```text
client -> broker      wire protocol version
broker -> broker      replication / internal API compatibility
broker -> controller  KRaft metadata, registration, heartbeat, feature boundary
```

`broker -> controller` 很容易牽涉 `metadata.version`，應該獨立成後續章節。

## Client -> Broker 的機制

基本流程：

```text
TCP connection
  -> ApiVersionsRequest
  -> ApiVersionsResponse
  -> client records NodeApiVersions per broker
  -> each request chooses latest usable API version
  -> no overlap => UnsupportedVersionException
```

重要概念：

- `ApiVersionsResponse` 回報 broker 支援的 API key 與 min/max version。
- client 本地也有自己支援的 oldest/latest version。
- 送 request 前，client 取交集。
- 有交集就選交集中的最大值。
- 沒交集就不要送出 request，改成本地失敗。

## Evidence Map

### 官方設計文件

- `docs/design/protocol.md`
  - Kafka client compatibility policy。
  - client 應使用 client 與 broker 都支援的最高 API version。
  - `ApiVersionsRequest` 用來取得 broker 支援的 API versions。

### Upgrade 文件

- `docs/getting-started/upgrade.md`
  - rolling upgrade 會 one broker at a time。
  - 這證明 mixed-version broker state 是 Kafka 官方 upgrade path 需要處理的狀態之一。

### Source code

- `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`
  - 送 request 前取得 `NodeApiVersions`。
  - 呼叫 `latestUsableVersion(...)` 選版本。
  - 捕捉 `UnsupportedVersionException` 後本地 abort，不送出 request。

- `clients/src/main/java/org/apache/kafka/clients/NodeApiVersions.java`
  - `latestUsableVersion(...)` 計算本地 allowed range 與 broker supported range 的交集。
  - 無 API 或無交集時丟 `UnsupportedVersionException`。

### Protocol schema

- `clients/src/main/resources/common/message/*.json`
  - `validVersions` 定義該 API 支援的 schema versions。
  - `flexibleVersions` 定義哪些 versions 使用 flexible encoding。
  - 每個 API 有自己的 version timeline。

### Runtime command

可在 lab 中執行：

```bash
docker compose -f study/version-control/lab/tour0/docker-compose.yml exec broker-1 \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server broker-1:19092
```

曾觀察到的例子：

```text
DescribeClientQuotas(48): 0 to 1 [usable: 1]
Produce(0): 0 to 13 [usable: 13]
Fetch(1): 4 to 18 [usable: 18]
Metadata(3): 0 to 13 [usable: 13]
```

## Blog 草稿結構

寫作方式：

```text
先規劃大綱 -> 每次展開一節 -> 每節補 evidence -> 最後再整理成可發佈文章
```

每一節都用同一個檢查順序：

```text
問題是什麼 -> Kafka 怎麼處理 -> repo/code/command 證據 -> 使用者看到什麼 -> 這代表什麼限制
```

### 1. 先界定問題

不要從 `ApiVersionsRequest` 開始。

先從使用者和維運者會遇到的問題開始：

```text
Kafka cluster 不一定所有 binary 都同一版。
client、broker、controller、admin tool 也可能在不同時間點升級。
既然雙方可能不是同一版，request/response schema 要怎麼選？
```

### 2. 拆開三種 version

用一張表或短段落說明：

```text
release version        binary/distribution version
wire protocol version  per-API request/response schema version
metadata.version       cluster-wide feature and metadata compatibility boundary
```

### 3. 先看 client -> broker

用最小流程圖：

```text
client
  -> ApiVersionsRequest
broker
  -> ApiVersionsResponse: API key -> min/max version
client
  -> choose latest usable version per request
```

### 4. 用 code 證明選擇規則

核心公式：

```text
chosen version = max(intersection(client range, broker range))
```

接著對應到：

- `NodeApiVersions.latestUsableVersion(...)`
- `NetworkClient.doSend(...)`

### 5. 講使用者看到錯誤時代表什麼

`UnsupportedVersionException` 不只是「版本太舊」。

可能代表：

- 對方沒有這個 API。
- 對方 API version range 太舊。
- 對方 API version range 太新。
- client 嘗試使用的必要欄位無法被 chosen version 表示。
- protocol support 被新 major version 截斷。

### 6. 再接 metadata.version

等 wire protocol version 講清楚後，再補：

```text
wire protocol version = 這次 RPC 使用哪個 request/response schema
metadata.version      = cluster 目前允許哪個 metadata/features era
```

metadata.version 不應提前混入 client -> broker 的基本協商流程。

### 7. 最後整理成 slide

blog 先保留完整推理與證據。

slide 只抽：

- 問題：不同版本 binary 可能同時存在。
- 機制：ApiVersions negotiation。
- 規則：選雙方都支援的最高 API version。
- 失敗：沒有交集就本地報錯，不送出錯格式 request。
- 邊界：wire protocol version 不等於 metadata.version。

## 逐段展開計畫

### Part 1：先界定問題

要回答：

```text
為什麼 Kafka 不能假設所有節點與 client 都使用同一套 request/response schema？
```

重點：

- 真實維運可能出現不同版本 broker 同時存在。
- rolling upgrade 是常見場景，但不是唯一場景。
- client、broker、controller、admin tool 的升級節奏也可能不同。
- 因此每次通訊都需要確認雙方都理解同一個 schema version。

要避免：

- 不要直接說「因為 rolling upgrade」。
- 不要讓讀者誤以為 Kafka 鼓勵長期任意 mixed broker versions。

### Part 2：先拆清楚三種 version

要回答：

```text
Kafka release version、wire protocol API version、metadata.version 差在哪？
```

重點：

- `Kafka 4.1.0` 是 binary / release version。
- `Produce v13` 是某個 API 的 request/response schema version。
- `metadata.version` 是 cluster-wide feature / metadata compatibility boundary。
- `ApiVersions v5` 不代表 Kafka 5.0，也不代表所有 API 都是 v5。

預期產出：

```text
看到 "version" 時，先問是哪一層：
binary version, RPC schema version, or cluster feature boundary?
```

### Part 3：client -> broker 的版本協商流程

要回答：

```text
client 為什麼不能直接用自己支援的最新版 API version 送 request？
```

重點：

- client 不知道目前連上的 broker 支援哪些 API versions。
- 同一個 cluster 內不同 broker 可能處於不同 binary / capability 狀態。
- request 送出前要先知道對方支援範圍。
- `ApiVersionsRequest/Response` 負責交換這個資訊。

Complexity / tradeoff：

```text
per broker connection:
  ApiVersionsResponse size = O(number of API keys)

whole client process:
  if connected to N brokers
  total negotiated state = O(N * number of API keys)
```

這個成本是可接受的，因為：

```text
ApiVersions negotiation 通常發生在 connection 建立或重連時，
不是每個 request 都重新協商。
```

後續 request 使用本地快取：

```text
nodeId -> NodeApiVersions -> apiKey -> latest usable version
```

可寫成 blog tradeoff：

```text
Kafka 用一次連線初始化成本，換取後續 request 不需要反覆協商版本。
```

可重複 playground / test：

```text
clients/src/test/java/org/apache/kafka/clients/NodeApiVersionsTest.java
```

新增 focused tests：

```text
testPlaygroundClientChoosesHighestCommonProduceVersion
testPlaygroundClientAbortsWhenProduceVersionsDoNotOverlap
```

Demo 1：有交集，選最高共同版本

```text
broker supports Produce: 0-10
client allows Produce: 0-13
chosen version: 10
```

Demo 2：沒有交集，client-side abort

```text
broker supports Produce: 0-10
client allows Produce: 11-13
result:
  UnsupportedVersionException
  The node does not support PRODUCE with version in range [11,13].
  The supported range is [0,10].
```

驗證指令：

```bash
./gradlew :clients:test \
  --tests org.apache.kafka.clients.NodeApiVersionsTest.testPlaygroundClientChoosesHighestCommonProduceVersion \
  --tests org.apache.kafka.clients.NodeApiVersionsTest.testPlaygroundClientAbortsWhenProduceVersionsDoNotOverlap
```

已驗證結果：

```text
PASSED
```

待討論：為什麼不由 controller 對外提供單一 endpoint？

觀察：

```text
如果所有版本協商與 routing 都由 controller 統一管理，
client 會簡單很多，外部也可能只需要看見一個 endpoint。
```

但 Kafka 的既有設計是：

```text
client 直接連 broker
client 根據 metadata 找 partition leader
client 對每個 broker connection 做 ApiVersions negotiation
```

可能的設計取捨：

```text
Kafka chooses smarter clients + direct broker data path
over simpler clients + centralized controller endpoint.
```

原因假設，待後續用 docs/code 補證據：

- Kafka 的 data plane 是 client -> broker leader，不是 client -> controller -> broker。
- controller 應主要處理 control plane，不應成為 produce/fetch proxy。
- direct broker data path 少一跳，也避免 controller 成為吞吐瓶頸。
- 代價是 client 需要處理 metadata refresh、leader routing、per-broker ApiVersions、retry/reconnect。

可形成 blog 問題：

```text
為什麼 Kafka 願意讓 client 複雜，而不是把 cluster 藏在一個 controller endpoint 後？
```

最小流程圖：

```text
client
  -> ApiVersionsRequest
broker
  -> ApiVersionsResponse: API key -> min/max version
client
  -> NodeApiVersions
  -> choose latest usable version for each request
```

### Part 4：latest usable version 的規則

要回答：

```text
Kafka 到底怎麼選出要使用的 API version？
```

核心公式：

```text
chosen version = max(intersection(client allowed range, broker supported range))
```

例子：

```text
client allowed: 0-3
broker supports: 1-2
chosen: 2
```

沒有交集：

```text
client allowed: 3-4
broker supports: 0-2
chosen: none
=> UnsupportedVersionException
```

主要 code evidence：

- `clients/src/main/java/org/apache/kafka/clients/NodeApiVersions.java`
- `clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`

### Part 5：使用者看到 version error 時該怎麼理解

要回答：

```text
UnsupportedVersionException 代表什麼？使用者該檢查什麼？
```

重點：

- 可能是 API 不存在。
- 可能是 API version range 沒有交集。
- 可能是 client 太新、broker 太舊。
- 可能是 broker 太新、client 太舊。
- 可能是新的 major version 已移除舊 protocol support。
- 也可能是 request builder 需要的欄位無法用 chosen version 表示。

使用者檢查順序：

```text
1. client library version
2. broker version
3. kafka-broker-api-versions.sh output
4. failing API name
5. whether protocol support was removed or gated by feature/version
```

### Part 6：再補 metadata.version，但不要混在前面

要回答：

```text
metadata.version 跟 wire protocol version 的關係是什麼？
```

重點：

- wire protocol version 是單次 RPC 的 schema 選擇。
- metadata.version 是 cluster-wide feature / metadata compatibility boundary。
- feature finalization 可能讓 binary 已經升級，但 cluster 仍維持舊 capability。
- 這可以解釋為什麼「binary version 已更新」不等於「所有新功能都已啟用」。

Eric presentation 深挖：metadata.version 控制什麼？

核心 claim：

```text
metadata.version 是 cluster-wide contract。
它不只是顯示目前 cluster 版本，而是實際決定哪些 metadata record schema、internal RPC version、feature gate 可以被使用。
```

Evidence 1：feature gates

```text
MetadataVersion.isScramSupported()
MetadataVersion.isMetadataTransactionSupported()
MetadataVersion.isDelegationTokenSupported()
MetadataVersion.isDirectoryAssignmentSupported()
MetadataVersion.isElrSupported()
MetadataVersion.isCordonedLogDirsSupported()
```

Evidence 2：metadata record schema version

```text
MetadataVersion.registerBrokerRecordVersion()
MetadataVersion.partitionChangeRecordVersion()
MetadataVersion.partitionRecordVersion()
```

Example:

```text
ClusterControlManager.registerBroker(...)
  -> writes RegisterBrokerRecord with metadataVersion.registerBrokerRecordVersion()

PartitionChangeBuilder
  -> writes PartitionChangeRecord with metadataVersion.partitionChangeRecordVersion()
```

Evidence 3：broker-broker internal RPC version

```text
RemoteLeaderEndPoint.fetch(...)
  -> FetchRequest.Builder.forReplica(metadataVersion.fetchRequestVersion, ...)

RemoteLeaderEndPoint.fetchEarliestLocalOffset(...)
  -> ListOffsetsRequest.Builder.forReplica(metadataVersion.listOffsetRequestVersion, ...)
```

可講成：

```text
broker-broker 不只是每條 connection 自己協商。
它依賴 controller finalized 的 metadata.version 作為安全邊界。
```

這也解釋 Chia 考點：

```text
controller 升 metadata.version 前必須檢查 registered brokers/controllers 的 supportedFeatures。
否則 cluster 可能開始寫新版 metadata record，或送新版 internal request，而某些 broker 還看不懂。
```

補充 homework：fenced broker 是否會擋 feature version upgrade？

code reading + unit test 結論：

```text
Feature upgrade validation 看的是 broker/controller registration 裡的 supportedFeatures。
broker 下線後通常先被 fence；fence 不會移除 broker registration。
因此 fenced broker 仍可能被納入 feature support validation。
只有 unregister broker 才會移除 registration。
```

相關 code path：

- `FeatureControlManager.updateFeatures(...)`
- `FeatureControlManager.reasonNotSupported(...)`
- `ClusterControlManager.brokerSupportedFeatures()`
- `ClusterControlManager.replay(UnregisterBrokerRecord)`
- `ReplicationControlManager.maybeFenceOneStaleBroker()`

新增驗證測試：

- `metadata/src/test/java/org/apache/kafka/controller/ClusterControlManagerTest.java`
- `testFencedBrokerRegistrationStillBlocksFeatureUpdate`

測試設計：

```text
1. 建立一個 fenced=true 的 broker registration。
2. 該 broker 只支援 test.feature.version 0-1。
3. 嘗試把 feature 升到 2。
4. 預期 updateFeatures 回傳 INVALID_UPDATE_VERSION：
   Broker 1 only supports versions 0-1
```

這證明 feature update validation 沒有因為 broker fenced 就忽略該 broker registration。

對 4.2 / 4.3 例子的精準說法：

```text
如果 cluster 還有 4.2 broker registration，要把 metadata.version / release-version 升到 4.3，會被 4.2 broker 擋。
如果一台 4.3 broker 下線但仍 registered/fenced，它本身通常不會擋升到 4.3，因為它支援 4.3。
真正會擋的是任何 registered broker/controller 的 supportedFeatures 不包含 target feature level。
```

這題應該放在 metadata.version / feature finalization 章節，不要混進 client -> broker wire protocol negotiation。

## Chia 7712 考點：fenced broker 會不會擋 feature upgrade？

短答：

```text
會，可能會擋。
只要 broker registration 還存在，feature update validation 就會檢查它的 supportedFeatures。
fenced=true 只代表 broker 不 active serving，不代表它從 feature compatibility 檢查中消失。
```

精準規則：

```text
feature version upgrade 需要所有相關 registered brokers/controllers 都支援 target feature level。
fenced broker 仍是 registered broker。
unregister broker 才會移除 registration。
```

可以用這張圖記：

```text
broker crash / heartbeat timeout
          |
          v
      fenced=true
          |
          v
  still in brokerRegistrations
          |
          v
  FeatureControlManager.reasonNotSupported(...)
          |
          v
  checks broker.supportedFeatures
          |
          v
  may block feature upgrade

unregister broker
          |
          v
  removed from brokerRegistrations
          |
          v
  no longer checked there
```

考題版本：

```text
一群 broker 中有一台升過新版，後來 crash 下線。
它被 fence 之後，會不會還影響 feature version upgrade？
```

回答方式：

```text
要看它是否仍 registered，以及它註冊時的 supportedFeatures 是否支援 target feature level。

如果只是 fenced，registration 還在，仍會被檢查。
如果它的 supportedFeatures 不包含 target feature level，就會擋。
如果它已 unregister，registration 被移除，就不會因為該 broker 擋。
```

注意不要答錯成：

```text
fenced broker 已經下線，所以 feature upgrade 不會看它。
```

這個說法不精準，因為 code path 看的是 registration，不是 active serving 狀態。

Chia 追問版本：

```text
broker 升級 metadata.version 時，controller 會檢查所有 brokers 能不能支援。
假設有一台版本很舊的 broker 在這時突然下線，其他 broker 還能升級 metadata.version 嗎？
```

精準回答：

```text
通常不能，前提是那台舊 broker 仍然 registered，且它的 supportedFeatures 不包含 target metadata.version。
突然下線通常只會讓 broker heartbeat timeout 後被 fenced。
fenced 不會移除 broker registration，所以 feature update validation 仍會檢查它。
```

例外：

```text
如果那台 broker 已經被 unregister/decommission，registration 被移除，就不會因為它擋。
如果它雖然 fenced，但 supportedFeatures 本來就包含 target metadata.version，也不會因為它擋。
```

一句話背法：

```text
MV upgrade 看 registered brokers/controllers 的 supportedFeatures，不是只看 currently alive/unfenced brokers。
```

測試證據：

```text
metadata/src/test/java/org/apache/kafka/controller/ClusterControlManagerTest.java
testFencedBrokerRegistrationStillBlocksFeatureUpdate
```

測試摘要：

```text
registered broker:
  fenced=true
  supported test.feature.version = 0-1

requested feature upgrade:
  test.feature.version = 2

result:
  INVALID_UPDATE_VERSION
  Broker 1 only supports versions 0-1
```

驗證指令：

```bash
./gradlew :metadata:test --tests org.apache.kafka.controller.ClusterControlManagerTest.testFencedBrokerRegistrationStillBlocksFeatureUpdate
```

已驗證結果：

```text
PASSED
```

### Part 7：版本截斷與發展至今

要回答：

```text
為什麼新版本 Kafka 會移除很舊的 protocol support？這會如何影響使用者？
```

重點：

- 相容性有成本。
- 每個舊 schema 都可能增加測試、維護與演進負擔。
- major version 可能成為移除舊支援的時間點。
- 使用者需要看 compatibility matrix、release notes、KIP，而不是只看 cluster 能不能啟動。

這一節暫時排後面，等前面的協商機制講清楚後再展開。

## 下一個學習問題

先回答：

```text
client 為什麼不能直接用自己支援的最新版 API version 送 request？
```

這個問題會自然帶到：

- `ApiVersionsRequest/Response`
- `NodeApiVersions`
- `latest usable version`
- `UnsupportedVersionException`

## Blog draft current state

Draft file:

```text
study/version-control/kafka-version-negotiation-blog-draft.md
```

已放入：

- Demo 0：Docker lab output，對應 release version、finalized features、broker API versions。
- Demo 1：client-side API version intersection tests 與 console output。
- Demo 2：fenced broker registration blocks feature update test 與 console output。
- Source anchors：`NetworkClient`, `NodeApiVersions`, `RemoteLeaderEndPoint`, `MetadataVersion`, `FeatureControlManager`, `ClusterControlManager`, `PartitionChangeBuilder`。

下一步：

- 對 draft 做一次「claim -> evidence」審稿。
- 把 blog 精華抽成 Eric presentation slides。

## 2026-06-29 - 補上版本截斷

已補進 draft：

```text
## 版本截斷：不是所有 API 都會永遠支援 v0
```

核心結論：

```text
新版本 Kafka 可能不再支援某些過舊的 wire protocol API versions。
因此 ApiVersions 回傳的 minVersion 可能大於 0。
版本協商只能在雙方仍有交集時工作；舊 protocol 被移除後，結果會變成 no usable version。
```

證據：

- Demo 0 broker output: `Fetch(1): 4 to 18`, `ListOffsets(2): 1 to 10`。
- `docs/getting-started/upgrade.md:229`: Kafka 4.0 removed old protocol API versions.
- `docs/design/protocol.md:108`: client should choose highest API version supported by both client and broker.
- `docs/design/protocol.md:115`: protocol version deprecation is marked in protocol documentation.
- `clients/src/main/resources/common/message/FetchRequest.json:61`: `validVersions: 4-18`。
- `clients/src/main/resources/common/message/ListOffsetsRequest.json:45`: `validVersions: 1-11`。
- `clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java:287`: ApiVersionsResponse uses oldest/latest version as advertised min/max.
