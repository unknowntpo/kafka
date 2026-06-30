# Kafka 通訊時的版本選擇：從 wire protocol 到 metadata.version

> 這篇是「Kafka 版本」系列的一篇，專注在**溝通當下到底選哪個版本**：client 跟 broker 怎麼談、談不攏時使用者會看到什麼、以及這套協定發展至今的「版本截斷」。
>
> 整個大主題會拆成系列，其他塊各自成篇（前傳／續集），本篇只在需要時做前情提要、不重講。會被點到的相鄰兩篇：
>
> - 前傳《Kafka 叢集版本定義與 KIP-1170》：binary version（上限與規則）vs `metadata.version`（叢集當前啟用範圍）、MV 如何決定 record schema、feature gates 與啟動檢查。
> - 《升級 Kafka》：怎麼查版本、`MetadataVersion` 的 IV 命名、升級／降級流程與限制。
>
> 正文聚焦「流程」；對應的 source code 片段都收在文末 [附錄 A：原始碼對照](#附錄-a原始碼對照)，想深入的人再翻。

---

## Part 1：為什麼需要這個設計（為什麼這麼麻煩）

### 1. 問題：為什麼要在「溝通當下」選版本？

先講一個現實：**Kafka 的 client 很長壽**。broker 由平台團隊維護、會跟著升級；但 client 是「嵌在各個應用程式裡」的函式庫——某個多年前的 batch job、某個沒人敢動的 legacy service，可能到今天還抱著很舊的 kafka-client 在跑。要全公司應用同一天升 client，基本不可能。

這不是隨口說的。Apache Kafka 從 0.8.0（2013）到 3.x，**整整九年保留了「每一個」protocol API 版本**——就因為總有舊 client 還在連。社群甚至在 3.7 加了 metric `DeprecatedRequestsPerSec`（依 client 名稱與版本分類）讓維運者找出「還有哪些老 client 在連」，直到 4.0（[KIP-896](https://cwiki.apache.org/confluence/display/KAFKA/KIP-896%3A+Remove+old+client+protocol+API+versions+in+Kafka+4.0)）才把 baseline 砍到 2.1（2018）。

所以 Kafka 很早就立了一個承諾（`docs/design/protocol.md:94`）：「new clients can talk to old servers, and old clients can talk to new servers」。

新舊 client 與新舊 broker 要能任意交叉相容。為此，broker 從 0.10.0.0（KIP-35）起會用 `ApiVersions` 公告「我每個 API 支援哪些版本」，讓 client 在連線當下挑一個雙方都懂的版本——這就是「溝通當下選版本」的由來。

**不這麼做會怎樣？** 那 client 跟 broker 就得「版本對齊」才能溝通。broker 一升級，舊 client 送的 request broker 看不懂、或新 broker 回的 response 舊 client 解不開，連線就壞。結果是每次升 broker 都得把所有 client 一起升——**lockstep 升級**，等於每次升級全體停機協調。

**而這套機制最想保住的是可用性——讓你能 rolling upgrade、不停機。** 升 broker 是一台一台來：關一台、換 binary、起來、再下一台。過程中 cluster 必然「新舊 broker 混在一起」，client 同時連到新的和舊的 broker。若沒有「每條連線各自挑版本」，client 對著還沒升或剛升好的 broker 總有一邊會壞。版本協商讓每條連線都選一個對方真的支援的版本，於是混版狀態下整個 cluster 仍能正常收送——**升級全程不必停機**。

一句話：這層複雜度不是設計過度，而是「**用一點協商成本，換掉整場停機**」。

> **旁註：不是只有 Kafka 這樣，但 Kafka 粒度最細。**
> PostgreSQL 在連線開始用 `StartupMessage` 談一個 protocol 版本（major 從 2003 的 3.0 至今幾乎沒動，靠 `NegotiateProtocolVersion` 談 minor）；MongoDB 用 `hello` 交換一個全域的 `min/maxWireVersion`，driver 挑交集、沒交集就報 incompatible——概念跟 Kafka 一樣是 client 挑。
> 差別在**粒度**：Postgres／Mongo 是「整套協定一個版本號」，Kafka 是「每個 API 各自一段 range」（Produce 加一版不影響 Fetch）。API surface 大又各自獨立演進，正是 Kafka 要做到 per-API、也因此最容易遇到「版本截斷」的原因。

### 2. 為什麼一個版本號不夠：三層

大多數人講「Kafka 版本」時，腦中只有一個數字（像 3.6、4.1）。但實際在維運、debug、升級時，會遇到**三種互相獨立、可以各自不同步**的「版本」。前兩層留給前傳，這裡快速複習，重點擺在第三層：

```text
release version
  例如 apache/kafka:4.1.0，代表 binary/distribution 版本。

metadata.version / feature version
  controller finalized 的 cluster-wide contract，決定 cluster 目前允許哪些 metadata schema、features、internal behavior。

wire protocol API version
  單次 request/response 使用的 schema version，例如 Produce v13、Fetch v18。
```

一句話：

```text
ApiVersions answers: can two endpoints speak this RPC schema?
metadata.version answers: is the cluster allowed to use this feature/metadata era?
release version answers: what can this binary possibly support?
```

關鍵是**這三層不會自動一起變**：你可以升了 binary（release version）卻還沒 finalize `metadata.version`——升級那篇處理的正是這個落差；也可以兩個 broker binary 同版，卻因連線當下協商出不同 wire protocol version 而行為不同。**三層不同步，正是大多數 version error 的根源。**

而且這三層的「版本選擇」不是同一套規則：

```text
client -> broker   用 ApiVersions negotiation 決定單次 RPC schema（連線層級）
broker -> broker   internal RPC 受 finalized metadata.version 約束
```

Part 2 就沿著這兩條線，逐層講「使用者遇到 version error 時，怎麼判斷現況、怎麼解」。

---

## Part 2：使用者看到 version error，怎麼知道現況、怎麼解

### 3. 先分層：現在是哪一層出問題？

不要只說「版本不合」。先用「失敗發生在哪個動作」把問題歸到兩層之一：

```text
send request 前失敗
  -> wire protocol mismatch
  -> NodeApiVersions / NetworkClient

kafka-features upgrade 失敗
  -> feature/MV finalization mismatch
  -> FeatureControlManager.reasonNotSupported
```

下面就照這兩層各自講「機制」與「怎麼解」；中間插一段「版本截斷」，解釋第一層為什麼有時根本沒交集。

### 4. 第一層：client ↔ broker wire protocol mismatch

這是最常遇到的一層。client 不能直接用自己支援的最新版 API version，因為 broker 不一定支援。

規則：

```text
chosen version = max(intersection(client allowed range, broker supported range))
```

例子——有交集取最高、沒交集就壞：

```text
client allows Produce: 0-13 , broker supports Produce: 0-10  -> chosen 10
client allows Produce: 11-13, broker supports Produce: 0-10  -> UnsupportedVersionException
```

repo docs 把規則、以及「為什麼要每條連線重問」講得很清楚：「clients should choose the highest API version supported by both client and broker」（`protocol.md:108`）、「supported API versions are connection-scoped and should be refreshed after disconnect, because the broker may have been upgraded or downgraded」（`protocol.md:116`）。

機制上，client 送出前會先取交集決定版本：

```text
NetworkClient.doSend(...)
  -> apiVersions.get(nodeId)
  -> NodeApiVersions.latestUsableVersion(...)
  -> builder.build(version)
```

`NodeApiVersions.latestUsableVersion(...)` 對 client 與 broker 的範圍取交集：有交集回最高版，沒有就丟 `UnsupportedVersionException`；`NetworkClient` 接到這個 exception 會**跳過 socket、不送出**，把 request 丟進 `abortedSends`。（程式碼見[附錄 A · §4](#a4--第一層client--broker)）

**錯誤怎麼一路回到使用者？** 注意這條路徑很多時候不經過 socket：

```text
application
  -> KafkaProducer / KafkaConsumer / AdminClient
  -> NetworkClient.doSend(...)
  -> NodeApiVersions.latestUsableVersion(...)
  -> no intersection -> UnsupportedVersionException
  -> skip socket send -> abortedSends
  -> poll() returns ClientResponse.versionMismatch()
  -> producer/consumer/admin completes with error
```

最後 producer / consumer / admin 各自在收到 `response.versionMismatch()` 時，把錯誤交回應用層（程式碼見[附錄 A · §4](#a4--第一層client--broker)）。

判斷現況的關鍵：

```text
很多 UnsupportedVersionException 不是 broker 回來的 response。
client 可能在送出前就發現沒有可用 protocol version，直接本地 abort。
```

**怎麼解**：讓 client 與 broker 的 API version 範圍重新有交集——通常是把過舊的一端（多半是 client）升上來。4.0 之後最低要到 2.1；用 §1 提到的 broker metric `DeprecatedRequestsPerSec` 與 request log 的 `requestApiVersionDeprecated`，可以在升級前先抓出「還在用舊版的 client」是誰。

（可重複驗證見 Part 3 的 Demo 1。）

### 5. 為什麼「升一點點」有時不夠：版本截斷

上一層的「沒有交集」有一個容易被忽略的根因：**舊的 wire protocol API version 會被整個移除**。

Kafka protocol 是長期演進的。每個 API 都有自己的 version range，而這個 range 不保證永遠從 `0` 開始：

```text
新版本 Kafka 可能不再支援某些過舊的 wire protocol API versions。
因此 ApiVersions 回傳的 minVersion 可能大於 0。
```

這不是抽象概念。後面 Demo 0 的 broker API versions 就會顯示（見 Part 3）`Fetch(1): 4 to 18`、`ListOffsets(2): 1 to 10`——也就是 Fetch v0–v3、ListOffsets v0 都已不在 broker 廣播的支援範圍內。

repo docs 也明確寫到 Kafka 4.0 的截斷（`docs/getting-started/upgrade.md:229`）：

> Old protocol API versions have been removed. Users should ensure brokers are version 2.1 or higher before upgrading Java clients to 4.0, and vice versa.

protocol guide 則說明 deprecation 與 negotiation 的關係（`protocol.md:108`、`protocol.md:115`）：deprecation 是把某個 API version 標記為 deprecated，協商仍是「選雙方共同支援的最高版」。

source schema 也能直接看到截斷後的 valid range：`FetchRequest.json` 是 `"4-18"`、`ListOffsetsRequest.json` 是 `"1-11"`（min 都不是 0）。

> 兩個容易誤讀的點（審查 source code 時確認）：
>
> 1. source schema 的 max version 可能高於 lab broker 廣播的值。
>    本 checkout 的 `ListOffsetsRequest.json` 已是 `"1-11"`，但 Demo 0 的 4.1.0 broker 只廣播 `ListOffsets(2): 1 to 10`。
>    原因：v11 需要 `metadata.version >= 4.2-IV1`（見 `MetadataVersion.listOffsetRequestVersion()`），而 lab 用的 `apache/kafka:4.1.0` binary 根本沒有 v11。也就是說此 source checkout（dev/trunk）比 lab 的 released 4.1.0 新。
>
> 2. source schema 截斷 min version，不一定會反映在 broker 對外廣播的 ApiVersions。
>    `ProduceRequest.json` 的 `validVersions` 已是 `"3-13"`（min 從 0 升到 3），但 Demo 0 的 broker 仍廣播 `Produce(0): 0 to 13`。
>    原因：`ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION = 0` 讓 broker listener 對 Produce 仍宣告從 v0 開始，保留 wire 相容性。所以「source schema 截斷」與「broker 對外宣告截斷」是兩件事，不必然同步。

broker 回覆 `ApiVersionsResponse` 時，會把每個 API 的 min/max version 放進 response（程式碼見[附錄 A · §5](#a5--版本截斷)）。

因此，version error 的一種常見根因不是「broker 壞掉」，而是：

```text
client 太舊，只會說已被截斷的 API version
broker 太新，不再支援該舊 API version
intersection(client range, broker range) is empty
```

**怎麼解**：升級前先讓 client 與 broker 都滿足 upgrade guide 的版本下限（升 Kafka 4.0 前，兩邊都 ≥ 2.1）。版本協商只能在「雙方仍有交集」時工作；一旦舊 protocol 被移除，協商結果就會直接變成 no usable version——這時候「再升一點點」沒用，得跨過下限。

### 6. 第二層：feature / metadata.version finalization

這一層管的是 broker 之間、以及 cluster「能不能啟用某個 feature/MV」。

`MetadataVersion` 的 javadoc 裡有關鍵的一句（升級那篇也會引到）：「This is only for inter-broker communications — when communicating with clients, the client decides on the API version.」

那句話的後半（client 決定 API version）就是上一層在講的 client↔broker 協商；這一層補上前半——**inter-broker 溝通用 MV 決定版本**。

以 replica fetch 為例：`RemoteLeaderEndPoint` 建 Fetch request 時，version 直接取自 `metadataVersion.fetchRequestVersion`；而那個 method 就是一串 `if (isAtLeast(IBP_x)) return n`，把 finalized MV 對應到固定的 RPC 版本。replica `ListOffsets` 也是同樣作法。（程式碼見[附錄 A · §6](#a6--第二層feature--metadataversion)）

所以這裡的 claim 是：

```text
client-broker protocol version selection is per connection.
broker-broker internal protocol is constrained by finalized metadata.version.
```

那 `metadata.version` 到底還控制什麼？這段屬於前傳《版本定義》的範圍，這裡只快速複習，因為它是理解上面 broker-broker 那條線的前提（細節留給前傳）。`metadata.version` 是 cluster-wide contract（KRaft 用它表示 cluster 的 feature level，見 `docs/getting-started/zk2kraft.md:71`），至少控制三類行為：

```text
1. feature gates            解鎖邏輯功能，如 ELR、SCRAM、metadata transaction
2. metadata record schema   決定寫進 metadata log 的 record 版本
3. broker-broker RPC 版本   上面的 fetchRequestVersion() / listOffsetRequestVersion()
```

前兩類是前傳的重點，本篇真正在意的是第三類：**MV 不只決定「存什麼格式」，也決定 broker 之間「用哪個 RPC 版本溝通」**。

> 補一個邊界：把「版本治理」攤開來看其實有四個維度——record schema、feature gates、參數與邏輯檢查、磁碟 format layout——但 **MV 實際只管前兩個**；後兩個是由 running binary 決定（新 binary 會套自己的嚴格檢查、format 工具會寫自己的佈局，都不看 MV）。這也是 §2 講「三層不會自動一起變」的具體樣貌。
> 而且 MV 管的這條「RPC 協定版本」**只對 broker↔broker 成立**；client↔broker 那條 RPC 不歸 MV，而是本篇從頭到尾在講的 per-connection 協商。

**怎麼解**：用 `kafka-features.sh upgrade` 推升 MV/feature；但 controller 不能隨便 finalize 到更高 MV——它會先確認所有 registered broker/controller 都支援 target level，否則回 `INVALID_UPDATE_VERSION`。升之前先 `kafka-features.sh describe` 看每個 feature 的 `SupportedMaxVersion`，確認沒有節點落後。

> 延伸（非本篇主線）：這個「controller 會檢查所有 registered broker」的機制，衍生出一個來自 Q&A 的考點——**fenced broker 仍會擋升級**。它跟「溝通層」關係較遠，移到文末 [附錄 D](#附錄-d延伸考點--fenced-broker投影片點題用) 供投影片點題。

---

## Part 3：Demo（殿後驗證）

### Demo 0：同一個 lab 直接看三種 version

目前 lab 使用 Kafka 4.1.0：

```bash
docker compose -f study/version-control/lab/tour0/docker-compose.yml ps
```

重點輸出：

```text
kafka-version-tour0-broker-1-1       apache/kafka:4.1.0   broker-1       Up 3 days   0.0.0.0:29192->9092/tcp
kafka-version-tour0-broker-2-1       apache/kafka:4.1.0   broker-2       Up 3 days   0.0.0.0:39192->9092/tcp
kafka-version-tour0-broker-3-1       apache/kafka:4.1.0   broker-3       Up 3 days   0.0.0.0:49192->9092/tcp
```

查 finalized features：

```bash
docker compose -f study/version-control/lab/tour0/docker-compose.yml exec broker-1 \
  /opt/kafka/bin/kafka-features.sh --bootstrap-server broker-1:19092 describe
```

重點輸出：

```text
Feature: metadata.version       SupportedMinVersion: 3.3-IV3  SupportedMaxVersion: 4.1-IV1  FinalizedVersionLevel: 4.1-IV1  Epoch: 636900
Feature: transaction.version    SupportedMinVersion: 0        SupportedMaxVersion: 2        FinalizedVersionLevel: 2        Epoch: 636900
Feature: group.version          SupportedMinVersion: 0        SupportedMaxVersion: 1        FinalizedVersionLevel: 1        Epoch: 636900
Feature: kraft.version          SupportedMinVersion: 0        SupportedMaxVersion: 1        FinalizedVersionLevel: 0        Epoch: 636900
```

查 broker API versions：

```bash
docker compose -f study/version-control/lab/tour0/docker-compose.yml exec broker-1 \
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server broker-1:19092
```

重點輸出：

```text
broker-1:19092 (id: 4 rack: null isFenced: false) -> (
  Produce(0): 0 to 13 [usable: 13],
  Fetch(1): 4 to 18 [usable: 18],
  ListOffsets(2): 1 to 10 [usable: 10],
  Metadata(3): 0 to 13 [usable: 13],
  ApiVersions(18): 0 to 4 [usable: 4],
  UpdateFeatures(57): 0 to 2 [usable: 2],
  GetTelemetrySubscriptions(71): UNSUPPORTED,
  StreamsGroupHeartbeat(88): UNSUPPORTED
)
```

這三段輸出對應三層：

```text
apache/kafka:4.1.0       -> release version
metadata.version 4.1-IV1 -> finalized cluster contract
Produce(0): 0 to 13      -> wire protocol API version range
```

### Demo 1：client 選最高共同 Produce version

對應 §4。可重複測試：

```bash
./gradlew :clients:test \
  --tests org.apache.kafka.clients.NodeApiVersionsTest.testPlaygroundClientChoosesHighestCommonProduceVersion \
  --tests org.apache.kafka.clients.NodeApiVersionsTest.testPlaygroundClientAbortsWhenProduceVersionsDoNotOverlap
```

console output：

```text
NodeApiVersionsTest > testPlaygroundClientAbortsWhenProduceVersionsDoNotOverlap() PASSED
NodeApiVersionsTest > testPlaygroundClientChoosesHighestCommonProduceVersion() PASSED
BUILD SUCCESSFUL in 2m 2s
```

測試重點：broker 支援 Produce 0–10、client 允許 0–13 時選到 10；client 改成只允許 11–13 時沒有交集，丟出 `UnsupportedVersionException`，訊息為 `The node does not support PRODUCE with version in range [11,13]. The supported range is [0,10].`（測試碼見[附錄 A · §4](#a4--第一層client--broker)）

---

## 附錄 A：原始碼對照

> 正文把流程講完，這裡放對應的 source 片段與 file:line，想深入的人再看。

### A4 — 第一層：client ↔ broker

`NodeApiVersions.latestUsableVersion(...)` 取交集（`clients/src/main/java/org/apache/kafka/clients/NodeApiVersions.java:149`）：

```java
Optional<ApiVersion> intersectVersion = ApiVersionsResponse.intersect(supportedVersion,
    new ApiVersion()
        .setApiKey(apiKey.id)
        .setMinVersion(oldestAllowedVersion)
        .setMaxVersion(latestAllowedVersion));

if (intersectVersion.isPresent())
    return intersectVersion.get().maxVersion();
else
    throw new UnsupportedVersionException(...);
```

`NetworkClient.doSend(...)` 接到 `UnsupportedVersionException` 後跳過 socket，丟進 `abortedSends`（`NetworkClient.java:591` 呼叫、`:597` catch）：

```java
} catch (UnsupportedVersionException unsupportedVersionException) {
    // If the version is not supported, skip sending the request over the wire.
    ClientResponse clientResponse = new ClientResponse(..., unsupportedVersionException, ...);
    if (!isInternalRequest)
        abortedSends.add(clientResponse);
}
```

Producer / Consumer 收到 `response.versionMismatch()`（`Sender.java:595`、`ConsumerNetworkClient.java:614`）：

```java
// Producer
} else if (response.versionMismatch() != null) {
    completeBatch(batch,
        new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION),
        correlationId, now, null);
}

// Consumer
} else if (response.versionMismatch() != null) {
    future.raise(response.versionMismatch());
}
```

Demo 1 測試片段（`clients/.../NodeApiVersionsTest.java`）：

```java
NodeApiVersions brokerVersions = NodeApiVersions.create(ApiKeys.PRODUCE.id, (short) 0, (short) 10);
short chosenVersion = brokerVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 0, (short) 13);
assertEquals(10, chosenVersion);

UnsupportedVersionException exception = assertThrows(UnsupportedVersionException.class,
    () -> brokerVersions.latestUsableVersion(ApiKeys.PRODUCE, (short) 11, (short) 13));
assertEquals("The node does not support PRODUCE with version in range [11,13]. " +
    "The supported range is [0,10].", exception.getMessage());
```

其他 anchor：`NetworkClient.poll(...)` drains aborted sends at `NetworkClient.java:651` / `:940`。

### A5 — 版本截斷

broker 組 `ApiVersionsResponse` 時放進每個 API 的 min/max（`clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java:287`）：

```java
return Optional.of(new ApiVersionsResponseData.ApiVersion()
   .setApiKey(messageType.apiKey())
   .setMinVersion(oldestVersion)
   .setMaxVersion(latestVersion));
```

Produce 對外仍宣告 v0 的特例：`ApiKeys.PRODUCE_API_VERSIONS_RESPONSE_MIN_VERSION = 0`（`ApiKeys.java:159`）。

schema valid range：`FetchRequest.json:61` = `"4-18"`、`ListOffsetsRequest.json:45` = `"1-11"`、`ProduceRequest.json` = `"3-13"`。

### A6 — 第二層：feature / metadata.version

`RemoteLeaderEndPoint` 建 replica Fetch request（`core/src/main/scala/kafka/server/RemoteLeaderEndPoint.scala:215`）：

```scala
val metadataVersion = metadataVersionSupplier()
val version: Short = if (!fetchData.canUseTopicIds) {
  12
} else {
  metadataVersion.fetchRequestVersion
}
val requestBuilder = FetchRequest.Builder
  .forReplica(version, brokerConfig.brokerId, brokerEpochSupplier(), maxWait, minBytes, fetchData.toSend)
```

`MetadataVersion.fetchRequestVersion()`（`server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java:273`）：

```java
public short fetchRequestVersion() {
    if (isAtLeast(IBP_4_1_IV1)) {
        return 18;
    } else if (isAtLeast(IBP_3_9_IV0)) {
        return 17;
    } else if (isAtLeast(IBP_3_7_IV4)) {
        return 16;
    } else if (isAtLeast(IBP_3_5_IV1)) {
        return 15;
    } else if (isAtLeast(IBP_3_5_IV0)) {
        return 14;
    } else {
        return 13;
    }
}
```

replica `ListOffsets` 同樣由 MV 決定版本（`MetadataVersion.listOffsetRequestVersion()` at `MetadataVersion.java:289`）：

```scala
val metadataVersion = metadataVersionSupplier()
val requestBuilder = ListOffsetsRequest.Builder.forReplica(
  metadataVersion.listOffsetRequestVersion,
  brokerConfig.brokerId)
```

## 附錄 B：可抽成 slide 的精華

```text
Slide 1: Kafka 的 version 至少有三層
  release version / metadata.version / wire protocol API version

Slide 2: Client-broker 如何選 RPC schema
  ApiVersions -> NodeApiVersions -> latest usable version

Slide 3: 沒有共同 API version 時發生什麼
  client-side UnsupportedVersionException，request 不送出

Slide 4: 版本截斷
  old protocol API versions can be removed; minVersion may be greater than 0

Slide 5: Broker-broker 為什麼不能只看 ApiVersions
  finalized metadata.version constrains internal RPC and metadata records

Slide 6: metadata.version 控制什麼
  feature gates / metadata record schema / internal RPC versions

Slide 7: 點題（來自 Q&A，非主線）：fenced broker 仍擋升級
  fenced broker still registered; registered supportedFeatures can block feature/MV upgrade（細節見附錄 D）

Slide 8: Debug version errors
  classify into wire protocol mismatch, feature/MV mismatch
```

## 附錄 C：Evidence checklist

| Claim | Evidence | Demo |
| --- | --- | --- |
| client 很長壽、舊版需長期相容 | [KIP-896](https://cwiki.apache.org/confluence/display/KAFKA/KIP-896%3A+Remove+old+client+protocol+API+versions+in+Kafka+4.0)（自 0.8.0 保留全版本、3.7 加 `DeprecatedRequestsPerSec`、4.0 baseline 砍到 2.1）, `docs/design/protocol.md:94` | Part 1 |
| Kafka 版本要拆三層 | `docker compose ps`, `kafka-features.sh describe`, `kafka-broker-api-versions.sh`, `docs/getting-started/zk2kraft.md:71` | Demo 0 |
| client 選最高共同 API version | `docs/design/protocol.md:108`, `NodeApiVersions.latestUsableVersion(...)` at `NodeApiVersions.java:149` | Demo 1 |
| 沒交集時 client 不送 request | `NetworkClient.doSend(...)` catches `UnsupportedVersionException` at `NetworkClient.java:597` and appends `abortedSends` | Demo 1 |
| 新版 Kafka 可能截斷舊 protocol versions | `docs/getting-started/upgrade.md:229`, `FetchRequest.json:61`, `ListOffsetsRequest.json:45`, `ApiKeys.java:287` | Demo 0 |
| broker-broker replica fetch 受 MV 約束 | `RemoteLeaderEndPoint.scala:215`, `MetadataVersion.java:273` | code evidence |
| MV 控制 metadata record schema | `MetadataVersion.java:225`, `MetadataVersion.java:253`, `PartitionChangeBuilder.java:464` | code evidence |

## 附錄 D：延伸考點 — fenced broker（投影片點題用）

> 來自 Q&A、跟本篇「溝通層」主題關係較遠，故移出主線；投影片可在此點題一下，想深入的人再看。

**結論**：controller 升 feature/MV 前會檢查「所有 registered broker」的 `supportedFeatures`，而它迭代的是所有 `brokerRegistrations`、**不** filter `fenced`。所以一個「fenced 但仍註冊」的舊 broker，照樣會擋住 MV/feature 升級（fence ≠ unregister，只有 `UnregisterBrokerRecord` 才移除 registration）。另外，broker 啟動時若 finalized MV 已高過它支援的範圍，會在 registration 階段直接丟 `UnsupportedVersionException`。

**怎麼解**：把已不用的舊 broker `unregister / decommission` 掉；或升該 broker 的 binary。

**驗證**：`ClusterControlManagerTest.testFencedBrokerRegistrationStillBlocksFeatureUpdate`——fenced broker（supported `0-1`）對 feature 升到 `2` → `INVALID_UPDATE_VERSION`、`Broker 1 only supports versions 0-1`。

**anchors**：`FeatureControlManager.java:321`/`:334`（升級檢查、迭代 brokerSupported）、`ClusterControlManager.java:836`（迭代 brokerRegistrations，不 filter fenced）、`ClusterControlManager.java:595`（`UnregisterBrokerRecord` 移除 registration）、`ReplicationControlManager.java:1762`（逾時 fence）、`ClusterControlManager.processRegistrationFeature`（註冊時檢查 finalized features）。
