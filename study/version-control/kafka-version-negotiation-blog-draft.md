# 溝通當下的版本選擇

> **幾萬個節點之上的版本控制 · 第三場**　｜　長壽的 client、滾動升級的 broker，如何協商出共同版本
>
> 本篇專注在**溝通當下到底選哪個版本**：一條連線怎麼決定要講第幾版的 wire protocol、`metadata.version`（MV）在這裡扮演什麼角色、以及協商不出版本時會看到什麼錯誤。
>
> 整個主題拆成系列，相鄰兩場各自成篇；本篇只在需要時做前情提要、不重講：
>
> - 《Kafka 叢集版本定義與 KIP-1170》：release version（上限與規則）vs `metadata.version`（叢集當前啟用範圍）、MV 如何決定 record schema、feature gate 與啟動檢查。
> - 《運行時的版本升降》（明彥那場）：`kafka-features` 的 finalize 驗證、feature↔MV 依賴與降級規則、fenced broker 擋升級等「升降當下」的行為。
>
> 正文聚焦「機制」；對應的 source code 片段與 file:line 收在文末 [附錄 A：原始碼對照](#附錄-a原始碼對照)，想深入的人再翻。commit 基準 trunk `b7b1c0a8`。

---

## Part 1 — 為什麼單一版本號不夠

### 1. 直覺假設：一個版本打天下

大多數人談「Kafka 版本」時，腦中只有一個數字（例如 3.6、4.1），並隱含一個假設：**client 與 broker 同版、一起升級**——「一個版本打天下」。這個假設在單機或測試環境成立，但在生產叢集會被兩個現實打破。

第一，**client 很長壽**。broker 由平台團隊維護、會跟著升級；但 client 是嵌在各個應用程式裡的函式庫——某個多年前的 batch job、某個沒人敢動的 legacy service，可能到今天還抱著很舊的 kafka-client 在跑。要全公司應用同一天升 client，基本不可能。這不是隨口說的：Apache Kafka 從 0.8.0（2013）到 3.x，整整九年保留了「每一個」protocol API 版本，就因為總有舊 client 還在連；直到 4.0（[KIP-896](https://cwiki.apache.org/confluence/display/KAFKA/KIP-896%3A+Remove+old+client+protocol+API+versions+in+Kafka+4.0)）才把 baseline 提到約 2.1（2018）。

第二，**broker 逐台滾動升級，過程必然新舊並存**。升 broker 是一台一台來：關一台、換 binary、起來、再下一台。過程中叢集必然「新舊 broker 混在一起」，client 也同時連到新的和舊的 broker。要不停機，就不能要求全叢集同版。

因此版本不能事先對齊，只能在「連線當下」由雙方協商決定——這就是本場主題的由來。

### 2. 一個數字表達不了三種 scope

「一個版本打天下」不夠，根本原因是要同時表達的，其實是三種 scope 不同、變動時機也不同的「版本」：

```text
release version        我這台裝了哪版 binary       per-node（ops 換 binary，逐台滾動）
metadata.version       整個叢集一致認定的能力世代    cluster-wide（admin 手動 finalize，刻意跟換 binary 脫鉤）
wire protocol API ver  這條連線實際講第幾版        per-connection（runtime 每條連線各自協商）
```

- **release version**：這台節點裝了哪一版 binary。per-node，由維運換 binary、逐台滾動。
- **`metadata.version`**：整個叢集一致認定、已 finalize 的「能力世代」。cluster-wide，由 admin 手動 finalize，刻意跟換 binary 脫鉤——升了 binary 不代表 MV 就跟著升。它決定 inter-broker 複製面用哪個協定版本，以及哪些需要 MV 的功能可啟用。
- **wire protocol API version**：這條連線實際講第幾版。per-connection，runtime 每條連線各自協商。

這三層不會自動一起變：可以升了 binary（release version）卻還沒 finalize `metadata.version`；也可以兩個 broker binary 同版，卻因連線當下協商出不同 wire protocol version 而行為不同。

### 3. 具體例子：同一顆 broker 講多個 wire 版本

用一支 `Fetch` 就看得出「一個版本號」塞不下實際狀況。同一支 `Fetch` API 有兩種身分，各用一套機制選版本：

```text
consumer fetch   Kafka 2.4 client（validVersions 0-11）  對 4.1 broker 取交集 → Fetch v11   ← 協商
replica  fetch   follower                                 版本 = fetchRequestVersion(MV) → v17  ← 由 finalized MV 決定
```

於是同一顆 4.1 broker，會**同時**對 Kafka 2.4 老 client 協商出 Fetch v11（其 `validVersions` 是 0-11，取交集的最高共同版本）、對 follower 用 finalized MV 決定的 v17——**同一個 release，同時存在多個 wire 版本**。一個版本號根本表達不了這件事。

> **小測驗 1**：可以「一個版本打天下」嗎？（答案見文末 [附錄 B](#附錄-b常見誤解--隨堂考)）

---

## Part 2 — 用 RPC 介紹版本與溝通機制

### 4. 關鍵機制：Builder 留多少版本自由度，決定一切

不是「client 協商、broker 之間一律用 `metadata.version`」這種二分。準確講：**每條連線底層都會做 `ApiVersions` 握手；真正決定版本的是那支 request 的 `Builder` 留多少版本自由度。**

送出端建 request 時，`AbstractRequest.Builder` 帶一個 `[oldestAllowedVersion, latestAllowedVersion]`；底層 `NetworkClient` 送出前會把它跟對方 `ApiVersions` 廣播的範圍取交集：

| Builder 給的範圍 | 效果 |
| --- | --- |
| 全範圍 `[oldest, latest]` | `ApiVersions` 協商，挑交集最高版 |
| pin 成 `[MV, MV]` | MV 決定，握手只做驗證（不支援即 `UnsupportedVersionException`） |
| pin 成固定常數 | 寫死，不看 MV 也無協商空間 |

下面分角色看每條路徑實際落在哪一格。

### 5. 三個角色

**client ↔ broker：協商。** 一般 producer / consumer / admin API 的 Builder 給全範圍，`NetworkClient` 用 `NodeApiVersions.latestUsableVersion` 挑交集最高版。`MetadataVersion` 的 javadoc 也寫明：「when communicating with clients, the client decides on the API version.」

**broker ↔ controller：也是協商（broker 當 client）。** broker 對 controller 送的 `BrokerHeartbeat`、`BrokerRegistration`、`ControllerRegistration`、`AssignReplicasToDirs` 等，走 `NodeToControllerChannelManager` 的 `NetworkClient` + `ApiVersions`。`BrokerHeartbeatRequest.Builder` 未 pin 版本 → 全範圍協商。這裡 broker 扮演的是 client 角色，不由 MV 決定。

**broker ↔ broker（複製面）：三種混合。** 這是 MV 真正上場的地方，但只在複製面，而且三支 RPC 作法不同（見 `RemoteLeaderEndPoint`）：

- 先解釋兩個名詞：`Fetch` = 從指定 offset 讀 partition log 的資料；`ListOffsets` = 把時間戳／哨兵（earliest / latest / by-timestamp）換算成一個 offset（consumer 的 `seekToBeginning`、`offsetsForTimes` 就靠它，follower 則用來找截斷點）。
- `Fetch`：版本 = `metadataVersion.fetchRequestVersion`，建成 `[v, v]` → **由 finalized MV 直接 pin**（exact pin），握手只做驗證。
- `ListOffsets`：`Builder.forReplica(metadataVersion.listOffsetRequestVersion, brokerId)`，建成 `[oldest, MV]` → **以 finalized MV 為上限再協商**，不是 exact pin；MV 值只是上界，實際版本仍在此範圍內由 `ApiVersions` 挑。
- `OffsetsForLeaderEpoch`：`Builder.forFollower(...)` → `new Builder((short)4, (short)4, data)` → **寫死常數 v4**，非 MV、非協商。

**KRaft metadata-log Fetch：協商 + 獨立 feature。** broker 以 observer、controller quorum 彼此之間抓 metadata log 的 Fetch，走 `FetchRequest.SimpleBuilder`（全範圍）→ `ApiVersions` 協商，不是 MV。這條路徑的能力由**獨立於 MV 的 `kraft.version` feature** 治理。

**收斂：MV 的角色其實很窄。** `MetadataVersion` 裡跟 RPC 版本有關的方法只有兩個——`fetchRequestVersion()` 與 `listOffsetRequestVersion()`，只作用在複製面的 `Fetch` / `ListOffsets`；其餘路徑多半仍是協商。

### 6. feature 與 RPC 是兩條正交的軸

RPC 版本與 feature 是**兩條正交的軸，要用一個功能得兩個都滿足**：

- **RPC 支援（wire 能力）**：兩端「能不能講」這支 RPC / 這個版本——由 binary 能力 + `ApiVersions` 協商決定（複製面由 MV）。
- **feature（叢集政策）**：叢集「准不准用」這個功能——由 metadata log 裡 finalized 的 feature level 決定。

要點：

- **feature 不決定 RPC 版本**。只有 MV 對複製面的 `Fetch` / `ListOffsets` 這麼做；feature 產出的是布林能力或 record 格式版本。
- **feature 真正 gate 的是 broker 要不要 honor 一支 RPC。** honor 指 broker 收到請求後「認這筆請求、照該功能的語意去處理」，而不是拒絕、回錯或忽略。RPC 在 binary 裡一直存在、也能協商成功送達，但 broker 會查 finalized feature 決定是否 honor：`group.version`（KIP-848）未開時，`handleConsumerGroupHeartbeat` 直接 fail；`share.version` 未開時不受理 `ShareFetch`。
- **對外 feature 需要 RPC 承載**：新功能通常帶新 RPC（如 `ConsumerGroupHeartbeat`、`ShareFetch` / `ShareAcknowledge`）或現有 RPC 的更高版本。純內部 feature 不需要對外 RPC（只改 record 格式或內部行為）。

> **小測驗 2**：replica fetch（broker↔broker）的 `Fetch` 版本怎麼決定？（答案見文末 [附錄 B](#附錄-b常見誤解--隨堂考)）

---

## Part 3 — 失敗會有什麼訊息

本場只講「通訊當下協商不出版本」的錯誤。finalize / 升降當下的錯誤——`kafka-features upgrade` 的 `INVALID_UPDATE_VERSION`、fenced broker 擋住 feature/MV 升級、feature↔MV 依賴與降級規則——屬同系列《運行時的版本升降》那場，本場不展開，只在此點一句。

### 7. 沒有版本交集：client 端本地中止

client 不能直接用自己支援的最新版 API version，因為 broker 不一定支援。規則是取交集的最高版：

```text
chosen version = max(intersection(client allowed range, broker supported range))
```

當交集為空，`NodeApiVersions.latestUsableVersion(...)` 丟出 `UnsupportedVersionException`；`NetworkClient` 接到後**跳過 socket、不送出**，把 request 丟進 `abortedSends`，最後 producer / consumer / admin 各自在收到 `response.versionMismatch()` 時把錯誤交回應用層。關鍵：這類 `UnsupportedVersionException` 很多時候**不是 broker 回來的 response**，而是 client 在送出前就發現沒有可用 protocol version、本地 abort。

```text
client allows Produce 0-13 , broker supports 0-10  -> chosen 10
client allows Produce 11-13, broker supports 0-10  -> UnsupportedVersionException（送出前中止）
```

### 8. 繞過協商、自刻不支援版本：broker 關線

若繞過協商、自刻一個 broker 不支援的 API version 硬送，broker 端會丟 `UnsupportedVersionException` 並直接關閉連線（`SocketServer`）。

唯一的例外是 `ApiVersions` 本身：它是 bootstrap 的逃生口——即使 client 送的 `ApiVersions` 版本超出 broker 支援範圍，broker 也不會關線，而是回一個 v0 的 response 帶 `UNSUPPORTED_VERSION` 錯誤碼與自己支援的版本範圍，讓 client 得以 recover、重新協商。

### 9. 版本截斷：為什麼「升一點點」不夠

前面「沒有交集」有一個容易被忽略的根因：**舊的 wire protocol API version 會被整個移除**。每個 API 都有自己的 `validVersions` 範圍，而這個範圍不保證永遠從 `0` 開始。

Kafka 4.0 就移除了一批舊 wire API 版本，例如 `FetchRequest.json` 的 `validVersions` 已是 `"4-18"`（Fetch v0–v3 移除，min 升到 4）。因此若 client 太舊、只會講已被截斷的版本，就會落到交集外——協商結果直接是 no usable version。這時「再升一點點」沒用，得跨過 upgrade guide 的版本下限（升 Kafka 4.0 前，client 與 broker 都要 ≥ 2.1）。

> **小測驗 3**：自刻 client 送了 broker 不支援的版本會怎樣？（答案見文末 [附錄 B](#附錄-b常見誤解--隨堂考)）

---

## 附錄 A：原始碼對照

> 正文把機制講完，這裡放對應的 source 片段與 file:line。commit 基準 trunk `b7b1c0a8`。

### A1 — Builder 版本範圍與協商

- `AbstractRequest.Builder` 帶 `[oldestAllowedVersion, latestAllowedVersion]`，`NetworkClient` 送出前取交集。
- client↔broker 取交集最高版：`NodeApiVersions.latestUsableVersion(...)`（`clients/src/main/java/org/apache/kafka/clients/NodeApiVersions.java:149`）。
- Doc：`server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java:31`「when communicating with clients, the client decides on the API version.」

### A2 — broker ↔ controller（broker 當 client）

- `BrokerLifecycleManager.java:580`：`channelManager.sendRequest(new BrokerHeartbeatRequest.Builder(data), handler)`。
- `BrokerHeartbeatRequest.Builder` 只有 `super(ApiKeys.BROKER_HEARTBEAT)`、未 pin 版本 → 全範圍協商。
- 底層：`NodeToControllerChannelManagerImpl.java:67`（`private final ApiVersions apiVersions`）、`:115` / `:129`（`NetworkClient` 吃 `apiVersions`）。
- 走這條管線的 request：`BrokerHeartbeat`、`BrokerRegistration`、`ControllerRegistration`、`AssignReplicasToDirs`。

### A3 — broker ↔ broker（複製面，三種混合）

`core/src/main/scala/kafka/server/RemoteLeaderEndPoint.scala`：

- `Fetch`：`metadataVersion.fetchRequestVersion`（`:215`）→ exact pin，`FetchRequest.java:170-172` 建 `[v, v]`。
- `ListOffsets`：`Builder.forReplica(metadataVersion.listOffsetRequestVersion, brokerId)`（`:122`）→ MV 當上限的協商，`ListOffsetsRequest.java:88-90` 建 `[oldestVersion(), allowedVersion]`。
- `OffsetsForLeaderEpoch`：`Builder.forFollower(...)` → `new Builder((short)4, (short)4, data)`（`OffsetsForLeaderEpochRequest.java:60-65`）→ 寫死常數 v4。
- 底層 `BrokerBlockingSender.scala:82` / `:96` 仍是 `NetworkClient` + `ApiVersions`，握手照做，只是 Builder 已把版本鎖定。

`MetadataVersion` 裡跟 RPC 版本有關的方法只有兩個：`fetchRequestVersion()`（`:273`）與 `listOffsetRequestVersion()`（`:289`）。

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

### A4 — KRaft metadata-log Fetch（協商 + 獨立 feature）

- `KafkaRaftClient.buildFetchRequest()`（`:2985`）→ `RaftUtil.singletonFetchRequest(...)` → `KafkaNetworkChannel.buildRequest`（`:192`）包成 `FetchRequest.SimpleBuilder`。
- `SimpleBuilder` = `super(ApiKeys.FETCH)` 全範圍（`FetchRequest.java:133`）→ `ApiVersions` 協商，不是 MV。
- `kraft.version`（`KRaftVersion.java`）是獨立於 MV 的 feature；`KafkaRaftClient.java:185` 的 `localSupportedKRaftVersion: SupportedVersionRange` 是各節點自報的支援範圍。

### A5 — feature 與 honor

- `group.version`（KIP-848）未開 → `handleConsumerGroupHeartbeat` 直接 fail（`KafkaApis.scala:2642-2650`）。
- `share.version` 未開 → 不受理 `ShareFetch`，toggle off 時清掉 share session（`SharePartitionManager.java:633`、`KafkaApis.scala:4290`）。
- 帶版本的 feature 反向用 RPC 版本推能力：`transactionVersionForAddPartitionsToTxn(request)` 看 request 版本 > 3 → client 支援 TV2（`TransactionVersion.java:67`）。

### A6 — 失敗路徑

client 端取交集（`clients/src/main/java/org/apache/kafka/clients/NodeApiVersions.java:149`）：

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

`NetworkClient.doSend(...)` 接到 `UnsupportedVersionException` 後跳過 socket、丟進 `abortedSends`（`NetworkClient.java:591` 呼叫、`:597` catch）；`NetworkClient.poll(...)` drains aborted sends（`:651` / `:940`）。Producer / Consumer 收到 `response.versionMismatch()`（`Sender.java:595`、`ConsumerNetworkClient.java:614`）。

broker 組 `ApiVersionsResponse` 時放進每個 API 的 min/max（`clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java:287`）；`ApiVersions` 對外仍宣告 v0 的特例讓它成為 bootstrap 逃生口。版本截斷的 schema 證據：`FetchRequest.json:61` = `"4-18"`、`ListOffsetsRequest.json:45` = `"1-11"`、`ProduceRequest.json` = `"3-13"`。upgrade guide 對 4.0 截斷的說明：`docs/getting-started/upgrade.md:229`。

---

## 附錄 B：常見誤解 / 隨堂考

> 三題對應三段主線，每題附「直覺答案（多半錯）」與正解，可當現場有獎徵答。

### Q1：可以「一個版本打天下」嗎？（client 跟 broker 共用同一個版本）

- **直覺**：可以，大家同一版、升級一起升。
- **正解**：不行。client 嵌在各 app、超長壽，且 broker 逐台滾動升級、要不停機——混版是常態，所以版本只能在「溝通當下」各自協商決定。
- **出處**：`docs/design/protocol.md:94`、[KIP-896](https://cwiki.apache.org/confluence/display/KAFKA/KIP-896%3A+Remove+old+client+protocol+API+versions+in+Kafka+4.0)

### Q2：replica fetch（broker↔broker）的 `Fetch` 版本怎麼決定？

- **直覺**：兩個 broker 用 `ApiVersions` 協商取交集。
- **正解**：不協商。由 finalized `metadata.version` 決定（`fetchRequestVersion(MV)`，建成 `[v, v]` exact pin）；所有 broker 從同一個 MV 推出同一版。
- **出處**：`RemoteLeaderEndPoint.scala:215`、`MetadataVersion.java:273`

### Q3：自刻 client 送了 broker 不支援的 API version，會怎樣？

- **直覺**：broker 一律回一個 `UNSUPPORTED_VERSION` 錯誤碼。
- **正解**：一般 API → broker 丟 `UnsupportedVersionException` 並關閉連線。唯一例外是 `ApiVersions`：它是 bootstrap 逃生口，會回 v0 response 帶 `UNSUPPORTED_VERSION` 錯誤碼 + 支援範圍讓 client recover。
- **出處**：`SocketServer`（關線）、`ApiKeys.java`（`ApiVersions` 特例）
