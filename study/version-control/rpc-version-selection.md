# RPC version 是怎麼訂的：四種機制、三個角色

> 供整合進 blog。所有結論以 codebase / doc 為據，附 file:line。commit 基準 trunk `b7b1c0a8`。

## 一句話結論

不是「client 協商、broker 之間用 metadata.version」這種二分。準確講：**大多數連線底層都會送 `ApiVersionsRequest` 協商；真正決定版本的是那支 request 的 `Builder` 留多少版本自由度。** 兩個常見誤解要先破：

- **「broker↔broker 不協商」是錯的**：唯一不協商（`discoverBrokerVersions=false`）的是 **follower→leader 複製流程**（`ReplicaFetcherThread`/`RemoteLeaderEndPoint`）；同是 broker↔broker 的 **transaction markers**（`WriteTxnMarkers`）仍照 KIP-35 協商。
- **「唯一不協商的 RPC」也是錯的**：不協商的是複製流程這一**條連線**，上面依序發 **三支** 不協商 RPC——OffsetsForLeaderEpoch（對齊）、ListOffsets（定位）、Fetch（抓資料）；「唯一」修飾的是流程/連線，不是 RPC。「replica fetch」其實是 Fetch 的 replica 身分（一支 RPC），不是連線名。
- **「MV 只釘 replica fetch」還是錯的**：MV 在 **wire RPC 版本**上只釘 Fetch / ListOffsets 兩支；但它另外還釘一票 **metadata log record 版本**（`registerBrokerRecordVersion`、`partitionRecordVersion`… 見 §3 末），所以「由 MV 釘版」是通用機制、不等於「只有 replica fetch」。

## 關鍵機制：Builder 的版本範圍決定一切

送出端建 request 時，`AbstractRequest.Builder` 帶一個 `[oldestAllowedVersion, latestAllowedVersion]`；底層 `NetworkClient` 送出時會把它跟對方 ApiVersions 廣播的範圍取交集。因此：

| Builder 給的範圍 | 效果 |
| --- | --- |
| 全範圍 `[oldest, latest]` | ApiVersions 協商，挑交集最高 |
| pin 成 `[MV, MV]` | MV 決定，握手只做驗證（不支援就 UnsupportedVersionException） |
| pin 成固定常數 | 寫死，不看 MV 也無協商空間 |

## 共同底層：發起端都用 NetworkClient；是否查詢版本由 discoverBrokerVersions 決定

四條路徑的**發起端（送請求那側）都用同一顆 `org.apache.kafka.clients.NetworkClient`，各帶一份 `ApiVersions` cache**。但「會不會在連線後先送 `ApiVersionsRequest` 查詢對方支援區間」由建構參數 `discoverBrokerVersions` 決定，**並非四條都查**：

- client → broker：producer / consumer 直接用 `NetworkClient`，`discoverBrokerVersions=true` → 連線後先查詢。
- broker → controller：`NodeToControllerChannelManagerImpl.java:115`（`new NetworkClient`）+ `:128`（**`true`**）+ `:129`（餵 `apiVersions`）→ 連線後先查詢。
- broker → broker（replica fetch）：`BrokerBlockingSender.scala:82`（`new NetworkClient`）+ `:95`（**`false`**）+ `:96`（`new ApiVersions`）→ **不送 `ApiVersionsRequest`**。版本已由 finalized MV 決定，直接照該版本送；對方不支援即失敗。**這是四條裡唯一 `false` 的。**
- broker → broker（transaction markers）：`TransactionMarkerChannelManager.scala:86`（`new NetworkClient`）+ `:99`（**`true`**）→ 連線後先查詢。**同是 broker↔broker，但走協商**——證明「broker↔broker」不是鐵板一塊。
- KRaft（quorum 彼此 / broker 以 observer 抓 metadata log）：`KafkaRaftManager.scala:239`（`val discoverBrokerVersions = true`）+ `:241`（`new NetworkClient`）+ `:254`（餵 `apiVersions`）→ 連線後先查詢；再由 `:186` 注入 `KafkaNetworkChannel`（`KafkaNetworkChannel.java:99` 收 `KafkaClient`）。

用語註：Kafka 沒有把這一步稱為「握手（handshake）」的官方用語（`SaslHandshake` 才是 handshake）；準確說法是「送 `ApiVersionsRequest` 查詢對方各 API 的支援版本區間」（KIP-35）。

**界線（發起端 vs 接收端）**：`NetworkClient` 是「**主動發起／送請求**」那一側的工具；因此「版本協商握手」講的是發起端的行為。**接收端**（broker／controller 收請求、解析、回應）不是 `NetworkClient`，而是 `SocketServer` + `KafkaApis`。

## 逐路徑（實證）

### 1. Client → Broker：協商

- 一般 producer / consumer API，Builder 全範圍，`NetworkClient` 用 `NodeApiVersions.latestUsableVersion` 挑交集最高。
- Doc：`MetadataVersion.java:31`「when communicating with clients, the client decides on the API version.」

### 2. Broker → Controller：也是協商（broker 當 client，不是 MV）

- `BrokerLifecycleManager.java:580`：`channelManager.sendRequest(new BrokerHeartbeatRequest.Builder(data), handler)`。
- `BrokerHeartbeatRequest.Builder` 只有 `super(ApiKeys.BROKER_HEARTBEAT)`、**未 pin 版本** → 全範圍協商。
- 底層：`NodeToControllerChannelManagerImpl.java:67`（`private final ApiVersions apiVersions`）、`:115`/`:129`（`NetworkClient` 吃 `apiVersions`）。
- 走這條管線的 request：`BrokerHeartbeat`、`BrokerRegistration`、`ControllerRegistration`、`AssignReplicasToDirs`。

### 3. Broker → Broker（replica fetcher）：三種混合

`RemoteLeaderEndPoint.scala`：

- `Fetch`：版本 = `metadataVersion.fetchRequestVersion`（`:215`）→ **exact pin**（`FetchRequest.java:170-172` 建 `[v, v]`）。
- `ListOffsets`：`Builder.forReplica(metadataVersion.listOffsetRequestVersion, brokerId)`（`:122`）→ **MV 當上限的協商**，不是 exact pin：`ListOffsetsRequest.java:88-90` 建 `[oldestVersion(), allowedVersion]`，`allowedVersion` = MV 值只是上界，實際版本仍在此範圍內由 ApiVersions 挑。
- `OffsetsForLeaderEpoch`：`Builder.forFollower(...)` → `new Builder((short)4, (short)4, data)`（`OffsetsForLeaderEpochRequest.java:60-65`）→ **寫死常數 v4**，非 MV、非協商。
- 底層 `BrokerBlockingSender.scala:82/95` 是 `NetworkClient`，但 `discoverBrokerVersions=false` → **不送 `ApiVersionsRequest`**，版本全靠 Builder 鎖死。三支 RPC 共用 `RemoteLeaderEndPoint` 的**同一個 `blockingSender`**（Fetch `:78`、ListOffsets `:125`、OffsetsForLeaderEpoch `:156`）——所以「唯一不協商」講的是一條**連線**、上面三支 RPC，不是「一支 RPC」。

`MetadataVersion` 裡跟 **RPC 版本**有關的方法**只有兩個**：`fetchRequestVersion()`（`:273`）與 `listOffsetRequestVersion()`（`:289`）。其餘是 metadata log 的 **record 版本**——由 controller 寫 record 時從 MV 取、同樣不協商，但不是 wire RPC：`registerBrokerRecordVersion`（`ClusterControlManager.java:462`）、`registerControllerRecordVersion`（`ControllerRegistration.java:192`）、`partitionRecordVersion`（`PartitionRegistration.java:408`）、`partitionChangeRecordVersion`（`PartitionChangeBuilder.java:464`）。

### 4. KRaft 層（Controller quorum + broker 以 observer 抓 metadata log）：協商 + 獨立 feature

- metadata log 複製用的 Fetch：`KafkaRaftClient.buildFetchRequest()`（`:2985`）→ `RaftUtil.singletonFetchRequest(...)` → `KafkaNetworkChannel.buildRequest`（`:192`）包成 `FetchRequest.SimpleBuilder`。
- `SimpleBuilder` = `super(ApiKeys.FETCH)` 全範圍（`FetchRequest.java:133`）→ **ApiVersions 協商**，不是 MV。
- raft peer 之間確實交換 ApiVersions（`KafkaRaftClient.handleApiVersionsResponse`、add-voter 流程 `:2228`）。
- `kraft.version`（`KRaftVersion.java`）是**獨立於 MV 的 feature**，治理 raft 層能力 / record（如動態 voter 重設），`KafkaRaftClient.java:185` 的 `localSupportedKRaftVersion: SupportedVersionRange` 是各節點自報的支援範圍。

## 匯總表

| 路徑 | 版本怎麼訂 | 證據 |
| --- | --- | --- |
| Client → Broker | ApiVersions 協商 | `NodeApiVersions.latestUsableVersion`；`MetadataVersion.java:31` |
| Broker → Controller | ApiVersions 協商（broker 當 client） | `NodeToControllerChannelManagerImpl.java:67/115`；`BrokerHeartbeatRequest.Builder` 未 pin |
| Broker → Broker · transaction markers | ApiVersions **協商**（`WriteTxnMarkers`） | `TransactionMarkerChannelManager.scala:86/99`（`discoverBrokerVersions=true`） |
| Broker → Broker · replica Fetch | MV **exact pin**（`[v,v]`）· 不協商 | `RemoteLeaderEndPoint.scala:215`；`FetchRequest.java:170-172`；`MetadataVersion.java:273` |
| Broker → Broker · ListOffsets | MV 當**上限**再協商（`[oldest, MV]`） | `RemoteLeaderEndPoint.scala:122`；`ListOffsetsRequest.java:88-90`；`MetadataVersion.java:289` |
| Broker → Broker · OffsetsForLeaderEpoch | 寫死常數 v4 | `OffsetsForLeaderEpochRequest.java:60-65` |
| KRaft quorum / broker 抓 metadata log | ApiVersions 協商；能力由 `kraft.version` 治 | `KafkaNetworkChannel.java:192`；`FetchRequest.java:133`；`KRaftVersion.java`；`KafkaRaftClient.java:185` |

## 為什麼這樣分：broker↔controller 協商、複製面才用 MV

> 以下是設計推論（rationale），不是 codebase 註解；但每個推論都對得上前面已驗證的機制。

先修正一個常見誤解：**這不是「broker 加入前協商、加入後改 MV」的階段切換**。`BrokerHeartbeat` 每隔幾秒送一次、貫穿 broker 整個生命週期，從頭到尾都是協商，從不改用 MV。真正的區分是**永久按 RPC 角色分**：控制面（broker↔controller）永遠協商、複製資料面永遠 MV。

三個理由：

1. **Bootstrap 的雞生蛋**。finalized MV 本身存在 metadata log 裡；broker 是靠「跟 controller 抓 metadata log」才知道 MV。若抓 log 的那支 Fetch 版本要由 MV 決定就會循環——要知道 MV 得先抓 log，要抓 log 又得先知道 MV。所以啟動期的 RPC（registration、抓 metadata log 的 Fetch）**不能被 MV gate，只能用自足的 ApiVersions 協商**。鐵證：KRaft 抓 metadata log 的 Fetch 用 `FetchRequest.SimpleBuilder`（全範圍協商），不是 MV（`KafkaNetworkChannel.java:192`、`FetchRequest.java:133`）。

2. **控制面 vs 資料面對「一致性」的需求不同**。broker↔controller 是點對點（broker 對現任 controller），每條連線各自挑最好版本即可，**不需要全叢集一致**。複製資料面則需要**所有 follower↔leader 講同一版**，滾動升級才有確定行為——MV 就是那個「集中、一次切換」的開關。

3. **不拿權威發的值去 gate 通往權威的通道**。controller 是 MV 的來源，用「它發的 MV」去決定「連到它的那條 RPC」的版本並不合理。

一句話：**MV 只在「broker 已經知道 MV、且需要全叢集版本一致」的複製面上場；啟動期與控制面因為雞生蛋與點對點特性，只能協商。**

## feature 與 RPC 的關係（以及 honor 的意思）

RPC 版本與 feature 是**兩條正交的軸，要用一個功能得兩個都滿足**：

- **RPC 支援（wire 能力）**：兩端「**能不能講**」這支 RPC / 這個版本——由 binary 能力 + ApiVersions 協商決定（複製面由 MV）。
- **feature（叢集政策）**：叢集「**准不准用**」這個功能——由 metadata log 裡 finalized 的 feature level 決定。

**feature 不決定 RPC 版本**（只有 MV 對 Fetch / ListOffsets 這麼做）；feature 產出的是布林能力或 record 格式版本。feature 真正 gate 的是 **broker 要不要 honor 一支 RPC**。

**honor 的意思**：broker 收到請求後，「認這筆請求、照該功能的語意去處理」，而不是拒絕、回錯或忽略。RPC 在 binary 裡一直存在、也能協商成功送達，但 broker 會查 finalized feature 決定是否 honor：

- `group.version`（KIP-848）未開 → `handleConsumerGroupHeartbeat` 直接 fail（`KafkaApis.scala:2642-2650`）。
- `share.version` 未開 → 不受理 `ShareFetch`，且 toggle off 時清掉 share session（`SharePartitionManager.java:633`、`KafkaApis.scala:4290`）。

其他要點：

- **對外 feature 需要 RPC 承載**：新 RPC（`ConsumerGroupHeartbeat`、`ShareFetch` / `ShareAcknowledge`）或現有 RPC 的更高版本（TV2 的 `AddPartitionsToTxn` / `EndTxn`）。**純內部 feature 不需要對外 RPC**（只改 record 格式 / 內部行為）。
- **帶版本的 feature 反向用 RPC 版本推能力**：`transactionVersionForAddPartitionsToTxn(request)` 看 request 版本 > 3 → client 支援 TV2（`TransactionVersion.java:67`）。方向與「feature → RPC 版本」相反。
- **順序**：RPC 支援是前提——先升 binary 取得 RPC 能力，再 finalize feature 讓叢集 honor。

## 對「Fetch 一直出現」的解釋

Fetch 反覆出現不是巧合：KRaft 下，控制面協調走 metadata log（Raft），ZK 時代的 `UpdateMetadata` / `LeaderAndIsr` / `StopReplica`（controller→broker push）在 4.0 已移除。剩下真正的 broker↔broker **資料面** RPC 就是複製——follower 用 Fetch 抓資料、用 ListOffsets 問截斷點。這些必須全叢集版本一致（滾動升級才穩），才由 MV pin。
