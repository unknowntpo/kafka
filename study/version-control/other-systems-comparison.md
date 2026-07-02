# 其他系統怎麼做版本協商：MongoDB / PostgreSQL vs Kafka

> 供之後參考／可能併進 blog 的補充；**slide 不展開這節**。每項claim附來源 URL。

## 一句話

- 最強類比：**MongoDB `featureCompatibilityVersion`(FCV) ≈ Kafka `metadata.version`(MV)**——都是 cluster-wide、operator 手動 finalize、刻意跟 binary 脫鉤的能力世代。
- client↔server 協商三家都有，但**粒度不同**：Kafka per-API、Mongo/PG 單一全域版本。
- mismatch 失敗點：Kafka 與 Mongo 都傾向**client 端、送出前 fail**；PostgreSQL 是反例（major 不合 server 端關線、minor 優雅降版）。
- 節點間複製：Mongo 有 FCV、Kafka 有 MV → 可線上滾動升級；**PostgreSQL 實體複製硬鎖同一 major**，是好用的反例。

## MongoDB

### client ↔ server
- 機制：每條連線跑 `hello`（舊別名 `isMaster`）握手，server 回 `minWireVersion` / `maxWireVersion`。driver 拿自己的支援範圍跟 server 範圍比對，運作在雙方共同支援的最高 wire version。
- 粒度：**單一全域 wire version 整數**（非 per-command）；新功能再靠 wire version / FCV 往上 gate。
- 來源：<https://www.mongodb.com/docs/manual/reference/command/hello/>、<https://specifications.readthedocs.io/en/latest/mongodb-handshake/handshake/>

### server–client mismatch（≈ Kafka `UnsupportedVersionException`）
- 偵測在 **client 端、SDAM 監控/握手時**：`minWireVersion > clientMaxWireVersion` 或 `maxWireVersion < clientMinWireVersion` → topology 標記 incompatible，operations fail fast，不送上 wire。
- 使用者看到的訊息（spec 規定）例如：`"Server at host:port reports wire version 7, but this version of the driver requires at least 8 (MongoDB 4.2)."`
- 解法：升 server 或降 driver 讓範圍重疊；driver 會隨版本丟掉舊 wire version（如 sync driver v5.5 起最低 wire version 提到 8，切掉 4.2 以前）。
- 來源：<https://specifications.readthedocs.io/en/latest/server-discovery-and-monitoring/server-discovery-and-monitoring/>

### 節點間：featureCompatibilityVersion(FCV) —— 對應 Kafka MV
- FCV 是 cluster-wide 能力世代，用來「停用在混版叢集中會有問題的功能」、提供升降級的安全保證。
- **跟 binary 脫鉤、手動設定**：binary 升級後仍留在舊 FCV，直到 operator 在 primary 上跑 `setFeatureCompatibilityVersion`（要 `confirm: true`、需多數成員在線）。
- **混版成員可複製**（滾動升級靠這個）；但只支援退**前一個** major（8.0 允許 8.0/7.0、不允許 6.0）→ 不能跳版，升級走 6.0→7.0→8.0。
- 來源：<https://www.mongodb.com/docs/manual/reference/command/setfeaturecompatibilityversion/>、<https://github.com/mongodb/mongo/blob/master/src/mongo/db/repl/FCV_AND_FEATURE_FLAG_README.md>、<https://www.mongodb.com/docs/manual/release-notes/8.0-upgrade-replica-set/>

## PostgreSQL

### client ↔ server
- 機制：frontend 的 startup 封包指定 protocol major.minor（現行 v3.x）；minor 不支援時 server 回 `NegotiateProtocolVersion` 給最高支援 minor（9.3.21 引入）。
- 粒度：**單一全域 protocol 版本**（+ 具名 `_pq_.` 選項），非 per-command。
- 來源：<https://www.postgresql.org/docs/current/protocol-flow.html>、<https://www.postgresql.org/docs/current/protocol-overview.html>

### server–client mismatch
- **major 不合 → server 端 connect 時 `ErrorResponse` + 立即關線**（連任何 query 前）。
- **minor 不合 → 優雅降版**（server 給最高支援 minor，認證續行；client 談不動才 abort）。
- 實務上 protocol v3 自 7.4（約 2003）沿用至今，極穩定，mismatch 罕見。
- 來源：<https://www.postgresql.org/docs/current/protocol-flow.html>

### 節點間複製
- **實體/streaming 複製要求同一 major**（傳 raw WAL bytes、standby 位元組相同）；沒有 FCV/MV 式的相容模式 → **無法**做實體複製叢集的線上滾動 major 升級。
- **邏輯複製可跨 major**（decode WAL 成 row changes，獨立於實體儲存格式）。
- 升級路徑：`pg_upgrade`（快、但要停機、產出升級後的複本）或邏輯複製（近零停機，另建新 major 目標叢集再 failover；PG17+ 有 `pg_createsubscriber` 等改善）。
- 來源：<https://www.postgresql.org/docs/current/warm-standby.html>、<https://severalnines.com/blog/postgresql-streaming-replication-vs-logical-replication/>

## 對照表

| 面向 | Kafka | MongoDB | PostgreSQL |
| --- | --- | --- | --- |
| client↔server 機制 | `ApiVersions`，per-API `[min,max]` | `hello` 握手，全域 `[minWireVersion,maxWireVersion]` | startup protocol major.minor + `NegotiateProtocolVersion` |
| 粒度 | per-API（最細）| 單一全域 wire version | 單一全域 protocol 版本 |
| 選版 | 取每 API 最高共同版 | driver 用範圍重疊的最高 wire version | client 提最高，server 可降 minor |
| mismatch 失敗點 | 多為 **client 端、送出前** | **client 端、監控/握手時** fail fast | major：server connect 時關線；minor：降版 |
| 節點間版本治理 | **MV**（cluster-wide、手動 finalize、脫鉤 binary）| **FCV**（同上）| **無**——能力＝binary major 版 |
| 混版複製？ | 可，MV gate | 可，FCV gate（只退一 major）| 實體：**否**；邏輯：可跨 major |

## 可搬進 talk 的點（若日後要用）

1. **FCV ≈ MV**：Kafka 不是首創；MongoDB 從 3.4 起就用 FCV 做「升完 binary、跑相容模式、再手動 finalize」同一套。
2. **粒度對比**：Kafka per-API 協商 vs Mongo/PG 單一全域版本——Kafka 把協商下推到單一 RPC，因為 protocol 面積大、client 各自獨立演進。
3. **失敗模式共通**：偵測在握手/監控、送出前就 fail（Kafka `UnsupportedVersionException` ≈ Mongo SDAM）。
4. **PostgreSQL 反例**：實體複製硬鎖同一 major → 無法線上滾動升級複製層；凸顯 Kafka MV/replica-fetch 版本機制的價值。
