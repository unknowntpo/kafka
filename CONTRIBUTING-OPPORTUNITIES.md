# Apache Kafka 貢獻機會清單

> 目標：成為 Apache Kafka committer。以下為經 code 掃描確認的改善機會，已標註 JIRA 狀態與建議 reviewer。
> 動工前請先到 JIRA 認領 ticket，並在 dev@kafka.apache.org 留言避免撞車。

---

## 快速起手（小型，不需 KIP）

| # | 項目 | 檔案 | JIRA | Reviewer |
|---|---|---|---|---|
| A3 | Streams `TaskExecutionMetadata` 固定 5s backoff → `ExponentialBackoff` | `streams/.../TaskExecutionMetadata.java:38` | 未追蹤，需新開 | Matthias J. Sax (Confluent) |
| A6 | Streams `KStreamSessionWindowAggregate` 與 TimeWindow 抽共用函式 | `streams/.../KStreamSessionWindowAggregate.java:241` | 未追蹤 | Bill Bejeck (Confluent) |
| B2 | Connect `KafkaStatusBackingStore` 無限 retry → 加 backoff + 上限 | `connect/runtime/.../KafkaStatusBackingStore.java:283` | 未追蹤 | Chris Egerton (Aiven) |
| C8 | Core `KafkaApis.sizeOf` 讓 `MessageUtil.sizeOf` 接受 data object，移除 `resolvedResponseData` | `core/.../KafkaApis.scala:4278` | 未追蹤 | Chia-Ping Tsai (PMC) |
| A4 | Streams `TaskExecutor` / `DefaultTaskExecutor` TimeoutException retry 邏輯整併 | `streams/.../TaskExecutor.java:105` | 未追蹤 | Bill Bejeck (Confluent) |

**可重用**：`ExponentialBackoff.java`（A3/B2）、`MockTime.java`（測試用）

---

## 中型（可能需 KIP 或 dev list 討論）

| # | 項目 | 檔案 | JIRA | Reviewer |
|---|---|---|---|---|
| **C1** ⭐ | Metadata `ClientQuotasImage.describe()` O(N) → 建 entity 索引；加 JMH benchmark | `metadata/.../ClientQuotasImage.java:121` | **KAFKA-13022**（可認領） | Chia-Ping Tsai / José Sancio |
| **C2** ⭐ | Tiered Storage `RemoteLogMetadataCache` leader epoch 未清理（記憶體漏） | `storage/.../RemoteLogMetadataCache.java:105` | **KAFKA-12641**（可認領） | Satish Duggana / Luke Chen |
| B1 | Connect `Cast` SMT 支援 nested field（dotted notation，需 KIP） | `connect/transforms/.../Cast.java:60` | 未追蹤 | Chris Egerton (Aiven) |
| B3 | Connect `DistributedHerder` single-task config 更新不強制全局 rebalance | `connect/runtime/.../DistributedHerder.java:2455` | 未追蹤 | Greg Harris (Aiven) |
| C3 | Group Coordinator `GroupConfig` validation 條件重構 | `group-coordinator/.../GroupConfig.java:428` | KAFKA-20337（待驗證） | David Jacot (Confluent) |
| C4 | Core `KafkaApis.writeTxnMarkers` 多 producerId → 單次 append | `core/.../KafkaApis.scala:1751` | 未追蹤 | Justine Olshan (Confluent) |

---

## 已有 JIRA 可直接認領

| JIRA | 說明 | 模組 |
|---|---|---|
| KAFKA-18191 | Streams topology 命名不一致（6 處 TODO 指向此 ticket） | Streams |
| KAFKA-16212 | `Partition._topicId` 漸進式遷移至 `TopicIdPartition`（適合多個小 PR） | Core |
| KAFKA-13560 | `AbstractFetcherThread` 實作非同步 tiered fetch | Core / Storage |
| KAFKA-10315 | GlobalStateManager 改用 `addReadOnlyStateStore` | Streams |
| KAFKA-12887 | GlobalStateManager / ProcessorStateManager 例外分類處理 | Streams |

---

## 誰在負責哪塊（Confluent vs 非 Confluent）

| 模組 | Confluent 主力 | 非 Confluent 活躍 | 貢獻難度 |
|---|---|---|---|
| Streams | Matthias J. Sax、Bill Bejeck、Alieh Saeedi | TengYao Chi（台灣） | ⚠️ 需先 dev list 對齊 |
| Core broker | José Sancio、Jun Rao | **Chia-Ping Tsai**（最活躍，台灣）、PoAn Yang | ✅ 最多元開放 |
| Group Coordinator | David Jacot | — | 中等 |
| Tiered Storage | — | Satish Duggana、Luke Chen、Mickael Maison | ✅ 缺人，歡迎貢獻 |
| Connect | Randall Hauch | Chris Egerton、Greg Harris（Aiven） | ✅ 中等 |
| Clients (new consumer) | Kirk True、Lianet Magrans | Andrew Schofield（IBM） | ❌ KIP-848 重構中，勿碰 |

---

## 標準貢獻流程

```
1. JIRA 搜尋確認未有人做 → 認領或新開 ticket
2. 訂閱 dev@kafka.apache.org → 留言認領
3. 本地實作 + 測試：./gradlew <module>:test --tests <TestClass>
4. 品質檢查：./gradlew checkstyleMain spotlessCheck spotbugsMain
5. 開 PR，標題格式：KAFKA-xxxxx: <description>
6. 等待 review（Streams ~1 週、Core ~1-2 週）
```

JIRA 搜尋：`https://issues.apache.org/jira/issues/?jql=project%3DKAFKA+AND+text+~+"關鍵字"`
GitHub PR 確認：`https://github.com/apache/kafka/pulls?q=KAFKA-xxxxx`
