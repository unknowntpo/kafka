# Eric review — round 2（deck 用語與事實精確度）

> 2026-07-02。逐條記錄 + 查證結果 + 修法。✅＝已修。

| # | 位置 | 問題 | 查證／修法 | 狀態 |
|---|---|---|---|---|
| 1 | 本場大綱 | 「兩段，加 2 題隨堂考穿插」像廣告詞 | 改「議程」，內容列 Part 1／Part 2 | ✅ |
| 2 | 點題 | 「叢集達到一定規模」不對——重點是生產環境不能停機 | 改為：生產環境的 Kafka 通常是全公司資料的主幹道（訂單事件、log、metrics pipeline），停機升級＝整條資料鏈路停擺 | ✅ |
| 3 | 動機 | 「client 內嵌在各 app」太模糊 | 用具體例子：IoT 裝置、車載系統、多年前的 batch job——隨應用長期運行、難以更新 | ✅ |
| 4 | 動機 | 「不同節點能力／版本不同」——「能力」不是 Kafka 術語 | 對應術語是 **supported versions**（ApiVersions 廣播的各 API `[min,max]`；feature 的 SupportedMin/MaxVersion）。改「各節點支援的版本範圍（supported versions）不同」 | ✅ |
| 5 | 術語 | 「能力世代」是發明的詞 | 對應術語是 **feature level**（KIP-778：`metadata.version` 是一個 finalized feature level）。改「叢集 finalized 的 feature level」；finalize 定義句保留 | ✅ |
| 6 | 架構(a) | 「ApiVersions 握手」——握手不是 Kafka 術語；且「broker↔broker 也先握手」缺佐證 | 查證結果：**replication 路徑根本不送 ApiVersionsRequest**（`BrokerBlockingSender.scala:95` `discoverBrokerVersions=false`）；client→broker、broker→controller（`NodeToControllerChannelManagerImpl.java:128`=true）、KRaft（`KafkaRaftManager.scala:239`=true）才會送。改用「送 `ApiVersionsRequest` 查詢」、不用「握手」；replication 改為「不查詢、由 MV 決定」——原 slide 陳述有誤，已更正 | ✅ |
| 7 | 架構(b) | 「複製」可否用 partition replication？ | Kafka 官方語彙是 **partition 的 replication**（follower 從 leader 複製 partition log；replica.fetchers）。統一用「partition replication（follower 從 leader 抓 log）」 | ✅ |
| 8 | 截斷 | 「升 4.0 需至少 2.1」——誰要 2.1？ | `upgrade.md`：**雙向**。升 client 到 4.0 前 broker 要 ≥2.1；升 broker 到 4.0 前 client 要 ≥2.1 | ✅ |
| 9 | 隨堂考 | 「自刻」不是術語；「關線」也是 | 改「繞過協商、直接送出 broker 不支援的 API version」；「關閉連線」 | ✅ |
| 10 | 隨堂考正解 | 「bootstrap 逃生口」沒解釋 | 補：ApiVersions 是協商的第一支 request（連線剛建立、雙方還不知道彼此版本）；若它也直接關閉連線，client 永遠問不到支援範圍，所以 broker 回 v0 + `UNSUPPORTED_VERSION` 讓 client 重新協商 | ✅ |

## 連帶修正

- `rpc-version-selection.md`「共同底層」一節原稱「四條路徑都先做 ApiVersions 握手」——**不精確**，replica fetcher 帶 ApiVersions cache 但 `discoverBrokerVersions=false`、不發送查詢。已更正。
- §2a 數線圖 SVG 的「複製」改「partition replication」。
- talk-outline.md 同步以上用語。
