# Kafka 相容性測試的代價（compat-testing-cost）

> 專攻「Kafka 為 wire protocol 相容性，在**測試**上付出什麼」。是 [rpc-range-cost.md](rpc-range-cost.md) 第 3 點的深挖版。所有數字對 apache trunk 固定 commit `b7b1c0a83d856766390ee0c05e33b63711eee80e`（下稱 UP）核對，headline 皆 grep 驗證。取捨評價與投影片用。

## 一句話

為了維護每支 RPC 的 `[min,max]` wire 版本相容，Kafka 在測試上背三層固定成本：**每次 build** 對 94 支 RPC、**308 個現役請求版本**做全版本序列化 round-trip；**每次 release** 的系統測試要**下載並起 22 個歷史 Kafka 版本**（2.1.1→4.3.0）跑 client 相容與升級/降級矩陣；再加 **25 個現役 MetadataVersion** 的功能矩陣。這套測試面隨版本只增不減，KIP-896 明文把「testing matrix」列為維護成本。

## 兩層測試

### 第一層 — build 時的全版本序列化 round-trip（自動擴張）

- `RequestResponseTest.testSerialization`（`RequestResponseTest.java:340`）對 `ApiKeys.values() × allVersions()` 全積：每個 request 版本 build→序列化→反序列化→`checkResponse/checkErrorResponse` 逐一比對。雙迴圈在 `:351-359`。
- 想跳過某個歷史版本要進 `toSkip` 白名單特案——目前只 4 支 RPC 拿得到豁免（METADATA v0、DESCRIBE_LOG_DIRS v0-2、ELECT_LEADERS v0、UNREGISTER_BROKER v0），`RequestResponseTest.java:341-350`。
- `MessageTest`（`:689-760`、`:782-806`）：ByteBuffer + JSON 雙 round-trip，`fromVersion..highestSupportedVersion` 逐版跑，斷言 size/equals/hashCode/toString；30 個 `@Test`。
- `ProtoUtilsTest.java:28` 對 `ApiKeys.values()` 逐一驗屬性。
- **關鍵性質**：這層靠迴圈跑 `allVersions()`／`ApiKeys.values()`，**新增版本自動涵蓋、免手改**——設計上刻意讓相容性測試「零維護擴張」，代價轉嫁成每次 build 的執行量單調上升。

### 第二層 — release 時的跨版本系統測試（手動擴張）

- **測試映像下載 22 個歷史版本二進位**（2.1.1 → 4.3.0），`tests/docker/Dockerfile:79-100` 逐一 `curl … | tar`；版本清單釘在 `tests/kafkatest/version.py:138-258`（22 個 `LATEST_*`）。
- **client 相容**：`client_compatibility_features_test.py:109-131`、`client_compatibility_produce_consume_test.py:69-91`——固定 client 打不同版本 broker，各 **23 param（22 歷史 + DEV）× 2 支測試**。
- **升級/降級矩陣**：`upgrade_test.py:167-188`，4 個 `@matrix` 測試法 × 11 個 from_version（LATEST_3_4…4_3 + DEV）≈ 44 組（再乘 KRaft mode / num_nodes）；混版交易 `transactions_mixed_versions_test.py:192-198` 再 11×2 = 22 組。
- **MetadataVersion 矩陣**：`MetadataVersionTest.java:219-313` 7 個 `@EnumSource` 把 25 個現役 MV 全展開（`MetadataVersion.java` MINIMUM=IBP_3_3_IV3、25 個 enum）；工具面 `FeatureCommandTest` 用 `@ClusterTest` 在不同硬編 MV 起 KRaft 叢集。
- **手動擴張**：每發一個 release，`version.py` 加 `V_x_y`+`LATEST_x_y`、`Dockerfile` 加一條下載、各 compat/upgrade 測試的 `@parametrize`/`@matrix` 版本清單各 +1。

## 量化總結

- **94** 支 RPC（`ApiKeys` enum）、request/response JSON 各 94。
- **308** 個現役請求版本（所有 `*Request.json` 的 `validVersions` 加總）＝每次 build round-trip 的量級（扣 `toSkip` 5 個 case）；加 response 幾近翻倍。
- **22** 個歷史版本被系統測試釘住（version.py 22 個 `LATEST_*` ↔ Dockerfile 22 條下載，一一對應）。
- client 相容每支測試 **23 param**（22 + DEV），兩支測試。
- 升級測試 **4 @matrix × 11 from_version ≈ 44 組**；混版交易 **22 組**。
- **25** 個現役 MetadataVersion，被 7 個 `@EnumSource` 全展開。
- **官方自述**：KIP-896 Motivation 原句「the cost of maintaining support for all these versions goes up (both in code complexity and **the testing matrix**)」。

**無法量化**：CI 上系統測試的實際 wall-clock／機器數（ducktape 在外部 Jenkins 跑，repo 內無數字）；round-trip 的實際 assertion 次數需執行期展開，以「308 request 版本 + response」作代理量級。

## 值不值得（專家評語）

值得，但要看清成本結構。相容性測試被拆成「自動擴張的 build 層」與「手動擴張的 release 層」——前者聰明（新增版本免補測試），把成本藏進 CI 執行量；後者誠實（每個歷史版本都是一筆持續支出：一個要下載的二進位、一組要維護的 `@parametrize`）。真正的取捨是：**Kafka 給的相容承諾＝「一次寫、永遠不能少測」**，測試面只能加不能減，直到 KIP-896 這種 major 邊界才敢整批砍舊版本止血。面積小、client 受控的系統照抄這套，就是白養一座跨版本測試矩陣。

## 可直接上 slide 的濃縮句

> 每次 build 對 94 支 RPC、308 個版本全 round-trip；每次 release 還要養 22 個歷史 Kafka 版本＋25 個 MV 的跨版本矩陣——相容承諾的隱形帳單，KIP-896 自己都認了。

## 來源（全部 UP = b7b1c0a83d）

- `clients/src/test/java/org/apache/kafka/common/requests/RequestResponseTest.java:340-360`（testSerialization + toSkip 341-350、雙迴圈 351-359）
- `clients/src/test/java/org/apache/kafka/common/message/MessageTest.java:689-760, 782-806`
- `clients/src/test/java/org/apache/kafka/common/protocol/ProtoUtilsTest.java:28`
- `clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java`（94 RPC enum）
- `clients/src/main/resources/common/message/*Request.json`（validVersions 加總 = 308）；`README.md:27-32,61,255-264`（版本 bump 規則）
- `server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java:50,147`（MINIMUM=IBP_3_3_IV3、25 個 enum）
- `server-common/src/test/java/org/apache/kafka/server/common/MetadataVersionTest.java:219-313`（7 個 @EnumSource）
- `tools/src/test/java/org/apache/kafka/tools/FeatureCommandTest.java:49,88,127-129`
- `tests/kafkatest/version.py:138-258`（22 個 LATEST_*）
- `tests/docker/Dockerfile:79-100`（22 條下載，2.1.1→4.3.0）
- `tests/kafkatest/tests/client/client_compatibility_features_test.py:109-131`、`client_compatibility_produce_consume_test.py:69-91`
- `tests/kafkatest/tests/core/upgrade_test.py:167-188`、`transactions_mixed_versions_test.py:192-198`
- KIP-896（Motivation 原句）、KIP-584（feature/MV）
