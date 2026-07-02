# Kafka per-API version-range 協商的工程代價帳單

> 「Kafka 為 RPC version-range support 付出什麼代價」的專門頁面。所有數字由 repo 實測（`codex/palantir-decomposition` @ `4e71e7cccb`），headline 數字已逐一 grep 驗證。取捨評價用。

## 一句話帳單

Kafka 用「每支 RPC 各自維護一段 `[min,max]` wire 版本區間、連線時以 `ApiVersions` 取交集」換到 client 與 broker 各自獨立演進、跨約七年版本仍互通；代價是 **90 支 API × 308 個現役請求版本的協定面積、一個 6,856 行的 generator 吐出約 18 萬行逐版本序列化碼、每次建置全版本 round-trip 測試＋涵蓋 23 個 broker 版本的系統測試矩陣，以及「版本一旦釋出即不可改」的硬約束**——大到 Kafka 自己在 KIP-896 明文承認「維護成本上升、價值下降」，4.0 一次砍掉 2.1 以前的舊版本。

## 逐面向（皆附證據）

### 1. 協定面積：90 支 API、308 個現役請求版本 ✅ 驗證

`clients/src/main/resources/common/message/` 有 202 個 schema JSON（94 `*Request.json` + 94 `*Response.json`）。94 支請求中 4 支標 `validVersions: "none"`（ZK 時代的 `LeaderAndIsr`／`StopReplica`／`UpdateMetadata`／`ControlledShutdown`，除役），**現役 90 支、請求 wire 版本合計 308 個**（平均每支約 3.4 版）。跨度最大：`FetchRequest.json:61` `"4-18"`（現役 15 版）、`MetadataRequest` 0–13、`ListOffsets` 1–11、`Produce` 3–13。

### 2. 每加一版的成本：專職 generator + 約 18× 碼量放大 ✅ 驗證

Kafka 養一個獨立 `generator/` 模組（27 檔、6,856 行 main）把 JSON 編譯成逐版本 Java：`MessageDataGenerator.java`（1,688 行）產 read/write/size、`VersionConditional.java`（220 行）生成 `if (version >= …)` 分支、`Versions.java`（199 行）做版本區間代數。放大倍率：clients 的 9,890 行 schema JSON → **約 397 個生成檔、約 18 萬行 Java（≈18×）**。單例：125 行 `FetchRequest.json` → 2,157 行 `FetchRequestData.java`，含 83 處 `version >=/<=` 分支。一次真實 bump（KIP-1242 ApiVersions v4→v5）觸及 38 檔、約 446 行，橫跨 clients／core／connect／streams／tools。`ApiKeys.java` 還要維護 per-API 的 `requestHeaderVersion`（`:304`）——連 header 版本都隨 API 版本變。

### 3. 相容性測試矩陣：每次建置跑 308 版 round-trip、系統測試跑 23 個 broker 版本 ✅ 驗證

- `RequestResponseTest.java:340`（4,151 行）的 `testSerialization()` 對 `ApiKeys.values() × allVersions()` 全積掃過——每次建置序列化 308 個請求版本，還要手工維護 `toSkip` 例外清單。
- `MessageTest.java`（930 行、30 test）大量逐版迴圈；另有 `ProtocolRoundTripConsistencyTest`。
- 系統測試：`client_compatibility_features_test.py:109` 對 **23 個 broker 版本**（2.1 → 4.3 + DEV）逐一 parametrize，各起真實叢集驗證新 client 對舊 broker。

### 4. 向後相容硬約束：釋出即凍結，於是又長出 tagged fields

版本一旦釋出、wire layout 不可再動——KIP-482 tagged fields／flexible versions 正是為「不 bump 版本也能加欄位」而生（`docs/design/protocol.md:104`），但這層補丁自帶成本：序列化分裂成 flexible／非 flexible 兩制、`TaggedFields.java`（193 行）等 runtime 機制、每個生成類別背 `_unknownTaggedFields`。凍結的直接物證：`SaslHandshakeRequest.json:22-26` 註解明言「Version cannot be easily bumped due to incorrect client negotiation for clients <= 2.4」（KAFKA-9577）——一支 API 因舊 client 協商 bug 被永久凍在 v0–1。

### 5. 非 Java client 的實作負擔：協商是公開契約

`docs/design/protocol.md:100/108-116` 把「client 支援一段版本區間、與 broker 取共同最高版」＋KIP-35 五步 `ApiVersions` 流程寫成協定規範（含「版本資訊僅對該連線有效、斷線重連須重取」）。任何第三方 client（librdkafka、kafka-python、sarama…）都得自行實作：連線先發 `ApiVersionsRequest`、UNSUPPORTED_VERSION 降版重試、per-API 取交集選版、tagged-field 編解碼。

### 6. 截斷的維護動機：KIP-896 是官方自白

KIP-896 動機原句：「the cost of maintaining support for all these versions goes up (both in code complexity and the testing matrix) and the value goes down」——正對應本帳單 2、3 項。自 0.8.0（2013-12）累積，以 2.1（2018-11）為新基線截斷；疤痕可觀察：24 支現役請求最低版本 > 0（Fetch 從 v4、Produce 從 v3 起跳）。`docs/getting-started/upgrade.md:229`。

### 7. 對照：MongoDB／PostgreSQL 用單一全域版本，省掉的正是 1–5

見 [other-systems-comparison.md](other-systems-comparison.md)。兩者都無 per-command 版本矩陣，因此沒有「逐版序列化碼、全版本 round-trip 測試、per-API 交集選版」這一整層。一句：**粒度是對面積的回應，面積是粒度的帳單。**

## 值不值得（專家評語）

值得，但只對 Kafka 這種形狀的系統值得。90 支 API × 多語言、由第三方各自演進、升級週期與 broker 脫鉤的 client 生態——若採單一全域協定版本，任一支 API 要動、全體 client 就得同步跨全域版本檻，在「broker 一年兩個大版、client 生態數十實作」下不可行。per-API range 是面積逼出來的必然。但要誠實面對代價結構：成本**隨 API 數 × 版本數乘積成長**，且大部分是持續性支出（生成碼、測試矩陣、第三方實作）而非一次性投資；KIP-896 證明這條曲線陡到 Kafka 自己得截斷歷史止血、KIP-482 證明純靠 bump 版本撐不住。結論：**正確但昂貴**——面積小、client 受控的系統照抄，就是付 Kafka 的帳單、買不到 Kafka 的收益。

## 可直接上 slide 的濃縮句

> Kafka 用 90 支 API、308 個 wire 版本、約 18× 的生成碼放大率，買到 client 與 broker 七年互通——貴到 4.0 得靠 KIP-896 砍掉舊版本止血。

## 來源

- schema：`clients/src/main/resources/common/message/`（94 Request + 94 Response）；`FetchRequest.json:61-62`、`SaslHandshakeRequest.json:22-26`（KAFKA-9577）、`ApiVersionsRequest.json:21-30`
- generator：`generator/src/main/java/org/apache/kafka/message/`（27 檔、6,856 行）；`MessageDataGenerator.java`、`VersionConditional.java`、`Versions.java`
- 生成碼：`clients/build/generated/main/java/`（約 397 檔、~18 萬行）；`FetchRequestData.java`（2,157 行、83 分支）
- `ApiKeys.java:304`；`TaggedFields.java`、`RawTaggedField.java`
- `NetworkClient.java:1037-1068`（協商自舉/v0 fallback）、`NodeApiVersions.java:142-149`、`ApiVersionsResponse.java:232`
- `RequestResponseTest.java:340`、`MessageTest.java:209`、`client_compatibility_features_test.py:109`（23 broker 版本）
- `docs/design/protocol.md:100/104/108-116`、`docs/getting-started/upgrade.md:229`
- KIP-35 / KIP-482 / KIP-896（motivation 原句）
