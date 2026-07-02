# 大綱：溝通當下的版本選擇

> 幾萬個節點之上的版本控制 · 第三場。本檔是投影片／blog 的結構 spec，事實依據見 [rpc-version-selection.md](rpc-version-selection.md)。
> 三段式：先講問題 → 用 RPC 介紹機制 → 失敗訊息。每段旁附一題小測驗（共 3 題），Demo 已移除。

## 小測驗互動規格

- 全程共 **3 題**，各貼在對應知識點的下一張。
- 呈現方式：**先只顯示題目與選項，蓋住答案**；按「下一步」才顯示正解與說明（pptx 用 appear-on-click 動畫，或「題目頁 + 正解頁」兩張）。

---

## Part 1 — 為什麼單一版本號不夠

- 直覺假設：client 與 broker 同版、一起升級。
- 打破假設的兩個現實：
  - client 內嵌在各應用程式、長期不更新（Kafka 曾保留每個 protocol 版本約九年；4.0 才把下限提到約 2.1，KIP-896）。
  - broker 逐台滾動升級，過程必然新舊並存；要不停機就不能要求全叢集同版。
- 一個版本號表達不了三種 scope 不同、變動時機也不同的「版本」：
  - `release version`：我這台裝了哪版 binary（per-node）
  - `metadata.version`：整個叢集一致認定、已 finalize 的「能力世代」——決定 inter-broker 複製用哪個協定版本、以及哪些需要 MV 的功能可啟用（cluster-wide，手動 finalize）
  - wire protocol API version：這條連線實際講第幾版（per-connection）
- 具體例子（一支 Fetch）：同一顆 4.1 broker，對 Kafka 2.4 老 client 協商出 Fetch v11、對 follower 由 finalized MV 決定 v17 → 同一個 release 同時存在多個 wire 版本。

**小測驗 1**：可以「一個版本打天下」嗎？
- 正解：不行——client 長壽 + 要不停機，版本只能在連線當下決定。

---

## Part 2 — 用 RPC 介紹目前的版本與溝通機制

核心：**每條連線底層都會做 ApiVersions 握手；真正決定版本的是那支 request 的 Builder 留多少版本自由度。** 分角色看：

- **client ↔ broker**：ApiVersions 協商，取交集最高版（`NodeApiVersions.latestUsableVersion`）。
- **broker ↔ controller**：也是協商——broker 對 controller 當 client（`BrokerHeartbeat` / `BrokerRegistration` 等，走 `NodeToControllerChannelManager` 的 NetworkClient + ApiVersions）。
- **broker ↔ broker（複製）**：
  - 名詞：`Fetch`＝從指定 offset 讀 partition log；`ListOffsets`＝把時間戳／哨兵（earliest / latest / by-timestamp）換算成一個 offset（consumer 的 `seekToBeginning`、`offsetsForTimes` 就靠它，follower 則用來找截斷點）。
  - `Fetch`：由 finalized `metadata.version` **直接 pin**（`fetchRequestVersion`，建 `[v,v]`）。
  - `ListOffsets`：以 finalized `metadata.version` 為**上限**再協商（`listOffsetRequestVersion` 當上界，`forReplica` 建 `[oldest, MV]`）。
  - `OffsetsForLeaderEpoch`（follower）：Builder 寫死 v4。
- **KRaft（quorum / broker 抓 metadata log）**：Fetch 走 `SimpleBuilder` 全範圍 → ApiVersions 協商；能力由獨立的 `kraft.version` feature 治理。
- 收斂：**MV 的角色其實很窄——只釘複製用的 Fetch / ListOffsets 兩支；其餘多半仍是協商。**

**小測驗 2**：replica fetch（broker↔broker）的 Fetch 版本怎麼決定？
- 正解：由 finalized `metadata.version` 決定（`fetchRequestVersion(MV)`），不做 per-connection 協商。

---

## Part 3 — 失敗會有什麼訊息

本場只講「通訊當下協商不出版本」的錯誤；finalize / 升降時的錯誤交給同系列「運行時的版本升降」那場。

- **wire 協商失敗（client ↔ broker）**：交集空 → client 端 `UnsupportedVersionException`，且多半在**送出前**本地中止；若繞過協商自刻不支援版本 → broker 端丟 `UnsupportedVersionException` 並關閉連線（`SocketServer`；`ApiVersions` 是例外，會回 v0 + `UNSUPPORTED_VERSION` 錯誤碼）。
- **版本截斷**：4.0 移除舊 wire API 版本（Fetch min 升到 4），太舊的 client 落到交集外——這是升級「升一點點」不夠的常見原因。
- 銜接（非本場、點一句）：finalize / 升降當下的錯誤——`kafka-features upgrade` 的 `INVALID_UPDATE_VERSION`、老 broker 註冊撐不住 finalized MV——屬「運行時的版本升降」那場，本場不展開。
- 收斂：本場聚焦「送 request 前 / 通訊當下」的 wire 版本錯誤；一句話帶到 finalize/升降錯誤由兄弟場處理。

**小測驗 3**：自刻 client 送了 broker 不支援的 API version，會怎樣？
- 正解：只有 `ApiVersions` 例外會回錯誤碼 + 支援範圍；其他 API → broker 丟 `UnsupportedVersionException` → 關線。

---

## 移除

- Demo 段（原 Demo 0 / Demo 1）整段刪除。
