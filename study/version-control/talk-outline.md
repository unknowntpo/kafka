# 大綱：溝通當下的版本選擇

> 幾萬個節點之上的版本控制 · 第三場。本檔是投影片／blog 的結構 spec，事實依據見 [rpc-version-selection.md](rpc-version-selection.md)。
> 兩段式：① 版本為何要在連線當下協商、又怎麼選 → ② 失敗訊息。標題後放一頁 TOC、結尾放一頁 Recap；共 2 題小測驗穿插，Demo 已移除。角色／RPC 用全名，不用 cb/bb 縮寫。
> Part 1 內部順序：動機（為何對不齊→協商）→ 術語（版本是哪一層）→ 架構（三種連線各自怎麼選，controller 在此一次登場）→ 以 Fetch 為例。刻意把「動機/術語/架構」拆成三個不同問題，避免重複講「一個版本不夠」。

## 小測驗互動規格

- 全程共 **2 題**（replica fetch 版本、直送不支援版本；「一個版本打天下」那題太簡單、已移除），各貼在對應知識點的下一張。
- 呈現方式：**先只顯示題目與選項，蓋住答案**；按「下一步」才顯示正解與說明（pptx 用 appear-on-click 動畫，或「題目頁 + 正解頁」兩張）。

---

## Part 1 — 版本為何要在連線當下協商、怎麼選

### 動機：為什麼版本天生對不齊，只能連線當下協商

- client 內嵌在各 app、長期不更新（Kafka 曾為舊 client 保留每個 protocol 版本近九年；4.0／KIP-896 才把下限提到 2.1）；broker 逐台滾動升級、過程必然新舊並存。
- 因果鏈：任一時刻各節點支援的版本範圍（supported versions）不同 → 不能鎖全叢集同一版（又要不停機）→ 只能在連線當下協商。這就是本場主題的由來。
- 註：三種通訊角色（下一節）是「協商發生的地方」，不是對不齊的「原因」——別把兩者用因果綁一起。

### 術語：講「版本」時是指哪一個？分三層

- `release version`：我這台裝了哪版 binary（per-node）
- `metadata.version`：叢集 finalized 的 feature level（cluster-wide，手動 finalize）
- wire protocol API version：這條連線實際講第幾版（per-connection）
- **finalize 首現需一句話定義**：管理員手動宣告全叢集一致採用的 feature level；怎麼宣告是第一場《版本定義》主題，本場只需知道它是叢集共識的一個值。
- 三個獨立的軸、各自變動；本場主角是 wire 版本。此節是「釐清術語」，不是再論「一個版本不夠」。

### 架構 (a)：三種連線；要協商，先送 ApiVersionsRequest

- **ApiVersionsRequest 首現需定義**：連線建立後，發起端先送一支 `ApiVersionsRequest`，查詢對方「你每支 API 支援哪個版本區間？」（KIP-35；KIP-35 的 REF 放這張、不放動機）。
- 三條線：`client ↔ broker`（讀寫資料、查 metadata…）、`broker ↔ controller`（註冊、心跳、轉發 admin…）、`broker ↔ broker`（partition replication）。各列僅代表性、非窮舉；replication 路徑不送 ApiVersionsRequest（discoverBrokerVersions=false），版本已由 finalized MV 決定。
- `NetworkClient` 共用一顆屬實作細節，slide 不提（留給 blog/REF）。

### 架構 (b)：查詢之後，最終版本誰說了算？

- **client ↔ broker**：協商，取交集最高版（`NodeApiVersions.latestUsableVersion`）。
- **broker ↔ controller**：也協商——broker 對 controller 當 client。
- **broker ↔ broker（partition replication）**：不協商——`Fetch` 由 finalized MV 決定（`fetchRequestVersion`）。
- Builder 用白話講：「決定權在組出這支 request 的程式碼宣告的允許版本範圍」；類名留 REF。
- **為什麼 replication 例外（must-have rationale）**：partition replication 要求所有 follower↔leader 講同一版，才交給 finalized MV 集中決定；其餘連線點對點、各自挑最好版即可。（深入版：雞生蛋——MV 存在 metadata log，抓 log 的路徑不能被 MV gate，見 [rpc-version-selection.md](rpc-version-selection.md)「為什麼這樣分」。）
- `ListOffsets` 以 MV 為上限、`OffsetsForLeaderEpoch` 寫死 v4：**slide 不展開，降為 note／blog 附錄**。
- 名詞：`Fetch`＝從指定 offset 讀 partition log；`ListOffsets`＝把時間戳／哨兵（earliest / latest）換算成 offset。
- （更細的四路徑機制、feature vs RPC、honor、KRaft `kraft.version` 見 [rpc-version-selection.md](rpc-version-selection.md)，slide 不展開。）

### 取捨層（取捨 review 後加入，散落各張；細節見 review-tradeoff-depth.md）

- 點題 note：預告帳單（per-API `[min,max]` 矩陣 vs Mongo/PG 全域版本號），Recap 回收。
- 架構(b) solve 補帳單：MV 集中決定 → 升級變兩階段——一致性拿彈性換。
- 新增一張「同一道題，別家怎麼答」（Kafka / MongoDB FCV / PostgreSQL 鎖 major）——PostgreSQL 是「複製層不做版本治理」的反事實。
- 截斷 note：九年相容窗的帳務結算（斷 2018 前的 client 換刪九年的碼，只有 major 邊界付得起）。
- Recap solve：立場句（為 client 生態極度分散量身的取捨，划算但非通用解）。
- blog 對應更完整：§1 帳單（KIP-482 措辭注意：進入 flexible version 之後 tagged fields 才免升版）、§3 一次 RTT 代價、§4「MV 帳單＋PostgreSQL 反事實」小節、§5 dual-role 評價（KIP-903 broker epoch）、§7 為何關線（response 序列化依 request version、無法保證對方讀懂）、§8 KIP-896 結算、Recap 立場、附錄 A3 OffsetsForLeaderEpoch 解讀（KAFKA-18465 清理殘留、非文件化決策）、附錄 C 三家對照。

### 以 Fetch 為例（架構下的舉例，不搶題）

- 放在架構之後，當「client↔broker 與 broker↔broker 兩條選版」的具體視覺化，別當開場主圖。
- 同一支 Fetch 兩種身分：consumer fetch 走 client↔broker、replica fetch 走 broker↔broker。
- 同一顆 4.1 broker：對 Kafka 2.4 老 client（`validVersions` 0-11）consumer 協商出 Fetch v11、對 follower replica 由 finalized MV 決定 v17 → 同一個 release 同時存在多個 wire 版本。

**小測驗 1**：replica fetch 的 `Fetch` 版本怎麼決定？→ 由 finalized `metadata.version` 決定（`fetchRequestVersion(MV)`），不做 per-connection 協商。

---

## Part 2 — 失敗會有什麼訊息

本場只講「通訊當下協商不出版本」的錯誤；finalize / 升降時的錯誤交給同系列「運行時的版本升降」那場。

- **wire 協商失敗（client ↔ broker）**：交集空 → client 端 `UnsupportedVersionException`，且多半在**送出前**本地中止；若繞過協商、直接送出不支援的版本 → broker 端丟 `UnsupportedVersionException` 並關閉連線（`SocketServer`；`ApiVersions` 是例外，會回 v0 + `UNSUPPORTED_VERSION` 錯誤碼）。
- **版本截斷**：4.0 移除舊 wire API 版本（Fetch min 升到 4），太舊的 client 落到交集外——這是升級「升一點點」不夠的常見原因。
- 銜接（非本場、點一句）：finalize / 升降當下的錯誤——`kafka-features upgrade` 的 `INVALID_UPDATE_VERSION`、老 broker 註冊撐不住 finalized MV——屬「運行時的版本升降」那場，本場不展開。
- 收斂：本場聚焦「送 request 前 / 通訊當下」的 wire 版本錯誤；一句話帶到 finalize/升降錯誤由兄弟場處理。

**小測驗 2**：client 繞過協商、直接送出 broker 不支援的 API version，會怎樣？
- 正解：只有 `ApiVersions` 例外會回錯誤碼 + 支援範圍；其他 API → broker 丟 `UnsupportedVersionException` → 關閉連線。

---

## 移除

- Demo 段（原 Demo 0 / Demo 1）整段刪除。
