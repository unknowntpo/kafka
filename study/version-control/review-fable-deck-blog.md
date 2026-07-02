# Review 報告：《溝通當下的版本選擇》投影片 + Blog

> Fable subagent 系統性 review（2026-07-02）。受眾設定：有 Kafka 底子、但對 MV／版本分層／RPC version 矇矇懂懂。
> 標 ✅ 的項目已在後續 commit 修復。

## 總評

Deck（14 張、2 段式）的主線設計是好的：動機→術語→架構→舉例→失敗→Recap，把「動機／術語／架構」拆成三個不同問題的做法有效避免了重複論證。最大的問題有三個：一是 blog 仍是舊的 3 段結構，與 deck 落差已大到不能只做小修；二是主線有一個明顯的 why 缺口——全場都在說「複製面由 MV 決定」，卻從沒回答「為什麼複製面不像其他連線一樣協商就好」，而這個答案（rpc-version-selection.md 的雞生蛋＋全叢集一致段落）其實已經 fact-check 好了，只是沒被用上；三是幾個關鍵術語（ApiVersions 握手、finalize、Builder）首次出場缺一句話定義，對「MV 矇矇懂懂」的受眾正好踩在最痛的點上。

## 面向 1：主線完整性

- ✅ **[高] 缺「為什麼複製面用 MV 而不協商」的理由**。架構 slide 直接宣告「Fetch 由 MV 決定」，受眾第一個問題必然是「為什麼它不也協商？」rpc-version-selection.md「為什麼這樣分」一節已有現成答案（複製面需全叢集同版、MV 是集中開關；抓 metadata log 被 MV gate 會雞生蛋）。修法：架構 slide 加一句 rationale。
- ✅ **[中] Recap 沒回收「舉例」的 punchline**：加「同一顆 4.1 broker 同時講 Fetch v11 / v17」回收點題想像圖。
- **[低] 截斷 slide 開頭加半句**「前一頁的『交集空』最常見的根因是——」焊接兩張。
- ✅ **[低] 出處不一致**：自刻版本的失敗路徑統一為「RequestContext 解析／SocketServer 關線」。

## 面向 2：漸進式揭露

- ✅ **[高] `ApiVersions` 握手首次出現無定義**：架構 slide 先定義「連線建立後，發起端先送 ApiVersions 問對方每支 API 支援哪個版本區間（KIP-35）」；KIP-35 REF 從動機 slide 歸位到此。
- ✅ **[高] `finalize` 無一句話交代**：術語 slide 加 note「finalize＝管理員手動宣告全叢集認定的能力世代；怎麼宣告是第一場主題」。
- ✅ **[中] `Builder` 憑空出現**：改白話「組 request 的程式碼宣告的允許版本範圍」。
- ✅ **[中] `NetworkClient` 是雜訊**：從 bullet 降級。
- ✅ **[低] `validVersions` 副標未定義**：改「只會講 Fetch v0–v11」。

## 面向 3：認知負荷

- ✅ **[中] 架構 slide 一張塞四件事**：拆成 (a)「三種連線，都先做 ApiVersions 握手」(b)「最終版本誰說了算」。
- ✅ **[中] ListOffsets「以 MV 為上限」降級**為 note；主訊息收斂成「複製面的 Fetch 由 MV 決定，其餘都是協商」。
- **[低] 動機 slide 第一條 bullet 太長**（九年＋KIP-896＋2.1 全塞括號），可移 note 或口頭。

## 面向 4：deck ↔ blog 落差

| # | 落差 | 修法 |
|---|---|---|
| 1 | [高] blog 3 段 vs deck 2 段 | 重構為兩段 |
| 2 | [高] Fetch 例在 blog 當開場主圖 | 移到三角色之後 |
| 3 | [高] Builder 表先於三角色（抽象先行）| 對調：先現象、後機制 |
| 4 | [中] §6 feature/honor 整段 | 移附錄，正文留一句防混淆 |
| 5 | [中] blog 3 題 vs deck 2 題 | 對齊 2 題 |
| 6 | [低] KRaft/kraft.version | 標「進階」或移附錄 |
| 7 | [低] Part 標題不一致 | 沿用 deck 的問句式標題 |

另：blog §7 的 `chosen = max(intersection(...))` 兩行對照例比 deck 清楚，deck 失敗訊息 slide 可反向吸收。

## 面向 5：受眾適配

- 講太深該移走：blog §6 honor 全段、NetworkClient、OffsetsForLeaderEpoch、kraft.version。
- 講太淺該補：①「為什麼複製面用 MV」rationale（全份材料最有洞見的一段沒被消費）②握手定義 ③finalize 一句話。
- 「MV pin → ListOffsets 上限 → v4」三連發會暈；受眾只需帶走「複製面的 Fetch 不協商、由 MV 決定；其他都協商」。

## 面向 6：範圍紀律

- ✅ **[中] gen_deck.py QUIZ list 藏 3 題兄弟場的題**（MV 全叢集一值、feature dependency、fenced broker）——未被呼叫但屬未爆彈，刪除。
- **[中] blog §6 是最大越界**（feature gate/honor 屬版本定義場）→ blog 重構時處理。
- Deck 本體範圍紀律良好。

## 建議的最終投影片順序（15 張）

Title → TOC → 點題 → 動機 → 術語(+finalize note) → 架構a(握手定義) → 架構b(誰說了算+為什麼複製面例外) → Fetch 舉例 → 隨堂考1(Q/A) → 失敗訊息 → 截斷 → 隨堂考2(Q/A) → Recap(+v11/v17 回收句)

## Top 5 must-fix

1. ✅ [高] 補「為什麼複製面用 MV、其餘協商」rationale（deck 一句；blog 收錄「為什麼這樣分」段）
2. ✅ [高] Blog 重構為 2 段（Fetch 後置、Builder 後置、§6 移附錄、2 題）
3. ✅ [高] 三術語首現定義（ApiVersions／finalize／Builder；NetworkClient 降級）
4. ✅ [中] 架構拆兩張 + ListOffsets 降級（15 張）
5. ✅ [中] 清理：刪兄弟場 QUIZ、統一自刻出處、Recap 加 v11/v17
