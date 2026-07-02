# Review：「專家評價設計取捨」深度檢視

> Fable subagent（2026-07-02）。焦點：素材是「事實轉述」還是「專家評價取捨」。依 chia 標準：專家要能評價設計的取捨。

## 1. 總評：光譜位置

**整體約在光譜 40% 處（10 分制：blog 5.5、deck 4）。機制正確性與敘事結構是強項，但「取捨」四問裡只答了一問。**

| 軸 | 現況 |
| --- | --- |
| WHY | 局部達標。blog §4「為什麼複製面用 MV」是唯一專家級段落；隨堂考 2 的 ApiVersions why 也好。其餘停在 HOW |
| 代價 | 幾乎全空。找不到一句「Kafka 為這個設計付出什麼」 |
| 反事實 | 材料備好了、一句沒用。other-systems-comparison.md 利用率 0%；gen_deck.py 的 PGURL/MONGO/KIP482/PBUF 是 dead constants |
| 立場 | 零。沒有一句「我認為這個取捨值得/不值得」 |

一句話診斷：**能讓聽眾「知道 Kafka 怎麼做」，還不能讓聽眾「學會像 committer 一樣想」。** 複製面 MV 段已示範正確寫法，其他機制照抄該模板即可。

## 2. 逐機制盤點（摘要）

| 機制 | 缺什麼 | 建議補的一句 |
| --- | --- | --- |
| 一個版本打天下→per-connection | 代價/反事實/立場 | 「拆開版本不是免費的：Kafka 揹上每支 API 的 [min,max] 矩陣；Mongo/PG 只維護一個全域版本號」 |
| per-API 粒度 | 全缺 | 「換到單支 API 獨立演進；付出協定面積與測試矩陣按 API 數放大——KIP-482 tagged fields 就是在止血」 |
| ApiVersionsRequest | 代價 | 「每條新連線先付一次 RTT，換之後每支 request 不用再猜版本——一次性成本換攤提」 |
| MV 跟 binary 脫鉤 | 代價 | 「買到安全回退窗；付出升級變兩段、多一個會被忘記的人工步驟」 |
| 複製面由 MV | 代價/立場 | 見 Top 3 之② |
| ListOffsets 上限/OffsetsForLeaderEpoch v4 | 全缺 | blog 附錄 A3 補一句：「三支 RPC 三種選版法不是失誤，是三次『夠用就好』」 |
| Fetch dual-role | 全缺 | 「共用 Fetch 是『replica 也只是 log 讀者』的抽象紅利；帳單是 replica 語意滲進共用 schema——KIP-903 還在付這筆債」（採用前需 fact-check KIP-903 細節） |
| 送出前本地中止 | 立場 | 「送出前 fail 是 Kafka/Mongo 共同收斂的答案：錯誤留在 client 端最可診斷」 |
| 直送→關線 | 代價 | 「版本不對連 request 都解析不完整，回應本身不可靠——關線是唯一誠實的動作」 |
| KIP-896 截斷 | 全缺 | 見 Top 3 之③ |

## 3. deck 建議（新增 ≤1 張）

1. 點題 slide 加 note：「先說清楚：拆開版本不是免費的——Kafka 揹上每支 API 一組 [min,max] 矩陣；Mongo/PG 選只維護一個全域版本號。這場也會講 Kafka 為什麼認為值得。」（性價比最高的一句）
2. 架構(b) solve 補半句：「……集中決定的帳單：升級變兩階段（先滾 binary、再 finalize），多一個人為步驟——一致性是拿彈性換來的。」
3. 版本截斷 slide 加 note：「九年相容窗是超保守的取捨。4.0 收斂到 2.1 baseline，用『斷最老的 client』換『刪掉九年的碼』——只有 major 版本邊界付得起。」
4. （1 張新 slide）「別家怎麼答同一題」對照頁（Kafka/MongoDB/PostgreSQL 三行 + 一句立場：「PostgreSQL 就是『複製面不做版本治理』的反事實——Kafka 的 MV 買的正是 PG 買不到的那件事」）。回收 dead constants PGURL/MONGO。
5. Recap 加一句立場句。

## 4. blog 建議

- §1 動機結尾：加「這個選擇的帳單」段（per-API 矩陣 vs 全域版本）
- §4 三理由之後：加「MV 的代價」段 + PostgreSQL 反事實段
- §5 Fetch 例結尾：加 dual-role 評價 2-3 句
- §7：加「為何關線而非回錯誤碼」一句
- §8：加「為什麼等九年、為什麼是 4.0」取捨段
- 附錄 A3 表格後：加一句「三次務實決策」解讀
- Recap：加作者立場收尾
- （選配）新增「別家怎麼答同一題」節，搬 other-systems-comparison.md 對照表

## 5. Top 3 取捨論述（文案草稿）

### ① per-API 協商的帳單（點題 + blog §1）

「一個版本打天下」不成立，但拆開版本也不是免費的。Kafka 選擇讓每支 API 各自帶 `[min, max]` 版本區間，於是協定面積、相容性測試、非 Java client 的實作負擔，全都按 API 數放大——MongoDB 和 PostgreSQL 面對同一個問題，選的是只維護一個全域版本號，把演進粒度讓給實作簡單。Kafka 買到的是單支 API 獨立演進、不必整包升版；付出的協定演進成本大到後來要用 KIP-482 的 tagged fields 來止血。評價：對一個 client 生態極度分散、API 數十支的系統，這筆帳划算——但它只在「client 由眾多第三方各自實作」的前提下划算，不是通用解。

### ② MV 集中決定的代價＋PostgreSQL 反事實（blog §4、deck 架構(b)）

複製面交給 finalized MV，買到「所有 follower↔leader 講同一版」的確定性；帳單有三筆：升級從一步變兩步（先滾 binary、再 finalize）、多一個會被忘記的人工步驟（忘了 finalize，複製面就一直講舊版）、以及沒有退讓空間——leader 不支援 MV 指定的版本就直接失敗。看反事實最快：PostgreSQL 的實體複製沒有 MV 這一層，能力直接等於 binary 的 major 版本，結果是實體複製叢集無法線上滾動升級 major，只能 pg_upgrade 停機或另建邏輯複製叢集切換。Kafka 的兩階段升級再麻煩，換到的正是 PG 給不了的那件事。這是全套設計裡最站得住的取捨。

### ③ KIP-896：九年相容窗收斂的取捨（blog §8、deck 截斷）

從 0.8.0 到 3.x，Kafka 保留了每一個 protocol API 版本整整九年——「相容性至上」推到極端的取捨：好處是任何老 client 永遠連得上；代價是每個舊版本都是活的程式碼路徑與測試矩陣。4.0 用 KIP-896 把 baseline 收到 2.1，本質是一次帳務結算：用「斷掉 2018 年以前的 client」換「刪碼、縮測試面、讓協定假設前進七年」。為什麼等這麼久？因為斷 client 是不可逆的破壞性變更，只有 major 版本邊界的社會契約付得起。評價：收得對，甚至偏晚——九年的窗說明這個專案在相容性上保守到近乎自虐，這本身就是理解 Kafka 一切版本設計的鑰匙。

## 附註

- Fetch dual-role 那句的 KIP-903 細節採用前需再 fact-check file:line。
- gen_deck.py 的 PGURL/MONGO/KIP482/PBUF dead constants：採納建議 3-4/Top①則回收，否則刪除。
