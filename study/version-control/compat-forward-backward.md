# 現實一/二 與 forward/backward compatibility（點題補充頁）

> 用途：《溝通當下的版本選擇》點題那張的延伸材料。若現場問到「這跟相容性有什麼關係／一般服務怎麼做」，或之後要把點題展開一張，拿這裡的內容補。事實與 [rpc-version-selection.md](rpc-version-selection.md)、[rpc-range-cost.md](rpc-range-cost.md) 一致。

## 一句話

「現實一/二」本質就是 backward／forward compatibility 問題；Kafka 的取巧是**主要只保證 backward**（broker 留住每個舊版），再用 **ApiVersions 協商把「forward」需求轉成「client 主動降版」**，於是幾乎不需要真正的 forward compat——而它跟一般服務的差別，在於用「每支 API 帶整數版本、雙方各報 `[min,max]` 區間、連線當下協商挑一版」（更像 TLS 選 cipher，而非固定 URL 版號）。

## 對應關係（現實 ↔ 相容性方向）

| 現實 | 情境 | 相容性方向 |
|---|---|---|
| 現實一 | 老 client 對新 broker | **backward**：新版 broker 仍服務舊 client |
| 現實二 | 滾動升級、混版並存 | 新對舊、舊對新兩方向都出現，理論上還要 **forward**（舊端容忍新端） |

**Kafka 的取巧**：主要只把力氣花在 backward——broker 保留每個舊 protocol 版本（曾近九年，4.0／KIP-896 才把下限提到 2.1）。至於「舊 broker 收到新 client」這個會需要 forward compat 的情境，靠**連線當下的 ApiVersions 協商**讓 client 自動降到 broker 支援的版本 → 舊 broker 永遠不會收到它不懂的版本，於是把 forward 需求轉成了一次 backward。

## 一般服務怎麼解同一問題

- **REST**：`/v1`、`/v2` 並存，client 用 URL 挑版；同一版內只加 optional 欄位、不刪不改 → 舊 client 忽略未知欄位。
- **protobuf／gRPC**：field number 永不重用、保留未知欄位 → 加欄位雙向都容忍，不必顯式版號。

共通策略是「**只加不改 ＋ 忽略未知欄位**」，相容性靠資料結構本身的寬容，沒有「連線當下協商挑版」這一步。

## Kafka 特別在哪（四點）

1. **不是「只加欄位」**：每支 API 帶一個**整數 wire 版本**，版本之間可以有實質的 layout／語意差異（不只是多幾個 optional field）。
2. **雙方各報 `[min,max]` 區間**：不是單邊決定，而是 client 與 broker 各自宣告支援範圍。
3. **連線當下用 ApiVersions 協商挑一版**：取交集、挑最高（更像 TLS 選 cipher suite，而非固定 URL 版號）；版本資訊僅對該連線有效、斷線重連要重問。
4. **per-API 粒度**：90 支現役 API 各有自己的區間，各自獨立演進；相容窗口近九年。

（tagged fields／flexible versions 是為「不 bump 版本也能加欄位」補的一層，讓 Kafka 在整數版本之外也享有 protobuf 式的加欄位彈性——見 [rpc-range-cost.md](rpc-range-cost.md) 第 4 點。）

## 可直接上 slide 的濃縮句

> 這其實就是 backward／forward compatibility：一般服務多靠「加欄位＋ URL 版號 /v1 /v2」；Kafka 特別在每支 API 各自宣告版本區間、連線當下協商挑一版（更像 TLS 選 cipher，而非固定版號），且主要只保證 backward、用協商把 forward 需求轉成 client 降版。

## 來源

- 協商規範：`docs/design/protocol.md:100/108-116`（client 支援一段版本區間、取共同最高；版本資訊僅該連線有效）
- ApiVersions 流程：KIP-35
- tagged fields／flexible versions：KIP-482
- 九年相容窗與截斷：KIP-896、`docs/getting-started/upgrade.md:229`
