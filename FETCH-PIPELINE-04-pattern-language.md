# FetchPipeline 04：接收 buffer 重用的 Pattern Language 分析

分析對象：commit `abfcc79882`（接收 buffer 重用），放在 FetchPipeline 的脈絡裡看。數字出處是 03 §5–§7，設計決定出處是 01 §5a。本文照 pattern-language skill 的流程走：Context 與 Intent → Forces → Problem Space → Solution Space → Patterns → 具體對應 → Resulting Context。

## 0. 核心張力與判定

**核心張力**：接收 buffer 是整個 fetch 路徑裡最便宜的記憶體，卻是最貴的 CPU。每個回應配一塊新的 heap buffer，JVM 要清零，JDK 還要從暫存 direct buffer 複製一次；這兩件事佔了背景執行緒三分之一的 CPU（F2）。要省掉它們就得重用 buffer；要重用 buffer 就得知道「誰最後用完它」。但 consumer 從來沒有定義過這塊 buffer 的擁有權：`Deserializer` 契約沒說輸入不可保留（F3），`RecordHeader` 是 lazy 解碼（F4），例外物件直接帶著 buffer 的 view 出去（F4）。

**判定**：這個改動的本質不是「加一個 pool」，而是**第一次替接收 buffer 定義擁有權邊界**。Pool 只是擁有權定義清楚之後順手能拿到的收益。所以設計裡最重的三個決定都在邊界上：完成計數釋放（PT2）、可證明的安全閘（PT3）、邊界處複製（PT4）。Pool 本身（PT1）反而是最薄的一層。這也解釋了為什麼 direct buffer 不值得：它只加深 PT1，沒有解決任何邊界問題，還引進新的 Forces（F7）。

## 1. Intent 與 Context

### 1.1 Intent（跟機制無關）

- 使用者層：單一 consumer 在同一台機器上能吃到更高的吞吐，而且每 GB 的 CPU 更少。
- 能力層：背景執行緒每 GB 的 CPU 秒數要降到能撐 2 GB/s 以上（03 §5 算出目前上限約 1.7 GB/s）。
- 非目標：不改公開 API；不新增設定；不動 producer、admin、share consumer；不做多執行緒 decode。

### 1.2 Context

- FetchPipeline 已經拿掉三道 gate，fetch 是回應驅動的（01 §4.5a）。背景執行緒現在是持續忙碌的，它的每 GB 成本直接決定吞吐上限。
- `MemoryPool` 抽象已存在（`org.apache.kafka.common.memory`），broker 端用它做 request 記憶體的 admission；client 端一律傳 `MemoryPool.NONE`，而且從來不呼叫 `receive.close()`。所以 client 端的接收 buffer 生命週期就是「誰還參照它，它就活著」。
- `FetchMetricsAggregator` 已經替每個 fetch 回應計數「哪些 partition 還沒 drain」，用來對 metrics。
- 三種 client（classic consumer、async consumer、share consumer）共用 `ClientUtils.createNetworkClient` 與 `NetworkClient`。

## 2. Force 清單

| ID | Force | 利害關係者 | 推向 | 證據 | 狀態 | 衝突 |
|---|---|---|---|---|---|---|
| F1 | 單一 consumer 的吞吐上限由背景執行緒的每 GB CPU 決定 | 使用者 | 減少背景執行緒的每 byte 工作 | 03 §5：背景 0.59 s/GB，app 0.27 s/GB | 觀察 | — |
| F2 | 每個回應配新 buffer 的清零（19%）與 JDK 暫存 direct buffer 的複製（14%）是可避免的成本 | 使用者 | 重用 buffer；或改用 direct buffer | 03 §5 profile | 觀察 | F3, F4, F8 |
| F3 | `Deserializer` 契約沒有規定輸入 buffer 不可保留；`ByteBufferDeserializer` 直接回傳 view | 相容性 | 不能假設 app 用完就放手 | `Deserializer.java` javadoc、`ByteBufferDeserializer.java` | 觀察 | F2 |
| F4 | `RecordHeader` 的 key/value 都是 lazy 解碼；`RecordDeserializationException` 帶著 key/value 的 view | 正確性 | record 交出去之前要先切斷對 buffer 的參照 | `RecordHeader.java`；`CompletedFetchTest` 抓到 key 也 lazy | 觀察 | F2 |
| F5 | 不新增設定，不把內部複雜度暴露給使用者 | 使用者（明講的規則） | 容量與開關都要從既有設定推導 | 01 §6 | 利害關係者要求 | F12 |
| F6 | `-Xmx` 要能框住 consumer 的全部記憶體 | 營運 | 不要 off-heap | 01 §6 原本的疑慮 | 推論 | F7 |
| F7 | direct buffer 帶來 native 記憶體、`Cleaner` 綁 GC、以及 direct buffer 的解析路徑 | 維護、營運 | 除非有明確吞吐收益，否則不用 direct | 03 §7.1：direct 沒有吞吐優勢 | 觀察＋推論 | F2 |
| F8 | 釋放時機必須可證明：釋放當下不能有任何 app 看得到的物件參照這塊 buffer | 正確性（硬約束） | 釋放點要放在最後一次使用之後 | 本次設計的核心 | 觀察 | F2, F9, F10 |
| F9 | 配置在網路執行緒，drain 在 app 執行緒；`retainAll` 的呼叫點全部在 app 執行緒（`assign`/`unsubscribe`/`subscribe`/`close`） | 正確性 | 釋放可以在 app 執行緒同步進行；但這是隱含不變式 | 逐一查過呼叫點 | 觀察 | F8 |
| F10 | 有些 `CompletedFetch` 不經 drain 就被丟掉（初始化錯誤、session partition 不符） | 正確性 | 釋放機制必須容忍漏釋放 | `FetchCollector` 錯誤路徑 | 觀察 | F8 |
| F11 | producer、admin、share consumer 共用 `NetworkClient` 與 `ClientUtils` | 範圍 | 改動要能只對 regular consumer 啟用 | 呼叫點 | 觀察 | — |
| F12 | 真實部署裡多數 deserializer 是自訂的（Avro、JSON、Protobuf） | 使用者 | 安全閘會把多數使用者擋在收益之外 | 業界常識 | 假設 | F5, F3 |
| F13 | 在記憶體頻寬小的機器上，兩條執行緒同時做記憶體密集的複製會互相拖慢 | 效能 | 每 byte 少一次觸碰記憶體比省指令更值錢 | 03 §4（N150 vs M1） | 觀察 | — |
| F14 | incremental fetch session 一個 node 只能一個 in-flight | 協定 | 每個 node 最多「一份在飛 + 一份已 buffer」的接收 buffer | 02 §3.35 | 觀察 | — |
| F15 | 優先用既有抽象，一句話說得清為什麼 | 維護（明講的規則） | 用 `MemoryPool`、用 `FetchMetricsAggregator`，不要新造一套 | 使用者規則 | 利害關係者要求 | — |

衝突要留著看：F2 推向重用，F3/F4/F8 說重用是危險的；F5 不給開關，F12 說沒開關就沒幾個人受益；F7 說 direct 有風險，F2 說 direct 省更多。這三組張力不會互相抵銷，它們決定了設計的形狀。

## 3. Problem Space

**P1 — 每個回應的固定配置成本**
- Forces：F1, F2, F13
- 張力：買一塊乾淨的記憶體要花清零與複製；但 buffer 用完就丟是最簡單的擁有權模型。
- 現況失敗處：背景執行緒 33% 的 CPU 在做跟資料無關的事。
- 可觀察的成功條件：背景每 GB 的 CPU 秒數下降 ≥ 25%，吞吐同步上升。
- 衍生問題：P2。

**P2 — 沒有人擁有接收 buffer**
- Forces：F3, F4, F8, F9, F10
- 張力：要重用就要有人負責「用完了」；但目前 buffer 的壽命是 GC 決定的，任何持有 view 的物件都能延長它。
- 現況失敗處：`ClientResponse` 拿不到 `NetworkReceive` 的釋放能力；record、header、例外都可能帶著 view。
- 可觀察的成功條件：存在一個釋放點，在它之後沒有任何 app 可見物件參照 buffer；漏釋放只退化成 GC，永遠不會 use-after-release。
- 衍生問題：P3。

**P3 — 在不改公開語意的前提下啟用重用**
- Forces：F3, F5, F12
- 張力：安全需要知道 deserializer 會不會保留輸入；契約沒說，也不能加設定。
- 現況失敗處：無條件重用會讓 `ByteBufferDeserializer` 的使用者讀到被覆寫的資料。
- 可觀察的成功條件：所有既有 deserializer 行為不變；內建的複製型 deserializer 拿到收益；自訂 deserializer 有一條未來能走的路。

**P4 — 範圍收斂**
- Forces：F11, F15
- 張力：接線要穿過共用的 `ClientUtils`，但只有 regular consumer 有釋放的管線。
- 可觀察的成功條件：producer、admin、share consumer 的行為與程式碼路徑不變。

問題層級：P1 是起點，解 P1 一定產生 P2，解 P2 一定產生 P3；P4 是橫向約束。

## 4. Solution Space

| 選項 | 滿足的 Forces | 削弱的 Forces | 新的 Forces／問題 | 證據 |
|---|---|---|---|---|
| S0 什麼都不做 | F3, F4, F6, F7, F11 | F1, F2 | — | 03 §5：上限 1.7 GB/s |
| S1 off-heap 有上限的池，池滿時停止讀取（01 §6 原計畫） | F2（兩種複製都省） | F6（`-Xmx` 不再是上限）、F7 | 新的 backpressure 語意要寫進 KIP；池滿的除錯 | 未做原型 |
| S2 direct buffer 重用池 | F2（兩種都省） | F7 | native 記憶體上限、`Cleaner` | 03 §7.1：吞吐與 heap 相同，背景只再省 0.05 s/GB |
| **S3 heap 重用池 + 完成計數釋放 + 安全閘（採用）** | F2（省清零）、F5、F6、F8、F9、F10、F11、F15 | F12（自訂 deserializer 不受益）；JDK 那次複製留著 | 池的上限要推導；隱含的「drain 在 app 執行緒」不變式 | 03 §7.2：+49% vs base |
| S4 在背景執行緒把 record 複製出來，立刻釋放接收 buffer | F8 變得瑣碎 | F1（多一次全量複製，背景更慢） | — | 與 F1 直接矛盾，不做原型 |
| S5 擴充 `Deserializer` API 讓自訂實作宣告「我會複製輸入」 | F12 | F5 的精神（雖然不是設定，是公開介面） | 另一個 KIP；跟 S3 相容，可疊加 | 未做 |
| S6 `GarbageCollectedMemoryPool` | — | F2 | — | 它只做額度記帳，不重用，跟 P1 無關 |

Solution Space 不算窄：S1、S2、S3 是真的結構不同的選項（記憶體在哪裡、誰負責釋放、失敗時怎麼退化）。決定性的證據是 03 §7.1：direct 沒有吞吐優勢。這讓 F7 的風險沒有對價，S2 出局；S1 同時帶著 F6 與 F7 的成本，出局。S3 是唯一一個在 F8 上有「漏釋放只退化成 GC」這種安全形狀的選項，這是它勝出的真正原因，不是它最快。

## 5. Patterns

只有在 Solution Space 攤開之後才看得出哪些是可重用的結構。下面五個是 Pattern，不是實作細節。

### PT1 有上限的 buffer 回收池，盡力歸還（Recycled Buffer Pool with Best-Effort Return）

- Context：高頻率、大尺寸的短命配置，清零與初始化成本佔主導。
- Intent：把每次配置的固定成本攤掉，而且不把「一定要歸還」變成正確性條件。
- Forces：F1, F2（推向重用）；F6（不離開 heap）；F10（一定會漏還）。
- Problem：P1。
- Solution Form：釋放時把大 buffer 放進有上限的快取；配置時先找快取裡容量夠的；找不到就照舊配新的。漏還的 buffer 交給 GC。**池從不阻塞、從不回 null**。
- Consequences：heap 常駐最多「上限」那麼多的 byte[]；重用率取決於釋放的可靠度；歸還後不可再碰，這個責任轉給 PT2。
- Resulting Context：產生 P2（誰負責歸還）；容量需要推導（PT5）。
- 證據：broker 端的 `SimpleMemoryPool`（形狀相同但語意是 admission，不是重用）；Netty 的 pooled allocator。本案：03 §7。
- 信心與問題：高。未量的是多 node 部署的重用率。

### PT2 完成計數釋放（Completion-Counted Release）

- Context：一塊 buffer 被切成 N 份（每個 partition 一份），各自在不同時間被消耗完。
- Intent：在最後一份用完的那一刻歸還整塊，而且不引進跨執行緒的擁有權轉移。
- Forces：F8（可證明的釋放點）；F9（drain 在 app 執行緒）；F10（有些份永遠不會回報）。
- Problem：P2。
- Solution Form：以「份」為單位計數；每一份在用完（drain）時回報；全部回報時執行釋放。回報必須發生在用完該份的那條執行緒上，而且在該份的資料已經複製出去之後。回報缺席時不釋放（交回 PT1 的 GC 退化）。
- Consequences：釋放的可靠度等於回報的完整度；「回報只在 app 執行緒」是隱含不變式，程式碼與測試都要把它說出來。
- Resulting Context：需要 PT4 保證回報當下沒有 view 逃出去；需要 PT3 處理無法證明的第三方程式碼。
- 證據：`FetchMetricsAggregator` 本來就為了 metrics 做完全一樣的計數，這是既有抽象（F15）；Netty `ByteBuf` 的 refcount 是同一個形狀。
- 信心與問題：高。開放問題：如果未來有人加了背景執行緒的 drain 路徑，安全性會靜默失效，目前沒有測試守這條線。

### PT3 可證明不變式的安全閘（Provable-Invariant Gate）

- Context：最佳化的安全性取決於呼叫者提供的程式碼（deserializer）是否遵守一個契約沒寫的性質。
- Intent：只在能「由構造證明」性質成立時啟用，其他情況維持舊行為，而且不需要使用者做任何事。
- Forces：F3（契約沒說）；F12（多數是自訂的）；F5（不加設定）。
- Problem：P3。
- Solution Form：列出已知滿足性質的實作（精確類別比對，子類別不算），預設關閉。閘的判斷在建構時做一次。
- Consequences：收益只到白名單；正確性不依賴使用者理解；產生擴充壓力（S5）。
- Resulting Context：新的 Force — 公開介面需要一個 opt-in 管道，這是另一個 KIP。
- 證據：`ConsumerUtils.receiveMemoryPool` 與 `ConsumerUtilsTest`（子類別被拒）。
- 信心與問題：中。白名單本身是維護負擔；每加一個內建 deserializer 都要有人記得評估。

### PT4 在信任邊界處複製（Copy at the Trust Boundary）

- Context：內部為了效能做 lazy 解碼，view 透過公開物件洩漏到 app。
- Intent：讓「交給 app 的東西不參照內部 buffer」成為構造上的事實，而不是文件上的請求。
- Forces：F4（header lazy、例外帶 view）；F8。
- Problem：P2 的一個子問題：逃逸參照。
- Solution Form：在解析邊界把所有 lazy 的部分立即物化；例外物件在建構時複製。成本只落在少見的資料上（header、錯誤路徑）。
- Consequences：有 header 的 record 多一次小複製；沒 header 的零成本。
- Resulting Context：邊界之後的物件可以自由存活，PT2 的釋放才成立。
- 證據：`CompletedFetchTest.testRecordsDoNotReferenceTheReceiveBuffer` 用「抹掉 buffer 再讀」的方式直接測這條性質，而且第一次就抓到 header key 也是 lazy。
- 信心與問題：高。開放問題：`ConsumerInterceptor` 與 `ConsumerRecord` 的其他欄位是否還有 view？目前查到的沒有。

### PT5 從既有設定推導容量（Derived Capacity）

- Context：內部資源需要上限，但不允許新設定。
- Intent：讓上限跟使用者已經理解的量綁在一起，而且能從協定推出理由。
- Forces：F5；F14（每 node 一份在飛一份已 buffer）。
- Problem：PT1 的容量問題。
- Solution Form：上限 = 2 × `fetch.max.bytes`。理由：單 node 穩態最多兩塊大 buffer 同時存活。
- Consequences：多 node 時快取可能不夠，重用率下降，但只是效能退化不是錯誤（PT1 的退化性質）。
- Resulting Context：如果多 node 量測顯示重用率差，下一步是乘上 node 數，仍然不需要設定。
- 證據：01 §4.5a 的 credit 也是同一個推導方式。
- 信心與問題：中。多 node 未量。

### 5.6 組合成語言

```text
P1 固定配置成本
  └─ PT1 回收池（盡力歸還）
       ├─ 需要容量 ──► PT5 推導容量（F5, F14）
       └─ 產生 P2 擁有權
            └─ PT2 完成計數釋放（F8, F9, F10）
                 ├─ 前提：沒有 view 逃出去 ──► PT4 邊界處複製（F4）
                 └─ 前提：第三方程式碼可證明 ──► PT3 安全閘（F3, F12）
                                                    └─ 產生新 Force：opt-in API 壓力 ──► S5（另一個 KIP）
```

順序是被迫的，不是選的：先有 PT1 才有擁有權問題；PT2 沒有 PT4 與 PT3 就不安全；PT5 是 PT1 的參數。每套一層都改變 Context：PT1 之後「漏釋放」從 bug 變成可接受的退化，PT2 之後「drain 的執行緒」從無關緊要變成不變式，PT3 之後「哪些 deserializer 會複製」從沒人在意變成需要維護的清單。

## 6. 具體對應

| Pattern | Kafka 機制 | 程式碼 | 可替換性 |
|---|---|---|---|
| PT1 | `MemoryPool` 介面 | `CachingMemoryPool` | 可換成任何 `MemoryPool` 實作；`Selector` 不知道差別 |
| PT2 | fetch 回應的 partition 集合 | `ClientResponse.releaseBuffer()` → `FetchMetricsAggregator(onComplete)` → `NetworkReceive.close()` | 計數也可以獨立於 metrics，目前共用是為了 F15 |
| PT3 | consumer 建構時的 deserializer 型別 | `ConsumerUtils.receiveMemoryPool`、`COPYING_DESERIALIZERS` | 未來由 S5 的 API 取代白名單 |
| PT4 | record 解析邊界 | `CompletedFetch.parseRecord` 的 header 物化、`copyOf` | 若 `RecordHeader` 改成 eager，這裡可以移除 |
| PT5 | `fetch.max.bytes` | `2L * FETCH_MAX_BYTES_CONFIG` | 乘上 node 數不需改介面 |
| （範圍） | `ClientUtils.createNetworkClient` 的 `MemoryPool` 參數 | producer/admin/share 傳 `NONE` | 之後 share consumer 接上 PT2 就能開 |

被拒絕的選項留著：S1（off-heap + backpressure）與 S2（direct）都可以用同一個 `MemoryPool` 接口重新實例化，PT2–PT5 全部不用動。這正是 Pattern 與工具分開的價值：如果哪天 JDK 的 socket 讀取不再需要暫存 direct buffer，或哪台機器證明 direct 有吞吐優勢，只要換 PT1 的實作。

## 7. Resulting Context 與未解的 Forces

**新的 Context**：
- 接收 buffer 現在有擁有權：網路執行緒配置，最後一個 drain 的 partition 歸還，中間任何人拿到的都是複製品。
- 「drain 只在 app 執行緒」從偶然變成不變式。
- 內建 deserializer 分成兩類：會複製的、會交出 view 的。

**未解的 Forces**：
- F12：自訂 deserializer 拿不到收益。這是 PT3 的直接後果，出路是 S5。
- F2 的另一半：JDK 那次複製（14%）留著。只有 direct 能省，而 F7 目前沒有對價。
- F13：這套改動在頻寬小的機器上收益最大（少一次觸碰記憶體），在 M1 這類機器上差距會小；03 §4 已經看到 fp 本身在 M1 上沒有退步。
- PT5 的多 node 容量未量。

**可以推翻這個論證的驗證問題**：
1. 是否存在任何路徑，讓使用白名單 deserializer 的 app 拿到接收 buffer 的 view？目前的答案是「沒有」，證據是 `CompletedFetchTest` 的抹除測試，但它只涵蓋 header 與例外。
2. 是否存在背景執行緒呼叫 `drain()` 的路徑？目前查過 `retainAll` 的四個呼叫點都在 app 執行緒。未來加路徑時這個不變式沒有測試保護。
3. 多 broker 部署下，`2 × fetch.max.bytes` 的快取重用率是多少？低於單 node 的話，PT5 需要乘上 node 數。
4. heap 常駐是否真的不超過上限？`CachingMemoryPoolTest.testCacheIsBounded` 測了池本身；整體 RSS 未在真實負載下量。

## 8. 一句話

這個改動的可重用部分不是 pool，是「把一塊共享 buffer 的擁有權定義成：最後一個用完的人歸還，用不完就交給 GC，而且交出去的東西一律是複製品」。Pool 是這個定義成立之後最便宜的收益；direct buffer 是這個定義還沒成立時最貴的誘惑。
