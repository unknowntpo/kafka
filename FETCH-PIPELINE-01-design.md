# FETCH-PIPELINE 01：consumer fetch 路徑的設計（討論稿）

基底 `e90d6f42c2`。這份是給你和之後 reviewer 來回討論的稿，不是定案。所有數字都附出處；沒有出處的地方我會寫「待量」。

---

## 0. 一句話

**consumer 的吞吐瓶頸不在協定、不在背景迴圈，而在 fetch 路徑的三件事：續發的時機被綁在應用執行緒、接收 buffer 每次重新配置並複製兩次、每筆 record 配置了可以每批共用的物件。** 這三件事可以分開做、分開量、分開進 trunk。

---

## 1. 先把「兩倍」拆開，否則論證會站不住

「FetchPipeline 讓吞吐變兩倍」這句話對小 record 大致成立，但直接這樣寫進 KIP 會被打回來，因為**最大的單一槓桿在舊架構上就拿得到**。

### 1.1 2×2 實驗（前一條線，1 partition × 100 B，交錯執行取中位數）

| | MB/s | 倍率 | CPU 秒/GB |
|---|---:|---:|---:|
| 舊架構（trunk 行為） | 444 | 1.00 | 2.50 |
| **舊架構 + 回應驅動續發** | **849** | **1.91** | 2.53 |
| 新架構，不續發 | 484 | 1.09 | 2.21 |
| 新架構 + 續發 | 1,039 | 2.34 | 2.27 |

`max.poll.records=50`（每秒約一萬次 poll）時：337 / 519（1.54×）/ 447（1.33×）/ 877（2.60×），CPU 秒/GB 3.44 / 3.41 / 2.37 / 2.53。

**讀法**：預設 poll 頻率下的 1.91× 幾乎全部來自「回應到達就續發」，這件事不需要新架構。架構本身只多 9%，但在高 poll 頻率下它把續發的增益從 1.54× 撐到 2.60×。

### 1.2 只有事件迴圈的對照（安靜機器，修掉 R11 bug 之後）

| 場景 | trunk | 只有迴圈 |
|---|---:|---:|
| 1p 100B MB/s | 404 | 392（−3%） |
| mpr50 MB/s | 305 | 306（0%） |
| 6p 1KB MB/s | 627 | 635（+1%） |

**迴圈本身與 trunk 打平。** 所以吞吐的增益全部在 fetch 路徑，這也是我們把 fetch 單獨拿出來談的理由。

### 1.3 完整 fetch 路徑的穩態（morefine，CRC 開啟，`byte[]` 仍複製一次）

| | T2（4 核）MB/s | T2 CPU 秒/GB | T1（1 核）MB/s | T1 CPU 秒/GB |
|---|---:|---:|---:|---:|
| trunk | 1,078 | 1.18 | 920 | 1.07 |
| librdkafka | 1,295 | 1.32 | 1,218 | 0.88 |
| franz-go | ~1,000 | 1.32 | 1,024 | 0.91 |
| **本設計（bytes）** | **1,502** | **1.08** | **1,236** | **0.81** |
| 本設計（ByteBuffer，見 §5 的限制） | 1,770 | 0.84 | 1,385 | 0.68 |

1 partition × 100 B：trunk 496 MB/s · 2.74 秒/GB；本設計 924 MB/s · 1.98（per-batch 物件共用之後）。

**所以誠實的說法是**：6p×1KB 是 1.39×、CPU/GB −8%；小 record 是 1.86×；零複製路徑再多 15–25% 但有語意代價。「兩倍」只在小 record 且加上零複製時成立。

---

## 2. 現況的三個成本（trunk 程式碼位置）

**C1 續發被三道 gate 擋住，不是一道。**（以下是 trunk 原始碼的行號；三道都已在本 branch 拆除，實際落地的形式見 §4.6）

- **C1a 時機綁在應用執行緒。** `FetchRequestManager.java:155-157`：沒有 `pendingFetchRequestFuture` 就直接回 `PollResult.EMPTY`。那個 future 只有 `CreateFetchRequestsEvent` 會設，由應用執行緒在 `AsyncKafkaConsumer.java:2120` 送出（呼叫點 `:965`，拿到非空 fetch 之後）。**回應到達本身不會觸發下一個 fetch。**
- **C1b 已經有 buffer 的 partition 被排除在下一個 fetch 之外。** `AbstractFetch.java:343-350` 的 `fetchablePartitions(isNotBuffered)`：註解自己寫明「for which we don't already have some messages sitting in our buffer」。per-partition 的管線深度被結構性地鎖在 1。
- **C1c 只要一個 node 上有任何 partition 有 buffer，那個 node 上的所有 partition 都不發 fetch。** `AbstractFetch.java:466-471`：

  ```java
  } else if (bufferedNodes.contains(node.id())) {
      // While a node has buffered data, don't fetch other partition data from it. Because the buffered
      // partitions are not included in the fetch request, those partitions will be inadvertently dropped
      // from the broker fetch session cache. In some cases, that could lead to the entire fetch session
      // being evicted.
  ```

**C1c 是三道裡最根本的，而且它解釋了為什麼收益那麼大。** 沒有預抓 cursor 的話，一個已經有 buffer 的 partition 陷入兩難：把它放進下一個 incremental fetch 請求，就會重抓已經拿到的資料；把它省略，broker 的 fetch session cache 就會把它丟掉（嚴重時整個 session 被 evict）。trunk 選的是第三條路——**那個 broker 完全不發 fetch，整條連線閒置，直到 app 執行緒把 buffer 吃完**。

這三道是獨立的，而且只修前兩道不夠：C1a 讓續發等 app 執行緒，C1b 讓 partition 等自己的 buffer 排空，C1c 讓**整個 broker** 等任一 partition 的 buffer 排空。**只解決 C1a 拿不到 §1.1 的 1.91×。**

好消息是三道用**同一個改動**解決：給每個 partition 一個私有的預抓 cursor（「已請求到哪」，與 position 的「已交付到哪」分開）之後，每個可 fetch 的 partition 都能永遠出現在每一個送給它 leader 的 fetch 請求裡，於是 session cache 不會掉 partition，也沒有「要不要跳過 buffered partition」這個問題——`buffered` / `bufferedNodes` 這整套機制連同它要防的 eviction 一起消失。consumer-ng 的 `FetchPipeline` 沒有這段程式碼，原因就是它有 cursor。

前一條線量到：三方（app、背景、broker）各只用 22–29% CPU，全部在等彼此。

順帶一個對照事實：trunk 每個 broker 也是**一個 fetch 在飛**（`AbstractFetch.java:462` 的 `nodesWithPendingFetchRequests.contains`），跟 consumer-ng 相同。所以差別不在每個 broker 的在途深度，而在 C1a 的觸發時機與 C1b 的管線深度。

**C2 接收 buffer 每次重新配置。** consumer 的 `Selector` 用的是不帶 `MemoryPool` 的六參數建構子（`ClientUtils.java:279-284`），所以走 `MemoryPool.NONE`：每個回應配置一塊新的 heap buffer，JVM 先清零，再從 socket 讀進去，再複製一次給 record 解析。profile：背景執行緒 memset 佔 15%、JDK 暫存複製佔 10%。

**C3 每筆 record 配置可以每批共用的物件。** leader epoch 的 `Optional`、`TimestampType`、空的 `RecordHeaders`（含內部 `ArrayList`）都是每筆一份。profile：app 執行緒 value 複製 28%、每筆 6–8 個物件。

---

## 3. 三個可分離的槓桿

| | 機制 | 證據 | 需要 KIP？ | 風險 |
|---|---|---|---|---|
| **L1** | 回應到達就續發，用 credit 做 admission | 1p100B 1.91×（舊架構上量到）、6p 1KB +58%（迴圈那條線） | 否（內部行為，但記憶體佔用行為變了，建議在 KIP 說明） | 預抓深度改變既有測試對「第一次 poll 拿幾筆」的期待 |
| **L2** | leader epoch 與 timestamp type 每批一份、空 headers 延遲配置 | 1p100B 788 → 924 MB/s、2.22 → 1.98 秒/GB | 否（純內部） | 低 |
| **L3** | 池化 direct 接收 buffer + 在 buffer 上原地迭代 record | 6p1KB 1,502 · 1.08（含 L1、L2）；heap 256 MB 時 RSS 326 MB | **是** | **零複製 deserializer 的生命週期，見 §5** |

建議的順序就是 L1 → L2 → L3，理由：L1 收益最大、風險最小；L2 幾乎零風險；L3 才需要公開語意的討論。

---

## 4. L1：回應驅動續發與 credit admission

### 4.1 機制

今天的 gate（`pendingFetchRequestFuture`）改成 admission 規則：

```
可以再發一個 fetch  ⟺  在途 bytes + 已排隊未交付 bytes < credit
```

- **在途 bytes**：已送出但回應未到的 fetch，以 `fetch.max.bytes` 計（保守估）。
- **已排隊未交付 bytes**：已收到、還沒被應用執行緒取走的 record bytes。
- **credit**：`4 × fetch.max.bytes`。**不新增設定**，理由見 §6。
- 觸發點從「應用執行緒送 event」改成「回應在背景執行緒完成時，同一條執行緒的下一步」。

### 4.2 為什麼需要 credit 而不是固定深度

固定深度（例如「每個 broker 兩個 fetch 在飛」）會讓記憶體佔用隨 partition 數與 record 大小飄動。用 bytes 記帳，上限與 partition 數無關，而且應用執行緒消費得慢時自動退讓——這就是 backpressure：資料留在 broker，不留在 client。

### 4.3 fencing

預抓會讓「已經收到但還沒交付」的資料存在，所以 `seek` / reset / 撤銷指派必須讓這些資料作廢。做法是每個 partition 的 buffered 與 in-flight 資料都帶 position epoch，不匹配即丟棄。前一條線的實驗 hack 沒有做這件事，結果是 seek 之後偶爾停滯——**這是 L1 唯一真正的正確性風險，必須有測試**。

### 4.4 兩件我原本以為需要、實際上不需要的事

- **控制請求優先**：我在另一條線上加了「有控制請求待送就停 fetch」的規則，因為那個引擎刻意讓每個 broker 只有一個 fetch 在飛。**trunk 不需要**：`ConsumerUtils.java:77` 的 `CONSUMER_MAX_INFLIGHT_REQUESTS_PER_CONNECTION = 100`，控制請求不會被 fetch 擠掉。這條規則不進這個 KIP。
- **新的排程模型**：§1.2 已經證明迴圈本身是打平的。L1 不需要任何排程改動。

### 4.5a 實際落地的形式（本 branch，五個 commit）

實作之後有四點跟 §4.1–§4.3 的原始構想不同，以落地的為準：

1. **credit 是 per-node、只算已 buffer 未交付的 bytes，上限 `1 × fetch.max.bytes`**（`AbstractFetch.creditBytes`），不是 §4.1 的「在途 + 已排隊 < 4×」全域帳。在途本來就被「每個 node 一個 fetch 在飛」結構性地限制住（`nodesWithPendingFetchRequests`），不需要再記帳；而 4× 的全域帳在 broker 數超過 4 時會把多 broker 的 fetch 全部擋住。1× 是「一份在 buffer、一份在飛」所需的最小值，每個 node 的最壞情況從 trunk 的約 1 份變成約 2 份，這是 KIP 要明說的記憶體變更。
2. **不需要 position epoch。** §4.3 假設 fencing 要在 `SubscriptionState` 加 epoch。實際上 `FetchCollector.java:258-263` 現有的 `position.offset != fetchOffset` 守衛天生能處理管線化：同一個 partition 的多份 buffer 是連續 offset 且依序初始化；seek 之後會逐份被丟棄。cursor 端的 fencing 是「沒有在途、沒有 buffer 時 cursor 必須等於 position，否則重設」（`FetchCursors.nextFetchOffset`），雙向 seek 都覆蓋，`SubscriptionState` 零改動。
3. **gate 1 保留一個「app 要過一次」的閂**（`FetchRequestManager.fetchingStarted`）。完全拆掉會讓 `assign()` + `endOffsets()` 而從不 `poll()` 的 consumer 在背後預抓；`KafkaConsumerTest.testListOffsetShouldUpdateSubscriptions` 因此卡死 120 秒。第一次 `poll()` 之後續發完全由回應驅動。
4. **`poll()` 回傳資料前送的 `CreateFetchRequestsEvent`（`sendPrefetches`）整個刪除。** 背景執行緒自己續發之後它只剩設閂的作用，而 `AsyncPollEvent` 已經設了。每次回傳資料的 `poll()` 少一個 event、一次 queue 操作、一次跨執行緒 wakeup。

`AbstractFetch` 是 classic 與 async consumer 共用的，所以兩者都吃到這個行為變更；`FetcherTest` 的失敗與修改就是這樣來的。share consumer 用 `ShareConsumeRequestManager`，不受影響。

### 4.5 與 KAFKA-20854 的關係

那張票的形狀是「空的 fetch 結果觸發一次沒有進展的喚醒，造成 app 與背景執行緒 ping-pong」。L1 把續發搬到背景執行緒之後，**空回應不得觸發任何跨執行緒喚醒**，否則會重現同一個形狀。驗收條件寫在 §8。

---

## 5. L3 的核心風險：零複製 deserializer 的生命週期

這一節是整份文件最需要你先拍板的地方。

池化的前提是 buffer 會被回收再用。原地交付的前提是 record 直接指向接收 buffer。兩者合起來，**如果 `Deserializer` 回傳的物件仍指向那塊 buffer，buffer 回池之後使用者手上的內容會被下一個回應靜默覆寫**。

- **`ByteArrayDeserializer`（預設）**：在交付時把 value 複製成 `byte[]`，所以 buffer 回收是安全的。上表 1,502 MB/s 這一列就是這個路徑。
- **`ByteBufferDeserializer`（KIP-863）**：回傳的是指向 buffer 的 slice。使用者只要在 `poll()` 回傳之後還持有那個 `ByteBuffer`，就可能讀到別的 record 的位元組。這是 **use-after-free 等級的問題**，而且不會有例外，只會讀到錯的資料。上表 1,770 MB/s 這一列建立在這個路徑上，**所以那個數字目前不能無條件引用**。

三個可能的出路，我傾向第二個：

1. **只對複製型 deserializer 啟用池化**。乾淨但無法偵測使用者自訂的零複製 deserializer，等於把安全性交給使用者。
2. **在 KIP 明文定義生命週期**：`ConsumerRecord` 的 key/value 若由零複製 deserializer 產生，只保證在下一次 `poll()` 之前有效。這是公開語意變更，需要 KIP 投票，但它把契約講清楚，而且與其他系統的 zero-copy API 慣例一致。
3. **不回收，只做額度型的池**（配置了就不重用，只用來限制總量）。這樣省掉的只有「總量無上限」，省不到 memset 與複製，L3 的大部分收益就沒了。前一條線曾經得到「接收 buffer 重用不安全、只能做額度型池」的結論，正是因為沒有解決這個契約問題。

**請你決定走哪一條**，這會決定 L3 要不要拆成獨立的 KIP。

### 5a. 已落地：接收 buffer 重用（heap，只對複製型 deserializer）

KIP 現在包含這一項。它走的是 §5 的第一條路，而且只做 heap。數字在 03 §7：6p×1KB 最終版本對 base 吞吐 +49%、CPU/GB −15%。

省掉的是背景執行緒每個回應新配 buffer 的配置與 memset（03 §5 的 19%）。direct 重用還能再省掉 JDK 暫存 direct buffer 的那次複製（14%），但原型裡它沒有吞吐優勢，還多了 native 記憶體與 `Cleaner` 的風險。所以不做 §6 原本寫的 off-heap 池。

設計決定（不新增 config）：

1. **新的 `CachingMemoryPool`**（`org.apache.kafka.common.memory`）：只用 heap；只快取 ≥ 1 MiB 的已釋放 buffer；上限 `2 × fetch.max.bytes`（每個 node 一份已 buffer、一份在飛）。它從不阻塞。漏掉一次釋放只是少一次重用，buffer 會被 GC 收走。
2. **釋放點**：`NetworkClient` 把 `NetworkReceive` 的釋放交給 `ClientResponse.releaseBuffer()`。`AbstractFetch` 再把它交給 `FetchMetricsAggregator`，後者本來就知道一個 fetch 回應的所有 partition 何時都被取完。取完一定發生在 app 執行緒，而且 record 已經複製出來。
3. **安全閘**（`ConsumerUtils.receiveMemoryPool`）：key 與 value deserializer 都是內建的複製型（`ByteArray`、`Bytes`、`String`、`Integer`、`Long`、`Short`、`Double`、`Float`、`Boolean`、`UUID`、`Void`）才重用。`ByteBufferDeserializer` 會把 buffer 直接交出去，自訂 deserializer 可能留住它，所以兩者都維持 `MemoryPool.NONE`。
4. **`CompletedFetch` 不再讓任何東西引用接收 buffer**：header 的 key/value 改成立即解碼（`RecordHeader` 原本是 lazy），`RecordDeserializationException` 裡的 key/value 改成複製。
5. **範圍**：producer、admin、share consumer 維持 `MemoryPool.NONE`。share consumer 沒有釋放的管線。

**未來工作**：

- 擴充 `Deserializer` API，讓自訂 deserializer 可以宣告「我會複製輸入」，它們也就能用上重用。這是公開介面變更，另走 KIP。
- 03 §5 裡 app 執行緒的項目（例如每批快取 leader epoch 的 `Optional`）還沒量。

---

## 6. 公開介面影響

**不新增設定。** credit 是 `1 × fetch.max.bytes`（§4.5a），接收 buffer 池的上限是 `2 × fetch.max.bytes`（§5a）。兩者都從既有設定推導。理由是你定過的規則：參數越少越好，不要把內部複雜度暴露給使用者。如果 reviewer 要求可調，我的立場是先給固定倍數，證明有人真的需要調再說。

**記憶體行為（落地版本，跟原本的 off-heap 計畫不同）**：
- 接收 buffer 仍在 heap 上，`-Xmx` 仍然框住整個 consumer 的記憶體。差別只在於已釋放的大 buffer 會被留下來重用，總量上限 `2 × fetch.max.bytes`。
- 池從不阻塞、從不回 null。快取裡沒有合用的 buffer 就照舊配一塊新的。所以沒有新的 backpressure 行為，也沒有「池滿」這種狀態要對使用者解釋。
- 對使用者可見的差異只有：steady state 下 heap 裡多常駐最多 `2 × fetch.max.bytes` 的 byte[]，換來背景執行緒少 25% 的 CPU。

**新增 metrics**：目前沒有。原本設想的池使用量與耗盡次數在落地版本裡沒有意義，因為池不會耗盡。預抓深度是否要暴露，留到 reviewer 討論。

**`Deserializer` 契約**：不變。重用只在 §5a 的安全閘成立時啟用，任何現有 deserializer 的行為都不受影響。讓自訂 deserializer 宣告「我會複製輸入」是後續另一個 KIP。

---

## 7. 相容性與遷移

- 公開 API、wire protocol、既有設定全部不變。
- 三種 consumer 共用 `AbstractFetch` / `FetchBuffer` / `FetchCollector`（share consumer 也用），所以任何改動的半徑是三者。**L1 我打算只改 regular consumer 的續發時機**，share consumer 維持原樣，第一階段不動共用類別的語意。這一點需要在設計上明確切開，否則會像我在另一條線上那樣意外打斷 share consumer 的心跳。
- 每個階段都能單獨 revert，且不依賴後續階段補正確性。

---

## 8. 測試計畫

**正確性（每個階段都必須全綠才進下一階段）**
- `clients-integration-tests` 的 12 個 consumer 類別（嵌入式 KRaft）。
- ducktape `consumer_test.py` 的 `group_protocol=consumer` 全部 case。
- share consumer 與 Streams 的既有套件（因為共用類別）。

**L1 專屬**
- seek / reset / 撤銷指派之後，buffered 與 in-flight 的舊資料必須作廢（fencing 回歸測試）。
- 空回應不得觸發跨執行緒喚醒（KAFKA-20854 的形狀）。
- 已知會被改變期待的既有測試：`testFetchHonours(FetchSize|MaxPartitionFetchBytes)IfLargeRecordNotFirst` 期待第一次 `poll()` 只拿到小的那筆；有了預抓之後兩個回應都已到達，會一次交付兩筆。**這是刻意的行為差異，要在 KIP 裡寫明並修改測試**，不是 bug。

**L3 專屬**
- 把池容量壓到極小（`fetch.max.bytes=10 KiB`）跑整套整合測試：任何漏釋放都會在幾個回應內停住。我在另一條線就是這樣抓到「非 fetch 回應的 buffer 從未回池」。
- 單核 `taskset -c 0` 加 CRC 檢查：交接競態在四核上跑一天也碰不到，單核幾分鐘就重現。
- 「路徑 × 釋放者」矩陣：每一條退出路徑（正常交付、例外、pause、撤銷、close）都要指名誰釋放。

**效能（同一把尺）**
同 base、同機器獨佔、同 JDK、fetch 路徑外的差異為零、交錯執行 ≥5 輪取中位數並報離散度，先跑 A/A 建立包絡。工作負載：6p×1KB 穩態、1p×100B、`max.poll.records=50`、閒置長視窗、以及 seek 密集的情境。

---

## 9. 被拒絕的替代方案

- **系統屬性開關預抓深度**（前一條線的實驗 hack）：繞過設定系統、無文件、無 fencing，seek 之後會停滯。正規做法是 §4 的 credit。
- **放棄 fetch session 改用全量 fetch**：實驗 hack 這樣做過，broker 端每次全量，成本轉給 broker。保留 incremental session。
- **把 per-record 工作搬到背景執行緒**：前一條線量過，broker/storage 側先飽和，而且要求 `Deserializer` thread-safe。不做。
- **每個 partition 一條執行緒**：交接成本與 partition 數成正比。
- **固定預抓深度**：見 §4.2。

---

## 10. 分階段落地

| 階段 | 內容 | 驗收 | 需要 KIP |
|---|---|---|---|
| 0 | 這份設計文件 + 在 trunk 上重現 §2 的三個成本（profile 佐證） | profile 數字可重現 | — |
| 1 | **L1**：回應驅動續發 + credit + fencing | 整合測試與 ducktape 全綠；1p100B 與 6p1KB 有可重現增益；閒置不退步 | 建議在 KIP 說明行為差異 |
| 2 | **L2**：per-batch 物件共用 | 小 record 場景 CPU/GB 下降；無行為變更 | 否 |
| 3 | **L3**：池化 direct buffer + 原地交付 | §8 的 L3 專屬測試全綠；記憶體上限可驗證 | **是**（§5、§6） |

每一階段都是可以單獨送審的 PR。

---

## 11. 待你拍板的問題

1. ~~**§5 的三條路要走哪一條？**~~ 已決定：走第一條，heap 重用加安全閘（§5a）。L3 不拆成獨立 KIP；自訂 deserializer 的 opt-in API 才是另一個 KIP。
2. **KIP 的範圍**：一個 KIP 涵蓋 L1+L2+L3，還是 L1+L2 走 JIRA、L3 單獨一個 KIP？我傾向後者，因為只有 L3 動到公開語意。
3. ~~**要不要在第一階段就把 share consumer 一起改？**~~ 已決定：不改。share consumer 維持原本的 fetch 時機與 `MemoryPool.NONE`。
4. **metrics 要暴露到什麼程度？** 落地版本沒有池耗盡與 admission 拒絕這兩種事件（§6），所以目前一個都沒加。要不要暴露預抓深度，留給 reviewer。
5. **效能的驗收門檻怎麼定？** 我建議用「同機器 A/A 包絡之外才算訊號」而不是固定百分比，因為前一條線在這件事上吃過苦頭。
