# BUFFER-REUSE-03：接收 buffer 重用的現況

日期：2026-11-20。工作線：consumer 的 fetch response 重用 receive buffer（從 `kip-fetch-pipeline` 拆出的第二條）。

- Worktree：`/Users/unknowntpo/repo/unknowntpo/kafka/consumer-receive-buffer-reuse`
- 分支：`consumer-receive-buffer-reuse`，基準 upstream trunk `5b648ca9cb`。只在本機，沒有 push，沒有設 upstream。
- 英文 issue 草稿（未送出）：`/Users/unknowntpo/repo/unknowntpo/kafka/consumer-receive-buffer-reuse/BUFFER-REUSE-02-issue-draft.md`
- 量測的原始數據與腳本：`/Users/unknowntpo/repo/unknowntpo/kafka/consumer-receive-buffer-reuse/BUFFER-REUSE-01-results-local/`

## 1. 分支裡有什麼

來源是 `git -C /Users/unknowntpo/repo/unknowntpo/kafka/kip-fetch-pipeline diff e90d6f42c2 HEAD`，只取 buffer 重用的部分，手動套用。

| 檔案 | 改動 |
|---|---|
| `common/memory/CachingMemoryPool.java`（新） | 保留至少 1 MiB 的 buffer 重用，上限是建構時給的 bytes |
| `common/network/NetworkReceive.java` | `close()` 不再宣告 `IOException` |
| `clients/ClientResponse.java`、`clients/NetworkClient.java` | `releaseBuffer()` |
| `clients/ClientUtils.java`、`NetworkClientDelegate.java`、`ConsumerUtils.java` | 把 `MemoryPool` 傳進 `Selector`；`receiveMemoryPool`、`COPYING_DESERIALIZERS` |
| `FetchMetricsAggregator.java` | `onComplete` hook。類別維持 package-private |
| `AbstractFetch.java` | 一行：把 `resp::releaseBuffer` 接到 `onComplete` |
| `CompletedFetch.java` | `parseRecord` 先讀出 header 的 key 與 value；`RecordDeserializationException` 拿到複製的 key 與 value |
| `FetchCollector.java` | 第 3 節的修正（這不是從 `kip-fetch-pipeline` 來的） |
| `ClassicKafkaConsumer`、`AsyncKafkaConsumer` | 傳 `ConsumerUtils.receiveMemoryPool(...)` |
| `ShareConsumerImpl`、`KafkaProducer`、`KafkaAdminClient` | 傳 `MemoryPool.NONE` |

沒有帶入：`FetchCursors`、每個 broker 的額度、position generation、`DefaultRecord` / `Deserializers` / `RecordHeaders` 的改動、`CompletedFetch` 與 `FetchMetricsAggregator` 改成 public。

## 2. 對另外兩條工作線的依賴

沒有。只有這一條時，全 repo 的 `compileJava compileTestJava compileScala compileTestScala` 通過。

合併時會衝突的檔案：`CompletedFetch.java`（三條都改 `parseRecord` 附近）、`AbstractFetch.java`（建立 `FetchMetricsAggregator` 的那一行）、`FetchCollector.java`（`initialize()`）、`FetchMetricsAggregator.java`（建構子；第三條要把它改成 public）。

trunk 在 `e90d6f42c2` 之後多了兩個測試輔助類別，它們呼叫被改了參數的 method，所以這個分支多改了兩個檔案：`ClientInstanceIdCapture.java`、`NetworkClientDelegateInstanceIdCapture.java`。

## 3. 已知問題的修正

問題：`FetchCollector.initialize()` 丟出例外時，`finally` 仍然向 `FetchMetricsAggregator` 回報這個 partition。如果那個 `CompletedFetch` 有資料，它留在 `FetchBuffer` 裡，下一次 `poll()` 會再讀它。同一個 response 的其他 partition 拿完後，buffer 就被釋放並可能被重用。

修正：`initialize()` 只在回傳 null（丟掉這個 `CompletedFetch`）的路徑回報；丟出任何例外或 `Error` 時都不回報。`collectFetch` 在把那個 `CompletedFetch` 從 `FetchBuffer` 移除的同一處回報。留在 `FetchBuffer` 裡的 `CompletedFetch` 之後會經由 `initialize()` 回傳 null、或 `drain()`（`retainAll`、`close`）回報。

測試：`FetchCollectorTest.testFetchResponseIsNotCompleteWhileAFailedFetchIsStillBuffered`。修正前失敗（`expected: <0> but was: <1>`），修正後通過。`FetchCollectorTest.testFetchResponseIsCompleteWhenAFailedFetchIsRemoved` 在修正前就通過，它保護的是「移除時要回報」：拿掉 `collectFetch` 裡的回報，它會失敗（`expected: <1> but was: <0>`，已實際執行）。

附帶的行為改變：

- trunk 上，這種 `CompletedFetch` 每次 `poll()` 失敗都會再回報一次。如果同一個 response 的其他 partition 都已經回報過，`FetchMetricsAggregator` 每次都會把整個 response 的 `bytes-fetched`、`records-fetched` 再記一次。修正後不會。這一點是讀程式碼得到的，沒有用測試驗證。
- 同一個 response 其他 partition 的 metrics，現在等到失敗的 `CompletedFetch` 離開 `FetchBuffer` 才記錄。

## 4. 測試

最後一次完整執行（commit `f60c958471` 加上未 commit 的測試）：32 個測試類別、1199 個測試、0 失敗。補上 `Error` 的測試後重跑其中 13 個類別（682 個測試，0 失敗）；`checkstyleMain`、`checkstyleTest`、`checkstyleTestFixtures`、`spotlessCheck` 通過。包含：`CachingMemoryPoolTest`、`NetworkClientTest`、`FetchCollectorTest`、`CompletedFetchTest`、`FetcherTest`、`FetchRequestManagerTest`、`ConsumerUtilsTest`、`FetchMetricsAggregatorTest`、`KafkaConsumerTest`、`AsyncKafkaConsumerTest`。

沒有跑：整個 `clients:test`、其他模組的測試、integration test、system test。

## 5. 量測

條件與完整的表在 issue 草稿的 Measurement 一節。摘要（本機，Apple M1 Pro，一個 broker 與 client 同機，trunk `5b648ca9cb`，`kafka-consumer-perf-test`，不讀 value，heap 2 GB，都是量到的）：

| 情境 | 每消費 1 MB 的配置量 | CPU 秒／GB | MB/s |
|---|---|---|---|
| 1 KB、`consumer` protocol、5 輪 | 2.38 → 1.20 | 1.97 → 1.86 | 272.5 → 298.8 |
| 1 KB、`classic` protocol、3 輪 | 2.37 → 1.34 | 2.09 → 1.92 | 224.0 → 262.7 |
| 100 B、`consumer` protocol、3 輪 | 5.20 → 3.76 | 5.44 → 4.29（不可靠，見下） | 各輪差太多（48 到 161），不比較 |

單位：MB 是 1024 × 1024 bytes（工具輸出與 GC log 都是）。100 B 那三輪的吞吐量差太多，所以同一批的 CPU 數字也不可靠；只有配置量可信。

這些數字不能和 GCP 上整個 `kip-fetch-pipeline` 的 1.6–1.7 倍相比：機器、broker 數、網路都不同。

## 6. 獨立 review 的結果

做了兩次獨立 review（兩個不同的 agent，都只讀程式碼）。兩次都沒有在一般路徑找到「buffer 被重用時仍有人讀」或不安全的重複釋放。

依第一次的意見已修：四處和程式不符的註解；補上 `AbstractFetch` 接線的測試（`FetchRequestManagerTest.testResponseBufferIsReleasedOnceTheFetchIsDrained`；拿掉 `resp::releaseBuffer` 它會失敗，已實際執行）。

依第二次的意見已修（commit `f60c958471`）：

- `initialize()` 丟出 `Error` 時仍會回報，buffer 可能過早釋放。現在只有回傳 null 的路徑回報。
- 提前讀出 header 的 key 與 value 原本對所有 consumer 都做。現在只在會重用 buffer 時做（`ConsumerUtils.copiesInput`），其他 consumer 維持延遲解碼。
- `receiveMemoryPool` 的 javadoc 寫「每個 node」是錯的，上限是整個 consumer 的。

第三次 review 只看 commit `f60c958471`，沒有找到正確性的問題。它指出兩點：

- `initialize()` 丟出 `Error` 的路徑沒有測試。已補 `FetchCollectorTest.testFetchResponseIsNotCompleteWhenInitializeThrowsAnError`；把 `recordMetrics` 的初值改回 `true`，它會失敗（`expected: <0> but was: <1>`，已實際執行）。
- `ConsumerUtils.copiesInput` 對每筆有 header 的 record 都算一次，結果其實整個 consumer 都一樣。沒有改：只影響有 header 的 record，量測裡看不到。

補測試之後沒有再做一次獨立 review，因為 production 程式碼沒有變。

沒有修、需要決定的事：

1. **快取不會縮小。** consumer 收過大的 response 之後，最多保留 `2 × fetch.max.bytes`（預設 100 MiB）直到關閉。trunk 上這些記憶體由 GC 回收。Kafka Streams、Connect、MirrorMaker 都用 `ByteArrayDeserializer`，所以每個 consumer 都會啟用。
2. **response 大小變動時命中率下降。** 新 buffer 的大小剛好等於 response；快取裡太小的 buffer 直接丟掉。命中率沒有量過。
3. **重用的路徑沒有端到端的測試。** 單元測試用 `MockClient`，不會從 `CachingMemoryPool` 配置 buffer。把 consumer 傳給 `Selector` 的 pool 換成 `MemoryPool.NONE`，不會有任何測試失敗。沒有測試在重用發生後檢查 record 的內容；量測只證明有重用（JFR），沒有檢查資料。要不要在這份交付裡加一個 integration test，需要決定。
4. **非 fetch 的 response 會拿走快取裡的 buffer。** 例如 1 MiB 以上的 Metadata response 會拿到快取的大 buffer，而且不會歸還。後果是少一次重用。
5. **暫停的 partition 會讓整塊 buffer 不歸還**，直到 resume、revoke 或 seek。後果是少一次重用；heap 佔用和 trunk 相同。出處：`/Users/unknowntpo/repo/unknowntpo/kafka/consumer-resilience/RESILIENCE-03-fetch-buffer-release-point.md`。

## 7. 還沒做

- 共用機器（morefine、GCP）上的量測。需要 PM 排程與使用者同意。
- 第 6 節的三件事。
