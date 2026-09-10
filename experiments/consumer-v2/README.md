# Consumer v2 資料契約原型

此階段是單一 background owner 的 partition prefetch window，尚未接到雙佇列、NetworkClient 或 broker，不是新 consumer。架構與驗收規格在本 task outputs/consumer-v2-architecture.md。

## 本次實作

PrefetchWindow 分開預抓 cursor 與 app decode position；一個 in-flight request；byte admission 與 lease slot 雙限制。回覆超過 budget 仍被計帳，停止後續 fetch；預算不是 RSS 硬上限。seek 失效舊 generation，但保留未 release bytes 與舊 in-flight request。close 禁止新工作，晚到資料丟棄，仍可 release 舊 lease。release 回傳是否從不可發轉為可發，未來由 event loop 入 ready queue。

完成方法呼叫者必須交付合法成功的完整 partition records，負責 protocol error、retry/backoff、leader epoch 與 response buffer 所有權。它不是 AbstractFetch/FetchCollector 的替代。空回覆不留 lease；下一次網路請求不能以本地自迴圈模擬完成。

lease 的 MemoryRecords 必須保持 immutable，app 最後一次使用後先丟棄引用，再將 release 送回 owner。此程式只計帳，未實作回收池。此層按單 partition records size 計算；共享多 partition response 的 backing buffer 與全局預算尚未接入。

## 驗證

- 收到回覆後，app 尚未消費即可取得下一個 request offset。
- 使用固定 trunk CompletedFetch 方法本體解碼，部分消費不釋放整包 bytes。
- READ_COMMITTED：aborted transactional batch 與 abort marker 不交付；合法 offset gap 被保留，耗盡後 decode cursor 與 speculative cursor 一致。
- seek fence、舊 in-flight 回覆、重複 release/completion、失敗、空回覆、超大回覆、slot cap、close 後回覆。
- 10,000 次 close/release 循環，容量歸零，不重新啟動。
- CRC 損毀不推進 cursor；初版背景提前檢查 CRC，app 仍驗證，雙重成本尚未量測。
- 刻意移除 seek generation 的反向測試，必須因接受舊 lease 失敗。

編譯 Java 11 target，執行 JDK 17 / 128 MB heap / 2 effective processors，每個程序 45 秒 timeout。未跑 Gradle/Checkstyle 或 broker benchmark。

## 依賴限制

沿用既有 membership-event-classpath.txt，包括已生成的隔離 classes 與 cached Kafka 4.3.1 jar，並非全量固定 trunk build。CompletedFetch.java 直接由本地 trunk 來源生成，只將 BufferSupplier、CloseableIterator 兩個 import 從 utils.internals 改成 utils，以相容 cached RecordBatch API；方法本體未改。這個調整存在 runner，沒有修改 production source。測試不證明完整 trunk 二進位相容。

重跑（在 next-poll-condition-trunk 根目錄）：

```
/opt/homebrew/bin/python3 experiments/consumer-v2/validate.py
```

證據包保存來源、runner、generated decoder、log/hash，依賴既有本地 classpath，並非自包含 Kafka distribution。

## 尚待整合

app-thread 立即可見的 seek fence、thread-safe lease 交接、不漏醒、跨 response transaction 邊界、partial/truncated wire response、壓縮/反序列化失敗、多 partition 共用 buffer、fetch sessions、leader truncation、rebalance/commit handshakes。現有 single-owner valid() 不是 app 可並行呼叫的 API。不能將這些單執行緒測試宣稱為跨執行緒 race 驗證。

下一個里程碑：把 lease/release 與 app fence 接入雙執行緒通道，再與 NetworkClient 的唯一 selector owner 接線。端到端吞吐、CPU/GB、memory、p99 均尚未量測；沒有 2× 結論。

## 後續進展：雙執行緒與 TCP

FetchBridge/NetworkFetchTransport 已加入；10,000 次交接、100 次 select 競爭與 1,000 次 loopback Fetch 回覆通過。完整範圍與限制見 task outputs/consumer-v2-thread-network-validation.md。原文的尚待整合清單是資料契約階段狀態，請以這份後續報告為準。

## 後續進展：完整 envelope 與 app decoder

NetworkFetchTransport 已交付完整 PartitionData，AppFetchDecoder 已接回 cached Kafka CompletedFetch。零 byte metadata lease 也參與容量與 close 契約。跨 TCP 回覆的 aborted transaction 過濾及反向測試通過。最新範圍與限制見 task outputs/consumer-v2-envelope-validation.md；先前 records-only 描述為歷史階段，不代表目前介面。

## 最新：真實 broker benchmark 已完成

已完成單 partition、手動 assign、關閉 auto-commit 的 30 個 broker 樣本。原型已使用 FetchSessionHandler。最新結果、完整限制與重現入口見 task outputs/consumer-v2-broker-benchmark.md；此為探索性資料路徑比較，不是完整新 consumer 的 2× 驗收。先前「尚無吞吐資料」描述屬歷史階段。
