# 真實三個 manager：等價檢查與 JMH

**結果：200 個生命週期步驟與 3,000 個 benchmark 步驟輸出一致；等待路徑每輪配置量由約 152–184 B 降至近 0 B。這批 CPU 耗時波動太大，未證明穩定加速，BUSY 也沒有一致勝出。**

## 比較什麼

固定 Kafka trunk commit `820533b870106cc0e0ac60e2076b8644d68bd85f` 作掃描 baseline。從該 commit 編譯 CoordinatorRequestManager、AbstractHeartbeatRequestManager、ConsumerHeartbeatRequestManager、AbstractMembershipManager；network loop 使用既有同 commit baseline 編譯產物。EVENT 使用前幾輪隔離 event-only 候選與真實 membership 接線。

兩組都使用 coordinator、heartbeat、membership 三個真實 manager，相同 fixture、事件佇列、metrics、request payload、非阻塞 transport 與相容性邊界。只有這三個 managers 登記排程，不能代表完整 consumer。掃描版保留原 maximumTimeToWait 掃描；事件版沿用實驗 loop 移除它的版本。

Fixture 的 manager entries 集合在建立時固定，避免每次讀 entries 都產生新的 list，讓掃描版承擔不必要的額外成本。RebalanceMetrics 使用真實實作。Metadata/commit/offsets/fetch 替身僅用於 setup 和正確性情境；3,000 步驟檢查及 JMH teardown 都驗證計時 pass 沒有呼叫這些 Mockito 邊界。

## 等價檢查

九種情境共 200 步：IDLE、TIMER、FENCED、FATAL、STALE、ASSIGNMENT、METADATA、UNSUBSCRIBE、CLOSE。

每一步比對：
- 全部已送出 request 的型別、完整 data 與順序。
- membership state、member epoch、已套用 partitions。
- background event 型別與順序。

只將每次建立 client 隨機產生的 member ID 正規化為同一標記；assignment 的 topic UUID 固定。步數也必須一致，不會忽略額外輸出。

第一輪在 metadata 情境有一個差異：fixture 先修改 topicNames，下一個 network callback 才通知，讓掃描版早一輪看到 metadata。修正為在同一個 transport completion 內更新資料並交付通知；修正後 200 步全部一致。原始差異紀錄保留，不是丟棄慢樣本或放寬比對。

另外以 benchmark 使用的三種 workload 各跑 1,000 步，核對實際 request data、順序、累計 request 數和 membership state，一共 3,000 步完全一致，才開放 JMH。

此等價是固定輸入 trace 的 request/state 等價，不是完整形式證明。沒有要求兩版的 loop wait timeout 相同，也未驗證真實 OS 阻塞／喚醒或所有 deadlines 的外部可觀察行為；時钟由測試驅動。

## JMH workload 定義

- **DORMANT**：已入群、穩定、沒有 in-flight heartbeat，下一次 interval 尚未到。
- **INFLIGHT**：已入群、穩定、有 heartbeat 等待回覆。
- **BUSY**：heartbeat interval 設為 0，持續交付成功回覆並產生下一個 request。這個 transport pipeline 約每兩輪完成／產生一個 heartbeat，並不是三個 managers 每輪都有新輸入。

計時時 logical clock 固定，避免混入 max.poll.interval 或真實等待時間。這測量排程與 RPC 完成處理的成本，不是 broker 吞吐、timer latency 或端到端 consumer latency。

每次 benchmark invocation 執行 256 passes；JMH 正規化為每 pass。每組 2 forks、3 次 300 ms warmup、5 次 300 ms measurement、單 worker、128 MB heap、GC profiler。A1/A2 都是同一掃描版，九組測試用固定 seed 交錯排列。Mockito Java agent 僅支援 setup final-class 邊界替身，兩版使用相同 agent。

## 完整結果

ns/pass 後的 ± 是 JMH 報告的 99.9% confidence interval half-width；低於零的推算下限沒有物理意義，反映短測量的高變異。

| Workload | 組別 | ns/pass ± JMH error | B/pass |
|---|---|---:|---:|
| DORMANT | A1 | 336.91 ± 212.71 | 184.04 |
| DORMANT | EVENT | 164.69 ± 61.95 | 0.02 |
| DORMANT | A2 | 262.28 ± 166.67 | 184.05 |
| INFLIGHT | A1 | 1063.67 ± 1428.54 | 152.08 |
| INFLIGHT | EVENT | 349.68 ± 374.52 | 0.04 |
| INFLIGHT | A2 | 398.30 ± 307.25 | 184.06 |
| BUSY | A1 | 681.64 ± 517.78 | 448.09 |
| BUSY | EVENT | 427.95 ± 130.54 | 444.06 |
| BUSY | A2 | 384.22 ± 203.86 | 432.04 |

## 可以得出的結論

1. **等待時減少 allocation 有證據。** DORMANT 掃描版約 184 B/pass，INFLIGHT 約 152–184 B/pass；事件版兩者近 0 B/pass。微小非零值包含量測／攤提成本，不能稱為整個 consumer 零配置，也不能推論 retained heap 或 RSS 同幅下降。
2. **CPU 加速未獲證實。** A/A 對照本身差異很大，原始 iteration 存在顯著尖峰，誤差範圍重疊。BUSY 事件版約 428 ns/pass，兩次掃描版約 682 與 384 ns/pass，無一致贏面。所有樣本都保留。
3. **BUSY allocation 沒有明顯優勢。** 事件版約 444 B/pass，掃描版約 432–448 B/pass。
4. **不能宣稱本方案普遍更快或最快可達多少。** 目前支持的是：保留 manager 邏輯、改成事件排程，在這些 trace 保持輸出，並減少等待時的配置與空轉工作。

## 下一個判斷

如果 KIP 要主張明顯 CPU 效益，下一步應先取得較安靜、可控的量測環境，延長 warmup／measurement，重做同樣的 A/A 與候選比較，並量 timer／稀疏事件延遲。不要把這批最大倍數寫成效能結論，也不要在 CPU 改善未確認前繼續疊加優化。

目前仍未覆蓋完整 AsyncKafkaConsumer 的 application 阻塞、所有 manager migration、commit/fetch 真實 RPC、auto-commit 與 callback failure 等情境；原 production source 與 Kafka Git 歷史未改動。

## 重跑與證據

在 next-poll-condition-trunk 中，保留先前 isolated harness 產物：

```sh
/opt/homebrew/bin/python3 experiments/next-poll-condition/event-only/lifecycle/validate.py
/opt/homebrew/bin/python3 experiments/next-poll-condition/event-only/lifecycle/benchmark.py
```

benchmark runner 檢查等價 gate 與主要 fixture source hashes，記錄每組完整 command，並核對 JMH pattern。證據包包含原始 JSON、iteration log、命令、baseline/candidate trace、第一次 metadata 時序差異、fixture/runner source 與 SHA-256。依賴既有 cached jars，不是獨立可攜建置包。

完整 Gradle／Checkstyle 未執行；git diff --check 通過。沒有啟動 broker 或跨語言 throughput 比較。
