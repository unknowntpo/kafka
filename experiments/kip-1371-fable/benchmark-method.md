# Consumer benchmark method: what we got wrong and the rules that follow

2026-09-10。這份文件記錄本輪（KIP-1371 event-loop semantics）量測過程中犯的錯與對應的規則。它不是 KIP 的一部分；KIP §8 只放結論與數字。之後任何人要在這條線上量 consumer，先讀這份。

適用範圍：Kafka consumer 的 client 端量測，包含 loop-level（`MockClient` 驅動）與 end-to-end（真 broker）。

## 1. 一句話結論

JMH 的 warmup 只暖被測 JVM 內部。**任何活在 JVM 外面的暖機狀態——OS page cache、broker 行程、連線——JMH 都管不到，而且它們通常只會單向變熱**，所以會在一個 session 內產生單調漂移，讓交錯設計失效。

## 2. 本輪實際發生的事

### 2.1 end-to-end：漂移蓋過訊號

設定：`AsyncConsumerBrokerBenchmark`，2,000,000 × 128 B（約 256 MB）單 partition，broker 與 consumer 同機，`-f 1 -wi 5 -w 2s -i 5 -r 5s`，交錯 T,B,B,T，consume 與 unavailable 各 2 對、idle 1 對。

結果：**同一個 variant 內部，consume 吞吐跨 run 從 1.22M 爬到 4.21M records/s**。兩個 variant 之間的差是 +3.0%，比漂移小一個數量級。

原因：

- topic 資料 256 MB，第一次讀來自磁碟，之後來自 page cache。page cache 跨 fork 共用，JMH 每個 fork 的 warmup 無法重設也無法預熱它。
- broker 是獨立行程，它自己的 JIT 也在暖，同樣跨所有 run 共用。

後果：「consume 與 idle 在雜訊內」這個結論仍然成立，但它只證明「沒有大到蓋過漂移的回歸」。**不能**宣稱「回歸不超過 X%」，因為偵測下限被撐得很寬。

### 2.2 loop-level：沒有這個問題

`ConsumerNetworkThreadPassBenchmark` 用 `MockClient` 加 `MockTime`，沒有磁碟、沒有 broker、沒有真實時間，JMH 的 warmup 就足夠。這組的數字比 e2e 可信得多，也是本輪唯一支撐行為改變的證據（`BLOCKED_HEARTBEAT_INFLIGHT` 的 zero-wait pass 從 0.968/pass 降到 0）。

**規則**：能用 loop-level 回答的問題，不要用 e2e 回答。e2e 的用途是抓「loop-level 模型漏掉的東西」，不是量細微差異。

### 2.3 benchmark 抓到我們自己的回歸（正面案例）

C2（operation semantics）的過期掃描原本每輪都 `new LinkedList<>(unsentOffsetCommits)` 加一條 stream，即使 queue 是空的。coordinator unknown 時這段每個 pass 都跑，於是 `BLOCKED` 與 `RECOVERY` 慢 29%、配置量加倍。加 `isEmpty()` 守衛後回到與 trunk 齊平。

**規則**：在每個 pass 都會執行的路徑上新增程式碼，必須有一個對應的 loop-level 情境，而且要看 `gc.alloc.rate.norm`，不是只看時間。時間會被雜訊蓋掉，配置量不會。

### 2.4 e2e 的 `unavailable` 模式根本沒測到我們修的東西

harness 把 bootstrap 指向一個**關閉**的 port，連線立刻被拒絕，client 停在 reconnect backoff——這條路徑 trunk 本來就正確。我們修的 busy loop 需要的是「接受連線但永不回應 heartbeat」的 peer。

**規則**：情境的名字不等於情境的內容。每個 benchmark 情境要能說出「它讓程式走到哪一段」，並且用 counter 證明（例如 zero-wait pass 數）。說不出來就是還沒設計好。

補充（重跑後才學到）：光是「接受連線但不回應」也不夠。silent peer 讓連線停在 `CHECKING_API_VERSIONS`，此狀態下 `isReady(node)` 為 false，任何 request 都送不出去，`FindCoordinator` 送不出 → coordinator 永遠 unknown → `AbstractHeartbeatRequestManager.poll` 在第一個 guard 就回 `EMPTY`，根本不會產生 heartbeat。要走到 heartbeat in-flight，需要一個讓 `ApiVersions`、`Metadata`、`FindCoordinator` 正常通過、只吞掉 `ConsumerGroupHeartbeat` 的 peer。Kafka 的 wire format 是「4 bytes 長度 + message，message 前 2 bytes 是 api key」，所以一個 byte-level proxy 就夠，不需要假 broker。

### 2.6 主指標選錯會看不見 busy loop

`heartbeat-blackhole` 模式下，trunk 與 branch 的 polls/s 幾乎一樣（10.0 vs 9.3），但 CPU 差 71 倍（1,699 vs 24 cpuMs/s）、每次 poll 的配置量差約 9,000 倍（154 MB vs 17 kB）。原因是 busy loop 發生在 `poll()` **內部**，不會改變 `poll()` 被呼叫的次數。

**規則**：量 busy loop 類問題時，主指標是 CPU 與配置量，不是吞吐或呼叫次數。

### 2.5 清理腳本殺掉了別人的 broker

`kafka-server-stop` 是用 pattern 比對殺掉所有 `kafka.Kafka` 行程，連帶停掉了另一個 session 的 broker。

**規則**：只殺自己記下 pid 的行程。啟動時把 pid 寫進自己的 run 目錄，清理時用那個 pid，不要用任何「按名字殺」的腳本。

## 3. 規則清單

### 3.1 設計階段

1. **先問這個問題需不需要真 broker**。需要磁碟、需要真實網路、需要 broker 行為時才用 e2e；其餘用 loop-level。
2. **每個情境要有 counter 證明它走到目標路徑**。例如 zero-wait pass 數、送出的 request 數、收到的 response 數、記錄數。沒有 counter 的情境不能拿來下結論。
3. **決定 dataset 與 page cache 的關係**。要嘛小到必定全部命中（並事先預熱），要嘛大到必定不命中；不要停在中間，那是漂移的來源。

### 3.2 執行階段

4. **量測前先做 session 級預熱**：用一個丟棄的程序把整個 dataset 讀幾輪，讓 page cache 與 broker JVM 在第一個正式 run 之前就到穩態。JMH 的 `-wi` 不能取代這一步。
5. **先跑 A/A**：用完全相同的 binary 跑一輪，量出這台機器在這個 workload 下的雜訊包絡。任何小於這個包絡的 A/B 差異都不能宣稱。這一步本輪沒做，是最大的疏漏。
6. **至少 5 對交錯**，順序 T,B,B,T,T,B,…。2 對面對單調漂移太弱。
7. **記錄環境**：JDK 版本與 build、機器、其他在跑的行程、broker 版本與設定、jar 的 SHA、兩邊的 source commit。共用機器要註明。
8. **自己的 pid 自己管**（見 2.5）。

### 3.3 報告階段

9. **報逐對比值與中位數，不要報把所有 iteration 併起來的平均**。交錯設計的好處在配對，彙總平均會把它丟掉。本輪 e2e 的摘要犯了這個錯。
10. **同時報時間與配置量**。配置量對雜訊不敏感，常常是唯一看得出差異的指標。
11. **把偵測下限寫出來**：「漂移 ±N%，所以本輪只能偵測大於 N% 的回歸」。不要讓「在雜訊內」被讀成「沒有回歸」。
12. **不宣稱沒量到的東西**。本輪的 1.6 core 數字來自另一份 harness 的另一種 unavailable 設定，不是我們這個分支量到的，KIP 裡有明說。

## 4. 這份分支上待補的事

| 項目 | 為什麼 | 成本 | 狀態 |
|---|---|---|---|
| e2e 依 3.2 重跑（預熱、A/A、5 對、逐對比值） | 目前的 e2e 只能排除大回歸，說不出偵測下限 | 約 30–40 分鐘機器時間 | 2026-09-10 完成。A/A 包絡 ±16%（吞吐）／±0.1%（配置量），consume 逐對 median 0.897 在包絡內。 |
| `unavailable` 模式改成「接受連線但不回應」的 listener | 現在測不到本分支修的 busy loop（見 2.4） | 小，改 harness 加一個 TCP listener | 2026-09-10 完成，但**光是「接受連線但不回應」還不夠**：silent peer 卡在 `CHECKING_API_VERSIONS`，走不到 heartbeat。真正走到的是新的 `heartbeat-blackhole` 模式——在 broker listener 前面擺一個吞掉 `ConsumerGroupHeartbeat` 的 byte-level proxy。trunk 1.7 核心 / 154 MB per poll，branch 24 cpuMs/s / 17 kB per poll。 |
| ducktape `consumer_test.py` 等系統測試 | 尚未跑 | 需要 Jenkins 或本機 docker | 未做 |

## 5. 本輪的原始資料

- 2026-09-10 依本文件 §3 重跑的 e2e：`e2e-ab2/summary.md`（含 A/A 包絡、逐對比值、偵測下限），原始 JSON 同目錄
- loop-level：`jmh-ab/results/`（第一版）與 `jmh-ab/results2/`（修掉 2.3 的回歸後重跑），`jmh-ab/results/summary.md`
- end-to-end：`e2e-ab/`
- 前一條線（NextPollCondition 審查）的真 broker harness 與數字：`../next-poll-condition/fable-review/bench/`
