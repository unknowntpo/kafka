# Async consumer background loop benchmarks

兩個 JMH benchmark，量測 `AsyncKafkaConsumer` 背景執行緒 (`ConsumerNetworkThread`) 的迴圈成本。
原始碼在 `jmh-benchmarks/src/main/java/org/apache/kafka/jmh/consumer/`。

## 1. `ConsumerNetworkThreadPassBenchmark`（單次 pass，無 broker）

用 `MockClient` + `MockTime` 接上真正的 `RequestManagers` 與 `NetworkClientDelegate`，
量測一次 `runOnce()` pass 的平均時間（ns/op，op = 一次 pass；每次 invocation 跑 1000 個 pass）。
MockTime 每個 pass 前進 1 ms。

```bash
./jmh-benchmarks/jmh.sh ConsumerNetworkThreadPassBenchmark
# 只跑特定情境、調整參數
./jmh-benchmarks/jmh.sh -p scenario=IDLE,CONSUME -p recordsPerFetch=500 ConsumerNetworkThreadPassBenchmark
```

情境 (`-p scenario=`)：

| scenario | 意義 |
|---|---|
| `IDLE` | member 已 STABLE、有一個 assigned partition、coordinator 已知、沒有 fetch 需求。pass 應該什麼都不用做；每 5000 ms（mock 時間）送一次 heartbeat 並得到回應。 |
| `BLOCKED` | FindCoordinator 永遠沒有回應。request 在 mock 時間 `request.timeout.ms` 後逾時、backoff、重送，週期循環。 |
| `BLOCKED_HEARTBEAT_INFLIGHT` | coordinator 已知，但第一個 heartbeat 永遠沒有回應（逾時循環同上；FindCoordinator 一律回應）。 |
| `CONSUME` | 每個 pass 都要求 fetch。pass N 送出的 fetch 在 pass N 結束後被回應（`recordsPerFetch` 筆），pass N+1 的 network poll 收到，pass N+1 結束後由 `FetchCollector` 清空 buffer。所以一次 fetch 佔兩個 pass；看 `fetchResponses`、`recordsCollected` 計數。 |
| `RECOVERY` | 把 coordinator 標成 unknown，等 backoff 後送出的 FindCoordinator 保持 `recoveryPasses` 個 pass 沒有回應，然後回應、下一個 pass 收到，再重來。只涵蓋 FindCoordinator 來回，coordinator 已知的那一個 pass 不會送 heartbeat。 |

AuxCounters（`Type.EVENTS`，JMH 會把各 measurement iteration 的值**加總**）：

- `passes`、`managerPolls`、`zeroWaitEmptyResults`（`timeUntilNextPollMs == 0` 且沒有 request 的 PollResult，busy-loop 訊號）、
  `zeroNetworkTimeoutPasses`、`zeroMaximumTimeToWaitPasses`、`requestsSent`、`responsesFed`、`fetchResponses`、`recordsCollected`：
  除以 `passes` 得到每個 pass 的比例。
- `minNetworkTimeoutMs`、`lastMaximumTimeToWaitMs`：非計數值，一樣被加總，除以 measurement iteration 數（預設 5）；`-1` 代表 wait forever。

## 2. `AsyncConsumerBrokerBenchmark`（端到端，真 broker）

真的 `KafkaConsumer`（`group.protocol=consumer`、auto-commit 開啟）接到一個跑中的 broker。
每次 invocation 是一次 `poll()`，主結果是 polls/s；`records` counter 是 records/s。

先準備 topic：每筆 record 的 value 前 8 bytes 是 big-endian 的序號，等於該筆的 offset
（與 `experiments/next-poll-condition/fable-review/bench/harness/Bench.java` 相同格式）。

```bash
# consume：讀完 kafka.bench.records 筆就 seekToBeginning，驗證 offset 連續與序號
./jmh-benchmarks/jmh.sh -jvmArgs "-Dkafka.bench.bootstrap=127.0.0.1:9092 -Dkafka.bench.topic=jmh-input -Dkafka.bench.records=2000000 -Dkafka.bench.mode=consume" AsyncConsumerBrokerBenchmark
# idle：坐在 log end，每次 poll 1 s
./jmh-benchmarks/jmh.sh -jvmArgs "-Dkafka.bench.mode=idle" AsyncConsumerBrokerBenchmark
# unreachable：bootstrap 指向關閉的 port（預設 127.0.0.1:1，可用 -Dkafka.bench.unreachable 改）
./jmh-benchmarks/jmh.sh -jvmArgs "-Dkafka.bench.mode=unreachable" AsyncConsumerBrokerBenchmark
# unavailable：benchmark JVM 自己開一個 ServerSocket，只 accept 不回應
./jmh-benchmarks/jmh.sh -jvmArgs "-Dkafka.bench.mode=unavailable" AsyncConsumerBrokerBenchmark
# heartbeat-blackhole：proxy 擋掉 ConsumerGroupHeartbeat，其他照轉（見下表的前置條件）
./jmh-benchmarks/jmh.sh -jvmArgs "-Dkafka.bench.mode=heartbeat-blackhole -Dkafka.bench.blackhole.port=39594 -Dkafka.bench.blackhole.target=127.0.0.1:39494" AsyncConsumerBrokerBenchmark
```

模式 (`-Dkafka.bench.mode=`)：

| mode | peer | client 停在哪 |
|---|---|---|
| `consume` | 真 broker | 正常讀取。 |
| `idle` | 真 broker | `seekToEnd` 後每次 poll 1 s。 |
| `unreachable` | 關閉的 port | 每次 connect 直接被拒絕，client 一直在 reconnect backoff，**沒有**連線建立。 |
| `unavailable` | benchmark 自己開的 `ServerSocket`，accept 後不讀不寫 | TCP 連上了，但 `ApiVersions` 永遠沒有回應，連線卡在 `CHECKING_API_VERSIONS`，直到 `request.timeout.ms`（預設 30 s）把它拆掉重連。 |
| `heartbeat-blackhole` | byte-level TCP proxy，擋在某個 broker listener 前面 | `ApiVersions`、`Metadata`、`FindCoordinator` 全部照常成功，只有 `ConsumerGroupHeartbeat` request 被 proxy 吞掉。**第一個 heartbeat 永遠 in-flight**，也就是 KAFKA-21031 busy loop 需要的狀態。 |

### 為什麼 `unavailable` 測不到 heartbeat busy loop

一個「accept 但不回應」的 peer 走不到 heartbeat。`NetworkClient.handleInitiateApiVersionRequests` 把連線標成
`CHECKING_API_VERSIONS`，這個狀態下 `isReady(node)` 是 false；`NetworkClientDelegate.doSend` 因此不送出任何
request，metadata 拿不到，`FindCoordinator` 送不出去，`AbstractHeartbeatRequestManager.poll` 在
`coordinator().isEmpty()` 就回 `PollResult.EMPTY`，`makeHeartbeatRequest` 不會被呼叫，`requestInFlight` 永遠是
false。所以 `unavailable` 量的是「連線建立但 ApiVersions 無回應」這條路徑，不是本分支修的 busy loop；兩者都保留，
但不要互相代替。

### `heartbeat-blackhole` 的前置條件

Kafka request 是「4 bytes big-endian 長度 + message」，message 前 2 bytes 就是 api key，所以 proxy 不需要懂任何
schema：除了 `ApiKeys.CONSUMER_GROUP_HEARTBEAT` 之外原封不動轉發。兩個前置條件：

1. **proxy 後面那個 listener 必須把 proxy 的位址 advertise 出去**。否則 client 從 `Metadata` /
   `FindCoordinator` 拿到 broker 的真實位址，會直接繞過 proxy，heartbeat 就被正常回應了。單機 KRaft 的做法是加一個
   listener：

   ```properties
   listeners=PLAINTEXT://127.0.0.1:39492,PROXIED://127.0.0.1:39494,CONTROLLER://127.0.0.1:39493
   advertised.listeners=PLAINTEXT://127.0.0.1:39492,PROXIED://127.0.0.1:39594
   listener.security.protocol.map=PLAINTEXT:PLAINTEXT,PROXIED:PLAINTEXT,CONTROLLER:PLAINTEXT
   ```

   然後 `-Dkafka.bench.blackhole.port=39594 -Dkafka.bench.blackhole.target=127.0.0.1:39494`。
   `consume` / `idle` 照舊用 39492，不經過 proxy。
2. 這個模式會把 `request.timeout.ms` 調到 300 s，否則被吞掉的 heartbeat 30 s 後就 timeout、連線被拆掉，
   busy loop 的視窗就斷了。

證據：proxy 每吞一個 heartbeat 會印一行 `[hb-blackhole] swallowed ConsumerGroupHeartbeat #N` 到 stderr。
正常情況下應該只在 warmup 看到一兩行，measurement iteration 期間 `blackholedHeartbeats` 應該是 0
——代表那一個 heartbeat 一直卡在 in-flight，沒有被重送。

預設 `@Fork(1)`、warmup 10 x 1 s、measurement 5 x 5 s，都可用 `-f`、`-wi`、`-w`、`-i`、`-r` 覆蓋。

AuxCounters（`Type.OPERATIONS`，每秒）：`records`、`emptyPolls`、`seeksToBeginning`、
`peerConnections`（`unavailable` / `heartbeat-blackhole` 的 listener accept 到的連線數）、
`blackholedHeartbeats`（proxy 吞掉的 heartbeat 數）、
`processCpuMillis`（process CPU 毫秒 / 秒，1000 = 一顆核心滿載）、
`networkThreadCpuMillis`（名稱含 `consumer_background_thread` 的執行緒 CPU 毫秒 / 秒）。

`heartbeat-blackhole` 的主指標是 `processCpuMillis`，不是 polls/s：busy loop 發生在 `poll()` 內部，
poll 還是每 100 ms 回一次，差別在那 100 ms 是 spin 還是 block。

## 注意事項

- Benchmark 1 的 pass 是 `ConsumerNetworkThread.runOnce()` 的**複本**（`runOnceReplica`），因為 `runOnce()` 是 package-private。
  改動 `runOnce()` 時要同步更新。
- Benchmark 1 把 `OffsetCommitCallbackInvoker` 傳 `null`（建構子 package-private），因此關閉 auto-commit；不影響五個情境。
- 兩者都在筆電上跑會有雜訊：關掉省電、固定 fork 數、看 error 範圍，不要拿單次數字比較。
- Benchmark 2 若 broker 在同一台機器，broker 與 consumer 搶 CPU，`processCpuMillis` 只算 benchmark JVM 自己。
- Benchmark 2 的 `idle` 模式會先 `seekToEnd`；`consume` 模式的 group id 每次 trial 隨機，不會沿用舊 commit。
- Benchmark 2 的 `unavailable` / `unreachable` / `heartbeat-blackhole` 三個模式都拿不到 assignment，
  所以 trial setup 不等 assignment 就回來。
- e2e 的量測方法（session 級預熱、A/A、交錯對數、逐對比值）見
  `experiments/kip-1371-fable/benchmark-method.md`，跑之前先讀。
