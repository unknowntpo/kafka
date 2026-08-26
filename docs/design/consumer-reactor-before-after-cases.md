<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Consumer Reactor Before/After 案例

本文件以三個 Kafka Consumer 歷史問題檢查 KIP-1371 的核心主張：`ConsumerReactor` 不新增 thread，也不取代
各 `RequestManager` 的本地規則；它收回跨元件排程與 application-visible action 的最終決策權。

共同邊界如下：

```text
RequestManager
  └─ 回報 requests、completed state transitions、timeUntilNextPollMs

ConsumerReactor
  ├─ 排序 input
  ├─ 合併所有 manager 的結果
  ├─ 發布 state 與 ReactorSchedule
  └─ 最後決定並執行 ReactorAction
```

Before 的 `ConsumerNetworkThread` 是既有 background event loop，但不是跨元件決策中心。After 的
`ConsumerReactor` 沿用同一個 background execution context，差異在於它取得 state transition、schedule 與
application-visible action 的統一排序權。

## 案例一：空 Fetch 結果造成 application/background busy loop

對應問題：[KAFKA-20854 / PR #23014](https://github.com/apache/kafka/pull/23014)

### Before

```text
FetchRequestManager 回傳語意不明的空結果
        ↓
局部路徑判斷 application thread 可能需要重新檢查
        ↓
喚醒 application thread
        ↓
application thread 發現沒有 records 或可見狀態變化
        ↓
再次觸發 background processing
        ↓
application thread 與 ConsumerNetworkThread 反覆互相喚醒
```

空結果只代表本輪沒有建立 fetch request，不一定代表 application-visible progress。當 manager、application wait
與 background loop 都能局部決定 wakeup 時，同一個無進展結果可能形成 notification ping-pong。

### After

```text
FetchRequestManager.poll()
        ↓
回報 blocker / request / completed state transition
        ↓
ConsumerReactor 合併其他 manager 結果並發布 ReactorSchedule
        ↓
判斷是否存在 application-visible transition
        ├─ FETCH_BUFFER_HAS_DATA → WAKE_APPLICATION
        └─ RECONNECT_BACKOFF / REQUEST_IN_FLIGHT /
           NO_FETCHABLE_PARTITIONS → 不喚醒，依 schedule 等待
```

關鍵不是增加一個 `FETCH_BUFFER_HAS_DATA` 條件，而是只有 `ConsumerReactor` 可以把 manager facts 轉換為
`WAKE_APPLICATION`。`FetchRequestManager` 不再直接決定是否喚醒，application thread 也不再從另一份 mutable
state 重新推導相同決策。因此，wakeup 的條件、去重與 publish-before-wakeup ordering 只需要在一個位置定義。

### Reactor 帶來的簡化

- 新增 blocker 時，只需定義它如何影響 `ReactorSchedule`，不必修改多條 wakeup 路徑。
- 多個 manager 在同一輪產生相同 wake reason 時，Reactor 可合併成一次 action。
- 測試可直接驗證 `manager facts → published schedule → ReactorAction`，不必等待偶發 busy loop。

### 目前驗證狀態

`AsyncKafkaConsumerTest.testPausedPartitionDoesNotProduceNoProgressWakeup` 已覆蓋真 consumer、Reactor、fetch
manager 與 fetch buffer 的 deterministic component path；pre-PR baseline 會觀察到無效 wakeup。真 broker stress
仍待補充。

## 案例二：舊 OffsetFetch completion 修改新加入的 partition

對應問題：[KAFKA-17066 / PR #16885](https://github.com/apache/kafka/pull/16885)、
[KAFKA-17674 / PR #17342](https://github.com/apache/kafka/pull/17342)

### Before

```text
為 tp1 發出 OffsetFetch request
        ↓
request in flight 期間，assignment 加入 tp2
        ↓
tp1 的舊 response 完成
        ↓
completion 根據較新的 assignment 執行 position initialization
        ↓
舊 operation 錯誤 reset tp2，尚未先取得 tp2 committed offset
```

request 建立時捕捉的 partition scope 與 completion 執行時讀到的 assignment 不一致。即使 state 各自有 owner，
若 application event、network completion 與下一輪 position update 沒有共同排序者，較舊 operation 仍可能影響
後來才出現的 state。

### After

```text
ConsumerReactor 排序 assignment input
        ↓
OffsetsRequestManager 建立 OffsetFetch(scope={tp1}, operationId=10)
        ↓
assignment input 加入 tp2
        ↓
network completion(operationId=10, capturedScope={tp1})
        ↓
ConsumerReactor 只套用屬於 tp1 的 completed transition
        ↓
發布新的 ReactorSchedule
        ↓
下一輪再為 tp2 取得 committed offset
```

Manager 仍負責 position initialization 規則；Reactor 負責排序 assignment input、network completion、transition
publication 與下一次 manager poll。operation scope 成為 completed transition 的一部分，舊 completion 無法跨越
自己的 scope 修改 tp2。

### Reactor 帶來的簡化

- operation 的建立、完成與後續 poll 形成一條可追蹤的順序。
- 新 partition 不需要靠其他 thread 的 safety scan 保護；下一輪工作由已發布 schedule 驅動。
- stale completion 可用 operation ID、captured scope 與 transition ordering 直接測試。

### 目前驗證狀態

`AsyncKafkaConsumerTest.testReactorPreservesNewPartitionAcrossOlderOffsetFetchCompletion` 已覆蓋 application event、
Reactor、真 request managers、`MockClient` 與 subscription state。相同行為測試在 pre-KAFKA-17674 baseline
`6744a718c2` 失敗。真 broker rebalance stress 仍待補充。

## 案例三：Coordinator unavailable 時，Heartbeat deadline 造成 CPU spin

主要對應問題：[KAFKA-20253 / PR #22836](https://github.com/apache/kafka/pull/22836)。
[KAFKA-20426 / PR #22018](https://github.com/apache/kafka/pull/22018) 是相同 failure class 的另一個案例，但
trigger 是 manual assignment 使 heartbeat 無法前進，不是 re-authentication failure。

### Heartbeat request 需要哪些元件

`HeartbeatRequestManager` 不擁有 socket，也不直接執行 network I/O。它負責判斷是否需要 heartbeat、建立 request
內容，並回傳待送出的 `UnsentRequest`。完整路徑如下：

```text
CoordinatorRequestManager
  └─ 透過 FindCoordinator 找到 group coordinator broker
                     ↓ Optional<Node>
MembershipManager + HeartbeatRequestState
  └─ 提供 member state、epoch、assignment 與 heartbeat timing
                     ↓
HeartbeatRequestManager.poll()
  └─ 建立 ConsumerGroupHeartbeatRequest / ShareGroupHeartbeatRequest
                     ↓ PollResult<UnsentRequest>
ConsumerNetworkThread
  └─ poll 所有 RequestManager，收集 requests 與最短 wait
                     ↓
NetworkClientDelegate
  └─ 暫存 request、選擇連線、處理 timeout 與 completion correlation
                     ↓
KafkaClient / NetworkClient / Selector
  └─ 實際把 bytes 送到 coordinator broker，接收 response
                     ↓ callback on background thread
HeartbeatRequestManager + MembershipManager
  └─ 處理 response，更新 heartbeat timer、member epoch 或 coordinator state
```

Heartbeat 必須送到該 consumer group 的 coordinator，不能任選 broker。實作中的
`buildHeartbeatRequest()` 會把 `coordinatorRequestManager.coordinator()` 放進 `UnsentRequest`；而
`HeartbeatRequestManager.poll()` 在 coordinator 為空時會直接回傳 `PollResult.EMPTY`。因此，缺少 coordinator 時，
Heartbeat manager 可以知道「heartbeat 到期」，卻不能建立一個可送出的 heartbeat operation。

這不是 manager 被 blocking lock 卡住，而是 livelock：heartbeat path 正確地回傳空 request，但另一條
`maximumTimeToWait()` path 仍回傳 0 ms。Thread 每輪都有執行，卻重複觀察相同 state、建立不了新 request，也
沒有等待正在進行的 `FindCoordinator` completion。

### Before 時序

假設 heartbeat interval 為 3 秒，coordinator retry backoff 為 1 秒：

| 時間 | 外部事件或狀態 | Heartbeat manager | Coordinator manager | Application/background 行為 |
| --- | --- | --- | --- | --- |
| `t0` | Consumer 正常運作 | Heartbeat deadline 尚未到期 | Coordinator 已知 | thread 可正常等待 |
| `t1` | re-authentication 失敗，連線與 coordinator 失效 | 無法送 heartbeat | 開始或保留 `FindCoordinator` request | 仍可等待 network event |
| `t2` | Heartbeat deadline 到期 | `shouldHeartbeatNow=true`，但 coordinator 不存在，所以 `poll()` 無法建立 heartbeat request | `FindCoordinator` 仍 in-flight | 此時沒有可立即執行的新工作 |
| `t3` | Application thread 計算下一次 fetch wait | 舊 `maximumTimeToWait()` 只看到 heartbeat 已到期，回傳 `0` | — | `pollForFetches()` 以 0 ms 等待並立即返回 |
| `t4` | Background loop 計算 network poll timeout | Heartbeat `poll()` 回傳空結果 | backoff 已過，但 request 仍 in-flight；舊程式回傳 remaining backoff `0` | `NetworkClient.poll(0)` 進入 non-blocking `selectNow()` |
| `t5` | coordinator、request 與 timer 都沒有新變化 | 仍回傳 application wait `0` | 仍回傳 network wait `0` | application 與 background thread 立即重複 `t3`、`t4`，CPU spin |

問題可以縮成兩句：

```text
Heartbeat deadline 已到期，但 coordinator 不存在，所以 heartbeat 無法送出。
系統把「很急」誤當成「現在可以前進」，因此持續回傳 0 ms。
```

### PR #22836 如何止住這個 loop

PR #22836 沒有引入全域 Reactor，而是在三個產生 0 ms 的局部路徑加入 feasibility guard。

#### 1. Heartbeat application wait

`AbstractHeartbeatRequestManager.maximumTimeToWait()` 的判斷改為：

```text
pollTimer expired
    → 0 ms，application poll deadline 必須立即處理

coordinator unavailable 或 shouldSkipHeartbeat
    → heartbeatIntervalMs，因為目前無法送 heartbeat

shouldHeartbeatNow 且沒有 heartbeat in-flight
    → 0 ms，現在確實可以建立 heartbeat request
```

這個順序把「heartbeat timer 已到期」和「現在能否送 heartbeat」分開。Coordinator 不存在時，不再因過期的
heartbeat timer 永久回傳 0 ms。

#### 2. Coordinator background poll

`CoordinatorRequestManager.poll()` 增加：

```text
FindCoordinator request 已 in-flight
    → EMPTY / Long.MAX_VALUE
```

即使 retry backoff 已經歸零，也不能在舊 request 完成前送出另一個 request。因此 background loop 應等待
network completion，而不是再次呼叫 `NetworkClient.poll(0)`。

#### 3. In-flight auto-commit application wait

`CommitRequestManager.AutoCommitState.remainingMs()` 增加：

```text
auto-commit timer expired 且已有 commit in-flight
    → autoCommitInterval
```

新的 auto-commit 在前一個 request 完成前也無法開始。此處若回傳 0，同樣會讓 application thread 空轉。

### PR #22836 修正後時序

| 時間 | 狀態 | 局部修正後的決策 |
| --- | --- | --- |
| `t2` | coordinator unavailable，heartbeat deadline 已到期 | Heartbeat application wait 回傳正值 `heartbeatIntervalMs` |
| `t3` | `FindCoordinator` request 仍 in-flight | Coordinator manager 回傳 `EMPTY`，等待 response/failure event |
| `t4` | auto-commit deadline 到期，但 commit 仍 in-flight | Auto-commit wait 回傳正值 `autoCommitInterval` |
| `t5` | 沒有新的 network completion | application 與 background thread 都能阻塞，不再 busy-spin |

### 這與 ConsumerReactor 的關係

PR #22836 是正確且必要的 bug fix，但同一個「urgency 不等於 feasibility」規則需要分別修改三個位置：

```text
Heartbeat maximumTimeToWait()
CoordinatorRequestManager.poll()
AutoCommitState.remainingMs()
```

ConsumerReactor 的目標是讓 manager 回報局部 facts，由一個 owner 形成最終 schedule：

```text
Heartbeat：deadline expired + coordinator required
Coordinator：request in-flight
Commit：deadline expired + request in-flight
                    ↓
             ConsumerReactor
                    ↓
沒有立即可執行的 transition 或 request
                    ↓
發布 ReactorSchedule，等待 network completion 或有效 deadline
```

因此，新增第四個 manager 時，不必再自行猜測「0 ms 是否真的能產生進展」。Manager 回報自己的 request、
completed transition 與 `timeUntilNextPollMs`；Reactor 統一合併 feasibility、deadline 與 action ordering。

#### 目前 POC 與目標形狀

目前 POC 仍保留 migration bridge：`HeartbeatRequestManager.maximumTimeToWait()` 在 coordinator unavailable 時
回傳 `heartbeatIntervalMs`。`ConsumerReactor` 不讓 application thread 直接讀取這個 manager；它將此候選 wait
轉成 absolute application deadline，和所有 manager 的 deadlines 合併成 `ReactorSchedule`，發布後再讓
application thread 讀取。

因此目前的責任分配是：

```text
Heartbeat manager：提出局部候選 wait = heartbeatIntervalMs
其他 managers：提出各自的候選 deadlines
ConsumerReactor：選出最早的全域 deadline，發布 ReactorSchedule
Application thread：只讀取已發布 schedule 的 remaining time
```

完成 migration 後，coordinator unavailable 的 heartbeat manager 不需要為 application thread 製造週期性 wake：

```text
Heartbeat manager
  → no request
  → no completed transition
  → timeUntilNextPollMs = WAIT_FOREVER（等待 coordinator input）

Coordinator manager
  → FindCoordinator request in-flight
  → network completion 是下一個 input

ConsumerReactor
  → 合併其他 manager 的有效 deadlines
  → 發布 ReactorSchedule
  → NetworkClientDelegate.poll() 等待 response 或較早 deadline
```

若另一個 fetch manager 回報 500 ms，Reactor 的全域 deadline 是 500 ms；若 `FindCoordinator` response 在
120 ms 到達，network poll 會提早返回並觸發下一輪。此時 coordinator 已知，Heartbeat manager 才建立 heartbeat
request。Reactor 決定的是全域 ordering 與最早實際 wait，不取代 manager 對本地 timer 與 request feasibility 的
判斷。

### 目前驗證狀態

PR #22836 增加三類 unit regression tests：coordinator unavailable 時 heartbeat wait 必須大於 0、
`FindCoordinator` in-flight 時 manager 不得回傳 0，以及 Share Consumer skip heartbeat 時 wait 必須大於 0。
POC 已有 multi-manager schedule tests，但原始 re-authentication failure 的完整 Async consumer component/E2E
reproduction 尚未完成，因此 Reactor 方案對 KAFKA-20253 目前仍標示為 partial，而不是 covered。

## 三個案例的共同檢查問題

每次調整 KIP 或 POC 時，使用下列問題重新檢查：

1. Manager 回報的是 fact/result，還是仍直接執行 application-visible action？
2. 是否只有 `ConsumerReactor` 合併跨 manager schedule 並產生 `ReactorAction`？
3. State 與 `ReactorSchedule` 是否在 future completion、event publication 或 wakeup 前發布？
4. No-progress 結果是否可能產生 immediate poll 或 `WAKE_APPLICATION`？
5. 舊 completion 是否只能修改建立時捕捉的 operation scope？
6. 一個 manager 的更新是否會遺失其他 manager 尚未到期的 deadline？
7. 文件聲稱 covered 的案例，是否已有對應的 deterministic component 或 E2E reproduction？

## 摘要

| 案例 | Before 的分散決策 | Reactor 收回的決策權 | 預期結果 |
| --- | --- | --- | --- |
| 空 Fetch 結果 | manager、application wait 與 background loop 各自決定 wakeup | application-visible transition 與 `ReactorAction` | 無進展結果不再造成 wakeup ping-pong |
| 舊 OffsetFetch completion | assignment input、operation scope 與 completion 分別排序 | input、completion、transition publication 與下一輪 poll | 舊 operation 無法修改新 partition |
| Heartbeat 0 ms | heartbeat、coordinator 與 auto-commit 各自修正 urgency/feasibility | 跨 manager deadline aggregation 與唯一 `ReactorSchedule` | 無法前進時不再 0 ms busy spin |

三個案例共享同一個設計收益：局部元件只回報事實，`ConsumerReactor` 在一份有序 snapshot 上決定下一步，
並在執行外部 action 前先發布該決策。這使 correctness 從分散的慣例變成可集中實作與直接驗證的 invariant。
