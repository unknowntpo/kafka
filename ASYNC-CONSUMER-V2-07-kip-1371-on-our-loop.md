# ASYNC-CONSUMER-V2 07：在我們的事件迴圈上解決 KIP-1371 的四類問題（路線圖）

> 決定（2026-09-08）：保留降低輪詢成本的方向，並用同一個迴圈把 KIP-1371（KAFKA-20995）列的問題做成**結構性保證**，而不是再標記一次。manager 的程式碼盡量不動；每一步有守門測試與 pass 成本回歸。契約定義在 06 文件（R1–R10），本文件是實作順序。

## 對應表

| KIP-1371 問題 / busy loop 類型 | 做法 | 落點 | 守門 |
|---|---|---|---|
| 1 推不了的緊急工作、2 空結果語意不明；busy loop 第 1 類（自我觸發） | `RequestManager` 加 default `waitCondition()`（預設 `ANY_COMPLETION` = 今天的行為）；`ManagerTask` 記下宣告時看到的版本，只在「自己的 timer 到期」「宣告的輸入到達且版本嚴格前進」「command」三者之一時重跑 | `ManagerTask`、`ConsumerEventLoop` 的版本計數（每個完成 / command / metadata 變更前進並帶身分） | 每個 manager 一個凍結時鐘的執行次數上界測試；`KafkaConsumerTest.testResetUsingDurationBasedAutoResetPolicy` |
| 3 發佈與等待順序 | `runOnce` 尾端發佈不可變 `PassDecision`（pass 序號、下一個 deadline、position 是否齊全、reconciliation 序號、未交付錯誤）；應用端等待條件加版本；所有背景自主錯誤帶「仍成立」predicate | `ConsumerEventLoop.runOnce`、`PipelinedKafkaConsumer.awaitPollProgress`、`ErrorEvent` | 交錯測試（生產者在條件評估後、park 前發佈）；seek 後舊錯誤不得出現 |
| 4 生命週期分散；fatal error 順序 | `LifecycleSequencer`：`Close(options)` 一個 command，迴圈執行緒跑明確狀態機（commit → leave → 關 session → 停 coordinator 尋找 → 停執行緒），共用一個 timer；fatal error 的「commit 讀、heartbeat 清」成為兩個明確步驟 | `ConsumerEventLoop`、取代 `CommitOnCloseEvent` / `LeaveGroupOnCloseEvent` / `StopFindCoordinatorOnCloseEvent` | `ConsumerBounceTest.testAsyncClose`（coordinator 不可用）；fatal error 重排測試（先寫） |
| busy loop 第 2 類（元件間循環） | 進展帳：pass 跑了 manager 但淨狀態版本沒前進且無 timer / command → 計數；連續 K 次套有上限的指數 backoff；metric `passes-without-progress` | `ConsumerEventLoop.runDirtyWork` | 兩個互相觸發但淨狀態不變的 mock manager，pass 頻率必須衰減 |
| busy loop 第 3 類（外部風暴） | 維持 manager 內 backoff；接上同一個 metric | — | 既有 manager 測試 |
| busy loop 第 4 類（應用端全速 poll） | 已消除（volatile 寫入） | — | 三方 A/B mpr50 |
| busy loop 第 5 類（timer 回 0） | 已有界（1 ms，只跑自己）；遷移後由 R1 禁止 | `LoopTimer` | `LoopTimerTest` |

## 順序

1. **`PassDecision` + 錯誤通用化**（小；關掉問題 3 剩下的三成）。**已做（2026-09-08）**：`pipeline/PassDecision`（pass、stateVersion、nextDeadlineMs、allPositionsKnown、positionsAttemptInFlight、reconciliationCheckedPollSequence、backgroundEventsPending），`runOnce` 尾端一次 volatile 寫入發佈；`stateVersion` 在 request 完成、command、metadata 變更時前進（第 2 步版本驗證的基礎）；只有 app 可見欄位改變才叫醒應用執行緒（取代 housekeeping 的無條件喚醒，並補上 trunk 違反的「背景事件要喚醒」）；`EventLoopKafkaConsumer.pollForFetches` 在 position 嘗試在飛時不再每 100 ms 醒來，改等 loop 的下一個 deadline；`FetchPositionsErrorEvent` 改帶 `stillRelevant` predicate。測試：`decisionIsPublishedEveryPassAndItsVersionAdvancesOnlyOnInputsWithIdentity`、`applicationIsWokenOnlyWhenADecisionFieldItMayWaitForChanges`；`KafkaConsumerTest` 225/225、相關套件共 428/428；checkstyle / spotbugs 通過。
2. **`waitCondition()` + `ManagerTask` 版本驗證 + 安全退路**（結構性關掉第 1 類），然後逐個遷移：`CoordinatorRequestManager` → `HeartbeatRequestManager` → `CommitRequestManager` → `OffsetsRequestManager`（等 commit manager 的 OffsetFetch，即 R6 的宣告）→ `TopicMetadataRequestManager`。每遷移一個就跑三方 A/B 確認 pass 成本沒有回升。
   **基礎設施已做（2026-09-08）**：`pipeline/WaitCondition`（`ANY_INPUT` / `OWN_COMPLETION` / `TIMER_ONLY`）；`RequestManager.waitCondition()` default 回 `ANY_INPUT`（share consumer 的 manager 不受影響）；`ManagerTask` 在每次執行後記下宣告與當時的 `stateVersion` 與自己的完成數，`wantsRun(commandProcessed)` 只在「command」「`ANY_INPUT` 且版本嚴格前進」「`OWN_COMPLETION` 且自己的完成數前進」時為真，timer 路徑不變；應用端 poll 與有 request 的 position 嘗試都讓 `stateVersion` 前進（它們是帶身分的輸入）。**第一個遷移：`CoordinatorRequestManager`** 在 FindCoordinator 在飛時宣告 `OWN_COMPLETION`，其餘 `ANY_INPUT`（coordinator 可能被任何 manager 的 response 標成 unknown）。測試：`managerDeclaringOwnCompletionIsNotRerunByOtherManagersInputs`、`...IsRerunWhenItsOwnRequestCompletes`、`managerDeclaringTimerOnlyRunsOnlyOnItsTimerOrACommand`、`aManagerAskingForZeroDelayIsBoundedByTheTimerFloorAndItsDeclaration`、`CoordinatorRequestManagerTest.testWaitConditionIsOwnCompletionOnlyWhileARequestIsInFlight`；`KafkaConsumerTest` 225/225、`AsyncKafkaConsumerTest`、`ShareConsumerImplTest`、`ConsumerNetworkThreadTest` 不受影響（452/452 含 loop 測試）。三方 A/B 待機器負載低於 20 時重跑。
   **第二個遷移：`TopicMetadataRequestManager`**（`464696153b`）：有自己的 request 在飛 → `OWN_COMPLETION`；否則 `TIMER_ONLY`（新工作只會由 command 帶來，而 command 重跑全部；backoff 與逾時是 timer 的事）。順手消掉它 `PollResult(0, ...)` 造成的第 5 類 busy loop：送出後 1 ms 再跑一次就進入 `OWN_COMPLETION`。測試 `testWaitConditionFollowsTheInflightRequests`。
   **分析後決定維持 `ANY_INPUT` 的 manager 及理由**（這是 S1 的正確結果，不是未完成）：`HeartbeatRequestManager` 依賴 coordinator 是否已知（別的 manager 的 response 會把它標成 unknown）、membership 狀態（由 command 與應用端 poll 改變）、poll timer；`CommitRequestManager` 依賴 coordinator 與應用端 poll（auto-commit）；`OffsetsRequestManager` 依賴 commit manager 的 OffsetFetch 與 metadata。它們的準確宣告就是「任何輸入」；要再窄化必須先把跨 manager 依賴宣告成 `COMPLETION_OF(owner)` 一類的條件（R6），這是之後的擴充，不在本步。**修正**（`59e292e685`）：應用端的 `requestFetch()` 也是帶身分的輸入，必須讓 `stateVersion` 前進，否則 fetch manager 的驗證會擋住它到下一次 poll。
3. **進展帳與 metric**（第 2 類有界、可觀測）。**已做，定義修正（2026-09-08）**：原本「跑了工作但 `stateVersion` 沒前進就計數」在 S1 落實後恆為 0（manager 只能因版本前進、timer 或 command 而執行），且第 2 類循環的每一步都是真實 RPC 完成、版本必定前進，靠版本偵測不到；一般化的「淨狀態沒進展」需要領域知識，KIP-1371 也沒解。改為可觀測性：`background-pass-rate`、`manager-runs-without-requests-rate`（`AsyncConsumerMetrics`，close 時移除），並在 S7 明確定義第 2、3 類為「有界於 RTT 加 manager 內的 backoff，且可見」。
4. **`LifecycleSequencer`**（問題 4），先補 fatal error 順序的整合測試。
   **4a 已做（2026-09-08）**：測試 `managersRunInRegistrationOrderWhateverTriggeredThem` 重現「heartbeat 的 timer 在 dirty pass 之前執行 → 先清掉 coordinator 的 fatal error → commit manager 讀不到」；修法是結構規則：timer 到期只把 task 標成 due（`ManagerTask.isDue`），所有 manager 一律在 `runManagers` 內依 `entries()` 順序執行，不論觸發來源。這把 trunk「每輪依註冊順序跑全部」隱含的順序契約變成明文，且不多花任何成本。**4b 已做（2026-09-08）**：`LifecycleSequencer`（loop 執行緒）成為所有生命週期轉移的唯一入口：三個 close 事件仍由應用端依序送出（應用端必須自己跑 rebalance callback 與同步 commit，所以 close 仍跨兩條執行緒），但在 loop 端全部經由 sequencer 套用，步驟順序是資料（`Step` enum），亂序記錄 WARN，`shutdown` 依 `entries()` 順序呼叫 `pollOnClose`。測試 `closeTransitionsPassThroughTheLifecycleSequencerInOrderAndShutdownPollsManagersInRegistrationOrder`。`ConsumerBounceTest` / `PlaintextConsumerCloseTest` 待安靜時段執行。
   **量測註記**：step 2 的三方 A/B（`/tmp/s2-ab.out`）在執行中負載從 18 升到 146，同一輪內三個變體互相矛盾，不採用；需在安靜時段重跑。

## Jenkins 量測紀錄（2026-09-08，`kafka-e2e`，ducktape）

本機負載被其他容器佔滿（load 25–35），三方 A/B 改到 Jenkins 跑。CI 分支不含工作文件（rat 會擋未授權檔）：`async-consumer-v2-bench-candidate`（loop-only 主線 + 文件剝除）與 `async-consumer-v2-bench-baseline`（trunk `820533b870` + 同一個 ducktape 測試）。

| Build | 版本 | 測試 | 結果 |
|---|---|---|---|
| #934 | baseline `575f7d163b` | `consumer_protocol_benchmark_test.py` | UNSTABLE：我們的 8 個 cell 全 PASS；另外 10 個 FAIL 都是繼承來的基底 SSL / share-consumer case，與本工作無關 |
| #935 | candidate `1b5f8ab873` | 同上 | FAILURE：pipeline 自己的 `ducker-ak down` 對前一個 build 已 exited 的容器做 `podman kill`，尚未 checkout 任何程式碼（infra race） |
| #936 | candidate `1b5f8ab873` | `tests/client/consumer_test.py` | FAILURE：Jenkins 用 JDK 25 編譯，`-Xlint:dangling-doc-comments -Werror` 抓到 `CoordinatorRequestManager` 裡 `waitCondition()` 被插在 `markCoordinatorUnknown` 的 javadoc 與宣告之間；本機 JDK 21 沒有這個 lint |

baseline（trunk，`AsyncKafkaConsumer`）8 個 cell 的數字，10M × 100 B：

| cell | rec/s | MB/s |
|---|---|---|
| classic, 1p, mpr 500 | 263,380 | 25 |
| classic, 1p, mpr 50 | 297,247 | 28 |
| classic, 6p, mpr 500 | 667,557 | 64 |
| classic, 6p, mpr 50 | 345,185 | 33 |
| consumer, 1p, mpr 500 | 359,363 | 34 |
| consumer, 1p, mpr 50 | 250,357 | 24 |
| consumer, 6p, mpr 500 | 361,768 | 35 |
| consumer, 6p, mpr 50 | 455,996 | 43 |

**讀法上的限制**：ducktape 在同一台 docker host 上並行跑多個 test，8 個 cell 與 74 個基底 case 互相搶資源，絕對值比本機低一個量級（本機 1p 約 2.3M rec/s），同一 build 內的 cell 之間也不可直接比（classic 6p mpr 500 vs mpr 50 差近一倍就是干擾）。可用的比較只有「同一個 cell、同一份排程」的 baseline vs candidate；因此 candidate 重送時要用**同一個 revision（82 個 test 的排程）**，不能先套用下面的測試修正，否則 candidate 只跑 8 個 test、干擾模式不同。

**兩個修正（都在 loop-only 主線，並 cherry-pick 到本機的 CI clone，尚未 push）**：

1. `3d3e4ba179`：`ConsumerProtocolBenchmark` 改繼承 `Test` 而非 `Benchmark`。ducktape 會發現 class 上所有 `test_*`（含繼承），原版一共收集 82 個 test，修正後 8 個（本機 `ducktape --collect-only` 驗證）。留給 baseline/candidate 都重跑的下一輪使用。
2. `a4cfd52936`：把 `waitCondition()` 移到 `markCoordinatorUnknown` 的 javadoc 之前。本機用 JDK 25 重現 lint 錯誤並確認修正後通過；提交 Jenkins 前的 gate 一律改用 JDK 25 跑 `clean build -x test`。

**第二輪（同日，經授權 push + 重送）**：candidate 分支推到 `42fab4e0b2`（兩個修正都在）。

| Build | 版本 | 測試 | 結果 |
|---|---|---|---|
| #937 | candidate `1b5f8ab873`（原 revision，想保持與 #934 同排程） | benchmark | FAILURE：同 #936 的 JDK 25 lint。這是判斷錯誤：該 revision 本來就編不過，「同排程」的前提不存在 |
| #938 | candidate `42fab4e0b2` | `tests/client/consumer_test.py` | build stage 通過（javadoc 修正生效），ducktape 執行中 |

**結論**：可比的 bench 只能是 baseline 與 candidate 都用「只收集 8 個 test」的版本各跑一次（baseline 側為 `91ac9bfb29` = `575f7d163b` + 修正 1，待 push）。#934 的數字只當 trunk 的參考值，不與 8-test 版的 candidate 直接相減。

**#938 的結果與修正（同日）**：48 個 case 失敗 14 個，全在 `group_protocol=consumer`，型態是 join 逾時 / 等不到 STABLE / 等不到消費。TRACE log 顯示 member 拿到 target assignment 進入 RECONCILING 之後 60 秒內沒有任何 reconciliation 動作，app thread 每 100 ms 迭代但迴圈沒被叫醒；拿到 partition 的 consumer 送了 2 個 FETCH 後就停。根因是我們把「application poll」當成每次 `poll()` 呼叫一次，trunk 是每個等待迭代一次（`PollEvent` + `CreateFetchRequestsEvent`）；`VerifiableConsumer` 只呼叫一次 `poll(Long.MAX_VALUE)`，於是 auto-commit 開啟時唯一允許的 `maybeReconcile(true)` 不再被呼叫，空 fetch 之後也沒人再要求 fetch。修正：`ConsumerEventLoop.onApplicationPollIteration`（每個沒拿到資料的迭代登記序號、park 時喚醒）+ `pollForFetches` 在 RECONCILING 時把等待上界收到 `retry.backoff.ms`；回歸測試 `KafkaConsumerTest.testSingleLongPollJoinsReconcilesAndFetchesWithAutoCommit`（修正前 10 s FAIL，修正後 0.45 s PASS）。規則寫成 06 R11。

**為什麼本機沒抓到**：單元測試都用 `poll(ZERO)` 或短 poll；本機 ducker smoke 的 `test_group_consumption` 一個一個啟動 consumer、且 producer 已在寫入，join 走背景路徑、fetch 有資料，正好避開兩條需要 poll 輸入的路徑。教訓：e2e 前至少跑 `consumer_test.py` 整檔，而且要在 Jenkins（慢、並行）跑，本機通過不算。

**homelab 量測（同日）**：本機負載無法降到 20 以下，改在 `morefine` 跑三方 A/B，數字與讀法見 03 §2.1「安靜機器複測」；結論是迴圈本身吞吐 +2–3%、CPU/GB −5–7%，完整版 1p +46–69%。

## 不做

- TreeSet / bitmap / timer heap 之類的排程結構：per-task timer 加版本比對已足夠，成本是幾個整數比較。
- 把 manager 改成 callback 式 reactor、效果輪尾批次釋放：會把 fetch 續發拉回輪尾，抵銷 03 §2.1 的收益。
- 動 fetch 路徑：`FetchPipeline` 已是 (b) 層次的事件驅動（05 §3.1）。

## 完成後的論證

「每個 pass 的固定成本更低（高 poll 頻率 CPU/GB −11%、多 partition +58%），並且 KIP-1371 的四類問題與五類 busy loop 各有機械式保證或有界化，附守門測試」——這是純標記的排程器與 trunk 都給不出的組合。
