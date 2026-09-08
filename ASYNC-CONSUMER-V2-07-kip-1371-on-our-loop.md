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
   **4a 已做（2026-09-08）**：測試 `managersRunInRegistrationOrderWhateverTriggeredThem` 重現「heartbeat 的 timer 在 dirty pass 之前執行 → 先清掉 coordinator 的 fatal error → commit manager 讀不到」；修法是結構規則：timer 到期只把 task 標成 due（`ManagerTask.isDue`），所有 manager 一律在 `runManagers` 內依 `entries()` 順序執行，不論觸發來源。這把 trunk「每輪依註冊順序跑全部」隱含的順序契約變成明文，且不多花任何成本。**4b 待做**：`Close(options)` 狀態機取代三個 close 事件。
   **量測註記**：step 2 的三方 A/B（`/tmp/s2-ab.out`）在執行中負載從 18 升到 146，同一輪內三個變體互相矛盾，不採用；需在安靜時段重跑。

## 不做

- TreeSet / bitmap / timer heap 之類的排程結構：per-task timer 加版本比對已足夠，成本是幾個整數比較。
- 把 manager 改成 callback 式 reactor、效果輪尾批次釋放：會把 fetch 續發拉回輪尾，抵銷 03 §2.1 的收益。
- 動 fetch 路徑：`FetchPipeline` 已是 (b) 層次的事件驅動（05 §3.1）。

## 完成後的論證

「每個 pass 的固定成本更低（高 poll 頻率 CPU/GB −11%、多 partition +58%），並且 KIP-1371 的四類問題與五類 busy loop 各有機械式保證或有界化，附守門測試」——這是純標記的排程器與 trunk 都給不出的組合。
