# Codex 模型總結：KIP-1371 各 worktree 的定義、主張、證據與矛盾

整理自三份子代理人報告（2026-09-10），涵蓋 Codex 在 2026-09-05 至 09-08 之間產出的五個 worktree／方向。所有數字與引文保留原文，並註明來源檔案。本文件不含新的量測，也不對 gradle 結果做任何再驗證。

路徑縮寫：
- `W` = `/Users/unknowntpo/repo/unknowntpo/kafka/kip-1371-batched-decisions-poc`
- `D` = `W/docs/design/kip-1371-*.md`（29 個檔案，5,406 行）
- `A` = `/Users/unknowntpo/repo/unknowntpo/kafka/kip-1371-post-io-ablation`
- `C` = `/Users/unknowntpo/repo/unknowntpo/kafka/kip-1371-contracts-docs/docs/design`
- `H` = `/Users/unknowntpo/repo/unknowntpo/kafka/next-poll-condition-fable/experiments/next-poll-condition`
- `O` = `/Users/unknowntpo/Documents/Codex/2026-09-07/kafka-next-poll-condition-validation/outputs`

---

## 1. 各 worktree 摘要

### 1.1 `kip-1371-contracts-docs` / contracts-poc（KIP 敘事與 Approach 2 語意）

**角色**：KIP 敘事所在。`C/kip-1371-contract-guided-coordination.md`（491 行）與 `-v2.md`（466 行）。也是 Jenkins #931 數字與 `jenkins-932-noise-investigation.md`（未 tracked）的所在。

**定義（Codex 原文）**：

| 概念 | 引文 | 來源 |
|---|---|---|
| Contract-Guided Coordination（Approach 2） | "local state owners, narrow capabilities, explicit loop/routing contracts and effect-specific observation guarantees, without requiring a universal effect barrier" | `D/kip-1371-approach2-semantics-draft.md` |
| Centralized Coordination and Publication（Approach 3，原始 KIP） | "the original design's explicit cross-manager fact/command routing and shared publication-before-effect boundary" | 同上 |
| Admission boundary（parent POC） | "The selected admission boundary is the existing **next full pass**, not a new post-I/O owner poll." | `D/kip-1371-contracts-poc-evidence.md` |
| S3 useful activation | "Distinguish immediate productive continuation, time-driven retry, and input-driven waiting… a typed wait alone proves neither liveness nor absence of spin." | semantics-draft |
| Publication contract / S4 | "before releasing a result, error, or notification, make the state needed to interpret that particular effect observable… The current POC does not implement schedule-before-every-effect." | semantics-draft S4 |
| Effect contract（未決） | "Operation-local completion… does not promise completion of unrelated owner updates in the same batch" 對比 "Batch-wide publication before application effects… a stronger contract and a material migration decision" | `D/kip-1371-async-poll-metadata-delivery.md` |

**明示的範圍縮減**：
- semantics-draft："The new draft deliberately weakens the original universal publication barrier to necessary effect-specific ordering, and does not impose a physical single writer on every legacy SubscriptionState field."
- v2 draft 把 S4 從 "required state before its effect" 改名為 "result/error delivery and wait protocol"。
- v2 gate 表："Performance | Improvement not yet established"；"Extra post-I/O pass | Not a semantic requirement; active ablation removes it"。

**主張與證據**：此 worktree 本身沒有量測；效能數字全部引用他處（見 1.3、1.4）。

### 1.2 `kip-1371-batched-decisions-poc`（Approach 2 實作 POC）

**規模**：branch `codex/kip-1371-batched-decisions-poc`，merge-base `820533b870`，**66 commits**（非約 70），日期 2026-09-05 至 09-07。Production diff 20 檔，+677/−174（`git diff 820533b870..HEAD --stat -- clients/src/main`）。測試 diff +3,863/−29，20 檔。

**定義（Codex 原文）**：

| 概念 | 引文 | 來源 |
|---|---|---|
| Post-I/O pass / decision boundary | "After that poll returns, if it completed requests, the thread is still running, and no application command is already queued, perform **one** additional full manager pass with the post-I/O time." | `D/kip-1371-batched-decisions-poc.md` |
| Input cutoff / Decision cutoff | "Input cutoff: request completions handled by the one delegate poll… Decision cutoff: one full manager pass, not a fixed-point loop." | 同上 |
| Admission（對比 transmission） | "a manager builds an attempt and records its bookkeeping, then hands it to the transport queue. This is **not** proof of a socket write or a broker effect." | 同上 |
| NextPollCondition / typed activation | "A manager-local activation condition, not permission to bypass request eligibility. Input waits contribute no timer; the ordinary full manager pass still re-evaluates them." | `W/clients/src/main/java/org/apache/kafka/clients/consumer/internals/NextPollCondition.java` javadoc |
| Owner / stale owner / coordinator observation fencing | "Cross-owner observations are validated by that owner against the affected operation's scope and… captured identity/version before mutation is applied." / "A response for coordinator C7 must not invalidate C9" | semantics-draft S1；`D/kip-1371-coordination-design-v0.md` |
| CoordinatorAccess（capability narrowing） | "exposes only the five capabilities Commit currently needs: node, owner version, non-consuming fatal-error read, version-fenced invalidation, and version-fenced disconnect handling… a source-level dependency boundary, not a security sandbox" | `D/kip-1371-coordinator-access-poc.md` |
| FetchBufferProducer（capability narrowing） | "offers no raw buffer getter, consumption/wait API, or unconditional `wakeup()`" | `D/kip-1371-fetch-capability-poc.md` |
| Operation result | "A future is an operation-result handle, not a state-ownership boundary." / "Completes the named operation under its existing success/error/cancellation semantics" | coordination-design-v0 |
| "Publication is not notification" | "a wakeup barrier is not a visibility barrier" | `D/kip-1371-callback-safety-contract.md` |
| Callback safety | "Callback carries captured operation/attempt context and invokes a supported owner entry point. Running inline does not grant unrestricted access to peer state." | 同上 |
| Behavior-preserving order | "Coordinator precedes Commit, which precedes the corresponding Heartbeat… Commit handles the pending commit/offset-fetch operations before Heartbeat consumes and clears that failure" | `D/kip-1371-behavior-preserving-order-contract.md` |
| D1 retained rebalance retry snapshot | "retains the initial offset map for the complete pre-rebalance commit operation. There is no runtime toggle… This deliberately changes retry freshness" | `D/kip-1371-auto-commit-snapshot-audit.md` |
| Capture checkpoint | "The checkpoint promises capture readiness, not successful network transmission, broker acknowledgement, or settlement of every manager." | 同上 |
| Scheduling notification（latched wake） | "publishes the earlier aggregate deadline before a latched scheduling notification" | contract-summary |

**元件**：
- 新檔：`NextPollCondition.java`（65 行）、`CoordinatorAccess.java`（43 行）、`FetchBufferProducer.java`（86 行）。
- `ConsumerNetworkThread`：迴圈抽成 `pollAndStageRequests()`；post-I/O pass 條件 `running && applicationEventQueue.isEmpty() && networkClientDelegate.completedRequestsInLastPoll()`；`maximumTimeToWait` 以 `afterIoTimeMs` 計算；`scheduleWakeup` latch 只在 deadline 提前時觸發；`pendingAsyncPolls` 讓晚到的 metadata error 送達 `AsyncPollEvent`；transport poll 前後包 `beginResponseBatch()/completeResponseBatch()`。
- `NetworkClientDelegate`：`completedRequestsInLastPoll` 標記；`enableResponseBatching()`（"Local POC comparison seam"，`src/main` 從未呼叫）；`PollResult` 加 `NextPollCondition`，拒絕無 request 的 `POLL_IMMEDIATELY`。
- `CommitRequestManager`：改依 `CoordinatorAccess`；coordinator 未知時 `awaitInput(COORDINATOR_CHANGE)`，`maximumTimeToWait` → `Long.MAX_VALUE`；`tryAdmit()` 對比 `buildRequestWithoutAdmission()`（只用於 close）；`OffsetCommitRequestState.offsets` 改 `final`（D1）；`observedVersion` 貫穿 invalidation。
- `CoordinatorRequestManager`：實作 `CoordinatorAccess`；單調遞增 `coordinatorVersion`；`markCoordinatorUnknownIfCurrent(cause, now, observedVersion)`。
- Heartbeat 三者：D2（零 interval：`heartbeatIntervalMs > 0 ? heartbeatIntervalMs : Math.max(1L, pollTimer.remainingMs() / 2)`，commit `6588fa8c4a`）與 D3（in-flight 時 `awaitInput(NETWORK_COMPLETION)`，commit `95095ac064`，約 20 行）；version fencing。
- Fetch 側：`AbstractFetch`/`FetchRequestManager`/`Fetcher` 的原始 `FetchBuffer` 欄位換成私有 `FetchBufferProducer`；`createFetchRequests()` 回傳 detached observer futures。
- `AsyncPollEvent`/`ApplicationEventProcessor`/`AsyncKafkaConsumer`：error 在 `reconciliationCheckFuture` 完成前發布；`collectFetch()` 在 auto-commit 開啟時**每次**等 checkpoint。

遷移狀態：`NextPollCondition` 只有 Coordinator、Commit、AbstractHeartbeat、StreamsGroupHeartbeat；`TopicMetadataRequestManager`、`OffsetsRequestManager`、`ShareConsumeRequestManager`、`StreamsGroupTopologyDescriptionRequestManager`、membership managers 未遷移。`CoordinatorAccess` 只有 Commit 使用。`ShareConsumeRequestManager` 仍持有原始 `ShareFetchBuffer`。

**新測試**：`ConsumerBatchedDecisionTest` 1,181 行／24 方法（參數化後 76 案例）、`ConsumerAsyncPollMetadataTest` 393／14、`ConsumerOperationResultContractTest` 300／7（38 案例）、`FetchBufferProducerTest` 223／8、`ConsumerAdmissionContractTest` 208／4、`ConsumerPublicationContractTest` 110／4。整合：`ConsumerIntegrationTest` +79、`PlaintextConsumerCommitTest` +55。另有 Ducktape `W/tests/kafkatest/tests/client/consumer_contract_benchmark_test.py`。

**主張與證據（正確性，red→green 且保留失敗對照）**：
- D2："Both new `testZeroInitialHeartbeatIntervalAwaitsCoordinatorAndRecovers` tests failed on the unchanged heartbeat code"；"**362 tests** in [3 heartbeat suites] passed twice"（`D/kip-1371-heartbeat-activation-evidence.md`）。
- D3："Five new cases… failed before the change… First green: **443 tests in four suites**"；"`testInFlightHeartbeatDoesNotSpinConfiguredLoop`… passes with and without the extra post-I/O manager pass; **batching is not the no-spin protection**"（同上）。
- Post-I/O pass："Three assertions were observed failing against unchanged parent production code"，但這些 "distinguish admission timing, not an upstream bug"（batched-decisions-poc.md）。
- Version fencing 反向對照：移除 `observedVersion != coordinatorVersion` 守衛 "caused both variants of `testLateHeartbeatInvalidationCannotClearRediscoveredOwner` to fail"（callback-safety-contract）。
- 晚到 metadata error："Three tests in `ConsumerAsyncPollMetadataTest` failed before the production change"（async-poll-metadata-delivery）。
- Auto-commit capture gate 反向對照：恢復舊的 `hasPendingReconciliation` 條件 "caused the delayed public-poll safety assertion to fail"（snapshot audit）。

**回歸收據**（`D/kip-1371-local-acceptance.md`，皆在 `95095ac064`）：unit "97 suites, **3,383** tests, 0/0/0"；flaky "2 / 6"；integration "21 / **419**"；Streams "4 / 9"。Close/restart：consumes 0–49，"checks committed offset 50 with Admin, gracefully restarts all brokers, and consumes 50–99"。Codex 反覆提醒中間計數（1,262 / 1,462 / … / 3,392）"counts… are not additive"。

**效能（本機 macOS M1 Pro，非獨占主機，配對 AB/BA ×5，70M records，baseline `820533b870`）**：
- 吞吐 "2,041,351.96 (18,639.79) → 2,001,544.05 (14,701.13) … **-1.95%**; passes minimum ratio 0.95"。
- CPU "20.73 → 22.12 … **+6.71%**; descriptive cost, no CPU-time gate was declared"。
- Idle "+0.080726 percentage points"；first-record p99 "-1.815 ms"。
- 配對比 "0.995963, 0.987701, 0.982846, 0.984765 and 0.978469. Every pair is a little slower; do not describe passing the 5% allowance as 'no regression'"。
- 更早的 NOP-logging 50M run："-3.65%" / "+9.41%"，"inconclusive-short"。
- Jenkins：`W/benchmarks/contract-guided/jenkins-preflight.md` 記錄 build 929 "failed before tests at `:rat`"；HEAD 沒有 Jenkins 結果。Jenkins 930 數字只出現在 ablation branch 文件。
- Codex 自己的定位："This establishes the predeclared local workload gates, not statistical equivalence"；contract-summary："adopting the extra post-I/O pass requires a pinned comparison with the parent boundary… No new benchmark submission or performance claim is part of this assessment."

**KAFKA-20995 子議題**（`D/kip-1371-issue-coverage.md`、`D/kip-1371-historical-regression-provenance.md`）：列出 KAFKA-17066、-17674、-18641、-15529（PR 21476）、-20426、-20253、-20854、-20970、-20397、-18160、-19357、-18569。**10 項為 "Inherited"**（修復已是 `820533b870` 的祖先）；**只有 KAFKA-20970 與 KAFKA-20397 是 POC 自己的機制**（"KAFKA-20970 fixes `cd44c5de0f` / `ec65e09a04` are **not** ancestors of this baseline"）；KAFKA-18641 為 "POC repair / partial"。"No row is declared fully closed by this inventory." Baseline 早於上游 PR 23227（KAFKA-20970），D2/D3/commit no-spin 與已合併的上游工作重疊。

**Codex 拒絕的方案**：universal effect queue/publication barrier；uniform next-round mailbox（"recipient cutoffs, timeout winners, and shutdown obligations change"）；all-transitions-then-all-requests；`preIO()/postIO()`；actor-per-RM；dynamic dependency graph；centralized callbacks 作為前置條件；recapture-on-retry（D1 選了 retention）。Response batching 第一版被移除（"Of the 34 selected tests, one failed… That implementation was removed"）。`1f8486eefc`（fatal-fact/notification 分離）被 `5e09508617` revert，因為 "made newly queued operations fail on a previously notified error… an observable policy change"。

### 1.3 `kip-1371-post-io-ablation`（移除 post-I/O pass 的消融實驗）

**做了什麼**：commit `4289215a07`（diff `d3c6d866ad..HEAD`）刪除 `ConsumerNetworkThread.runOnce()` 中 transport poll 後的那一次完整 manager pass，只改 20 行 production code。Javadoc 由 "After a completed I/O batch, run one more full manager decision pass..." 改為 "This ablation leaves follow-up request admission to the next iteration's ordinary manager pass."。Response application、wait publication、notification 不變。

**本機 Mac 配對量測**（`A/docs/design/post-io-ablation-results.md`、`A/benchmarks/contract-guided/results/post-io-ablation-local/{summary,results,manifest}.json`）：
- Control `d3c6d866ad` vs no-pass `4289215a07`；70,000,000 × 256-byte records，4 partitions，5 AB/BA pairs，`group.protocol=consumer`，`enable.auto.commit=true`，`max.poll.records=500`，consumer heap 1g，broker heap 2g。
- Fetch rec/s 中位數（MAD）：control **1,813,330.57 (118,333.23)**，no-pass **2,068,496.79 (136,713.80)**，ratio **1.1407**（+14.07%）。
- 整 JVM CPU 秒：**24.39 (0.55)** vs **22.76 (0.57)**，-6.68%。Peak RSS **1,121,107,968** vs **1,125,908,480**，+0.43%。
- 配對比 `1.0495, 0.9813, 1.2005, 1.0648, 1.0040`；配對中位吞吐 **+4.95%**，CPU **-0.14%**，RSS **+0.82%**。Gate `pass`。
- 依執行順序的 rec/s：1,694,997 → 1,778,862 → 1,779,359 → 1,813,331 → 1,722,992 → 2,068,497 → 2,162,763 → 2,031,105 → 2,196,492 → 2,205,211。兩臂在同一 session 內都漂移約 +30%。
- 環境（manifest.json）：Darwin arm64 Apple M1 Pro，8 logical CPUs，Temurin 17.0.8，`exclusive_host: false`，**load_average_at_start [9.998, 9.125, 11.595]**。

**Jenkins**：
- #930（原候選 vs trunk）："throughput -4.36% and whole-JVM CPU +5.67%"，僅見於 `post-io-ablation-results.md` 第 61 行，repo 內無 artifact。
- #931（ablation A/B）："whole-process CPU medians 50.758 to 53.848 seconds and throughput medians 1,190,983 to 1,173,099 records/s"（`C/kip-1371-contract-guided-coordination-v2.md` 第 411 行）。換算：no-pass 的 CPU **+6.09%**、吞吐 **-1.50%**，與本機方向相反。無 artifact。
- #932（A/A，同一 code `d3c6d866ad`，harness `0ef509107a`）：每 run fetch 秒數 62.505, 53.696, 61.964, 52.144, 51.704, 65.860, 50.797, 68.427, 51.724, 51.777；CPU 46.878–59.827 s（`C/jenkins-932-noise-investigation.md`）。最大/最小差約 32%，大於所有已報告的 A/B 差值。

**正確性**：移除 pass 使 **7 / 91** 個選定測試失敗（4 個 poll/staging、2 個 terminal-error winner `GroupAuthorizationException`→`TimeoutException`、1 個 async-commit error audience）；另有一個測試卡在 unbounded join。Codex："not seven independent production bugs"，但證明 "passing publication and operation-result suites does not establish full equivalence."

**Codex 結論**："supplies a throughput improvement signal but does not establish a reliable CPU reduction or explain the earlier Jenkins CPU regression."；"No significance claim is made."；"Do not interpret the ratio of medians alone as the causal benefit."；"Neither this investigation nor #932 excuses #931's performance differences. No causal fix, CPU improvement, or performance acceptance has been established."

**#932 noise investigation**（`C/jenkins-932-noise-investigation.md`，未 tracked）：主要假說是記憶體預算壓力／zram swap（慢 run 伴隨 549–626 MB zram 讀、700–1,295 MB 寫；每 run 從 `sda` 讀約 18.2 GB，資料集 17.92 GB，屬 storage-inclusive）。Jenkins worker cgroup `memory.max` 2,097,152,000 B，小於 2g broker + 1g client heap。Ablation worktree 的**未提交**變更實作了下一步：`memory_diagnostics.py`（cgroup-v2 祖先解析、5 GiB headroom preflight）、`resource-time.py` 1 Hz 取樣、`-Xlog:gc*,safepoint`、`--memory-diagnostics` 旗標。本機 Linux 容器 17 tests pass，10,000-record smoke pass；尚未送 Jenkins。

**矛盾**：ablation 文件說 "Do not change the seven differing test assertions to claim acceptance"，但後續 commit `6a8dd86385`、`5021999ec5`（"Align consumer contract tests with next-pass admission"、"Verify canonical response and wait publication order"）改寫了正是那些期望（例如 `assertInstanceOf(GroupAuthorizationException...)` → `TimeoutException`；`testOnePostIoDecisionPassOnlyForACompletedBatch` 換成 `testResponseDeliveryPrecedesWaitPublicationWithoutAnotherDecisionPass`，`times(completedBatch ? 2 : 1)` → `times(1)`）。v2 draft 稱這是 "no production behavior change"，就那些 commit 而言為真，但斷言的語意已是消融後的語意。

### 1.4 `kip-1371-acceptance-baseline`（control build 來源）

`git status --short` 為空；HEAD `820533b870 KAFKA-20952: Remove Hamcrest from kstream package tests (#23308)`，branch `codex/kip-1371-acceptance-baseline`。角色（`docs/design/kip-1371-completion-work-log.md` 163–166 行、`benchmarks/contract-guided/README.md` 第 14 行）：原始本機配對 benchmark 的 **control build 來源**（"only generated build outputs are written there"）；`run-throughput.py` 原本斷言 `expected_base = "820533b870..."`。使用證據：`clients/build/libs/kafka-clients-4.5.0-SNAPSHOT.jar` 日期 Sep 6 23:35，對應 local-acceptance 的 -1.95% / +6.71%。Ablation branch 已把 control 改釘 `d3c6d866ad`，此 worktree 不再被目前 runner 引用；Jenkins 上 baseline 是 `unknowntpo/kafka` 的 shallow clone，非此 worktree。

### 1.5 NextPollCondition handoff（`next-poll-condition-fable`）

**來源**：`H/handoff/{README.md,FABLE-PROMPT.md,prepush-validation.json}`、`H/handoff/evidence/*.md`、`H/fable-review/REPORT.md`、`O/*.md`。

**定義（Codex 原文）**：
- 兩條 wait channel（`O/next-poll-wait-classification.md`）："`PollResult.timeUntilNextPollMs`／NextPollCondition：network loop 何時需要重新執行 manager；不是 manager 本身 sleep" 對比 "`maximumTimeToWait`：network thread 掃描各 manager 取最小值，快取給 app … 不是「期限前可安全跳過 manager」的承諾"。
- `O/rm-next-poll-condition-plan.md`："NextPollCondition.after 表達時間；Signal.await 表達狀態變化；anyOf 表達任一來源可觸發重查."
- Wait 分類：「真正時間限制」（coordinator retry backoff、heartbeat interval、max.poll.interval、auto-commit interval、API timeout）、「狀態等待」（coordinator in-flight、fetch with pending request）、「保守重查」（fetch 無 pending request → retryBackoffMs）。規則："事件使 manager 重新檢查，不代表立即發 RPC"。
- Ready set（`O/ready-set-contract.md`）："`AnyOf(AwaitChange(resource, observedGeneration)..., RetryAt(absoluteDeadline), Runnable)`"；"publish 只增加 resource generation、標記 waiter ready 及 latch wakeup，不呼叫 poll／send"；"generation 是通知序號，不是 coordinator 身分"；"any-of 必須涵蓋所有可能推進或取消的輸入；缺任何輸入就不能安全跳過 poll"。
- RM-owned continuation（`O/a4-rm-owned-continuation.md`）："Position 完成 → ApplicationEventProcessor 建立續行 → FetchRequestManager.enqueueFetchContinuation → inputChanged signal → scheduler ready bit → Fetch RM.poll"。
- App re-drive removal（`O/a4-remove-app-redrive-design.md`）：決定 "不以單純回傳 Long.MAX_VALUE 取代 maximumTimeToWait。改成 BG 持有未滿足需求、精確事件 activation、app 結果通知，以及獨立的 app 活性協定"。
- Operation contract（`O/operation-contract-lessons.md`）："Operation 是推理單位，manager 是 owner／政策容器"；"Observer 與 operation 分開。App timeout/cancel 可以結束 observer；不能由此推論 RPC 已取消或 commit 沒生效"；"發布保證應依 effect 決定"。
- Activation（`O/fetch-activation-kip1371-review.md`）："condition 採 manager 級只能省 activation 次數，不能自動讓一次 preparation 變成 partition 級增量更新"；"manager activation 與 app notification 分開"。

**主張與數字**：
- JMH 三方（`H/handoff/evidence/trunk-jmh.md`，trunk `820533b870`，N150，0.5 CPU quota，Java 17.0.20，27 forks）：condition/trunk consume rec/s "**-16.7% / -2.7% / -14.8%**"，CPU/Mrecord "**+20.6% / +5.4% / +22.5%**"；nospin/trunk unavailable CPU "**-93.0% / -93.5% / -93.4%**"；condition/trunk unavailable "-92.8% / -92.5% / -92.7%"；idle condition/trunk "+10.7% / +13.6% / +16.8%"。中位數：unavailable trunk 498.87、nospin 33.23、condition 36.24 ms/s；consume trunk 484.90 ms/s、1,373,505 rec/s、359.4 CPU ms/Mrec；condition 488.51、1,168,587、438.1。判定："本輪不支持為了效能採用完整 NextPollCondition 原型"。注意 "condition" 變體**含** no-spin patch。
- Native 三方（`trunk-threeway.md`）：UNAVAILABLE trunk 500.60 / nospin 37.39 / condition 40.39 ms/s；select/s 16890.1 / 21.9 / 21.7。IDLE 31.87 / 24.96 / 24.97 ms/s，但 select/s 18.4 / 17.8 / **37.9**。BUSY rec/s 1,401,128 / 1,315,802 / 1,403,987；CPU/Mrec 339.6 / 368.2 / 351.1。"Do not combine their scores" 與 JMH。
- Tree vs scan（`deadline-scan-component.md`）：ns/pass Tree→Scan：idle recheck 12.1→56.4（"+359.6% / +378.6% / +384.7%"），all-due 523.2→386.8（"-6.8% / -26.5% / -26.1%"），signal-cancel 121.0→137.3，mixed 119.2→189.3。真實 consumer 5 輪（`deadline-tree-vs-scan.md`）："配對幾何平均為吞吐 -2.44%、CPU/record +3.29%"，95% CI "-9.06%～+4.66%、-2.42%～+9.33%"，"兩個區間均包含零"。
- Ready-set PoC（`O/ready-set-results.md`）：poll 次數 full-pass vs ready-set：coordinator 5→2、commit 5→2、heartbeat 5→2、fetch fixture 5→4，總計 **20→10**；"不是 CPU 降低 50% 或吞吐提升的證據"。
- 其他（`O/current-session-handoff.md`）："最小修正 idle select 約 −40%、配對 CPU 中位 −34.1%，但 sparse CPU +10.6%；目前 A4 三輪 CPU/select 都高於 baseline"；error delivery latency "baseline 81.94 ms，單純放寬等待 986.39 ms，放寬＋受控發布通知 0.67 ms"；homelab 配對 "idle CPU 73.6116 vs 73.7885 ms/s; throughput 804063.67 vs 801463.01 records/s… no stable improvement"；"BUSY scheduler 成本仍為 scan 的約 3.5×／8.7×（8／32 個輕量合成 managers）"；baseline "idle BG CPU 0.76–0.84%，不支持普遍busy loop"；"之前 90.4% 是 no-spin 修正結果，不能歸因於 scheduler"；power "未量到瓦數或焦耳"。
- 測試計數：prepush 436 tests／0 fail（unit only，"spotbugs: Excluded"）；a4-background-auto-commit 311（+8）；rm-owned-continuation 351；redrive/notification 364；reconnect 344；validation completion 394；internal-wait 762。

**Fable review 判定**（`H/fable-review/REPORT.md`）：
1. Scheduler core（Signal/Waiting/TreeSet）無 missed-notification bug；但綁進來的行為變更有兩個 High 回歸：STALE member 永不 rejoin（`PlaintextConsumerPollTest.testAsyncConsumerRecoveryOnPollAfterDelayedRebalance` 3/3 fail）、一個 AWAIT_VALIDATION partition 擋住所有 fetch。
2. 真 broker A/B：consume CPU/Mrec "**+31.3%**"，process CPU "+24.0%"，idle net-thread alloc "+110%"；unavailable "−7%（兩版都 spin ~1.6 core）"——−93% 完全來自 no-spin patch，live snapshot 沒有它。
3. Profile：network thread 82–88% 時間在 kevent/read；`pollReady` 0.5%；額外成本在 app↔network handoff，不在資料結構。
4. 建議：不做完整 condition 化；先落 no-spin（KAFKA-21031），保留小件（BackgroundEventHandler wakeup、positionStateToken），丟棄 pollTimer credit、retained validation、Fetch demand queue、background auto-commit。
5. 替代：每 manager 零配置的 `ready` flag + `nextDeadlineMs`，bounded skip（readiness 是提示、polling 是底線），只限 Coordinator+Heartbeat。

**consumer-ng rules REVIEW.md**（對照上游 trunk `74fbd50061`）：標題結論 "「每一條原案都能以小 PR 進 trunk」不成立"。事實更正："R6：trunk 每條連線的 in-flight 上限是 100，不是 1"；"R10：`MAX_POLL_TIMEOUT_MS` 是 5000 ms，不是 100 ms"；R2/R9 trunk 已有 AsyncPollEvent dedup；P3 PR 21476 方向相反；R11 0.36 vs 1.96 s/min 非 trunk 比較。可移植性：R1 "原案否"、R2 "條件式"、R3 "原案否；通知局部修正條件式可"、R4 "部分可"、R5 docs/tests 可、pooling 否、R6 "原案不採作已證明的 trunk fix"、R7 trigger 表可、R8 不加 sequencer、R9 "無條件重送否"、R10 "刪除所有安全界限否"、R11 "固定普遍門檻否"、R12 方法可、不加 ng SPI。範圍限制："不改 `RequestManager` 契約、不改公開 API、不改 Streams／share 行為"。"歷史實驗的 PASS 數沒有重算"。

**Codex 自我撤回**：
- `operation-contract-lessons.md`："「先發布狀態與整體等待決策，再完成所有 future／wakeup」過度統一"；現有測試 "沒有證明 operation terminal outcome、遠端取消、assignment fencing 或 app publication"；"不新增通用 barrier、actor 或 operation framework"。
- `completion-progress-exploration.md`：reconnect-backoff fetch "到期後 manager.poll() 仍不送"；resume "不會恢復已消耗的需求"；auto-commit "maximumTimeToWait() 已為 0，manager.poll() 仍無提交"；"目前尚不能移除任何等待限制"。
- `ready-set-results.md`："目前不建議把額外 post transport 當成必要改動"。
- `current-session-handoff.md`："原版是I/O/event/deadline混合，不能把deadline本身當root cause"；"最後的主線決策是先保留原版掃描，不因呼叫數下降就採用完整 scheduler"。

---

## 2. 橫切表：概念 → Codex 定義 → 證據狀態

狀態定義：**measured** = 有數字；**tested** = 有 red→green 測試；**documented only** = 只有文件敘述，無測試或量測；**contradicted** = 被後續量測、review 或 Codex 自己的文件推翻。

| 概念 | Codex 定義（節錄） | 證據狀態 | 依據 |
|---|---|---|---|
| Post-I/O pass（one extra full manager pass） | "perform **one** additional full manager pass with the post-I/O time" | **contradicted**（效能）／tested（timing） | 本機 -1.95% 吞吐、+6.71% CPU；Jenkins 930 -4.36%／+5.67%；移除後本機 +4.95% 配對中位；Jenkins 931 反向；#932 A/A 雜訊 ~32%。D3 測試 "batching is not the no-spin protection" |
| Response batching（`enableResponseBatching`） | "Local POC comparison seam" | **documented only**（dead code） | `src/main` 從未呼叫；第一版 "one failed… That implementation was removed" |
| NextPollCondition / typed activation（POC 版） | "A manager-local activation condition, not permission to bypass request eligibility" | **documented only** | `NextPollCondition.Input` 在 `src/main` 從未被讀；`addAll` 塌成 `delayMs()`，`AWAIT_INPUT` ≡ `WAIT_FOREVER` |
| NextPollCondition 完整 scheduler（handoff 版） | "NextPollCondition.after 表達時間；Signal.await 表達狀態變化；anyOf 表達任一來源可觸發重查" | **contradicted** | JMH consume -16.7%／CPU +20.6%；Fable 真 broker CPU/Mrec +31.3%、alloc +110%；兩個 High 回歸 |
| No-spin（unavailable coordinator） | 屬 KAFKA-21031 patch，非 scheduler 屬性 | **measured** | nospin/trunk -93.0%；"之前 90.4% 是 no-spin 修正結果，不能歸因於 scheduler" |
| D2 零 interval heartbeat | `Math.max(1L, pollTimer.remainingMs() / 2)` | **tested** | 2 個新測試在舊 code 失敗；362 tests 兩次通過 |
| D3 in-flight heartbeat await | `awaitInput(NETWORK_COMPLETION)` when `requestInFlight()` | **tested** | 5 個新案例 red→green；443 tests |
| Version-fenced coordinator invalidation | "A response for coordinator C7 must not invalidate C9" | **tested** | 移除守衛使 `testLateHeartbeatInvalidationCannotClearRediscoveredOwner` 兩變體失敗 |
| 晚到 metadata error 送達 pending polls | `pendingAsyncPolls` | **tested** | `ConsumerAsyncPollMetadataTest` 3 tests 先失敗後通過 |
| Auto-commit capture checkpoint | "promises capture readiness, not successful network transmission" | **tested**（gate）／documented only（副作用） | 反向對照失敗；"A zero-duration poll may return empty until capture completes" 無量測 |
| D1 retained rebalance retry snapshot | "deliberately changes retry freshness" | **contradicted**（流程）／untested（retry 路徑） | 自己先說 "Do not enable this as a complete fix yet"，後在 `0829a57bfa` 移除 toggle 並改寫既有測試；無 broker restart 證據 |
| CoordinatorAccess capability narrowing | "a source-level dependency boundary, not a security sandbox" | **documented only** | "confined to the consumer network thread" 無斷言；只有 Commit 使用 |
| FetchBufferProducer | "offers no raw buffer getter…" | tested（unit）／**documented only**（blast radius） | `FetchBufferProducerTest` 8 tests；classic `Fetcher.removePendingFetchRequest` 行為改變無文件 |
| Callback-safety contract | "Running inline does not grant unrestricted access to peer state" | **documented only** | "must not recursively poll" 只是註解；Codex："not mechanically enforced" |
| Behavior-preserving order | "Coordinator precedes Commit, which precedes the corresponding Heartbeat" | **documented only** | 只靠 constructor 順序 |
| Publication ≠ notification | "a wakeup barrier is not a visibility barrier" | **documented only** | S4 已弱化為 "result/error delivery and wait protocol" |
| Effect contract（operation-local vs batch-wide） | "Do not silently migrate to a stronger effect contract" | **documented only**（未決） | v2 gate 表仍為 open |
| Ready set（AnyOf/generation） | "generation 是通知序號，不是 coordinator 身分" | **measured**（call count）／**contradicted**（CPU） | poll 20→10；"BUSY 仍比 scan baseline 昂貴" |
| Deadline tree vs scan | "Scan 只將…TreeSet 改成遍歷" | **measured**（不顯著） | 吞吐 -2.44%、CPU/record +3.29%，CI 含零 |
| App re-drive removal / Long.MAX_VALUE | "不以單純回傳 Long.MAX_VALUE 取代 maximumTimeToWait" | **contradicted**（內部） | A4 目標 "移除 maximumTimeToWait" vs REVIEW.md R10 "全刪 bounds 否" |
| Liveness contract（合法等待超過 max.poll.interval 留組） | `a4-app-wait-liveness-decision.md` | **contradicted** | 與 REVIEW.md R7/C8 衝突；為 Fable 1.1/1.4 回歸根因 |
| Retained validation（AWAIT_VALIDATION） | handoff 附帶行為 | **contradicted** | Fable：一個 AWAIT_VALIDATION partition 擋住所有 fetch |
| Observer ≠ operation | "App timeout/cancel 可以結束 observer；不能由此推論 RPC 已取消" | **documented only**（lesson） | `operation-contract-lessons.md`、REVIEW.md C5 |
| KAFKA-20995 子議題覆蓋 | 12 項列出 | **documented only**（10 inherited） | 只有 KAFKA-20970、-20397 是 POC 自身機制；"No row is declared fully closed" |
| Benchmark 等價性 | "the predeclared local workload gates, not statistical equivalence" | **contradicted**（可信度） | #932 A/A spread > 所有 A/B delta；本機 load ≈ 10 on 8 CPUs |

---

## 3. 未測假設與矛盾清單

**batched-decisions-poc**
1. Post-I/O pass 建議反覆：compat ledger 先 "retain the next-iteration boundary as the default candidate"，再 "Correction… do not label the extra pass incorrect or select its removal"；local-acceptance "Retain the existing bounded conditional full pass"；ablation branch 移除並量到增益；HEAD 仍出貨帶 pass。
2. D1 違反自己的先前指引：decision-brief-zh "尚未選定正式 retry 語意… 不默默切換方案"；snapshot audit "Do not enable this as a complete fix yet"；`0829a57bfa` 在 "autonomous-work authorization" 下移除 toggle 並改寫期望 legacy recapture 的既有測試（"The assertion now proves the stronger selected outcome"）。Retry 路徑無 broker crash/restart 證據（close-broker-restart-evidence："does not test the opt-in retained snapshot strategy"）。
3. Typed activation 只寫不讀：`NextPollCondition.Input` 在 `src/main` 從未被讀；文件承認 "Raw-delay adapters remain" 卻仍以 S3 為保證宣傳。
4. 文件化但未強制：`CoordinatorAccess` 執行緒限制無斷言；`FetchBufferProducer.pendingNodes` 為普通 `HashSet`；`RequestManagers` 順序只靠 constructor；`failUnsentRequestsOnCoordinatorError` "must deliver it at the coordinator-unknown poll boundary" 未檢查；callback 禁止遞迴 poll 只有註解。
5. 死實驗留在 production：`NetworkClientDelegate.enableResponseBatching()`／`responseBatchingEnabled`／`beginResponseBatch/completeResponseBatch` 只有測試使用。
6. 效能成本未歸因：+6.71%（本機）／+5.67%（Jenkins 930）"profiles do not establish the cause"；候選有 post-I/O pass、每次 poll 的 checkpoint wait、`prepareFetchRequests` 的 `Set.copyOf`、observer-future relays。HEAD 無 A/A 雜訊 run；從未與 parent contracts-POC 邊界比較。
7. 未文件化的影響範圍：classic `Fetcher.removePendingFetchRequest` 改為只在 `pendingNodes.remove(nodeId)` 為真時 wake；`ShareConsumerImpl` 新增 `scheduleWakeup` 來源；`CommitRequestManager.maximumTimeToWait` 在 coordinator 未知時回 `Long.MAX_VALUE`（auto-commit interval 不再限制 app wait；ledger 稱 "Intended no-spin change, not pure refactoring"）。
8. 未解失敗：`PlaintextConsumerSubscriptionTest.testAsyncConsumerPatternSubscription()[2]` 在 `45e9cc7275` heap 耗盡，"its deeper cause is **not established**"，重跑通過。
9. Codex 自述的覆蓋缺口：loop 測試 mock membership/KafkaClient；"Membership transition is controlled, not a public subscribe() end-to-end test"；Share/Streams "regression passes are not complete variant migration proof"；KAFKA-18160/19357/18569 "exact historical trigger… remain unclosed"。
10. KIP 從未選定的 effect contract："operation-local outcome with a separate shared scheduling notification, or batch-wide publication before application effects" 仍 open。

**post-io-ablation / acceptance-baseline**
11. Jenkins 930/931 只有文字，repo 內無 artifact。
12. 本機 run 的 "one unchanged control broker" 主張：portable manifest 只記 hash，不記來源 export；`8933042675` 才在 Jenkins 側修正。
13. 本機 Mac 與 Linux Jenkins 結果方向相反；JDK patch level 不同（17.0.8 vs 17.0.20）。
14. 吞吐 gate（≥0.95）對任何正向漂移都「通過」；沒有 CPU gate。
15. "5 pairs" 實為同一 session 的 10 個 JVM，共用 OS/broker cache，無交錯 warmup；兩臂單調加速約 30%。
16. Ablation 文件禁止改寫 7 個差異斷言，`6a8dd86385`／`5021999ec5` 仍改寫。

**NextPollCondition handoff**
17. Handoff README 的 "-92.8%" unavailable 來自含 no-spin 的變體；live snapshot 無 no-spin，Fable 量到 −7% 且兩版都 spin。
18. Idle 方向衝突：JMH condition idle +10.7…+16.8%；native IDLE 24.97 vs 31.87（較低）但 select 次數倍增（37.9 vs 18.4）。Busy：JMH −16.7% 吞吐 vs native ≈0%。
19. Liveness contract（2026-09-08 採用）與 REVIEW.md R7/C8 衝突，且是 Fable 1.1/1.4 的根因。
20. A4 "移除 maximumTimeToWait" 與 REVIEW.md R10 及 `consumer-wait-design-history.md`（"定期重查是有記錄的正確性與簡化取捨"）衝突。
21. `fetch-activation-kip1371-review.md` 自認 "真實 FetchRequestManager.poll 為數百 ns 到 µs 未附本情境獨立量測"；`a4-rm-owned-continuation.md` 未量 condition/queue 配置成本，Fable 後來量到 busy +5%／idle +110% allocation。
22. Prepush 436 tests 只有 unit；Fable 的整合 run 才暴露 STALE 回歸。
23. Ready-set 20→10 與 bitmap "省成本" 與 "BUSY 仍比 scan baseline 昂貴" 並存——整組資料中 call-count 的減少從未轉成 CPU 減少。
24. Inventory 自認 "「部分遷移限制了效能收益」目前只是待驗證假說"。

---

## 4. 對獨立設計的可重用與反證

### 4.1 可重用（有測試或有明確教訓）

| 項目 | 為何可用 | 來源 |
|---|---|---|
| Version-fenced coordinator invalidation（單調 `coordinatorVersion` + `markCoordinatorUnknownIfCurrent(cause, now, observedVersion)`） | 有反向對照：移除守衛使 `testLateHeartbeatInvalidationCannotClearRediscoveredOwner` 兩變體失敗。約束 "A response for coordinator C7 must not invalidate C9" 與 scheduler 無關，可獨立落地 | `D/kip-1371-callback-safety-contract.md`、`D/kip-1371-coordination-design-v0.md` |
| 晚到 metadata error 送達 pending polls（`pendingAsyncPolls`） | `ConsumerAsyncPollMetadataTest` 3 tests red→green；error 在 `reconciliationCheckFuture` 完成前發布 | `D/kip-1371-async-poll-metadata-delivery.md` |
| D2/D3 heartbeat no-spin | 各自 red→green；但與上游 PR 23227（KAFKA-20970）、KAFKA-21031 重疊，應先確認上游狀態再決定是否仍需要 | `D/kip-1371-heartbeat-activation-evidence.md` |
| Callback-safety contract 作為**規則**（非機制） | "Callback carries captured operation/attempt context and invokes a supported owner entry point"；"a wakeup barrier is not a visibility barrier"。可作 review checklist，但需自行加強制（斷言或型別） | `D/kip-1371-callback-safety-contract.md` |
| "Observer ≠ operation" 教訓 | "App timeout/cancel 可以結束 observer；不能由此推論 RPC 已取消或 commit 沒生效"；"發布保證應依 effect 決定"。避免把 future 當 state-ownership 邊界 | `O/operation-contract-lessons.md`、REVIEW.md C5 |
| Wait 分類三型（真正時間限制／狀態等待／保守重查） | 純分析結果，不依賴實作；"事件使 manager 重新檢查，不代表立即發 RPC" | `O/next-poll-wait-classification.md` |
| Behavior-preserving order 的**內容** | Coordinator → Commit → Heartbeat 的因果理由（Commit 先處理 pending 再讓 Heartbeat 清除 fatal error）；需自行強制 | `D/kip-1371-behavior-preserving-order-contract.md` |
| Fable 的替代方案 | 每 manager 零配置 `ready` flag + `nextDeadlineMs`，bounded skip，只限 Coordinator+Heartbeat；readiness 是提示，polling 是底線 | `H/fable-review/REPORT.md` 第 5 點 |
| Benchmark 方法論教訓 | 先做 A/A 雜訊 run；非獨占主機、load ≈ 10、同 session 漂移 30% 的資料不能當接受證據；Jenkins worker cgroup `memory.max` 2,097,152,000 B < heap 總和需先修 | `C/jenkins-932-noise-investigation.md`、`A/docs/design/post-io-ablation-results.md` |
| KAFKA-20995 子議題來源核對法 | `merge-base --is-ancestor` 逐項驗證 inherited vs POC 自身；避免把上游已修的項目算成自己的成果 | `D/kip-1371-historical-regression-provenance.md` |

### 4.2 證據反對的方向

| 項目 | 證據 | 來源 |
|---|---|---|
| 額外 post-I/O pass | 本機 -1.95% 吞吐／+6.71% CPU；Jenkins 930 -4.36%／+5.67%；移除後本機配對中位 +4.95%；D3 證明 "batching is not the no-spin protection"；v2 draft "Not a semantic requirement"。移除會讓 7/91 測試改變 terminal-error winner，故若移除須重新定義那些語意，而非改斷言 | `D/kip-1371-local-acceptance.md`、`A/docs/design/post-io-ablation-results.md` |
| Response batching | 第一版 "one failed… That implementation was removed"；殘留 `enableResponseBatching()` 為 dead code；無任何量測支持 | `D/kip-1371-batched-decisions-poc.md` |
| NextPollCondition 完整 scheduler 化 | JMH consume -16.7%／CPU +20.6%；Fable 真 broker CPU/Mrec +31.3%、process CPU +24.0%、idle alloc +110%；profile 顯示 82–88% 在 kevent/read、`pollReady` 0.5%；兩個 High 回歸；Codex 自判 "本輪不支持為了效能採用完整 NextPollCondition 原型"。POC 版則是 write-only（`AWAIT_INPUT` ≡ `WAIT_FOREVER`） | `H/handoff/evidence/trunk-jmh.md`、`H/fable-review/REPORT.md` |
| Retained validation（AWAIT_VALIDATION 阻擋） | Fable：一個 partition 擋住所有 fetch；REVIEW.md C9 "一個 partition pending 不得藏住另一個 retry deadline" | `H/fable-review/REPORT.md` |
| D1 retained rebalance retry snapshot 作為預設 | 語意變更 "Can commit older offsets and increase replay"；retry 路徑無 restart 證據；違反自己的 "Do not enable this as a complete fix yet" | `D/kip-1371-auto-commit-snapshot-audit.md` |
| 每次 poll 的 auto-commit capture wait | "A zero-duration poll may return empty until capture completes"；為 +6.71% CPU 的候選之一，未歸因 | `D/kip-1371-auto-commit-snapshot-audit.md` |
| 移除 `maximumTimeToWait`／全域 `Long.MAX_VALUE` | REVIEW.md R10 "刪除所有安全界限否"；`completion-progress-exploration.md` "目前尚不能移除任何等待限制"；Codex 自己也決定 "不以單純回傳 Long.MAX_VALUE 取代" | `O/a4-remove-app-redrive-design.md` |
| Deadline tree（TreeSet）取代 scan | 真實 consumer 差異 CI 含零；micro：idle recheck +359.6%；Fable：成本在 handoff 不在資料結構 | `H/handoff/evidence/deadline-*.md` |
| Ready-set／call-count 作為效能指標 | 20→10 但 "BUSY 仍比 scan baseline 昂貴"；C10 wake count ≠ CPU | `O/ready-set-results.md` |
| 合法等待超過 max.poll.interval 仍留組 | 與 R7/C8 衝突；STALE member 永不 rejoin 回歸 | `O/a4-app-wait-liveness-decision.md`、`H/fable-review/REPORT.md` |
| Universal effect barrier／actor-per-RM／operation framework | Codex 兩條線都拒絕："不新增通用 barrier、actor 或 operation framework"；Approach 2 定義本身就是 "without requiring a universal effect barrier" | `O/operation-contract-lessons.md`、semantics-draft |
| 以 capability interface 當安全或執行緒保證 | Codex 自述 "not a security sandbox"；無執行緒斷言 | `D/kip-1371-coordinator-access-poc.md` |

### 4.3 對獨立設計的含意（一句話）

Codex 兩條線留下的可信資產是幾個窄修正（version fencing、晚到 error 送達、no-spin）與一組分析規則（observer ≠ operation、wait 三分類、effect-specific publication）；架構層（post-I/O pass、response batching、typed activation／完整 scheduler、retained validation）不是量到成本就是未被強制，獨立設計應從 Fable 的 `ready` + `nextDeadlineMs` 最小方案與先修 benchmark 雜訊起步，而非繼承這些機制。
