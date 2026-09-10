# RequestManager 排程遷移清單

本清單依目前完整原型改動集合的 live source 撰寫。普通 consumer group 的七個 managers 中，四個公開 `poll()` 回傳 `NextPollCondition`，三個仍使用 legacy 排程。這不代表四個已覆蓋所有未來的持續需求，也不代表每個 condition 都有 deadline。本次只補註解與盤點，沒有實作新的 manager 遷移。

原始碼目錄：`clients/src/main/java/org/apache/kafka/clients/consumer/internals/`。以下行號依本次註解完成後的版本；方法名稱是主要定位依據。

## 四個不同層次

| 層次 | 目前行為 | 遷移的意義 |
|---|---|---|
| Manager poll 選擇 | ConsumerNetworkThread.runOnce → RequestManagerScheduler.pollReady；condition manager 等 signal/deadline，legacy manager 仍依舊規則參與 | 減少無工作 manager 的 poll 呼叫 |
| Application 等待上限 | ConsumerNetworkThread 每輪仍掃全部 managers 的 maximumTimeToWait，快取給 application | condition 遷移不會自動消除這次掃描 |
| Application 等待與通知 | AsyncKafkaConsumer.pollForFetches 仍使用 maximumTimeToWait，並保留未完成 positions 等情況的短 retry；ApplicationPollWait 記錄等待狀態 | 目前不是完全只靠通知的 application loop |
| Network 喚醒 | network wakeup 讓 transport 離開等待；scheduler readiness 由 condition 決定 | 單獨呼叫 network wakeup 不等於指定 manager 已 ready |

定位：ConsumerNetworkThread.runOnce:222、runAtClose:324、maximumTimeToWait getter:357；RequestManagerScheduler.pollReady:81、pollPrepared:105、Waiting:192；AsyncKafkaConsumer.pollForFetches:2013；ApplicationPollWait:28；FetchBuffer.awaitNotEmpty:187、wakeup:240。Close 路徑直接呼叫各 manager 的 pollOnClose，不應把 close 的 legacy 結果算成正常 poll 遷移缺口。

Scheduler 的 ready、legacy、timer 索引分別負責可執行工作、相容舊回傳值、到期喚醒。Entry 保存 manager 狀態；Waiting 管理一次有效訂閱與 deadline。先固定 batch、再執行 managers，可避免同批通知改變本批執行集合；onPollBatchStart 也讓 Fetch 固定 continuation 邊界。Signal 是 network-thread confined 的版本通知；generation 與 armed/signalled 處理 capture 到訂閱之間的狀態變化，取消舊訂閱後才能重用 Waiting。ApplicationPollWait 是跨執行緒的同步狀態，不能直接套用 Signal 的執行緒假設。

## 普通 consumer group

| Manager | 公開 poll 狀態 | 已有依賴／通知 | 遷移剩餘工作與測試 |
|---|---|---|---|
| Coordinator | Condition | inputChanged、共享 stateChanged；關閉、coordinator 失效、response finally 發布；連線／請求 backoff deadline | 目前未確認缺少必要事件。保留 fatal、同批 fan-out、關閉與 signal-before-arm 測試 |
| ConsumerHeartbeat | Condition，繼承 AbstractHeartbeatRequestManager | 自身 inputChanged、coordinator.stateChanged、membership.heartbeatStateChanged；poll timer reset、response、close 發布；heartbeat/backoff/max-poll deadline | 阻塞／in-flight 時只保留可執行或真正必要的 deadline，避免 expired deadline 空轉。不能將內層 pollInternal 的 EMPTY 誤判成 legacy |
| Offsets | Condition | retained validation、metadata cluster listener、ApiVersions listener、position-state hook；enqueue/validation callback 發布；validation API expiry、retry/connect backoff | 逐操作確認 list-offset、reset、committed-offset 的取消、obsolete、deadline 與 retention 契約。已有 validation 喚醒，不能列為完全未接線；也不能未查 reaper 就斷言 API timeout 缺失 |
| Fetch | Condition | demand、continuation、position changes 發布；保留實際 reconnect backoff demand 及 API deadline；每批固定 continuation 數 | 目前主要是一次 preparation 加 application retry。若要持續保留被 metadata、buffer capacity、in-flight 等阻塞的 demand，需補完整 retention／喚醒契約；這是後續範圍，不能直接判為現有 liveness bug |
| Commit | 全部正常分支 legacy | 背景 auto-commit 與 application wait 已實作；尚無自身 Signal | 見下節：enqueue、retry、coordinator、member epoch、response、auto-commit、application wait、close 必須形成完整 owner 通知與 deadline 契約 |
| ConsumerMembership | legacy，繼承 AbstractMembershipManager.poll | heartbeatStateChanged 已供 heartbeat 使用；position changes 已通知 Offsets／Fetch | 需自身 readiness：assignment、metadata 解 UUID、subscription/poll gate、reconcile callback／commit completion、close/fenced/stale/rejoin。現有 heartbeat signal 不代表 membership 本身會被排程 |
| TopicMetadata | 全部正常分支 legacy | request append、TimedRequestState、poll prune expiry／send | enqueue、response/failure、連線可用性通知，以及 retry／API 絕對到期；測試等待 metadata、失敗重試、取消、到期與去重 |

主要定位：CoordinatorRequestManager.poll:110、markCoordinatorUnknown:189；AbstractHeartbeatRequestManager.poll:167、resetPollTimer:328；OffsetsRequestManager.poll:244、updateFetchPositionsAndAwaitValidation:407、onUpdate:847、onPositionStateChanged:882、onValidationDependencyChanged:889；FetchRequestManager.createFetchRequestsWithReconnect:83、poll:207、finishFetchDemands:340；AbstractMembershipManager.poll:1489；TopicMetadataRequestManager.poll:86。

## Commit：需要接上的八類觸發

目前 CommitRequestManager.poll:182 的 coordinator unknown、closing、無 pending、有 pending 等分支全為 legacy。已有背景 auto-commit 功能，不能把「尚未 condition 化」寫成「背景提交尚未實作」。

| 依賴 | 目前入口／owner | condition 遷移需做的事 | 必要案例 |
|---|---|---|---|
| 新 request／去重 request | PendingRequests.addOffsetCommitRequest:1478、addOffsetFetchRequest:1490；application event 由 network owner 處理 | enqueue 或重新形成待辦後發布自身 inputChanged；去重後保留正確 completion 與期限 | scheduler 初次 poll 後無強制輪詢，新增 sync／async／fetch request 仍可送出 |
| Coordinator 可用或 fatal | CoordinatorRequestManager.stateChanged | 訂閱共享 signal，先處理 fatal 與 pending lifecycle，再決定是否可送 | unavailable→available；fatal 只完成一次；等待 coordinator 時不因過期 retry 空轉 |
| Member epoch／身份 | onMemberEpochUpdated:735 | 更新 memberInfo 後發布 owner signal，讓待辦重新判斷 | member epoch 改變、離組與 stale epoch 的 request／completion 順序 |
| Response／failure／in-flight 清除 | OffsetCommit handleClientResponse:1076；OffsetFetch onFailure:1295、onSuccess:1357；auto-commit callback | 狀態更新／重新 enqueue 完成後發布，不只喚醒 network | 成功、可重試／不可重試失敗、in-flight 去重；不重複提交 |
| Retry 與 API timeout | request state／retry callbacks、既有 application event reaper | 建立可執行 retry deadline，保留必要 API 絕對到期；與 reaper 協調單次終結 | deadline 到期、取消、late response、coordinator 阻塞；無 spin／無 double completion |
| Auto-commit 時間 | timer reset:758/766、updateTimerAndMaybeCommit:801、maybeAutoCommitWhileAppWaits:236 | timer 更新後讓 manager 重算 condition；只保留當下可執行的 auto-commit deadline | deadline 自動提交、空 offsets 重新計時、in-flight 不重複 |
| Application 開始／結束等待 | RequestManagers application-wait listener:312，目前只喚醒 network | 用 owner-side event／version bridge 讓 network thread 發布 readiness；不能從 application thread 直接發布 thread-confined Signal | 等待開始後可觸發提交；app 已恢復或換一輪 wait 時拒絕過時 snapshot |
| Close | signalClose:215、drainPendingOffsetCommitRequests:774、application close events | 正常排程 close transition 發布通知；保留獨立 pollOnClose 契約 | 不接收新的背景提交；既有待辦在期限內完成或失敗 |

保留 maybeAutoCommitWhileAppWaits 的 wait epoch 雙重檢查，且不可持有 ApplicationPollWait lock 讀取 offsets。既有 CommitRequestManagerTest:1967–2038 已涵蓋背景提交、app active／notified／wait expired、snapshot race、backoff／coordinator、empty offsets／close；這些直接呼叫 legacy poll 的測試不等於 condition 喚醒測試。新測試必須經真實 scheduler，僅靠 publish／時間推進，不能用每輪手動 manager.poll 掩蓋漏通知。

## 已 condition 化者的邊界

Offsets 的 metadata、ApiVersions 與 positions hooks 已接線，retained validation 也已有 retry 與 deadline。onUpdate 仍可能直接推進既有 metadata retry，不能宣稱所有工作只在 poll 執行。應逐類操作檢查 retaining／retiring，而非一律加入新的 signal。

Fetch 現有保留策略只在捕捉到實際 connection backoff 時保留 reconnect demand；其他 preparation 會完成並依賴 app 後續再試。若要移除 application 的短 retry，需同時保留被阻塞的需求，並建立 metadata/leader、connection/API readiness、buffer drain、pause/resume、fetchability、response/in-flight completion 等事件。只加 signal、不保留 demand，無法保證工作會恢復。保留 poll(0) 的第一次嘗試、取消、close、late future 與批次邊界。

Membership 遷移不能任意把所有 reconciliation 移到背景。ApplicationEventProcessor 的 AsyncPollEvent:804 會先 maybeReconcile(true)，再準備 fetch 並標記 reconciliation check complete；需保留 canCommit gate、rebalance callback 與 revoke-before-fetch 順序。RequestManagers.wireMembershipPositionChanges:351 已將 assignment／pending revocation 變化通知 Offsets 和 Fetch。

## Share 與 Streams

| 模式 | Condition managers | Legacy managers | 額外依賴 |
|---|---|---|---|
| Share，四個 | Coordinator、ShareHeartbeat | ShareMembership、ShareConsume | membership 自身 reconcile；ShareConsume 的 member、fetch demand、ack queues、session metadata、response、close，以及 ack retry／expiry／lock renewal |
| Streams，八個 | Coordinator、Offsets、Fetch | Commit、StreamsHeartbeat、StreamsMembership、StreamsTopology、TopicMetadata | Streams heartbeat／membership 的獨立 lifecycle；topology push required、member/coordinator、response、retry 與 throttle |

定位：ShareMembershipManager.poll:177；ShareConsumeRequestManager.poll:150、fetch:448、ack commit:678/750、ackOnClose:826、response handlers:905/1048/1101/1180、close:1351、member epoch:1356；StreamsHeartbeatRequestManager.poll:453；StreamsMembershipManager.poll:1145；StreamsTopologyRequestManager.poll:64、maximumTimeToWait:88、onResponse:119。

StreamsRebalanceData.topologyPushRequired 目前是狀態，並非 ready Signal。拓撲請求須同時滿足 retry 與 throttle，actionable deadline 應遵守兩者的較晚時間，不能取已到期的較早值造成空轉。Share／Streams 尚無本輪正式 broker consume benchmark；共享 heartbeat 的單元測試不能替代這兩種模式的端到端驗證。

## 建議順序與效能驗證

1. **P0：先固定 owner／觸發契約與 scheduler 測試模式。** 每種待辦都回答誰保留它、誰改變它、誰發布、何時到期、誰取消。覆蓋 publish-before-arm、同批延後、late completion、obsolete/cancel/close 與 blocked deadline 不空轉。
2. **P1：先 TopicMetadata，再 Commit，各自獨立比較。** TopicMetadata 範圍較小，適合驗證遷移模式；Commit 行為價值較高，但需處理 app wait 與 snapshot 競態。這是風險與實作範圍排序，不是已量到的 hotspot 排序。
3. **P2：Membership 與持續 Fetch demand。** 先保留 reconciliation／fetch 契約，再考慮縮減 application fallback。Share／Streams 分開遷移與驗證，避免從普通 consumer 結果外推。

「部分遷移限制了效能收益」目前只是待驗證假說。後續每次只改一個 manager 或等待層，記錄各 manager poll 次數、legacy 次數、ready/deadline/transport wake 次數與 empty poll，再比較 CPU/record、吞吐與閒置 CPU。診斷計數與正式量測須分離或控制其成本；不能直接由減少 poll 次數推論 CPU 改善。

本輪 [TreeSet／直接掃描實驗](deadline-tree-vs-scan.md) 固定同一原型，只改 deadline index；結果不支持採用本次直接掃描，也不能用來證明全面 condition 化一定更快。Live 原型與 frozen benchmark 的 no-spin 修正狀態不同，勿將 frozen 實驗的通過結果視為 live 全部行為的驗證。

## 本次交付與限制

九個 Java 檔案新增架構／生命週期註解：[comments.patch](scheduler-documentation/comments.patch)。[詞法比較](scheduler-documentation/comments-only-validation.json) 確認去除註解後 code tokens 不變，Checkstyle／Spotless 通過：[style.log](scheduler-documentation/style.log)。沒有新增功能、更換 live deadline index，亦未為註解重跑功能測試。Benchmark 兩版各 342 測試與既有靜態警告的範圍，請見實驗報告。

較早的 event-only migration 文件只作歷史參考；本清單以目前原始碼為準。例如 BackgroundEventHandler.add 現已在 enqueue 後喚醒 application，不能沿用舊清單稱它尚未接線。
