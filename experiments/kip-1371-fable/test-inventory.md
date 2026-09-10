# KIP-1371 Consumer 測試盤點（test inventory）

日期：2026-09-10。基準：Kafka trunk worktree `/Users/unknowntpo/repo/unknowntpo/kafka/kip-1371-fable`。

本文件彙整三份子代理盤點：

1. clients 單元測試（`clients/src/test/java/org/apache/kafka/clients/consumer/`）。
2. Java 整合測試（`clients/clients-integration-tests/`）與 Scala 整合測試（`core/src/test/scala/integration/kafka/api/`）。
3. Streams JUnit（`streams/src/test`、`streams/integration-tests`）與 ducktape 系統測試（`tests/kafkatest/`）。

目的：在重新設計 consumer 事件迴圈（`ConsumerNetworkThread` / `ApplicationEventHandler` / poll 路徑）之前，先知道哪些測試真的驅動迴圈、哪些只是 mock 驗證、哪些情境根本沒有測試。

名詞：
- **consumer**：`group.protocol=consumer` 的 `AsyncKafkaConsumer`（KIP-848 新協定）。
- **share**：`KafkaShareConsumer` / `ShareConsumerImpl`（KIP-932）。
- **streams**：`group.protocol=streams`，`StreamThread.setupMainConsumer` 直接 `new AsyncKafkaConsumer<>(...)` 並帶入 `StreamsRebalanceData`（`streams/src/main/java/org/apache/kafka/streams/processor/internals/StreamThread.java` 約 L592）。
- **classic-reference**：舊 `ClassicKafkaConsumer` / `ConsumerCoordinator` 路徑，作為行為對照。

---

## 0. 測試 harness 的真實狀況（最重要的發現）

| 類別 | 路徑（相對 `clients/src/test/java/org/apache/kafka/clients/consumer/`） | 是否跑真實背景執行緒 |
|---|---|---|
| `ConsumerNetworkThreadTest`（14） | `internals/ConsumerNetworkThreadTest.java` | **唯一直接建構真實 `ConsumerNetworkThread`** 並呼叫 `runOnce()` 的類別；但所有協作者（`NetworkClientDelegate`、`RequestManagers`、`ApplicationEventProcessor`、`CompletableEventReaper`、`AsyncConsumerMetrics`）都是 Mockito mock，沒有 MockClient 流量。 |
| `AsyncKafkaConsumerTest`（112） | `internals/AsyncKafkaConsumerTest.java` | **從不啟動背景執行緒**。所有 `newConsumer(...)` 都注入 `applicationEventHandler = mock(ApplicationEventHandler.class)`（L182），另 mock `FetchCollector`、`ConsumerMetadata`、`CompletableEventReaper`。「async 行為」全靠 `verify(applicationEventHandler).add(...)` 與手動完成 future。 |
| `KafkaConsumerTest`（135） | `KafkaConsumerTest.java` | **唯一讓真實 `AsyncKafkaConsumer` + 真實 `ConsumerNetworkThread` 端到端跑起來的地方**，透過 package-private 建構子（helper 約 L3564）搭配 `MockClient`；`GROUP_PROTOCOL_CONFIG` 由 `GroupProtocol` 參數決定。 |
| `ShareConsumerImplTest`（39） | `internals/ShareConsumerImplTest.java` | mock `ApplicationEventHandler`（L125），同 `AsyncKafkaConsumerTest` 模式。 |
| `KafkaShareConsumerTest`（5） | `KafkaShareConsumerTest.java` | 真實 `KafkaShareConsumer` over `MockClient`，是 share 版的 `KafkaConsumerTest`，但只有 5 個測試。 |
| `ApplicationEventProcessorTest`（40） | `internals/events/ApplicationEventProcessorTest.java` | 真實 `ApplicationEventProcessor`，request manager 全 mock。 |
| `NetworkClientDelegateTest`（14） | `internals/NetworkClientDelegateTest.java` | 真實 `NetworkClientDelegate` over `MockClient`。 |
| `CommitRequestManagerTest`、`ConsumerHeartbeatRequestManagerTest` | `internals/…` | 各只在 `RealBootstrapDnsResolution` 測試裡建一個真實 `NetworkClientDelegate`。 |
| `FetchRequestManagerTest`、`ShareConsumeRequestManagerTest` | `internals/…` | 真實 manager + `MockClient`，無 network thread。 |
| `OffsetsRequestManagerTest`、`CoordinatorRequestManagerTest`、`TopicMetadataRequestManagerTest`、`Streams*RequestManagerTest`、所有 `*MembershipManagerTest` | `internals/…` | 純 Mockito，沒有 client。 |

沒有任何地方 `mock(ConsumerNetworkThread.class)` 或 `spy(ConsumerNetworkThread…)`：thread 要嘛是真的（`ConsumerNetworkThreadTest`、`KafkaConsumerTest` 的 CONSUMER 跑法），要嘛整個被 mock `ApplicationEventHandler` 繞過。

`KafkaConsumerTest` 的 GroupProtocol 切分：
- `@EnumSource(GroupProtocol…)` 共 **73** 處，其中 **40** 處是 `names = "CLASSIC"`（CONSUMER 直接跳過），約 33 處雙協定。約 **90 個方法**會在 CONSUMER 下帶真實背景執行緒跑。
- CLASSIC-only 代表：`testClose*` 多數變體（除 idempotent）、`testGracefulClose`、`testCloseTimeout`、`testCloseNoWait`、`testCloseInterrupt`、`testChangingRegexSubscription`、`testRegexSubscription`、`testWakeupWithFetchDataAvailable`、`testReturnRecordsDuringRebalance`、`testPauseFlagPreservedForRetainedPartitionAcrossRebalance`、`testPollThrowsInterruptExceptionIfInterrupted`。
- 雙協定代表：`testConstructorFailsOnNetworkClientConstructorFailure`、`testConsumerBootstrapResolutionExceptionPropagatedToPoll`、`testConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero`、`testPollSendsRequestToJoin`、`testPreventMultiThread`、`testResetToCommittedOffset`、`testResetUsingAutoResetPolicy`、`testCurrentLag*`、`testManualAssignmentChangeWithAutoCommitEnabled/Disabled`、`testPause`、`testCloseShouldBeIdempotent`、`testPollAuthenticationFailure`、`testSubscriptionOnInvalidTopic`、`testMissingOffsetNoResetPolicy`、`testFetchStableOffsetThrowInPoll`。
- 沒有 `assumeTrue/assumeFalse`；runtime 分支用 `if (groupProtocol == GroupProtocol.CLASSIC)`（L3005、3066、3102、3128、3157、3183、3630）。

整合層的參數化方式：
- Java `clients-integration-tests` 用 `@ClusterTest`（`ClusterTestExtensions`），**沒有** `@ParameterizedTest`。「雙協定」是方法複製：`testClassicConsumerX()` / `testAsyncConsumerX()` 各自 `@ClusterTest`，委派給 private `testX(GroupProtocol)`。原始 `@ClusterTest` 數約為邏輯測試數的 2 倍。
- Scala `core` 用 `IntegrationTestHarness` → `QuorumTestHarness` + `@ParameterizedTest @MethodSource`，helper 在 `core/src/test/scala/integration/kafka/server/QuorumTestHarness.scala:407-425`（`getTestGroupProtocolParametersAll` = classic+consumer、`...ClassicGroupProtocolOnly`、`...ConsumerGroupProtocolOnly`）。全部是 embedded KRaft，沒有 ZK。
- Streams integration 主要兩種：`@CsvSource({"false, false", ...})` → `(useNewProtocol/streamsProtocolEnabled, withHeaders)`；或 `@MethodSource` / `@ValueSource(strings = {"classic","streams"})`。
- ducktape：`tests/kafkatest/services/kafka/consumer_group.py` 定義 `all_group_protocols=[classic, consumer]`，**沒有 streams**；streams 系統測試用原始字串 `group_protocol=["classic","streams"]`，經 `base_streams_test.get_configs(group_protocol=...)` 寫成 `group.protocol=`。

---

## 1. 矩陣：use case × consumer 類型

欄位說明：每格列「代表測試類別（數量）→ 代表方法」。層次前綴：**U** = clients 單元測試、**I** = clients-integration-tests / core Scala、**S** = streams JUnit、**D** = ducktape。數量為各來源計得的 `@Test`/`@ParameterizedTest`/`@ClusterTest`/`def test_` 數。

| # | Use case | consumer（AsyncKafkaConsumer） | share | streams | classic-reference |
|---|---|---|---|---|---|
| 1 | 建構 / 啟動失敗 / 資源清理 | U `ConsumerNetworkThreadTest`(14)：`testStartupAndTearDown`、`testEnsureCloseStopsRunningThread`、`testNetworkClientDelegateInitializeResourcesError`、`testRequestManagersInitializeResourcesError`、`testProcessEventFailureCompletesFutureExceptionally`；U `ApplicationEventHandlerTest`(5)：`testFailOnInitializeResources`、`testDelayInInitializeResources`、`testInterruptInInitializeResources`、`testAddThrowsWhenBackgroundThreadDead`；U `RequestManagersTest`(2)；U `KafkaConsumerTest`(135)：`testConstructorClose`、`testConstructorFailsOnNetworkClientConstructorFailure`、`testMetricsRemovedOnClose`；U `AsyncKafkaConsumerTest`(112)：`testFailOnClosedConsumer`、`testGroupMetadataAfterCreation*`；I 薄：`ConsumerIntegrationTest.testAsyncConsumerWithConsumerProtocolDisabled`（在 poll 才失敗）、`PlaintextConsumerTest.test*ConsumingWithNullGroupId`、`test*NullGroupIdNotSupportedIfCommitting`；D 幾乎無（`verifiable_consumer.handle_startup_complete` 隱含） | U `ShareConsumerImplTest`(39)：`testSuccessfulStartupShutdown`、`testFailConstructor`、`testConstructorFailsOnNetworkClientConstructorFailure`、`testGroupIdNull/Empty/OnlyWhitespaces`、`testMetricsRemovedOnClose`；U `KafkaShareConsumerTest`(5)：`testShareConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero`；I `ShareConsumerTest.testPollNoSubscribeFails`；D `share_consumer_test.test_share_single_topic_partition` | U `RequestManagersTest.testStreamsGroupRequestManagersAndListenersWired`；S `KafkaStreamsTest`(78) 建構/啟動/close/狀態轉移但**實質 classic-only**（僅 2 處 `GROUP_PROTOCOL_CONFIG`）；S `StreamsConfigTest`(176/3) 只驗 config | U `KafkaConsumerTest` 同左（雙協定與 CLASSIC-only 方法） |
| 2 | assign / subscribe / pattern（RE2J） | U `AsyncKafkaConsumerTest`：`testSubscribePatternGeneratesEvent`、`testSubscribeToRe2JPatternGeneratesEvent`、`testSubscribeToRe2JPatternThrowsIfNoGroupId`、`testSubscribePatternAgainstBrokerNotSupportingRegex`、`testUnsubscribeWithPendingAssignmentEvent`；U `SubscriptionStateTest`(68)：`testSubscribeToRe2JPattern`；U `ConsumerHeartbeatRequestManagerTest`(31)：`testRegexInHeartbeatLifecycle`、`testRegexInJoiningHeartbeat`；U `ApplicationEventProcessorTest`(40)：`testUpdatePatternSubscription*`；U `KafkaConsumerTest`：`testSubscription*`、`testAssignOnNull/EmptyTopicPartition`、`testSubscribeToRe2jPatternNotSupportedForClassicConsumer`；I `PlaintextConsumerAssignTest`(18) 全部；I `PlaintextConsumerSubscriptionTest`(13+18+9)：`test*PatternSubscription`、`*ExpandingTopicSubscriptions`、`*ShrinkingTopicSubscriptions`、RE2J 區塊 CONSUMER-only（`testAsyncConsumerRe2JPatternSubscription`、`...Re2JPatternSubscriptionFetch`、`...Re2JPatternExpandSubscription`、`testTopicIdSubscriptionWithRe2JRegexAndOffsetsFetch`、`testRe2JPatternSubscriptionInvalidRegex`）；D `consumer_test.test_valid_assignment`、`test_group_consumption`；D 無 pattern-subscription 系統測試 | U `ShareConsumerImplTest`：`testSubscribeGeneratesEvent`、`testSubscribeToNullTopicCollection`、`testUnsubscribeGeneratesUnsubscribeEvent`；無 RE2J 覆蓋 | 無 RE2J 覆蓋；S `StreamThreadTest.testStreamsRebalanceDataWithStreamsProtocol/WithClassicProtocol/WithExtraCopartition`（topology → `StreamsRebalanceData`） | U `KafkaConsumerTest.testRegexSubscription`、`testChangingRegexSubscription`（CLASSIC-only）；I Scala `PlaintextConsumerAssignorsTest`(8)：5 ClassicOnly（`testRoundRobinAssignment`、`testMultiConsumerStickyAssignor` 等）、2 ConsumerOnly（`testRemoteAssignorInvalid`、`testRemoteAssignorRange`） |
| 3 | join / reconcile / rebalance / fence / leave | U `ConsumerMembershipManagerTest`(89)：26 個 `testFenc*`/`testLeav*`/`testReconcil*`（`testFencingWhenStateIsStable/Reconciling/PrepareLeaving/Leaving`、`testLeaveGroupEpochOnClose`、`testLeaveGroupDuringReconciliationThenRejoin`、`testTransitionToFenced/Fatal/StaleMarksPendingRevocationBeforeSignalingPartitionsLost`）；U `ConsumerHeartbeatRequestManagerTest`(31)、`AbstractHeartbeatRequestManagerTest`(9)：`testPollTimerExpiration*`、`testHeartbeatOnStartup`；U `CoordinatorRequestManagerTest`(10)：`testBackoffAfterRetriableFailure/FatalError`；I `PlaintextConsumerTest.test*GroupConsumptionWithTwoMembers`、`test*PauseStateNotPreservedByRebalance`、`testAsyncStaticMemberCloseWithLeaveGroupTriggersRebalance`（CONSUMER-only）；I `PlaintextConsumerPollTest.test*MaxPollIntervalMs*`；I `ConsumerBounceTest.java.test*CloseDuringRebalance`；I `ConsumerIntegrationTest.testSingleCoordinatorOwnershipAfterPartitionReassignment`；D `consumer_test.test_consumer_bounce`、`test_static_consumer_persisted_after_rejoin`、`test_fencing_static_consumer`；D `consumer_protocol_migration_test`(3) | U `ShareMembershipManagerTest`(57)：19 個 fence/leave/reconcile（含 `testMemberKeepsUnresolvedAssignmentWaitingForMetadataUntilResolved`）；U `ShareHeartbeatRequestManagerTest`(14)：`testHeartbeatOnStartup`、`testPollTimerExpiration`；I `ShareConsumerTest`(41+21+8)：`testShareConsumerAfterCoordinatorMovement`；D `share_consumer_test.test_share_consumer_bounce` | U `StreamsMembershipManagerTest`(98)：37 個 fence/leave/reconcile（`testReconcilingEmptyToSingleActiveTask`、`…ActiveTaskToStandbyTask`、`…ActiveTaskToWarmupTask`、`testOnPollTimerExpired(WhenInFatal/WhenInUnsubscribe)`）；U `StreamsGroupHeartbeatRequestManagerTest`(76)：`testSendingLeaveHeartbeatIfPollTimerExpired`、`testSendLeaveHeartbeatForStaticMember`、`testResetPollTimerWhenExpired`；U `StreamsGroupTopologyDescriptionRequestManagerTest`(13)；S `DefaultStreamsRebalanceListenerTest`(12/1)：`testOnTasksRevoked(State)`、`testOnTasksAssigned`、`testOnAllTasksLost`；S `StreamThreadTest`：`shouldNotEnforceRebalanceOnShutdownRequestUnderStreamsProtocol`、`shouldNotEnforceRebalanceOnTaskCorruptedExceptionUnderStreamsProtocol`；S `KafkaStreamsStaticMemberIntegrationTest`(6)、`RebalanceProtocolMigrationIntegrationTest`(2)；D `streams_static_membership_test.test_fencing_static_streams_member`、`streams_standby_replica_test.test_standby_tasks_rebalance` | U `ConsumerCoordinatorTest`(156)、`AbstractCoordinatorTest`(50)：`testWakeupDuringJoin`、`testRetainMemberIdAfterJoinGroupDisconnect`、`testHeartbeatRebalanceInProgressResponseDuringRebalancing`、`testPendingMemberShouldLeaveGroup`、`testLeaveGroupOnClose`；I Scala `ConsumerBounceTest.testRollingBrokerRestartsWithSmallerMaxGroupSizeConfigDisruptsBigGroup`；D `consumer_test.test_static_consumer_bounce_with_eager_assignment`（classic-only）、`consumer_rolling_upgrade_test` |
| 4 | position / seek / reset / OffsetsForLeaderEpoch 驗證 | U `OffsetsRequestManagerTest`(28)：`testResetPositionsSuccess_*`、`testValidatePositions*`、`testUpdatePositionsWithCommittedOffsets*`；U `SubscriptionStateTest`：`testSeekUnvalidated*`、`testTruncationDetectionWith/WithoutResetPolicy`、`testOffsetResetWhileAwaitingValidation`；U `AsyncKafkaConsumerTest`：`testSeekToBeginning/End(WithException)`、`testBeginningOffsetsWithZeroTimeout`、`testOffsetsForTimesWithZeroTimeout`；U `ApplicationEventProcessorTest`：`testResetOffsetEvent`、`testSeekUnvalidatedEvent`、`testUpdateFetchPositionsWithFetchCommittedOffsetsTimeout`；U `KafkaConsumerTest`（雙協定）：`testResetToCommittedOffset`、`testResetUsingAutoResetPolicy`、`testResetUsingDurationBasedAutoResetPolicy`、`testOffsetIsValidAfterSeek`、`testMissingOffsetNoResetPolicy`；I `PlaintextConsumerTest.test*Seek`、`*AutoOffsetReset`、`*EndOffsets`、`*FetchOffsetsForTime`、`*PositionRespectsTimeout`；I `PlaintextConsumerFetchTest.test*FetchOutOfRangeOffsetResetConfig{Earliest,Latest,ByDuration}`；I `ConsumerIntegrationTest.testLeaderEpoch`；I `ConsumerWithLegacyMessageFormatIntegrationTest`(4)；D 無專屬（最近的是 `truncation_test.test_offset_truncate`） | 無（share 無 seek/position 概念） | 無 | U `OffsetFetcherTest`、`OffsetForLeaderEpochClientTest`；I Scala `AuthorizerIntegrationTest.testOffsetsForLeaderEpochClusterPermission` |
| 5 | poll(0) / long poll / fetch / buffer / pause-resume | U `AsyncKafkaConsumerTest`：`testEnsurePollEventSentOnConsumerPoll`、`testLongPollWaitIsLimited`、`testPollDoesNotAddNewAsyncPollEventWhenOneIsAlreadyInFlight`、`testPollWithManualAssignmentDoesNotBusyLoop`、`testInflightPollResubmittedAfterCompletionWithEmptyBuffer`、`testPollSurfacesInflightPollErrorAndResumes`、`testBufferedRecordsReturnedWithoutResubmittingPollEvent`、`testPollWaitsForReconciliationCheckComplete`；U `FetchBufferTest`(5)、`FetchCollectorTest`(22)、`FetchRequestManagerTest`(102)：`testFetchOnPausedPartition`、`testEmptyFetchResponseWakesUpBuffer`、`testMaximumTimeToWaitBoundedWhenPartitionsSkippedDueToBackoff`；U `KafkaConsumerTest`：`testPause`、`testPauseFlagPreservedForRetainedPartitionOnManualAssignmentChange`、`testPollWithNoSubscription/EmptySubscription/EmptyUserAssignment`、`testPollIdleRatio(Zero)`、`verifyPollTimesOutDuringMetadataUpdate`；I `PlaintextConsumerPollTest`(24)：`test*PollEventuallyReturnsRecordsWithZeroTimeout`、`test*NoOffsetForPartitionExceptionOnPollZero`、`test*MaxPollRecords`、`test*RecoveryOnPollAfterDelayedRebalance`；I `PlaintextConsumerFetchTest`(18) 全部；I `PlaintextConsumerTest.test*PartitionPauseAndResume`、`test*StallBetweenPoll`、`test*OffsetRelatedWhenTimeoutZero`；D `consume_bench_test`(6)、`compression_test`；D 無 pause/resume | U `ShareFetchBufferTest`(8)、`ShareFetchCollectorTest`(8)、`ShareConsumeRequestManagerTest`(74)；U `ShareConsumerImplTest`：`testShouldSendOneShareFetchEventPerPoll`、`testPollDoesNotAddNewSharePollEventWhenOneIsAlreadyInFlight`、`testExplicitModeRenewAndAcknowledgeOnPoll`；I `ShareConsumerDeliveryTest`(21+5)：`testPollInBatchOptimizedMode`、`testPollInRecordLimitMode`；I `ShareConsumerTest` 多處 `Duration.ZERO`（L720、792、1519、1525、1563）；D `share_consume_bench_test`(4) | S `StreamThreadTest.testStreamsProtocolRunOnceWithoutProcessingThreads` / `WithProcessingThreads`；D `streams_smoke_test.test_streams`（`group_protocol=["streams"]` 區塊帶 `enable_assignment_batching`） | U `FetcherTest`(82)；U `KafkaConsumerTest.testPauseFlagPreservedForRetainedPartitionAcrossRebalance`、`testReturnRecordsDuringRebalance`（CLASSIC-only） |
| 6 | metadata / DNS / disconnect / reconnect / retry | U `NetworkClientDelegateTest`(14)：`testBootstrapResolutionExceptionPropagatedViaErrorEventOnce`、`testPollWithOnClose`、`testCheckDisconnectsWithOnClose`；U `TopicMetadataRequestManagerTest`(7)：`testExpiringRequest`；U `CommitRequestManagerTest`(53)：`testMaximumTimeToWaitDoesNotSpinDuringRealBootstrapDnsResolution`、`testCommitAsyncFailsWithRetriableOnCoordinatorDisconnected`、`testEnsureBackoffRetryOnOffsetCommitRequestTimeout`；U `ConsumerHeartbeatRequestManagerTest`：同名 DNS 測試、`testDisconnect`、`testMaximumTimeToWaitWhenFencedWaitsRetryBackoff`；U `FetchRequestManagerTest`：`testFetchDisconnected*`；U `ConsumerMetadataTest`；U `KafkaConsumerTest`（雙協定）：`testConsumerBootstrapResolutionExceptionPropagatedToPoll`、`testConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero`；I `PlaintextConsumerTest.test*CoordinatorFailover`、`test*PartitionsFor*`、`test*ListTopics`、`test*StaticConsumerDetectsNewPartitionCreatedAfterRestart`；I `ConsumerBounceTest.java`(11+2+1)：`test*ConsumptionWithBrokerFailures`、`test*SeekAndCommitWithBrokerFailures`、`test*SubscribeWhenTopicUnavailable`；I `ClientRebootstrapTest`(9)：`testConsumerRebootstrap`、`...RebootstrapDisabled`、`testRebootstrapOnMetadataClusterCheckFail`；I `ConsumerTopicCreationTest`(4)；D `consumer_test.test_broker_rolling_bounce`（`metadata.recovery.strategy=none`）、`test_broker_failure` | U `ShareConsumeRequestManagerTest`：`testFetchDisconnected`、`testServerDisconnectedOnShareAcknowledge`、`testWhenLeadershipChangedAfterDisconnected`；U `KafkaShareConsumerTest.testShareConsumerBootstrapResolutionExceptionPropagatedToPoll`；I `ShareConsumerTest.testShareConsumerAfterCoordinatorMovement`、`testLeaderRestartWithoutLeadershipChange{ExplicitAcknowledgementSync,Async,ImplicitAcknowledgement}`；D `share_consumer_test.test_broker_rolling_bounce`、`test_broker_failure` | U `StreamsGroupHeartbeatRequestManagerTest.testCoordinatorDisconnectFailureWhileSending`；U `StreamsGroupTopologyDescriptionRequestManagerTest.testNetworkExceptionLeavesFlagSetWithBackoff`；S `StreamThreadTest.testStreamsProtocolMissingSourceTopicRecovery`、`testStreamsProtocolIncorrectlyPartitionedTopics`；D `streams_broker_bounce_test`(3)、`streams_broker_down_resilience_test`(4)（均 `["classic","streams"]`） | U `ConsumerNetworkClientTest`(19)：`testDisconnectWithUnsentRequests`、`testDisconnectWithInFlightRequests`、`testDisconnectWakesUpPoll`；U `AbstractCoordinatorTest`：`testCoordinatorDiscoveryExponentialBackoff`、`testBackoffAndRetryUponRetriableError`；I `ClientRebootstrapTest.testClassicConsumerRebootstrap` |
| 7 | sync / async commit、auto-commit | U `AsyncKafkaConsumerTest`：`testCommitAsyncWithNullCallback`、`testCommitAsyncUserSuppliedCallback*`、`testCommitSyncAwaitsCommitAsyncCompletionWith*Offsets`、`testCommitSyncAllConsumed`、`testAutoCommitSyncDisabled`、`testInterceptorAutoCommitOnClose`；U `CommitRequestManagerTest`(53)：`testPollEnsureAutocommitSent`、`testAutocommitEnsureOnlyOneInflightRequest`、`testAutoCommitBeforeRevocationNotBlockedByAutoCommitOnIntervalInflightRequest`、`testAutoCommitSyncBeforeRevocationRetriesOnRetriableAndStaleEpoch`、`testOffsetCommitSyncFailedWithRetriableThrowsTimeoutWhenRetryTimeExpires`；U `KafkaConsumerTest`（雙協定）：`testMeasureCommitSyncDuration(OnFailure)`、`testCommitsFetchedDuringAssign`、`testCommittedThrowsTimeoutExceptionForNoResponse`；I `PlaintextConsumerCommitTest`(25)：`test*AsyncCommit`、`test*CommitSpecifiedOffsets`、`test*AutoCommitOnRebalance`、`test*AutoCommitIntercept`、`test*CommitMetadata`、`test*PositionAndCommit`；I `PlaintextConsumerAssignTest.test*AssignAndCommit{Async,Sync}NotCommitted`；D `consumer_test.test_consumer_failure(enable_autocommit=…)`、`transactions_test.test_transactions` | U `KafkaShareConsumerTest.testVerifyFetchAndCommitSyncImplicit`；U `ShareConsumeRequestManagerTest` implicit/explicit acknowledge commit 路徑 | S `EosIntegrationTest`(1/14)（`Arguments.of("classic"/"streams", …)`，eos-under-failure / commit） | U `ConsumerCoordinatorTest`：`testCommitOffsetAsyncWithDefaultCallback`、`testAsyncCommitCallbacksInvokedPriorToSyncCommitCompletion`、`testCommitOffsetSyncCallbackWithNonRetriableException`；D `group_mode_transactions_test`（未參數化） |
| 8 | callback 執行緒 / 重入 / acknowledgement | U `AsyncKafkaConsumerTest`：**`testCommitInRebalanceCallback`（L634）**、`testEnsureCallbackExecutedByApplicationThread`、`testEnsureCommitSyncExecutedCommitAsyncCallbacks`、`testEnsurePollExecutedCommitAsyncCallbacks`、`testEnsureShutdownExecutedCommitAsyncCallbacks`、`testListenerCallbacksInvoke`；U `OffsetCommitCallbackInvokerTest`(4)；U `SubscriptionStateTest.testAssignedPartitionsAwaitingCallback*`；U `RebalanceCallbackMetricsManagerTest`；I `PlaintextConsumerCallbackTest`(35)：從 `onPartitionsAssigned/Revoked` 內呼叫 `assign`/`assignment`/`beginningOffsets`/`position`/`seek`/`pause`/`commitSync`/`groupMetadata`（`test*RebalanceListenerAssignOnPartitionsAssigned`、`test*AwareSeekAndCommitOnPartitionsAssigned`、`test*AwarePauseStatePersistsAfterAssigned`、`testOnPartitionsAssignedCalledWithNewPartitionsOnlyForAsyncConsumer`、`test*RebalanceConsumerExpiredAfterAssignedCallback`）；I `ConsumerIntegrationTest.testFetchPartitionsAfterFailedListener*`、`testFetchPartitionsWithAlwaysFailedListener*`；D 透過 `verifiable_consumer` 的 `handle_partitions_revoked/assigned` 斷言 | U `AcknowledgementCommitCallbackHandlerTest`(4)、`AcknowledgementsTest`(20)；U `ShareConsumerImplTest.testAcknowledgementCommitCallbackRegistrationEvent(_Null)`；U `ShareConsumeRequestManagerTest.testAcknowledgementCommitCallbackMultiplePartitionCommitAsync`；I `ShareConsumerCallbackTest`(8)、`ShareConsumerRenewTest`(4+1)、`ShareConsumerDLQTest`(10+3)、`ShareConsumerTest` explicit/implicit ack 家族；D `share_consumer_dlq_test`(8)、`verifiable_share_consumer.handle_offsets_acknowledged` | U `StreamsRebalanceListenerInvokerTest`(16)：`testInvokeTasksAssigned/TasksRevoked/AllTasksLostWithWakeupException` 與 `…WithInterruptException`；S `DefaultStreamsRebalanceListenerTest`：`testOnTasksAssignedWithException`、`testOnAllTasksLostRecordsMetricsEvenWithException`；S `StreamsRebalanceListenerTest`(10) 為 classic listener | U `ConsumerCoordinatorTest`：`testWakeupFromAssignmentCallback`、`testRevokeExceptionThrownFirstNonBlockingSubCallbacks`、`testOnAssignmentExceptionThrownFirstNonBlockingSubCallbacks`；U `AbstractCoordinatorTest.testWakeupInOnJoinComplete`；I `PlaintextConsumerCallbackTest.testOnPartitionsAssignedCalledWithNewPartitionsOnlyForClassic{Cooperative,Eager}` |
| 9 | timeout / wakeup / interrupt / fatal | U `WakeupTriggerTest`(21)：`testWakeupFromFetchAction`、`testSettingActiveFutureAfterWakeupShouldThrow`、`testDisableWakeupWith/WithoutPendingTask`；U `AsyncKafkaConsumerTest`：`testWakeupBeforeCallingPoll`、`testWakeupAfterEmpty/NonEmptyFetch`、`testWakeupWhileWaitingOnReconciliationCheck`、`testWakeupCommitted`、`testNoWakeupInCloseCommit`、`testPollThrowsInterruptExceptionIfInterrupted`、`testBeginningOffsetsTimeoutOnEventProcessingTimeout`、`testBackgroundError`、`testMultipleBackgroundErrors`、`testProcessBackgroundEventsWithInitialDelay/WithoutDelay/TimesOut`；U `KafkaConsumerTest`：`testOffsetsForTimesTimeout`/`testBeginningOffsetsTimeout`/`testEndOffsetsTimeout`、`testPollAuthenticationFailure`（雙協定）；I `PlaintextConsumerTest.test*PositionRespectsTimeout`、`test*PositionRespectsWakeup`、`test*PositionWithErrorConnectionRespectsWakeup`；I `PlaintextConsumerCommitTest.test*AutoCommitOnCloseAfterWakeup`；I `ConsumerBounceTest.test*ConsumerReceivesFatalExceptionWhenGroupPassesMaxSize`；D `consumer_test.test_consumer_failure(clean_shutdown=False)`、`quota_test.test_quota`；D 無 wakeup 測試 | U `WakeupTriggerTest.testWakeupFromShareFetchAction`；U `ShareConsumerImplTest`：`testWakeupBeforeCallingPoll`、`testWakeupAfterEmpty/NonEmptyFetch`、`testBackgroundError`、`testProcessBackgroundEvents*`；I `ShareConsumerTest.testPollThrowsInterruptExceptionIfInterrupted`、`testWakeupWithFetchedRecordsAvailable`、`testAcquisitionLockTimeoutOnConsumer`；I `ShareConsumerCallbackTest.testAcknowledgementCommitCallbackCallsShareConsumerWakeup`；D `share_consumer_test.test_share_consumer_failure` | U `StreamsRebalanceListenerInvokerTest` wakeup/interrupt 例外傳播；S `StreamsUncaughtExceptionHandlerIntegrationTest`(0/8)：`shouldShutdownClient`、`shouldReplaceThreads`、`shouldEmitSameRecordAfterFailover`；D `streams_smoke_test`/`streams_relational_smoke_test`（`crash` 軸） | U `KafkaConsumerTest.testPollThrowsInterruptExceptionIfInterrupted`、`testCloseInterrupt`、`testWakeupWithFetchDataAvailable`（CLASSIC-only）；U `AbstractCoordinatorTest`：`testWakeupFromEnsureCoordinatorReady`、`testWakeupAfterJoinGroupSent/Received`、`testWakeupAfterSyncGroupSent/Received`、`testReturnUponRetriableErrorAndExpiredTimer` |
| 10 | close / pending work / late responses / 資源釋放 | U `CompletableEventReaperTest`(5)：`testExpired`、`testCompleted`、`testCompletedAndExpired`、`testIncompleteQueue`、`testIncompleteTracked`；U `AsyncKafkaConsumerTest`：`testCloseLeavesGroup`（參數化 `timeoutMs`）、`testCloseLeavesGroupDespiteOnPartitionsLostError`、`testCloseLeavesGroupDespiteInterrupt`、`testCloseRunsRevocationCallbackAndSendsLeaveGroupEventOnInterrupt`、`testCloseAwaitPendingAsyncCommitIncomplete/Complete`、`testReaperInvokedInClose/InUnsubscribe/InPoll`；U `ConsumerNetworkThreadTest`：`testCleanupInvokesReaper`、`testRunOnceInvokesReaper`、`testSendUnsentRequests`；U `CommitRequestManagerTest`：`testSignalClose`、`testPollWithClosingAndPendingRequests`；U `KafkaConsumerTest`（雙協定）：`testCloseShouldBeIdempotent`、`testMetricsRemovedOnClose`、`testClosingConsumerUnregistersConsumerMetrics`；I `PlaintextConsumerCloseTest`(4)：`test*CloseWithDefaultTakesAtLeastFetchMaxWaitMs`、`test*CloseWithTimeoutIgnoresFetchMaxWaitMs`；I `PlaintextConsumerCommitTest.test*AutoCommitOnClose`、`testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose`、`testCommitAsyncCompletedBeforeConsumerCloses`、`testCommitAsyncCompletedBeforeCommitSyncReturns`（後三者 CONSUMER-only，TODO 註明 classic 未修）；I `ConsumerBounceTest.test{Classic,Async}Close`、`test*CloseDuringRebalance`；I `PlaintextConsumerTest.test*CloseOnBrokerShutdown`、`test*UnsubscribeDoesNotCommitOffsetsWithAutoCommitEnabled`；I `GroupAuthorizerIntegrationTest.test*ConsumeCloseWithGroupPermission`；D `consumer_test.test_consumer_bounce(clean_shutdown=…, bounce_mode=…)`、`verifiable_consumer.handle_shutdown_complete/handle_kill_process` | U `ShareConsumerImplTest`：`testCloseWithInvalidTopicException`、`testCloseWithTopicAuthorizationException`、`testStopFindCoordinatorOnClose`、`testCloseWithBackgroundQueueErrorsAfterUnsubscribe`、`testVerifyApplicationEventOnShutdown`；U `ShareFetchBufferTest.testCloseClearsPendingAcknowledgementFetches`；U `KafkaShareConsumerTest.testVerifyFetchAndCloseImplicit`；I `ShareConsumerTest.testConsumerCloseOnBrokerShutdown`、`testConsumerCloseInGroupSequential`、`testExplicitAcknowledgeReleaseClose`；I `ShareConsumerCallbackTest.testAcknowledgementCommitCallbackOnClose`；D `share_consumer_test.test_share_consumer_bounce` | U `StreamsGroupHeartbeatRequestManagerTest.testPollOnCloseWhenIsLeaving/IsNotLeaving/WhenStaticMemberIsLeaving`；S `KafkaStreamsCloseOptionsIntegrationTest`(7/1)：`testCloseOptionsLeaveGroupStreamsProtocol`、`testCloseOptionsDefaultStreamsProtocol`、`testCloseOptionsRemainInGroupStreamsProtocol`、`testStaticMemberCloseUsesStaticLeaveEpochStreamsProtocol`；S `KafkaStreamsStaticMemberIntegrationTest.testStaticMemberCloseWithLeaveGroupTriggersRebalanceStreamsProtocol`；S `StreamThreadTest.shouldRouteDefaultToConsumerDefaultForStreamsProtocol`、`shouldRouteRemainInGroupToRemainInGroupForStreamsProtocol`；D `streams_static_membership_test`（streams-only 三個）；D `streams_shutdown_deadlock_test`(1) 為唯一 shutdown-deadlock 覆蓋但 **classic-only** | U `KafkaConsumerTest`：`testGracefulClose`、`testCloseTimeout`、`testCloseTimeoutDueToNoResponseForCloseFetchRequest`、`testCloseNoWait`、`testCloseInterrupt`、`testClassicConsumerCloseRunsRevocationCallbackAndAttemptsLeaveGroupWhenInterrupted`（CLASSIC-only）；U `ConsumerCoordinatorTest`：`testCloseDynamicAssignment`、`testCloseManualAssignment`、`testCloseCoordinatorNotKnownNoCommits/WithCommits`、`testCloseTimeout/MaxWaitCoordinatorUnavailableForCommit`、`testCloseNoResponseForCommit`、`testCloseNoResponseForLeaveGroup`；S `KafkaStreamsCloseOptionsIntegrationTest.testCloseOptionsRemainInGroupClassicProtocol`、`testCloseOptionsDefaultClassicProtocol` |

### 1.1 各層測試類別數量總表

clients 單元（`clients/src/test/java/org/apache/kafka/clients/consumer/`）：

| 類別 | 數量 | 類別 | 數量 |
|---|---|---|---|
| `KafkaConsumerTest` | 135 | `ShareConsumerImplTest` | 39 |
| `AsyncKafkaConsumerTest` | 112 | `KafkaShareConsumerTest` | 5 |
| `ConsumerNetworkThreadTest` | 14 | `ShareMembershipManagerTest` | 57 |
| `ApplicationEventProcessorTest` | 40 | `ShareHeartbeatRequestManagerTest` | 14 |
| `ApplicationEventHandlerTest` | 5 | `ShareConsumeRequestManagerTest` | 74 |
| `NetworkClientDelegateTest` | 14 | `ShareFetchBufferTest` / `ShareFetchCollectorTest` | 8 / 8 |
| `ConsumerMembershipManagerTest` | 89 | `AcknowledgementsTest` / `AcknowledgementCommitCallbackHandlerTest` | 20 / 4 |
| `ConsumerHeartbeatRequestManagerTest` | 31 | `StreamsMembershipManagerTest` | 98 |
| `AbstractHeartbeatRequestManagerTest` | 9 | `StreamsGroupHeartbeatRequestManagerTest` | 76（53 + 23） |
| `CommitRequestManagerTest` | 53 | `StreamsRebalanceListenerInvokerTest` | 16 |
| `FetchRequestManagerTest` | 102 | `StreamsRebalanceDataTest` | 29 |
| `FetchCollectorTest` / `FetchBufferTest` | 22 / 5 | `StreamsGroupTopologyDescriptionRequestManagerTest` | 13 |
| `OffsetsRequestManagerTest` | 28 | `ConsumerCoordinatorTest`（classic） | 156 |
| `SubscriptionStateTest` | 68 | `AbstractCoordinatorTest`（classic） | 50 |
| `CoordinatorRequestManagerTest` | 10 | `FetcherTest`（classic） | 82 |
| `TopicMetadataRequestManagerTest` | 7 | `ConsumerNetworkClientTest`（classic） | 19 |
| `WakeupTriggerTest` | 21 | `OffsetCommitCallbackInvokerTest` | 4 |
| `CompletableEventReaperTest` | 5 | `BackgroundEventHandlerTest` | 1（僅 metrics） |
| `RequestManagersTest` | 2 | | |

Java 整合（`clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/`）：

| 檔案 | @ClusterTest | 協定覆蓋 |
|---|---|---|
| `consumer/PlaintextConsumerTest.java` | 72 bare + 8 args + 3 `@ClusterTests` | 雙協定（Classic/Async 成對）；1 個 CONSUMER-only |
| `consumer/PlaintextConsumerCallbackTest.java` | 35 | 雙協定；3 個 cooperative/eager/async 變體 |
| `consumer/PlaintextConsumerCommitTest.java` | 25 | 雙協定，3 個 CONSUMER-only |
| `consumer/PlaintextConsumerPollTest.java` | 24 | 雙協定 |
| `consumer/PlaintextConsumerAssignTest.java` | 18 | 雙協定 |
| `consumer/PlaintextConsumerFetchTest.java` | 18 | 雙協定 |
| `consumer/PlaintextConsumerSubscriptionTest.java` | 13 bare + 18 args + 9 `@ClusterTests` | 雙協定；RE2J 區塊 CONSUMER-only |
| `consumer/PlaintextConsumerCloseTest.java` | 4 | 雙協定 |
| `consumer/ConsumerBounceTest.java` | 11 bare + 2 args + 1 `@ClusterTests` | 雙協定 |
| `consumer/ConsumerIntegrationTest.java` | 9 args + 2 `@ClusterTests` | 混合；數個 CONSUMER-only |
| `consumer/ConsumerTopicCreationTest.java` | `@ClusterTemplate` 產生 | 雙協定（4 方法） |
| `consumer/ConsumerWithLegacyMessageFormatIntegrationTest.java` | 4 | 雙協定 |
| `consumer/SaslPlaintextConsumerTest.java`、`SaslPlainPlaintextConsumerTest.java` | 6 / 6 | 雙協定 |
| `consumer/ShareConsumerTest.java` | 41 bare + 21 args + 8 `@ClusterTests` | share only |
| `consumer/ShareConsumerDeliveryTest.java` | 21 + 5 | share only |
| `consumer/ShareConsumerDLQTest.java` | 10 + 3 | share only |
| `consumer/ShareConsumerCallbackTest.java` | 8 | share only |
| `consumer/ShareConsumerLagTest.java` | 6 + 2 | share only |
| `consumer/ShareConsumerRenewTest.java` | 4 + 1 | share only |
| `consumer/ShareConsumerRackAwareTest.java` | 2 + 1 | share only |
| `security/GroupAuthorizerIntegrationTest.java` | 10 | 雙協定 |
| `ClientRebootstrapTest.java` | 9 | consumer 部分雙協定 |
| `admin/ClientTelemetryTest.java` | 2 | consumer 只是順帶使用 |

非測試 helper：`ClientsTestUtils.java`（含 `BaseConsumerTestcase` 共用斷言）、`consumer/ShareConsumerTestBase.java`、`consumer/ConsumerAssignmentPoller.java`、`consumer/RackAwareTestAssignor.java`。

Scala 整合（`core/src/test/scala/integration/kafka/api/`）——多數 `PlaintextConsumer*` 已遷到 Java，僅剩：

| 檔案 | 數量 | 參數化 |
|---|---|---|
| `AbstractConsumerTest.scala` | 0 | 基底：`TestConsumerReassignmentListener`、`CountConsumerCommitCallback`、`ConsumerAssignmentPoller`、`RetryCommitCallback` |
| `BaseConsumerTest.scala` | 3 | All → 雙協定：`testSimpleConsumption`、`testClusterResourceListener`、`testCoordinatorFailover` |
| `SslConsumerTest.scala`、`SaslSslConsumerTest.scala` | 0（繼承 3） | 雙協定 |
| `SaslMultiMechanismConsumerTest.scala` | 1 | 雙協定 |
| `PlaintextConsumerAssignorsTest.scala` | 8 | 5 ClassicOnly、2 ConsumerOnly、1 CsvSource（`testRebalanceAndRejoin`） |
| `ConsumerBounceTest.scala` | 1 | 雙協定 |
| `SaslClientsWithInvalidCredentialsTest.scala` | 4（3 個 consumer） | 雙協定 |
| `AuthorizerIntegrationTest.scala` | 129 `@Test` + 53 param（39 All） | consumer 相關皆雙協定 |
| `PlaintextAdminIntegrationTest.scala` | 68 `@Test` + 17 param | 多數雙協定 |
| `ClientOAuthIntegrationTest.scala` | 11 param | 雙協定 |
| `MetricsTest.scala`、`GroupCoordinatorIntegrationTest.scala` | 1 / 15 | 未依 group protocol 參數化；後者用 `@ClusterTest` |

Scala 沒有 share consumer 測試（只有 `AuthorizerIntegrationTest.scala`、`IntegrationTestHarness.scala`、`PlaintextAdminIntegrationTest.scala` 順帶引用）。

Streams JUnit（`streams/src/test`、`streams/integration-tests`）：

| 檔案 | @Test / @ParameterizedTest | 協定軸 |
|---|---|---|
| `processor/internals/StreamThreadTest.java` | 28 / 69 | **無**協定軸；`@ValueSource(booleans)` 為 `processingThreadsEnabled`。streams 協定靠獨立 `@Test` 設 `GROUP_PROTOCOL_CONFIG=STREAMS` |
| `processor/internals/DefaultStreamsRebalanceListenerTest.java` | 12 / 1 | streams-only |
| `processor/internals/StreamsRebalanceListenerTest.java` | 10 / 0 | classic listener |
| `processor/internals/TaskManagerTest.java` | 173 / 2 | `streamsProtocolEnabled` 旗標（2 處） |
| `processor/internals/DefaultStateUpdaterTest.java` | 99 / 0 | 無協定軸 |
| `KafkaStreamsTest.java` | 78 / 0 | **實質 classic-only** |
| `StreamsConfigTest.java` | 176 / 3 | 僅 config 驗證 |
| `integration/EosIntegrationTest.java` | 1 / 14 | `@MethodSource` classic/streams × processingThreads（× transactional） |
| `integration/RestoreIntegrationTest.java` | 0 / 9 | `@CsvSource` |
| `integration/StreamsUncaughtExceptionHandlerIntegrationTest.java` | 0 / 8 | `@CsvSource` |
| `integration/KafkaStreamsCloseOptionsIntegrationTest.java` | 7 / 1 | 每協定各自 `@Test` |
| `integration/KafkaStreamsStaticMemberIntegrationTest.java` | 6 / 0 | streams-only |
| `integration/RebalanceProtocolMigrationIntegrationTest.java` | 2 / 0 | 雙協定遷移 |
| `integration/SmokeTestDriverIntegrationTest.java` | 0 / 1 | `@CsvSource`：`shouldWorkWithRebalance` |
| `integration/KafkaStreamsTelemetryIntegrationTest.java` | 1 / 4 | `@ValueSource(strings = {"classic","streams"})` |
| `MetricsIntegrationTest`(0/3)、`InternalTopicIntegrationTest`(0/3)、`StandbyTaskCreationIntegrationTest`(0/2)、`RocksDBMetricsIntegrationTest`(0/1)、`ColdStartStickinessIntegrationTest`(0/1)、`HandlingSourceTopicDeletionIntegrationTest`(0/1)、`IQv2*IntegrationTest`(1–7) | | 協定字串 / CsvSource |
| `TopologyDescriptionPlugin*IntegrationTest`(4/2/1)、`KStreamRepartitionIntegrationTest`(0/13) | | STREAMS 固定，非協定軸 |

helper `utils/EmbeddedKafkaCluster.java`、`utils/IntegrationTestUtils.java` 預設 `GroupProtocol.CLASSIC`。

ducktape（`tests/kafkatest/tests/`）：

| 檔案 | `def test_` | group_protocol 參數化 |
|---|---|---|
| `client/consumer_test.py` | 9 | 是（`all_group_protocols`；`enable_assignment_batching` 區塊 pin `[consumer]`；`test_static_consumer_bounce_with_eager_assignment` pin `[classic]`） |
| `client/consumer_protocol_migration_test.py` | 3 | 是 |
| `client/consumer_rolling_upgrade_test.py` | 0（`rolling_update_test`） | 否（classic assignor upgrade） |
| `core/consumer_group_command_test.py` | 2 | 是 |
| `core/transactions_test.py` | 1 | 是 |
| `core/consume_bench_test.py` | 6 | 是 |
| `core/group_mode_transactions_test.py`、`core/quota_test.py`、`client/client_compatibility_*`、`client/truncation_test.py`、`client/compression_test.py`、`client/pluggable_test.py` | 各 0–1 | 否 |
| `client/share_consumer_test.py` | 6 | N/A（`enable_assignment_batching` 軸） |
| `client/share_consumer_dlq_test.py` / `share_consumer_dlq_tiered_storage_test.py` | 8 / 2 | N/A |
| `core/share_group_command_test.py` / `share_consume_bench_test.py` | 3 / 4 | N/A |
| `streams/streams_broker_bounce_test.py` | 3 | `["classic","streams"]` |
| `streams/streams_broker_down_resilience_test.py` | 4 | `["classic","streams"]` |
| `streams/streams_smoke_test.py`、`streams_relational_smoke_test.py` | 1 / 1 | `["classic","streams"]`；smoke 另有 `["streams"]` × `enable_assignment_batching` |
| `streams/streams_standby_replica_test.py` | 1 | `["classic","streams"]` + `["streams"]`-only |
| `streams/streams_static_membership_test.py` | 4 | 1 個雙協定，3 個 streams-only |
| `streams/streams_application_upgrade_test.py`(2)、`streams_broker_compatibility_test.py`(2)、`streams_named_repartition_topic_test.py`(1)、`streams_optimized_test.py`(1)、`streams_shutdown_deadlock_test.py`(1)、`streams_topology_description_plugin_test.py`(6)、`streams_upgrade_test.py`(3) | | 無協定軸（classic-only） |

服務：`services/verifiable_consumer.py`（572 行；`--group-protocol` 於 `node.version >= V_3_7_0`；handler `ConsumerEventHandler` / `IncrementalAssignmentConsumerEventHandler` / `ConsumerProtocolConsumerEventHandler`）、`services/verifiable_share_consumer.py`（352 行）、`services/console_consumer.py`（321 行，無 group_protocol 旋鈕）。

---

## 2. 迴圈驅動測試（loop-driving tests）— 重設計必須保持綠燈

這些測試真正執行 `ConsumerNetworkThread`、或讓真實 consumer 跑在 `MockClient` / 真 broker 上。mock `ApplicationEventHandler` 的測試（`AsyncKafkaConsumerTest`、`ShareConsumerImplTest`）不在此列——它們驗證的是「有沒有送出事件」，迴圈換掉後多半要改寫而非保綠。

### 2.1 單元層（真實 thread 或真實 client）

| 測試類別 | 路徑（`clients/src/test/java/org/apache/kafka/clients/consumer/`） | 驅動方式 | 保綠重點 |
|---|---|---|---|
| `KafkaConsumerTest`（約 90 個雙協定方法） | `KafkaConsumerTest.java` | 真實 `AsyncKafkaConsumer` + 真實 `ConsumerNetworkThread` over `MockClient` | 唯一端到端單元覆蓋；重點方法：`testPollSendsRequestToJoin`、`testPreventMultiThread`、`testResetToCommittedOffset`、`testResetUsingAutoResetPolicy`、`testCurrentLag*`、`testManualAssignmentChangeWithAutoCommit*`、`testPause`、`testCloseShouldBeIdempotent`、`testPollAuthenticationFailure`、`testFetchStableOffsetThrowInPoll`、`testConsumerBootstrapResolutionExceptionPropagatedToPoll`、`testCommittedThrowsTimeoutExceptionForNoResponse`、`testOffsetsForTimes/Beginning/EndOffsetsTimeout`、`verifyPollTimesOutDuringMetadataUpdate`、`testMetricsRemovedOnClose` |
| `ConsumerNetworkThreadTest`（14） | `internals/ConsumerNetworkThreadTest.java` | 真實 thread，直接呼叫 `runOnce()`，協作者全 mock | `testStartupAndTearDown`、`testEnsureCloseStopsRunningThread`、`testNetworkClientDelegateInitializeResourcesError`、`testRequestManagersInitializeResourcesError`、`testNetworkClientDelegateAndRequestManagersInitializeResourcesError`、`testProcessEventFailureCompletesFutureExceptionally`、`testCleanupInvokesReaper`、`testRunOnceInvokesReaper`、`testSendUnsentRequests`。迴圈結構若改，這個類別大概率要重寫，但語意（初始化失敗要關閉、close 要停 thread、reaper 每輪都跑、未送請求要送出）必須保留 |
| `ApplicationEventHandlerTest`（5） | `internals/ApplicationEventHandlerTest.java` | 真實 handler 啟動 thread | `testFailOnInitializeResources`、`testDelayInInitializeResources`、`testInterruptInInitializeResources`、`testAddThrowsWhenBackgroundThreadDead` |
| `KafkaShareConsumerTest`（5） | `KafkaShareConsumerTest.java` | 真實 `KafkaShareConsumer` over `MockClient` | `testVerifyFetchAndCommitSyncImplicit`、`testVerifyFetchAndCloseImplicit`、`testShareConsumerBootstrapResolutionExceptionPropagatedToPoll`、`testShareConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero` |
| `NetworkClientDelegateTest`（14） | `internals/NetworkClientDelegateTest.java` | 真實 delegate over `MockClient` | `testBootstrapResolutionExceptionPropagatedViaErrorEventOnce`、`testHasAnyPendingRequests`、`testPollWithOnClose`、`testCheckDisconnectsWithOnClose` |
| `FetchRequestManagerTest`（102）、`ShareConsumeRequestManagerTest`（74） | `internals/…` | 真實 manager + `MockClient`（無 thread） | fetch/pause/backoff/disconnect 語意 |
| `CommitRequestManagerTest`、`ConsumerHeartbeatRequestManagerTest` | `internals/…` | 各一個真實 `NetworkClientDelegate` | `testMaximumTimeToWaitDoesNotSpinDuringRealBootstrapDnsResolution`（防 busy-loop） |

### 2.2 整合層（真 broker，embedded KRaft）

Java `clients-integration-tests`（全部 `@ClusterTest`，Classic/Async 成對）：
- `PlaintextConsumerTest`、`PlaintextConsumerPollTest`、`PlaintextConsumerFetchTest`、`PlaintextConsumerAssignTest`、`PlaintextConsumerSubscriptionTest`、`PlaintextConsumerCommitTest`、`PlaintextConsumerCallbackTest`、`PlaintextConsumerCloseTest`、`ConsumerBounceTest`、`ConsumerIntegrationTest`、`ConsumerTopicCreationTest`、`ConsumerWithLegacyMessageFormatIntegrationTest`、`SaslPlaintextConsumerTest`、`SaslPlainPlaintextConsumerTest`、`ClientRebootstrapTest`、`security/GroupAuthorizerIntegrationTest`。
- share：`ShareConsumerTest`、`ShareConsumerDeliveryTest`、`ShareConsumerDLQTest`、`ShareConsumerCallbackTest`、`ShareConsumerLagTest`、`ShareConsumerRenewTest`、`ShareConsumerRackAwareTest`。
- 迴圈時序最敏感的方法：`PlaintextConsumerPollTest.test*PollEventuallyReturnsRecordsWithZeroTimeout`、`test*NoOffsetForPartitionExceptionOnPollZero`、`test*MaxPollIntervalMsShorterThanPollTimeout`、`test*RecoveryOnPollAfterDelayedRebalance`；`PlaintextConsumerCloseTest.test*CloseWithDefaultTakesAtLeastFetchMaxWaitMs`、`test*CloseWithTimeoutIgnoresFetchMaxWaitMs`；`PlaintextConsumerCommitTest.testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose`、`testCommitAsyncCompletedBeforeConsumerCloses`、`testCommitAsyncCompletedBeforeCommitSyncReturns`；`PlaintextConsumerTest.test*PositionRespectsTimeout`、`test*PositionRespectsWakeup`、`test*PositionWithErrorConnectionRespectsWakeup`、`test*OffsetRelatedWhenTimeoutZero`、`test*StallBetweenPoll`；`PlaintextConsumerCallbackTest` 全部 35 個（callback 內重入）；`ConsumerBounceTest.test*ConsumptionWithBrokerFailures`、`test*CloseDuringRebalance`。

Scala `core`（`QuorumTestHarness`，`getTestGroupProtocolParametersAll`）：
- `BaseConsumerTest`（`testSimpleConsumption`、`testClusterResourceListener`、`testCoordinatorFailover`，由 `SslConsumerTest`、`SaslSslConsumerTest` 繼承）、`SaslMultiMechanismConsumerTest`、`PlaintextConsumerAssignorsTest`、`ConsumerBounceTest`、`SaslClientsWithInvalidCredentialsTest`、`AuthorizerIntegrationTest`（consumer 相關 39 個 All）、`PlaintextAdminIntegrationTest`、`ClientOAuthIntegrationTest`。

Streams（`streams/integration-tests`，帶 streams 協定軸者）：
- `EosIntegrationTest`、`RestoreIntegrationTest`、`StreamsUncaughtExceptionHandlerIntegrationTest`、`KafkaStreamsCloseOptionsIntegrationTest`、`KafkaStreamsStaticMemberIntegrationTest`、`RebalanceProtocolMigrationIntegrationTest`、`SmokeTestDriverIntegrationTest.shouldWorkWithRebalance`、`KafkaStreamsTelemetryIntegrationTest`、`MetricsIntegrationTest`、`InternalTopicIntegrationTest`、`StandbyTaskCreationIntegrationTest`、`HandlingSourceTopicDeletionIntegrationTest`、`IQv2*IntegrationTest`。

ducktape（依 `group_protocol` 參數化者）：
- `consumer_test.py`(9)、`consumer_protocol_migration_test.py`(3)、`consumer_group_command_test.py`(2)、`transactions_test.py`(1)、`consume_bench_test.py`(6)；share：`share_consumer_test.py`(6)、`share_consumer_dlq_test.py`(8)；streams：`streams_broker_bounce_test.py`(3)、`streams_broker_down_resilience_test.py`(4)、`streams_smoke_test.py`、`streams_relational_smoke_test.py`、`streams_standby_replica_test.py`、`streams_static_membership_test.py`(4)。

---

## 3. GAPS（三份來源合併）

| # | 情境 | 現況 | 缺口 |
|---|---|---|---|
| G1 | **event timeout 後才到的 late response** | 單元：`CompletableEventReaperTest`（`testExpired`、`testCompletedAndExpired`、`testIncompleteQueue`、`testIncompleteTracked`）只測 reaper 本身；`ConsumerNetworkThreadTest.testCleanupInvokesReaper/testRunOnceInvokesReaper` 與 `AsyncKafkaConsumerTest.testReaperInvokedInClose/Unsubscribe/Poll` 只 `verify(reaper)`。`TopicMetadataRequestManagerTest.testExpiringRequest` 是 request-manager 層最接近的。整合：無任何測試以「回應在操作完成/逾時之後才到」為主題；最近鄰是 `PlaintextConsumerCommitTest.testCommitAsyncFailsWhenCoordinatorUnavailableDuringClose`。 | 沒有測試讓真實 broker 回應在事件已過期後抵達；reaper 在所有 consumer 層測試中都是 mock。 |
| G2 | **close 時仍有 pending positions / in-flight `updateFetchPositions`** | pending **commit** 有覆蓋：`AsyncKafkaConsumerTest.testCloseAwaitPendingAsyncCommitIncomplete/Complete`、`CommitRequestManagerTest.testPollWithClosingAndPendingRequests/testSignalClose`、`ConsumerCoordinatorTest.testCloseNoResponseForCommit`（classic）；整合 `PlaintextConsumerCommitTest.testCommitAsync*Close*`（CONSUMER-only）、`test*AutoCommitOnClose`。`testCloseLeavesGroup` 只存在單元層（`AsyncKafkaConsumerTest.java:975,987,1013`），無整合對應。 | 無任何測試針對 close 時尚未完成的 positions / `updateFetchPositions`；也沒有 `closeCommits` / `commitSyncOnClose` 命名的整合測試。 |
| G3 | **async consumer 在 callback 內 wakeup** | classic：`ConsumerCoordinatorTest.testWakeupFromAssignmentCallback`、`AbstractCoordinatorTest.testWakeupInOnJoinComplete`。async 最接近：`AsyncKafkaConsumerTest.testWakeupWhileWaitingOnReconciliationCheck`、`testNoWakeupInCloseCommit`。整合：`PlaintextConsumerCommitTest.test*AutoCommitOnCloseAfterWakeup` 是 wakeup 後 close，不是 callback 內 wakeup。share 反而有：`ShareConsumerCallbackTest.testAcknowledgementCommitCallbackCallsShareConsumerWakeup`、`testAcknowledgementCommitCallbackCallsShareConsumerDisallowed`。streams：`StreamsRebalanceListenerInvokerTest.testInvokeTasks*WithWakeupException` 是 listener 自己丟例外，非外部 wakeup。 | 沒有 async 測試在使用者 `ConsumerRebalanceListener` 或 `OffsetCommitCallback` 執行中呼叫 `wakeup()`；ducktape 完全沒有 wakeup 測試。 |
| G4 | **DNS / bootstrap 解析失敗（整合層）** | 單元有：`KafkaConsumerTest.testConsumerBootstrapResolutionExceptionPropagatedToPoll`、`testConsumerConstructorFailsWithConfigExceptionOnUnresolvableBootstrapWhenTimeoutZero`（雙協定）、`KafkaShareConsumerTest` 對應、`NetworkClientDelegateTest.testBootstrapResolutionExceptionPropagatedViaErrorEventOnce`、`CommitRequestManagerTest`/`ConsumerHeartbeatRequestManagerTest.testMaximumTimeToWaitDoesNotSpinDuringRealBootstrapDnsResolution`。整合：**無**；`ClientRebootstrapTest` 是殺/重啟 broker，不是破壞名稱解析。 | 兩個整合樹都沒有不可解析 bootstrap hostname 的測試。 |
| G5 | **reconnect backoff 與 fetch 的互動** | 單元 disconnect：`FetchRequestManagerTest.testFetchDisconnected*`、`ShareConsumeRequestManagerTest.testFetchDisconnected`/`testWhenLeadershipChangedAfterDisconnected`、`NetworkClientDelegateTest.testCheckDisconnectsWithOnClose`；backoff：`FetchRequestManagerTest.testMaximumTimeToWaitBoundedWhenPartitionsSkippedDueToBackoff`、`testPartitionsSkippedDueToBackoffDoesNotWakeUpBuffer`、`CoordinatorRequestManagerTest.testBackoffAfterRetriableFailure`、`ConsumerHeartbeatRequestManagerTest.testMaximumTimeToWaitWhenFencedWaitsRetryBackoff`。整合：`ConsumerBounceTest.java.test*ConsumptionWithBrokerFailures`、`test*SeekAndCommitWithBrokerFailures`、`PlaintextConsumerTest.test*CoordinatorFailover`、`test*CloseOnBrokerShutdown`、`ShareConsumerTest.testLeaderRestartWithoutLeadershipChange*`、Scala `ConsumerBounceTest`。 | 單元目錄裡沒有任何 `test*Reconnect*`；整合測試名稱也沒有 `reconnect.backoff.ms` 調校。 |
| G6 | **callback 重入（re-entrancy）** | 單元：只有 `AsyncKafkaConsumerTest.testCommitInRebalanceCallback`（L634）在 `ConsumerRebalanceListener` 內呼叫 consumer 方法；`SubscriptionStateTest.testAssignedPartitionsAwaitingCallbackKeepPositionDefinedInCallback` 只測狀態機。整合：`PlaintextConsumerCallbackTest`(35) 覆蓋 `assign`/`assignment`/`beginningOffsets`/`position`/`seek`/`pause`/`commitSync`/`groupMetadata` 在 `onPartitionsAssigned/Revoked` 內；`ConsumerIntegrationTest.testFetchPartitionsAfterFailedListener*`、`testFetchPartitionsWithAlwaysFailedListener*`。 | 單元層無 `seek`/`subscribe`/`assign`/`position` 在 listener 內；沒有任何層在 `OffsetCommitCallback` 或 `AcknowledgementCommitCallback` 內呼叫 consumer 方法（share 的 `...CallsShareConsumerDisallowed` 除外）。 |
| G7 | **Streams 協定的 close / fence 覆蓋** | 整合：`KafkaStreamsCloseOptionsIntegrationTest`（`testCloseOptionsLeaveGroupStreamsProtocol`、`testCloseOptionsDefaultStreamsProtocol`、`testCloseOptionsRemainInGroupStreamsProtocol`、`testStaticMemberCloseUsesStaticLeaveEpochStreamsProtocol`、`testStaticMemberLeaveGroupStreamsProtocol`）、`KafkaStreamsStaticMemberIntegrationTest`(6)。單元：`StreamThreadTest.shouldNotEnforceRebalanceOnShutdownRequestUnderStreamsProtocol`、`shouldRoute*ForStreamsProtocol`；`StreamsGroupHeartbeatRequestManagerTest.testPollOnCloseWhenIsLeaving/IsNotLeaving/WhenStaticMemberIsLeaving`。 | `KafkaStreamsTest`(78：建構/啟動/close/狀態轉移) 實質 classic-only；`StreamThreadTest` 69 個參數化測試（close、shutdown、fenced-producer、commit）以 `processingThreadsEnabled` 為軸，**不是**協定；ducktape `streams_shutdown_deadlock_test` 唯一 shutdown-deadlock 覆蓋且 classic-only；`consumer_group.py` 沒有 `streams` 常數。 |
| G8 | **poll(0) 語意** | 單元：`Duration.ZERO`/`ofMillis(0)` 在 `KafkaConsumerTest` 出現約 40 次，但都是「推一輪迴圈」的慣用法，沒有以 zero-timeout poll 為主題；只有 `AsyncKafkaConsumerTest.testBeginningOffsetsWithZeroTimeout`、`testOffsetsForTimesWithZeroTimeout` 針對 offset 查詢。整合有：`PlaintextConsumerPollTest.test*PollEventuallyReturnsRecordsWithZeroTimeout`、`test*NoOffsetForPartitionExceptionOnPollZero`、`PlaintextConsumerTest.test*OffsetRelatedWhenTimeoutZero`。 | 單元層缺 poll(0) 主題測試；整合層已有。 |
| G9 | **CLASSIC-only 的 close / interrupt / wakeup / regex-rebalance** | `KafkaConsumerTest` 40 個 `names = "CLASSIC"` 方法正好是 `testGracefulClose`、`testCloseTimeout`、`testCloseNoWait`、`testCloseInterrupt`、`testWakeupWithFetchDataAvailable`、`testReturnRecordsDuringRebalance`、`testPollThrowsInterruptExceptionIfInterrupted`、`testRegexSubscription`、`testChangingRegexSubscription`、`testPauseFlagPreservedForRetainedPartitionAcrossRebalance`。 | 這些在 async 路徑最難的情境，沒有帶真實 thread + `MockClient` 的單元覆蓋。 |
| G10 | **背景事件遞送** | `BackgroundEventHandlerTest` 只有 1 個測試（metrics）。 | 背景事件遞送幾乎只透過 `AsyncKafkaConsumerTest` 的 mock queue 間接驗證。 |
| G11 | ducktape 缺口 | 無 pattern-subscription、pause/resume、seek/position、wakeup 系統測試；startup 只靠 `handle_startup_complete` 隱含。 | 系統層以 bounce / migration / bench 為主，API 語意不在系統層驗。 |

---

## 4. 各層執行成本（runtime class）

| 層 | 位置 | 執行環境 | 單一測試量級 | 備註 |
|---|---|---|---|---|
| clients 單元 | `clients/src/test/java/org/apache/kafka/clients/consumer/` | JUnit 5 + Mockito + `MockClient` + `MockTime` | 毫秒級 | `KafkaConsumerTest` 的 CONSUMER 跑法會啟真實 `ConsumerNetworkThread`，但仍是 `MockClient`，無 socket。 |
| clients 整合 | `clients/clients-integration-tests/` | `ClusterTestExtensions`（`@ClusterTest`，embedded KRaft，`brokers = N`） | 秒級（每類別起一個 cluster） | Classic/Async 方法成對，原始 `@ClusterTest` 數約為邏輯數 2 倍。 |
| core Scala 整合 | `core/src/test/scala/integration/kafka/api/` | `IntegrationTestHarness` → `QuorumTestHarness`（embedded KRaft） | 秒到十秒級 | `@ParameterizedTest @MethodSource(getTestGroupProtocolParametersAll)`；需 Scala 編譯。 |
| Streams 單元 | `streams/src/test/java/` | JUnit 5 + Mockito | 毫秒級 | `StreamThreadTest` 用 mock `StreamsGroupHeartbeat`。 |
| Streams 整合 | `streams/integration-tests/` | `EmbeddedKafkaCluster` | 秒到分鐘級（EOS、restore 較慢） | 預設 `GroupProtocol.CLASSIC`，streams 軸由 `@CsvSource`/`@MethodSource` 開。 |
| ducktape 系統測試 | `tests/kafkatest/` | 需 Jenkins 或本機 docker（`tests/docker`），多節點 VM/容器 | 分鐘級以上 | `verifiable_consumer` 以 `--group-protocol` 驅動（`>= V_3_7_0`）；streams 用 `group_protocol=["classic","streams"]` 原始字串。本機無法在 IDE 內快速迭代。 |

建議的驗證順序：先跑 `KafkaConsumerTest`、`ConsumerNetworkThreadTest`、`ApplicationEventHandlerTest`、`KafkaShareConsumerTest`（毫秒級，真實 thread），再跑 `clients-integration-tests` 的 `PlaintextConsumer*Test` + `ShareConsumer*Test`（秒級），接著 core Scala 與 streams integration，最後才在 CI/Jenkins 跑 ducktape。
