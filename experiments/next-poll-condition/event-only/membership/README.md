# 真實 membership 與 AsyncPollEvent 整合驗證

**結論：七組狀態流程通過，且反向測試證實漏掉 metadata 通知會卡住 reconciliation。Request Manager 可以保留邏輯歸屬，以通知決定執行時機。尚未證明效能提升。**

## 本次進展

前輪使用 membership 測試替身。本輪改用 repository 的真實 ConsumerMembershipManager、AbstractMembershipManager、SubscriptionState、heartbeat payload builder 與 AsyncPollEvent 處理流程；事件透過佇列进入實際 ConsumerNetworkThread.runOnce。Scheduler 登記 coordinator、heartbeat 與 membership 三個 manager，不掃描其他尚未遷移的 managers。

補上的候選接線在隔離的 AbstractMembershipManager 副本：

- membership state 改變後，發布 membershipInput。
- metadata update listener 發布 membershipInput。
- reconciliation 完成並更新旗標後，發布 membershipInput，讓待處理的下一份 assignment 有機會繼續。
- membership.poll 先取得 generation 快照，再执行原 maybeReconcile(false)，最後回傳等待 condition。

Heartbeat 沿用上一輪 listener 接線。AsyncPollEvent 的 consumer 分支未改寫：仍先 maybeReconcile(true)，完成 reconciliation check，再執行 commit timer、onConsumerPoll、resetPollTimer、offset positions 與 fetch 流程。

## 本次通過的七組流程

1. **入群與閒置**：TopicSubscriptionChangeEvent → AsyncPollEvent → JOINING → FindCoordinator completion → heartbeat → STABLE。100 輪閒置期間 heartbeat 沒有再次 poll；AsyncPollEvent 重設期限後不在舊期限轉為 STALE。
2. **非空 assignment**：heartbeat 回覆帶入 topic ID／partition → RECONCILING → 真實 reconciliation → PartitionsAssignedEvent → application callback-completed event → acknowledgement heartbeat。
3. **Fenced**：FENCED_MEMBER_EPOCH 回覆 → 真實 membership 重新 JOINING → 發出 member epoch 0 heartbeat。
4. **Application poll 逾期**：期限到達 → leave heartbeat → STALE；新 AsyncPollEvent → 重新 JOINING。
5. **Fatal error**：GROUP_AUTHORIZATION_FAILED heartbeat 回覆 → FATAL，background queue 有 ErrorEvent，之後不再 poll heartbeat。
6. **Metadata 延後到達**：assignment 的 topic ID 尚未解析 → metadata listener 通知 → membership 繼續 reconciliation，無須額外 AsyncPollEvent。
7. **退群與關閉**：UnsubscribeEvent 與 LeaveGroupOnCloseEvent(LEAVE_GROUP) → epoch -1 heartbeat → UNSUBSCRIBED → response 完成事件 future，之後沒有重複 leave request。

heartbeat.maximumTimeToWait 在測試包裝器中直接拋錯；通過測試表示此隔離事件 loop 未走它。這不代表 production application 的舊等待契約已可刪除。

## 反向驗證

獨立編譯一份只將 metadata listener 的 publish 關掉的故障版本，跑同一個整合測試。它在「metadata completion 必須不靠下一個 application poll 繼續 reconciliation」的斷言失敗；正常版本通過。Runner 只接受這個特定失敗，其他失敗不能被當作成功。

這證實 metadata 通知是必要接線，而不只是測試剛好沒有遇到等待。它不是掃描 baseline 的效能對照。

## 邊界與尚未完成的範圍

- 真實 source：consumer membership 狀態機、subscriptions、AsyncPollEvent、ApplicationEventProcessor 的受測 consumer 分支；沿用先前隔離版 coordinator、heartbeat、scheduler、network loop、metrics/reaper。
- Metadata、commit、offsets、fetch 邊界仍為 Mockito 替身。Metadata listener 註冊是真實程式行為，但測試透過 transport completion 模擬通知交付，沒有真實 metadata 網路回覆。
- PartitionsAssignedEvent 真正由 membership 排入 background queue；application 端套用 assignment 與 callback-completed event 由 harness 模擬，未執行完整 AsyncKafkaConsumer.poll 或使用者 callback。
- Transport 非阻塞，不連 broker；thread.runOnce 在單一測試執行緒驅動，未量測 OS 喚醒延遲、跨執行緒競爭或 application 端阻塞等待。
- 所有 metadata／signal 回呼在本測試都位於 network 執行脈絡；不能把這些 Signal 視為可任意跨執行緒使用。
- Consumer 的 callback 撤銷失敗、REMAIN_IN_GROUP、static member、auto-commit 延後完成、多份 assignment 重疊、所有 coordinator fatal paths、Streams／Share 等尚未完整覆蓋。共享 abstract 類別的改動不能直接視為所有 client 皆已遷移。
- 保持 production Java source、原 Kafka 歷史不變。所有新增接線只在 runner 生成的隔離副本中。

## 編譯相容性處理

本地 cached Kafka jar 比固定 trunk source 舊，runner 明確編譯所需 source（包含新的 subscription API 支援類別）。ApplicationEventProcessor 的 Streams close 與 CurrentLag 兩條未測路徑在生成副本中改為明確 UnsupportedOperationException，以避免呼叫 jar 中不存在的方法；受測 consumer／AsyncPollEvent 分支保持原內容。

RequestManagers 使用 cached jar 的 setup constructor，entries 只列三個受測 managers。Inline Mockito 以啟動時 Java agent 支援 final 邊界替身；這個 correctness harness 不可直接拿耗時當效能結果。

## Benchmark 判斷

這一輪已具備建立「三個 manager 的固定 lifecycle trace」對照的基礎，但尚未建立掃描版與事件版逐步 request/state 輸出等價的 baseline。現在不能宣稱已適合端到端效能比較。

下一個關卡是先固定正常入群、稀疏輸入、全忙與 timer/retry trace，核對兩版 request 內容、順序、狀態與必要 deadlines，再移除計時區中的 Mockito 成本，使用相同 JMH 設定量 CPU／allocation／事件派送延遲。若輸出不等價，先修語意，不能用少做工作換出較好的數字。

## 重跑

在 next-poll-condition-trunk repository 中、已有前輪 isolated harness 產物時：

```sh
/opt/homebrew/bin/python3 experiments/next-poll-condition/event-only/membership/validate.py
```

單次 JVM 限制 128 MB、單 worker、45 秒 timeout；正常／故障控制分開執行。沒有啟動 broker 或完整 Gradle。git diff --check 通過；未宣稱完整 Checkstyle／Kafka suite 已通過。

證據包含 runner、probe、生成 source、patch、正常及故障控制 log、source SHA-256。它依賴既有 cached jars／classpath，不是自帶全部依賴的可攜式建置包。
