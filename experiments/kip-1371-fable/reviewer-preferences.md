# Apache Kafka reviewer 對 async consumer 背景執行緒變更的實際偏好（證據集）

日期：2026-09-10。範圍：`AsyncKafkaConsumer` / `ConsumerNetworkThread` / `RequestManagers` 相關的 KIP 討論、GitHub PR review、JIRA 評論。所有引文保留英文原文並附連結；本文只整理證據，不加個人意見。

原始快取（JSON）：`/private/tmp/claude-501/-Users-unknowntpo-repo-unknowntpo-kafka/f3c3830f-ef9e-41ce-8632-f3e8dcd6deb8/scratchpad/pr<n>_*.json`、`KAFKA-*.json`。

## 0. 結論（先講）

1. **15 個指定 PR 中，10 個 reviewer 明確偏好「局部、有界的小修」**，2 個要求結構性改動（16885、18737），其餘中性。要求結構性改動的兩個 PR，作者本來就是核心貢獻者（lianetm、frankvicky）且問題是正確性（race / 資料遺失），reviewer 的「結構性要求」其實是「把邏輯放到正確的 owner（RequestManager / 背景執行緒）」，不是「做更大的框架」。
2. **busy-loop 家族的處理方式已經定型**：每個 manager 的 `maximumTimeToWait()` / `timeUntilNextPollMs` 在「現在不能送、但時間會解決」時回 `retryBackoffMs`，在「永遠不會有下一次」時回 `Long.MAX_VALUE`；每個修法附一個 `...DoesNotSpin` 單元測試；跨 manager 的一致性問題由 committer（chia7712）另開 JIRA 統一處理（KAFKA-21010），而不是在單一 PR 內擴大範圍。
3. **reviewer 承認需要更全面的重構，但明確說「不是現在」**（AndrewJSchofield, #23014），而且 maintainer 自己（lianetm）已把 inflightPoll 生命週期重構認領為 KAFKA-20844。
4. **KIP-1371 目前沒有 dev list 討論串**（KIP 頁與 JIRA 都寫 `Discussion thread: TBD`），dev@ 上唯一相關信件是 JIRA 自動通知。因此「reviewer 對 KIP-1371 的反應」目前沒有第一手證據；最接近的代理證據是 PR #20521（kirktrue 的 `AsyncPollEvent` 結構性改動，lianetm 審 28 輪、反覆要求刪狀態、跑 system test）。
5. **沒有任何一個 PR 被要求提供 benchmark**；效能證據都是作者主動附的（flamegraph、CPU ms）。reviewer 要的是：能重現的測試、classic consumer 對照、註解/javadoc 說明「為什麼是這個值」、不動測試（測試若要改要解釋）。

## 1. KIP-1371 討論串

### 1.1 搜尋結果

| 來源 | 查詢 | 結果 |
|---|---|---|
| dev@kafka.apache.org（lists.apache.org API，`lte=1y`／`lte=3y`） | `KIP-1371` | 1 封：`[jira] [Created] (KAFKA-20995) [Draft][KIP-1371] Introduce a Consumer Reactor ...`（Eric Chang via Jira，epoch 1787882160）。無 `[DISCUSS]` 串。 |
| 同上 | `Consumer Reactor`、`Reactor` | 同一封 JIRA 通知，另一封無關的 Jenkins 信。 |
| users@kafka.apache.org | `KIP-1371` | 0 封。 |
| KIP 頁面（cwiki 449282795，last updated 2026-08-31） | — | `Current state: Draft`；`Discussion thread: TBD`。 |
| JIRA KAFKA-20995 | comments | **0 則評論**。描述已改名為「KIP-1371: Formalize Consumer Reactor Cross-Manager Coordination and Publication」，狀態 Open，assignee Eric Chang。 |

結論：目前沒有 reviewer 對 KIP-1371 本身的公開回應可摘錄。KIP 頁自己的「Rejected Alternatives」第一條就是本題的對立面：

> "Keep distributed decisions with local fixes: Local timeout conditions, application-side rescans, and generic wakeup booleans can address individual failures but retain multiple decision authorities. They cannot consistently preserve reason and publication ordering."
> — KIP-1371, https://cwiki.apache.org/confluence/spaces/KAFKA/pages/449282795

### 1.2 代理證據：KIP-1371 引用的 JIRA 上，committer 說了什麼

| JIRA | 回覆者 / 日期 | 立場 | 引文 |
|---|---|---|---|
| KAFKA-21031（HB in-flight busy loop，PR #23357） | Lianet Magrans 2026-09-04 | 接受為獨立 bug，排 4.5 再 backport | "from a first look seems like an old issue affecting CPU, discovered now, unrelated to KIP-909 ... If so I expect we fix it for 4.5 (remove 4.4 from fix version for now). Once merged we can consider backporting" https://issues.apache.org/jira/browse/KAFKA-21031 |
| KAFKA-20426（assign + group.id busy loop） | Lianet 2026-04-09 | 指定局部修法 | "If UNSUBSCRIBED (not in a group, no heartbeats), the maximumTimeToWait in the membershipMgr should return in a way that the app thread blocks on it's own poll timer ... One option is to return max_value if UNSUBSCRIBED" https://issues.apache.org/jira/browse/KAFKA-20426 |
| KAFKA-20854（KIP-909 busy loop） | Lianet 2026-08-11；Chia-Ping 2026-08-13 | 列為 4.4 blocker | "I just marked this as blocker for 4.4, makes sense? (we've seen already the regression on high CPU on the async consumer)" / "yes, this is definitely a blocker to 4.4.0" https://issues.apache.org/jira/browse/KAFKA-20854 |
| KAFKA-20844（inflightPoll 生命週期，Lianet 自己開、自己認領） | Lianet 2026-07-27 | **maintainer 承認局部修法易錯、要做結構整理** | "The lifecycle of the inflightPoll is managed across several methods ... and the event's progress is tracked with a mix of futures and flags, which has proven error-prone ... maybe explicit states or transitions around the fetching lifecycle, also review and better separate responsibilities (actual fetching/buffering, reconciliation, position validations, HB)" https://issues.apache.org/jira/browse/KAFKA-20844 |
| KAFKA-20904（async consumer CPU 加倍） | Evan Zhou 2026-08-27 | 局部修法（#23014）就把 CPU 壓回 classic 水準 | "CPU usage for the `consumer` protocol has come back down to levels similar for the `classic` protocol after the fix for KAFKA-20854 was merged. I will be closing this ticket" https://issues.apache.org/jira/browse/KAFKA-20904 |
| KAFKA-21049（retry.backoff.ms=0 busy loop，由 #23357 review 衍生） | Doyeon Kim 2026-09-08 | 又一個局部 floor | "Use a minimum positive value when retryBackoffMs is used as an application-thread wait, without changing request retry semantics." https://issues.apache.org/jira/browse/KAFKA-21049 |
| KAFKA-21059（合併 HB maximumTimeToWait 測試） | Lianet 2026-09-09 | 重複測試要收斂 | "there are tests for each component ... This task is to review if the dup tests are really needed, or could we move them to AbstractHeartbeatRequestManagerTest" https://issues.apache.org/jira/browse/KAFKA-21059 |
| KAFKA-20860（背景執行緒錯誤處理，Matthias 用 Claude 產的報告） | Lianet 2026-08-06 | 對「找不到可達路徑」的防禦性修法冷處理 | "this doesn't seem reachable to me on the consumer path ... I see an open PR just surounding with try/catch, will look into it, probably doesn't hurt being defensive." https://issues.apache.org/jira/browse/KAFKA-20860 ；PR #23023 至今無 committer review。 |
| KAFKA-18139（Philip Nee：network thread loop 需要進一步優化，2024-12） | — | Open、無 assignee、近兩年無進展 | 描述：AsyncKafkaConsumer polls NetworkClient ~2x classic，CPU >10%。https://issues.apache.org/jira/browse/KAFKA-18139 |
| KAFKA-16290（subscription state 經 queue 傳遞） | Lianet 2024-08-15 | 設計原則以「逐 API 拆 JIRA」落地 | "Basically we've been doing the changes per api call, to ensure that the subscription state is only updated in the background." https://issues.apache.org/jira/browse/KAFKA-16290 |

### 1.3 早期設計原則（cwiki「Consumer threading refactor design」）

單一寫者在背景執行緒、app/background 只透過 event queue 溝通；引文："A request manager represents the logic and state needed to issue a Kafka RPC requests and handle its response." / "Firstly, it simplifies the design and logic because the timing of the network request is more predictable." https://cwiki.apache.org/confluence/display/KAFKA/Consumer+threading+refactor+design

## 2. 逐 PR 表

判定欄：**小修** = reviewer 明確偏好或只接受局部修法；**結構** = reviewer 要求把修法改成結構性；**中性** = 證據不足或無爭議。

註：任務清單中的 **#18376 實際是 KAFKA-18391「Skip running Flaky Test Report job on forks」（CI 一行改動，與 consumer 無關）**。「移除 app-thread blocking」對應的是 **KAFKA-18376 → PR #20521**（kirktrue，`AsyncPollEvent`），已補在表末。

| PR | JIRA | 作者 | Reviewers | 輪數 / 規模 | Pushback 主題 | 代表引文（連結） | 判定 |
|---|---|---|---|---|---|---|---|
| [#23014](https://github.com/apache/kafka/pull/23014) | KAFKA-20854 | m1a2st | AndrewJSchofield, lianetm, chia7712 | ~4 輪；8 files +289/−65 | 等待值怎麼算、network vs app thread 快取過期、行為變化（reconciliation 延後一個 poll）、是否集中到 consumer 層 | Andrew: "One of the principles of the AsyncKafkaConsumer is that it tries to eliminate sharing of state between the application and network threads. Clearly, it didn't quite manage that, but the principle is still largely intact ... **I'd like to see a more comprehensive refactor, but probably not right now.**" [r3804039595](https://github.com/apache/kafka/pull/23014#discussion_r3804039595)；Andrew approve: "**I don't want to make yet another special case with additional tricky logic, so I would go with this patch.**" [review-4963020255](https://github.com/apache/kafka/pull/23014#pullrequestreview-4963020255)；chia7712: "Have we considered handling all these cases in the AsyncKafkaConsumer layer? ... consolidate the remaining cases" [r3781539485](https://github.com/apache/kafka/pull/23014#discussion_r3781539485)；lianetm: "Getting that time boundary right is key here imo, we're moving from waking up too much to just waking up in specific cases" [r3759394400](https://github.com/apache/kafka/pull/23014#discussion_r3759394400) | 小修 |
| [#23227](https://github.com/apache/kafka/pull/23227) | KAFKA-20970 | m1a2st | chia7712, AndrewJSchofield, frankvicky, lianetm | 3 輪；5 files +140/−6 | 該回 interval 還是 retryBackoff、Streams HB 也要 guard、測試要開真 DNS | chia7712: "**I'd prefer to keep the current change as is**, since it's consistent with the guard in AbstractHeartbeatRequestManager from KAFKA-20253. What the guard should actually return (for example retryBackoffMs) **deserves its own discussion, so I opened KAFKA-21010** to address it for all three heartbeat managers together." [r3904528109](https://github.com/apache/kafka/pull/23227#discussion_r3904528109)；lianetm: "agree with waiting a bit here, just wondering if the interval is the best "delay" to use?" [r3916789355](https://github.com/apache/kafka/pull/23227#discussion_r3916789355)；chia7712: "Would you mind enabling DNS resolution in this patch? It can help us catch unexpected latency or resource issues" [review-4994047696](https://github.com/apache/kafka/pull/23227#pullrequestreview-4994047696) | 小修 |
| [#23348](https://github.com/apache/kafka/pull/23348) | KAFKA-21010 | m1a2st | lianetm | 2 輪；10 files +199/−29 | 各 MemberState 該回什麼、註解冗餘、consumer/share 測試重複 | lianetm: "**I don't want to derail this PR, so I would be ok with shipping it with the fix it has** ... as follow-up, review the STALE/FENCE case to see if we can improve further?" [r3961566885](https://github.com/apache/kafka/pull/23348#discussion_r3961566885)；"Fatal should probably be just as unsubscribed (max value), no need to have the app thread responsive to the req managers any sooner than the poll timeout" [r3937740607](https://github.com/apache/kafka/pull/23348#discussion_r3937740607)；"Filed KAFKA-21059 to follow-up separately in case we can clean up." [r3970944336](https://github.com/apache/kafka/pull/23348#discussion_r3970944336) | 小修 |
| [#23357](https://github.com/apache/kafka/pull/23357)（open） | KAFKA-21031 | unknowntpo | dybyte（非 committer） | 1 則；6 files +542/−4 | 同一 floor 是否也該套到 fetch 路徑 | dybyte: "When `retry.backoff.ms` is 0, both can also result in a zero application-thread wait. Do you think the same floor should apply there, or is that case intentional?" [review-5109783063](https://github.com/apache/kafka/pull/23357#pullrequestreview-5109783063)；作者回："It would be good to keep it separate from this PR, though: this change is specifically about the heart-beat path." [issuecomment-5542567794](https://github.com/apache/kafka/pull/23357#issuecomment-5542567794) → 衍生 KAFKA-21049 / #23404 | 中性（尚無 committer review；+542 行中多數是測試，是這批最大的） |
| [#21991](https://github.com/apache/kafka/pull/21991)（open） | KAFKA-20397 | nileshkumar3 | kirktrue, smjn | 1 輪；2 files +73/−1 | 測試簡化、邊角 case | kirktrue: "I'm thinking about the corner case where we _don't_ loop again and call `checkInflightPoll()` because the timer is exhausted. I guess in that case we'll hit the error the next time the user calls `poll()`" [r3061393834](https://github.com/apache/kafka/pull/21991#discussion_r3061393834)；"Could this be simplified:" [r3061410941](https://github.com/apache/kafka/pull/21991#discussion_r3061410941) | 小修（卡在無 committer 注意力，不是被反對） |
| [#18737](https://github.com/apache/kafka/pull/18737) | KAFKA-18641 | frankvicky | lianetm, junrao, kirktrue | ~10 輪、35 force-push；14 files +357/−243 | 誰擁有 subscription state、snapshot vs 改觸發時機、app thread 等背景的 timeout | kirktrue: "We've done a lot of work to ensure the background thread "owns" the current subscription state. This seems to go against those efforts." [r1934527376](https://github.com/apache/kafka/pull/18737#discussion_r1934527376)；lianetm: "I wonder if we should fix here in the same way we're tackling the commit before revocation, by fixing **when** we auto-commit, not **what** we auto-commit?" [r1949663649](https://github.com/apache/kafka/pull/18737#discussion_r1949663649)；lianetm: "the fetching happening in the app thread is probably the elephant in the room here, but that is definitely food for thought for after 4.0 :)" [r1954732895](https://github.com/apache/kafka/pull/18737#discussion_r1954732895)；lianetm: "should we pass the default api timeout here? ... if the background thread is faulty (ie. died) ... the consumer would hang here indefinitely" [r1958555476](https://github.com/apache/kafka/pull/18737#discussion_r1958555476) | 結構（拒絕 snapshot 局部修法，改成「在 PollEvent 處理時才 reconcile/commit」） |
| [#18376](https://github.com/apache/kafka/pull/18376) | KAFKA-18391 | Wadimz | mumrah | 0 輪；1 file +1 | （CI 改動，與 consumer 無關） | mumrah: "Thanks for the fix! LGTM" [review-2557166406](https://github.com/apache/kafka/pull/18376#pullrequestreview-2557166406) | 中性（任務清單編號誤植） |
| [#22836](https://github.com/apache/kafka/pull/22836) | KAFKA-20253 | ezhou413 | AndrewJSchofield | 1 輪、27 小時；6 files +88/−1 | 只要求 share consumer 對稱測試 | Andrew: "Should there also be a `ShareHeartbeatRequestManagerTest.testMaximumTimeToWaitWhenCoordinatorUnavailableDoesNotSpin` added?" [review-4705234077](https://github.com/apache/kafka/pull/22836#pullrequestreview-4705234077)；作者主動把 classic 修法拆出去（"This PR fixes the heartbeat spin issue for the `consumer` protocol."） | 小修 |
| [#22018](https://github.com/apache/kafka/pull/22018) | KAFKA-20426 | brandboat | chia7712, kirktrue, lianetm, JiayaoS | 3 輪；5 files +109/−6 | 條件要**縮小**、Streams 另開 JIRA、測試變脆 | chia7712: "Maybe we could **narrow the condition to `state == MemberState.UNSUBSCRIBED`, focusing strictly on the manual assignment issue we are facing.**" [r3088354438](https://github.com/apache/kafka/pull/22018#discussion_r3088354438)；lianetm: "Just wonder if we should review the same for Streams? ... (we can track/fix separately if it ends up having more implications" [r3148290544](https://github.com/apache/kafka/pull/22018#discussion_r3148290544)；chia7712: "I'm a bit concerned that 'fewer requests' might uncover brittle test cases, but we can get through this :)" [r3154404841](https://github.com/apache/kafka/pull/22018#discussion_r3154404841)；作者附 flamegraph："CPU time ... (10916ms -> 50ms, ~99.54% decrease)" [issuecomment-4317605510](https://github.com/apache/kafka/pull/22018#issuecomment-4317605510) | 小修 |
| [#18590](https://github.com/apache/kafka/pull/18590) | KAFKA-18569 | frankvicky | kirktrue, lianetm, chia7712 | ~4 輪、15 force-push；7 files +74/−5 | 新 event vs `pollOnClose()`、close timeout 拆出去 | kirktrue: "If that approach works, we wouldn't need an extra `ApplicationEvent`." [issuecomment-2598999334](https://github.com/apache/kafka/pull/18590#issuecomment-2598999334)；lianetm: "right after it we could signal to the CoordinatorReq manager that it's closing (same effect as the HB thread shutdown in the classic I would say), so it does not generate any more FindCoord?" [issuecomment-2611089054](https://github.com/apache/kafka/pull/18590#issuecomment-2611089054)；lianetm: "I think we should file a separate jira for the close timeout considering the request timeout" [issuecomment-2613396568](https://github.com/apache/kafka/pull/18590#issuecomment-2613396568) | 中性（偏結構但嚴格限定範圍：一個 event 對 manager 發 closing 訊號） |
| [#19914](https://github.com/apache/kafka/pull/19914) | KAFKA-19357 | Mirai1129 | chia7712, frankvicky, kirktrue, AndrewJSchofield, TaiJuWu, +5 | 多輪、4 個月；3 files +86/−0 | 拒絕「close 時繼續找 coordinator」的行為改動、要求 IT + 時間斷言 | chia7712: "If the consumer doesn't have a coordinator running, does it make sense to find one during closing?" [r2132367537](https://github.com/apache/kafka/pull/19914#discussion_r2132367537)；Andrew: "If you start a consumer when there are no running brokers, close needs to complete promptly." [r2132406509](https://github.com/apache/kafka/pull/19914#discussion_r2132406509)；frankvicky: "we should complete the pending commits request exceptionally (e.g., `CommitFailedException`) if the consumer is closing and the coordinator is unknown" [review-3008497978](https://github.com/apache/kafka/pull/19914#pullrequestreview-3008497978)；kirktrue (changes requested): "because closing the `AsyncKafkaConsumer` is such a tricky area, we really do need to have some tests" [review-2973143429](https://github.com/apache/kafka/pull/19914#pullrequestreview-2973143429) | 小修（fail-fast 取代擴大行為） |
| [#16885](https://github.com/apache/kafka/pull/16885) | KAFKA-17066 | lianetm | kirktrue, AndrewJSchofield, chia7712 | ~6 輪；12 files +491/−477 | 邏輯該放哪個 owner、雙 deadline、每 poll 一個 event 的成本、refactor 不夾行為改動 | kirktrue: "I'd prefer to keep `ApplicationEventProcessor` focused on dispatching events to their corresponding `RequestManager` method(s)." [r1722502431](https://github.com/apache/kafka/pull/16885#discussion_r1722502431)；Andrew: "Couldn't the OffsetsRequestManager simply make the sequence of RPCs to complete the little dance, regardless of a timeout? I don't like caching the event" [review-2247401944](https://github.com/apache/kafka/pull/16885#pullrequestreview-2247401944)；kirktrue: "I get squeamish about changing things like this as part of a refactor." [r1745853353](https://github.com/apache/kafka/pull/16885#discussion_r1745853353)；Andrew: "I do like putting the logic into the offsets request manager although it's getting a bit hard to follow now." [review-2264398380](https://github.com/apache/kafka/pull/16885#pullrequestreview-2264398380) | 結構（作者本來就在做結構改動；reviewer 把它推到正確的 owner，並禁止夾帶行為變更） |
| [#17342](https://github.com/apache/kafka/pull/17342) | KAFKA-17674 | lianetm | chia7712 | 1 輪、2 天；3 files +70/−23 | reviewer 提了替代結構，自己撤回 | chia7712: "**I'm +1 on the current approach, as it's simpler and more readable.**" [r1785635984](https://github.com/apache/kafka/pull/17342#discussion_r1785635984)；chia7712: "could you explain how to reproduce the issue?" [review-2343668231](https://github.com/apache/kafka/pull/17342#pullrequestreview-2343668231) | 小修 |
| [#21476](https://github.com/apache/kafka/pull/21476) | KAFKA-15529 | majialoong | chia7712 | 1 輪；3 files +48/−2 | 結構性顧慮寫明「follow-up」 | chia7712: "I'm a bit concerned about relying on partial thread-safety within `CompletedFetch` ... Have we considered using a separate concurrent set to track the buffered partitions instead? **This can be addressed in the follow-up. BTW, could you please add a unit test?**" [review-4043020001](https://github.com/apache/kafka/pull/21476#pullrequestreview-4043020001) | 小修（結構留 follow-up） |
| [#18089](https://github.com/apache/kafka/pull/18089) | KAFKA-18160 | brandboat | chia7712, lianetm, kirktrue, dajac | ~4 輪；6 files +240/−97 | 語意改動需要 KIP、中間版本行為過寬被退回、背景執行緒送 fetch 的 race | chia7712: "addressing it may require a KIP (or further discussion) to define the new behavior of the callback in handling exceptions. **For now, this PR can focus on fixing the inconsistent behavior.**" [r1880221790](https://github.com/apache/kafka/pull/18089#discussion_r1880221790)；lianetm: "the risk is that with the new consumer the fetching happens in the background thread ... It's not conceptually right." [r1880843295](https://github.com/apache/kafka/pull/18089#discussion_r1880843295)；chia7712: "Please add a test case where the callback never succeeds, and ensure that no records can be polled." [r1880949459](https://github.com/apache/kafka/pull/18089#discussion_r1880949459) | 小修 |

### 2.1 補充 PR（不在清單，但直接關係本題）

| PR | JIRA | 作者 | Reviewers | 摘要 | 代表引文 | 判定 |
|---|---|---|---|---|---|---|
| [#20521](https://github.com/apache/kafka/pull/20521) | KAFKA-18376 | kirktrue | lianetm（28 次 review + approve，127 則 inline） | 引入 `AsyncPollEvent`，poll 不再阻塞 app thread；27 files +722/−304；2025-09-10 → 11-03 | lianetm: "Seems like unneeded states/transitions that make the `CompositePollEvent` more complex, but I could be missing why we may need it like this?" [r2398894476](https://github.com/apache/kafka/pull/20521#discussion_r2398894476)；"Having these `states` (started, ok, error) and the `result` (ok, error) seem somehow redundant to me." [r2407233098](https://github.com/apache/kafka/pull/20521#discussion_r2407233098)；"this whole flow is already looking much simpler, nice!" [r2402927027](https://github.com/apache/kafka/pull/20521#discussion_r2402927027)；"Trunk version passes locally for me, so let's revert this (and take it as good news again)" [r2414431575](https://github.com/apache/kafka/pull/20521#discussion_r2414431575)；"get a run of system tests to validate (we discovered issues on this area before, only with the sys tests)" [issuecomment-3437014866](https://github.com/apache/kafka/pull/20521#issuecomment-3437014866) | **結構性改動被接受**，但條件是：core committer 作者、有 CPU 證據（JIRA：maxPoll=5 時 CPU >50% vs classic 10%）、reviewer 反覆刪狀態機、不准改既有測試、要 system test 報告 |
| [#22979](https://github.com/apache/kafka/pull/22979) | KAFKA-20780 | lianetm | AndrewJSchofield | fetch gap 修法；+180/−13 | lianetm 自註："**this is the fix, keeping it minimal, but as follow-up I will refactor this path to make it less error prone**, ideas here KAFKA-20844, I will take it separately right after this PR" [r3667584607](https://github.com/apache/kafka/pull/22979#discussion_r3667584607) | 小修 + maintainer 自認領結構 follow-up |
| [#23124](https://github.com/apache/kafka/pull/23124)（closed） | KAFKA-20915 | lianetm | chia7712, m1a2st | 與 #23014 同根因，Lianet 主動關掉自己的 PR | chia7712: "Is this duplicate to #23014?" [issuecomment-5252061797](https://github.com/apache/kafka/pull/23124#issuecomment-5252061797)；lianetm: "the approach on the other PR is better imo, covers more, because it's getting the info from the prepare func (the one who knows why exactly we are generating no requests)" [issuecomment-5252948011](https://github.com/apache/kafka/pull/23124#issuecomment-5252948011) | 偏好「知道原因的那一層」提供資訊（與 KIP-1371 typed condition 的動機一致，但實作是 `FetchRequestPreparationResult` 這種局部型別） |
| [#23228](https://github.com/apache/kafka/pull/23228)（open） | KAFKA-20904 | MdTanwer | m1a2st | fetch in-flight 時不設上限 | m1a2st: "`maximumTimeToWait()` is computed on the network thread and cached, while the application thread may drain the buffer before reading it. A cached `Long.MAX_VALUE` can then become stale ... **The current simple `retryBackoffMs` constant avoids this stale-cache issue by not depending on buffer state at all, which I think is the right trade-off.**" [review-5015898852](https://github.com/apache/kafka/pull/23228#pullrequestreview-5015898852) | 小修（保守常數 > 依狀態的精準值） |
| [#23404](https://github.com/apache/kafka/pull/23404)（open） | KAFKA-21049 | dybyte | — | retry.backoff.ms=0 floor；+61/−8 | 尚無 review | — |
| [#23023](https://github.com/apache/kafka/pull/23023)（open） | KAFKA-20860 | lh0156 | lianetm（1 則） | 防禦性 try/catch | lianetm: "Am I missing a case where we could indeed have such "sync failure" in that section of the code? Or was the motivation here just to be defensive?" [issuecomment-5207342111](https://github.com/apache/kafka/pull/23023#issuecomment-5207342111) | 無可達路徑的修法不會被推進 |

## 3. 橫切模式

### 3.1 reviewer 一致獎勵的東西

| 模式 | 證據 |
|---|---|
| **小 diff、單一 manager、單一原因** | #22836 27 小時合併（+88）；#17342 兩天（+70）；#22018 要求「narrow the condition ... focusing strictly on the manual assignment issue」；#23227 chia7712「keep the current change as is」 |
| **會重現 spin 的測試**（`...DoesNotSpin`、loopCount、真 DNS） | JiayaoS 在 #22018 貼 "Busy loop detected! loopCount:78668 ... Proves that it indeed causes a busy loop."；chia7712 #23227 要求開真 DNS resolution；#23348 每個 manager 一個 `...DoesNotSpin` |
| **classic consumer 對照** | #18089 chia7712 用 classic 行為定義正確答案；#18737 lianetm "which is what the classic does"；#19914 Andrew 拿 close 行為對照 |
| **不動 public API** | 16885、20521、18737 等結構改動都沒開 KIP；#18089 一旦觸及 callback 語意就說 "may require a KIP" |
| **註解 / javadoc 說明「為什麼是這個等待值」** | #23348 lianetm 要求註解 STALE/FENCED 為何要喚醒 app thread；#23014 chia7712 要求更新 `pollForFetches` 註解；#18590 lianetm 要求 event javadoc；#18737 junrao 要求 `canCommit` javadoc |
| **既有測試不改；要改要解釋** | #20521 lianetm 多次 "let's revert this"、"I'm keen on fully understanding these kind of changes, to make sure we're not hiding an..."；#22018 chia7712 對 "fewer requests might uncover brittle test cases" 的警覺 |
| **把相鄰發現拆成新 JIRA** | KAFKA-21010（chia7712 從 #23227 拆）、KAFKA-21059（lianetm 從 #23348 拆）、KAFKA-20540（chia7712 從 #22018 拆 Streams）、KAFKA-21049（從 #23357 拆）、#18590 close timeout 拆 JIRA |
| **作者主動附 CPU 證據** | #22018 flamegraph；#20521 JIRA 附 profiling；KAFKA-20904 用排程效能測試驗證 |

### 3.2 reviewer 一致拒絕的東西

| 模式 | 證據 |
|---|---|
| **在 bug fix PR 內做全面重構** | Andrew #23014 "I'd like to see a more comprehensive refactor, but probably not right now." |
| **再加一個特例分支** | Andrew #23014 "I don't want to make yet another special case with additional tricky logic" |
| **refactor 夾帶行為改動** | kirktrue #16885 "I get squeamish about changing things like this as part of a refactor."；chia7712 同串 "could you please keep using `equals`" |
| **多餘的狀態機 / 包裝型別** | lianetm #20521 對 `CompositePollEvent` 狀態、`Result` 類別、`started` 狀態的連續砍除 |
| **app thread 直接碰 subscription state** | kirktrue #18737 "This seems to go against those efforts."；lianetm #20521 對 `hasAllFetchPositions` 在 app thread 的 race 顧慮 |
| **改變語意但沒有 KIP** | chia7712 #18089 "may require a KIP (or further discussion)" |
| **找不到可達路徑的防禦性修法** | lianetm KAFKA-20860 / #23023 |
| **新 event 若舊 hook 就能解** | kirktrue #18590 "we wouldn't need an extra `ApplicationEvent`"（後來因 `pollOnClose` 不夠才接受 event） |
| **要求 benchmark** | **沒有任何 reviewer 要求 benchmark**；但 poll 路徑的結構改動要 system test（#20521） |

### 3.3 對 busy-loop 家族「方向」的明確發言

- 承認需要更大整理、但延後：Andrew #23014（上引）；lianetm KAFKA-20844 "has proven error-prone ... maybe explicit states or transitions around the fetching lifecycle"；lianetm #18737 "the fetching happening in the app thread is probably the elephant in the room ... after 4.0"。
- 統一常數而非精準推導：chia7712 #23227 "the straightforward way is to return `retryBackoffMs`" [r3904470294](https://github.com/apache/kafka/pull/23227#discussion_r3904470294)；lianetm #23227 "In the fetch path we used the `retryBackoff` for the case of inflights"；m1a2st #23228 "simple `retryBackoffMs` constant ... the right trade-off"。
- 跨 manager 一致性由 committer 集中處理：chia7712 "opened KAFKA-21010 to address it for all three heartbeat managers together"。
- 集中到 consumer 層的提議被提出但未被要求：chia7712 #23014 [r3781539485](https://github.com/apache/kafka/pull/23014#discussion_r3781539485)。
- **沒有人說**「we keep patching maximumTimeToWait, we need a real fix」這類話；也沒有人說相反的「永遠只打補丁」。最接近的是 Andrew 的兩句（想要重構 / 不是現在）與 Lianet 自己認領 KAFKA-20844。
- 局部修法的效果已被外部量測確認：KAFKA-20904 "CPU usage ... has come back down to levels similar for the `classic` protocol after the fix for KAFKA-20854"。

## 4. 對新 KIP 的含意

依上面證據，最可能被接受的提案形狀：

1. **先確認是否需要 KIP。** 內部結構改動（#16885、#20521、#18737）都沒開 KIP；KIP-1371 目前唯一的 public surface 是四個 `reactor-*` metrics。若拿掉 metrics（或改用既有 metrics），整件事可退回「umbrella JIRA + 分段 PR」，這是 reviewer 已證明會處理的路徑；若保留 metrics，KIP 應縮到只定義 metrics 與「不改 API、不改 thread 名稱」的相容性承諾。
2. **切成 reviewer 已經在做的單位**，每片獨立可合、可 revert：
   - (a) `maximumTimeToWait` / `timeUntilNextPollMs` 契約：把「不能送但時間會解 → `retryBackoffMs`；不會有下一次 → `MAX_VALUE`」寫成 `RequestManager` javadoc + `AbstractHeartbeatRequestManagerTest` 共用測試。證據需求：每個 manager 一個 `...DoesNotSpin` 測試（沿用 KAFKA-21010/21059 的形式），並與 lianetm/Ken Huang 的 KAFKA-21059 對齊，避免像 #23124/#23014 那樣撞車。
   - (b) 「知道原因的那層提供原因」：#23014 的 `FetchRequestPreparationResult` 是 reviewer 已接受的先例（lianetm："getting the info from the prepare func (the one who knows why exactly we are generating no requests)"）。typed next-poll condition 若要進，應以此形式逐 manager 引入，而不是一次換掉 `PollResult`。
   - (c) inflightPoll 生命週期：KAFKA-20844 已由 lianetm 認領。應在該 JIRA 下提設計或 PR，不要另開平行結構。
   - (d) app/background 發佈順序（KAFKA-18641 類）：reviewer 的接受形式是「改觸發時機」（在 PollEvent 處理時做），不是「快照 + 集中 publisher」。
3. **每片需要的證據**（從 reviewer 實際要求歸納）：一個修前失敗、修後通過的測試；classic consumer 的對照行為；`retry.backoff.ms=0` 等邊界；若動到 poll 路徑，附 system test 報告與 CPU 對照（作者主動附，如 #22018 的 flamegraph）；既有測試零改動，或逐一解釋。
4. **不要做的事**：一次性 rename `ConsumerNetworkThread`（#20521 連 `CompositePollEvent` 多兩個狀態都被砍）；在同一 PR 改 Streams/Share（reviewer 慣例是另開 JIRA）；把「集中決策」當賣點——Andrew 承認原則沒守住，但接受的是「更清楚的 case 列表」而非新的決策中心。
5. **時程訊號**：busy-loop 修法被列 4.4 blocker 並 backport；結構性整理（KAFKA-18139 自 2024-12 open、KAFKA-20844 自 2026-07 open）沒有 release 壓力。提案若綁 release，只有「修 bug 的那片」會被排程。

## 5. 本機 skill 檔（`~/.claude/skills/kafka-reviewer-prefs/`）已記錄的內容

- 只有 `reviewers/chia7712.md` 一個檔；**沒有 Ken Huang（GitHub `m1a2st`）的檔**，也沒有 lianetm / kirktrue / AndrewJSchofield。
- chia7712 檔重點（與本題相關者）：
  - PR hygiene："Keep the PR focused; defer broad refactors to a follow-up PR."（證據 #22353、#22409）；"No unrelated changes."
  - 測試："No time-based / flaky tests."、"Poll for metadata sync with `TestUtils.waitUntilTrue`"、"Add tests for new classes/behavior, and prove specific edge cases with ITs"。
  - 風格：`static`、移除多餘布林條件、不做多餘防禦性複製、hot path 用 for-loop 不用 stream、單次使用的 helper 內聯、小 helper 巢狀在 owner 內。
  - Meta："Focused PR. Diff contains only the stated goal; spin off tangential refactors into their own PR"。
- 本次新觀察、可回寫 skill 的 generalizable 偏好：
  - chia7712：busy-loop 修法要開真 DNS resolution 的測試（#23227）；跨 manager 的一致性問題自己開 JIRA 統一處理而不是擴 PR（KAFKA-21010、KAFKA-20540）；有結構性顧慮會寫「This can be addressed in the follow-up」（#21476）。
  - lianetm：對狀態機/包裝型別零容忍地要求刪除（#20521）；poll 路徑改動要 system test；既有測試改動要逐一解釋；自己做修法時「keeping it minimal」並另開結構 JIRA（#22979 / KAFKA-20844）。
  - AndrewJSchofield：接受「更清楚列出各 case」的局部修法，拒絕再加特例；想要重構但不在 bug fix 內（#23014）。
  - kirktrue：優先用既有 hook（`pollOnClose`）而非新 event（#18590）；refactor 不得夾帶行為改動（#16885）；背景執行緒擁有 subscription state（#18737）。
  - Ken Huang（m1a2st）作為 reviewer：偏好不依賴跨執行緒快取狀態的保守常數（#23228）。
