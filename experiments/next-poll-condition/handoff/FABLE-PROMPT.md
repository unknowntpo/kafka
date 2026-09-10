請你獨立驗證 Kafka NextPollCondition 原型，並回答：「如果由你接手開發，會朝哪個方向前進與優化？為什麼？」請用自己的證據判斷，不必沿用前一位 agent 的結論。

Repository：git@github.com:unknowntpo/kafka.git
交接分支：codex/next-poll-condition-trunk
已驗證的原型 source commit：9f8dc35e285107ce9f926f35ab74486796161073（分支後續 handoff commit 只補交接資料）
基準 trunk：820533b870106cc0e0ac60e2076b8644d68bd85f

先 fetch 交接分支並記錄完整 SHA，從該 SHA 建立你自己的獨立 worktree 與新分支，例如 fable-next-poll-review / codex/fable-next-poll-review。遵守既有 worktree SOP：plain container 下放 trunk anchor 與 sibling worktree；不要使用或改動原 agent 的 worktree。若同名目錄／分支已存在，先檢查，不能覆蓋。所有修改與測試在你的 worktree 中進行。

先讀 AGENTS.md、experiments/next-poll-condition/handoff/README.md，以及該目錄 evidence 中的 scheduler-migration-inventory.md、trunk-jmh.md、deadline-tree-vs-scan.md。這是一份待驗證的完整原型快照，不是已證明改善效能的方案。

目前架構：ConsumerNetworkThread → RequestManagerScheduler → RequestManager.poll → PollResult/NextPollCondition。Scheduler 維護 ready、legacy 與 deadline TreeSet；Signal 與 Waiting 訂閱有 network-thread confinement、版本捕捉、一次性取消／重用與固定 batch 契約。ApplicationPollWait 是另一個跨執行緒同步邊界。普通 consumer 七個 managers 中 Coordinator、Heartbeat、Offsets、Fetch 使用 condition；Commit、Membership、TopicMetadata 仍為 legacy。maximumTimeToWait 仍掃所有 managers，application 也仍有短 retry；transport wakeup 不代表 manager ready。

請先完成三件事：
1. 從呼叫路徑與測試核對正確性：漏通知、publish-before-arm、同批 signal、取消與 late completion、deadline overflow、in-flight/backoff/未知 coordinator 的空轉、poll(0)、close、rebalance 順序，以及背景 auto-commit 的 wait epoch／offset snapshot 競態。檢查靜態分析排除項的 thread-confinement 理由。不要只用每輪手動 poll manager 的測試掩蓋漏通知。
2. 在你的 checkout 重跑相關測試與格式檢查，區分實際通過、環境阻塞、既有失敗。核對 live 與 frozen benchmark 的差異，尤其 no-spin；歷史 342 項通過不等於目前 commit 的驗證。
3. 評估成本與方向：區分 scheduler、所有 manager 的 maximumTimeToWait 掃描、application 等待與 transport wakeup。先量測或追蹤各 manager poll 次數、空 poll、ready/deadline/wakeup、配置與 CPU，再決定瓶頸。診斷計數不得污染正式效能比較。

已有實驗只供參考：完整原型相對 trunk 尚未證明整體效能收益；no-spin 在 coordinator unavailable 情境大幅減少 CPU，但不能推論一般吞吐無回退。固定同一原型的 TreeSet vs 直接 scan 實驗中，Scan 真實 consume 五輪 CPU/record 幾何平均 +3.29%，探索性區間包含零，未支持採用該版 scan。這不排除只掃一次／快取最小 deadline 的其他設計，也不證明 manager 全面遷移一定更快。

若要跑效能，先列清楚每版唯一變因、source SHA、JDK、資源限制、workload 與 correctness gate；使用真實 broker、交錯順序的多個獨立 JVM forks、暖機與每輪結果。至少關心正常 consume、idle、coordinator unavailable；不要用局部 ns/pass 倍率代替整個 consumer 的 CPU/s 或 CPU/record。外部主機依既有授權與使用規範；測試資源採獨立名稱並清理。

最後請交付：
- 獨立發現，依嚴重度附檔案／行號、觸發條件與可重現測試；沒發現也明確說明覆蓋範圍。
- 你會保留、簡化或捨棄的設計，以及理由。比較先修 no-spin／局部修正、遷移 TopicMetadata／Commit、持續 Fetch demand 與移除 app fallback、簡化 scheduler 等方向，無須預設全面 condition 化是目標。
- 你優先做的 1–3 個小改動：預期收益、風險、所需通知／deadline owner、驗收指標與停止條件。區分已量到的瓶頸與假說。
- 若小型驗證需要，可在你的分支加入 regression test 或最小實驗；先完成獨立評估，不直接展開全面重寫。不要合併到原分支或開啟長期 benchmark。回報你的分支、base SHA、測試命令與結果，讓我們決定下一階段實作。
