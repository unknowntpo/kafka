# ASYNC-CONSUMER-V2：分支地圖（2026-09-08）

| 分支 | 內容 | 用途 |
|---|---|---|
| `async-consumer-v2-loop-only`（本分支） | `ConsumerEventLoop` + `ManagerTask` + `LoopTimer` / `LoopSignal` + 零 event 的 poll；fetch 路徑是 trunk 原封不動的 `FetchRequestManager` / `FetchBuffer` / `FetchCollector`；`EventLoopKafkaConsumer` 由 `ConsumerDelegateCreator` 選用 | **主線**。後續依 07 文件在此解決 KIP-1371 的四類問題與 busy loop 分類 |
| `async-consumer-v2-with-fetch-pipeline`（tag `async-consumer-v2-full-2026-09-08`） | 上者加 `FetchPipeline`（credit 驅動續發、position epoch fencing）、`RecordSink` / `Parker`、`SinkCollector`、M0 `RecordHeaders`、M2 解碼路徑，以及 Codex 審查 F1–F9 的修正 | **備份**。fetch 路徑的優化屬 04 文件「可回移」類，之後獨立處理（回移進 `AsyncKafkaConsumer` 或另開分支） |
| `async-consumer-v2-review-fixes` | 與備份相同的 commit 序列（歷史保留） | 交叉審查的參照點 |
| `async-consumer-v2-exp-prefetch` @ `eb83eb21b1` | 02 文件 §3.7 的 100 行實驗 hack | 只供對照 |

文件 01–07 與 `ASYNC-CONSUMER-V2-bench/` 在本分支與備份分支各有一份，內容相同（2026-09-08 起以本分支為準）。03 文件 §2.1 的三方 A/B（trunk / loop-only / 完整版）說明兩個分支各自貢獻了什麼。
