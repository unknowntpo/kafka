# KIP-1034 Kafka Streams DLQ Presentation

## Project Overview

這是一個關於 **KIP-1034: Dead Letter Queue in Kafka Streams** 的 Slidev 簡報專案。

- **目的**: 週報告用，介紹 Kafka Streams 4.2 新增的 DLQ 功能
- **技術**: Slidev (Vue-based presentation framework)
- **位置**: `~/repo/unknowntpo/kafka/kip1034-presentation/` (kafka repo 的 worktree)

## 簡報結構 (slides.md)

| Slide | 內容 |
|-------|------|
| 1-2 | Title, Agenda |
| 3-5 | What is DLQ, Why DLQ |
| 6-9 | Before DLQ (舊的錯誤處理方式) |
| 10-12 | After DLQ (KIP-1034 新設計) |
| 13-17 | Basic Usage (設定、Handler 範例) |
| 18-19 | Architecture Overview |
| 20-23 | Demo section |
| 24-25 | Summary, Q&A |

## 常用指令

```bash
cd ~/repo/unknowntpo/kafka/kip1034-presentation

# 安裝依賴
npm install

# 啟動開發伺服器
npm run dev

# 建置靜態檔案
npm run build

# 匯出 PDF
npm run export
```

## Key References

- [KIP-1034](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1034:+Dead+letter+queue+in+Kafka+Streams) - DLQ 主提案
- [KIP-1033](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1033) - ProcessingExceptionHandler

## Kafka 原始碼重點檔案 (相對於 kafka repo root)

```
streams/src/main/java/org/apache/kafka/streams/errors/
├── DeserializationExceptionHandler.java
├── ProductionExceptionHandler.java
├── ProcessingExceptionHandler.java      # KIP-1033 新增
└── ErrorHandlerContext.java
```

從這個 worktree 可以直接查看：
```bash
# 查看 DLQ 相關原始碼
ls ../streams/src/main/java/org/apache/kafka/streams/errors/
```

## Demo 準備事項

簡報最後有 Live Demo，需要準備：

1. **Docker Compose** - 啟動 Kafka cluster
2. **範例程式** - 簡單的 Kafka Streams app with DLQ
3. **測試資料** - 正常 JSON + 故意錯誤的訊息

## TODO / 可改進項目

- [ ] 加入更多 code examples from actual Kafka source
- [ ] 準備 demo 用的 docker-compose.yml
- [ ] 準備 demo 用的 Java application
- [ ] 加入 KIP-1034 實作細節的 slides (如果時間允許)

## Git Info

- **Branch**: `kip-1034-dlq-presentation`
- **Base**: `trunk` (Kafka main branch)
- **Parent repo**: `~/repo/unknowntpo/kafka`
- **Worktree path**: `~/repo/unknowntpo/kafka/kip1034-presentation/`
