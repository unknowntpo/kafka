# ASYNC-CONSUMER-V2 圖解

用 [archify](https://github.com/tt-a1i/archify) 從 JSON 規格產生的自包含 HTML（含深淺色、導覽章節、搜尋、匯出）。

- `pipelined-consumer.architecture.json`：規格（archify `architecture` 型別，showcase 品質）。
- `pipelined-consumer.architecture.html`：直接用瀏覽器開啟。Viewer 的固定 UI 是英文（archify 只支援 en / zh-CN 的 UI 語系，內容為繁體中文）。

重新產生：

```bash
node ~/.claude/skills/archify/bin/archify.mjs deliver architecture pipelined-consumer.architecture.json pipelined-consumer.architecture.html --quality showcase --json
```
