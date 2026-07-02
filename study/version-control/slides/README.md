# slides — 「溝通當下的版本選擇」deck 產生原始碼

`kafka-version-negotiation.pptx`（在上層目錄）由 `gen_deck.py` 產生，內容依 [`../talk-outline.md`](../talk-outline.md) 三段式結構、事實依據 [`../rpc-version-selection.md`](../rpc-version-selection.md)。

## 重新產生 pptx

```bash
cd study/version-control/slides
python3 gen_deck.py          # 需 python-pptx；輸出到 ../kafka-version-negotiation.pptx
```

`gen_deck.py` 用相對路徑：icons 讀 `./icons/`、pptx 輸出到 `../`。

## 圖示與主圖

- `icons/*.png`：投影片用的 Tabler outline icon（透明背景）。
- `icons/s2a-rangeline.svg` / `.png`：§2a 的數線主圖（能力範圍取交集 vs finalized MV）。
  改圖後用 `rsvg-convert` 重新輸出 PNG（`cairosvg` 對中文會出豆腐框，需用 rsvg）：

  ```bash
  rsvg-convert -w 2080 -h 960 icons/s2a-rangeline.svg -o icons/s2a-rangeline.png
  ```

## 預覽 / 驗版面

本機沒有 pptx 直算圖時，用 LibreOffice 轉 PDF 再逐頁看：

```bash
soffice --headless --convert-to pdf --outdir /tmp ../kafka-version-negotiation.pptx
pdftoppm -png -r 110 /tmp/kafka-version-negotiation.pdf /tmp/slide
```
