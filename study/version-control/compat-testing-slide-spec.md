# 修改建議 · Part 1 · 代價 · 測試（給 main session 直接執行）

> 本檔是 fork session（相容性測試代價討論）產出的 hand-off spec。把「Part 1 · 代價 · 測試」兩張從「檔名/數字」抬高到「相容性要保證什麼」。所有 REF 行號對 apache trunk UP commit `b7b1c0a83d…` 已逐一驗證。

---

## 一、核心對照：相容性要保證的四件事 ↔ 對應測試（都有、無缺角）

| 義務 | 有測試？ | 測試檔（file:line, UP） | 誰固定 / 誰變版本 |
|---|---|---|---|
| **① 格式自洽**：每個宣稱支援的 wire 版本，自己序列化都要來回讀寫無誤 | ✅ | `clients/.../RequestResponseTest.java:340`（testSerialization, ApiKeys×allVersions）、`MessageTest.java:716` | build 時、單機、無對端 |
| **② 新 client → 舊 broker**：client 肯協商降版、不假設對方有新功能 | ✅ | `tests/kafkatest/tests/client/client_compatibility_features_test.py:132`（+ `client_compatibility_produce_consume_test.py`） | client 釘 **DEV**、broker 變 2.1→4.3 |
| **③ 舊 client → 新 broker**：broker 保留舊 wire 版本、繼續服務 | ✅ | `tests/kafkatest/tests/core/compatibility_test_new_broker_test.py:68` | broker 釘 **DEV**（`:69`）、producer/consumer 變舊版（`:83/:87`） |
| **④ 滾動升級混版**：新舊 broker 並存，inter-broker（MV 釘版）＋對 client 都不掉資料 | ✅ | `tests/kafkatest/tests/core/upgrade_test.py:167`、`transactions_mixed_versions_test.py:192` | 叢集 broker 從舊滾到 DEV |

- ②③ 是 client↔broker 的兩個方向，各有專門檔、剛好對稱（②變 broker、③變 client）。
- ②的 client↔broker 是**真 ApiVersions 協商**；④的 broker↔broker 段是 **MV 把 inter-broker 版本釘在舊值**、非協商。

---

## 二、gen_deck.py 修改（`study/version-control/slides/gen_deck.py`）

### 2.1 P dict 補一筆（反方向測試，目前缺）
在 `client_compat.py` 那行下方加：
```python
 "compat_newbroker.py":"tests/kafkatest/tests/core/compatibility_test_new_broker_test.py",
```

### 2.2 第一張 → 換成「相容要保證什麼」（WHAT）
把 `add_content("Part 1 · 代價 · 測試", "相容性測試（一）：每次 build 全版本 round-trip", …)` 整塊換成：
```python
add_content("Part 1 · 代價 · 測試", "「相容」到底要保證哪些事？", [
    ("bullet", "① 格式自洽：每個宣稱支援的 wire 版本，自己序列化都要來回讀寫無誤", "arrows-split"),
    ("bullet", "② 新 client → 舊 broker：client 肯協商降版、不假設對方有新功能", "point"),
    ("bullet", "③ 舊 client → 新 broker：broker 保留舊 wire 版本、繼續服務", "point"),
    ("bullet", "④ 滾動升級混版：新舊 broker 並存，inter-broker（MV 釘版）＋對 client 都不掉資料", "point"),
    ("solve", "這四條就是「相容承諾」的全部內容——每一條都得有測試長期盯著"),
], [A("RequestResponseTest.java",340,"①格式自洽"), A("client_compat.py",109,"②新→舊"), A("compat_newbroker.py",68,"③舊→新"), A("upgrade_test.py",167,"④滾動升級")])
```

### 2.3 第二張 → 換成「為什麼越來越貴」（SO-WHAT）
把 `add_content("Part 1 · 代價 · 測試", "相容性測試（二）：把過去七年的 Kafka 一起養著", …)` 整塊換成：
```python
add_content("Part 1 · 代價 · 測試", "守住相容，為什麼越來越貴", [
    ("bullet", "① 是「API × 版本」、②③④ 是「版本 × 版本」——測試面是乘積，不是加總", "arrows-split"),
    ("bullet", "有些自動擴張（新版本免補測試、但每次 build 越跑越久），有些得手動 +1（多養一個歷史版本、多一組參數）", "point"),
    ("solve", "所以它是只增不減的持續支出——貴到 Kafka 在 KIP-896 得砍掉 2.1 以前的舊版本止血"),
], [X("KIP-896",KIP896), A("version.py",138,"版本清單手動+1"), A("Dockerfile",79,"養 22 個歷史版本"), A("MetadataVersionTest.java",219,"MV 矩陣")])
```

### 2.4 改法要點（重建後校驗）
- 數字/檔名退出正文（308／22／25／testSerialization 只留 REF 與 `compat-testing-cost.md`）。
- 第一張講 **what**、第二張講 **so-what**；維持 1–2 頁。
- 重建後檢查是否溢出（bullet 盡量各 1 行；④那條較長，溢出就把「（MV 釘版）」拿掉）。

---

## 三、compat-testing-cost.md 該補的兩點（順手一起修）

1. **反方向測試漏了**：doc 只寫 `client_compatibility_*`（new client → old broker），缺 `core/compatibility_test_new_broker_test.py`（old client → new broker）。→ client↔broker 兩方向都要列。
2. **協商 vs MV 的區分**：跨版本互通裡，`client_compatibility_*` 是真 ApiVersions 協商；`upgrade`/`mixed_versions` 的 broker↔broker 段是 MV 把 inter-broker 版本釘在舊值、非協商。doc 應標明哪層協商、哪層 MV 釘。
