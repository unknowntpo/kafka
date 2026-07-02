# 修改建議 · Part 1 · 對照（PostgreSQL bullet 校正）給 main session

> fork session 查證：現況「複製層無法線上滾動升級」講太滿。PostgreSQL 其實**能**做近零停機大版升級——但要**改用 logical replication 做 blue/green 切換**，是 bolt-on、有代價。physical/streaming replication 因 **WAL 格式不能跨大版**才鎖 major。以下把「WAL 不能跨大版」稍微點一下、並把對比講精確。

---

## 一、gen_deck.py 修改（`study/version-control/slides/gen_deck.py`）

### 1.1 新增一個 URL 常數（放在其他 KIP/URL 常數旁）
```python
PGLOGREPL="https://www.postgresql.org/docs/current/logical-replication.html"
```

### 1.2 對照 slide：換 PostgreSQL bullet ＋ solve
找到 `add_content("Part 1 · 對照", "同一道題，別家怎麼答——各付什麼代價", …)`。

**PostgreSQL bullet**（原：`"PostgreSQL：協定 v3 逾二十年極穩定；但實體複製鎖 major → 複製層無法線上滾動升級（得停機 pg_upgrade 或另建叢集）"`）改成：
```python
    ("bullet", "PostgreSQL：協定 v3 逾二十年極穩定；physical replication 靠 WAL、跨大版不相容 → HA replica 不能就地跨大版滾。近零停機得改搭 logical replication 做 blue/green 切換（bolt-on，DDL/sequence 不複製）", "leaf"),
```

**solve**（原：`"沒有免費的選擇：Kafka 拿協定複雜度換到細粒度演進＋複製層線上升級——後者正是 PostgreSQL 買不到的"`）改成：
```python
    ("solve", "沒有免費的選擇：Kafka 拿協定複雜度換到細粒度演進＋複製層內建線上滾動升級——PostgreSQL 得另搭 logical replication 才換得到"),
```

### 1.3 REF 補一個 logical-replication 出處
把該 slide 的 ref（原 `[X("MongoDB hello / FCV",MONGO), X("PostgreSQL protocol",PGURL)]`）改成：
```python
], [X("MongoDB hello / FCV",MONGO), X("PostgreSQL protocol",PGURL), X("PG logical replication",PGLOGREPL)])
```

### 1.4 校驗
- PG bullet 變 2 行，重建後看 對照 slide 是否溢出；溢出就把「（bolt-on，DDL/sequence 不複製）」縮成「（bolt-on、有代價）」。

---

## 二、要點（why 這樣改）
- **只點到為止**：正文提「physical replication 靠 WAL、跨大版不相容」一句即可，不展開 WAL 細節。
- **對比反而更強**：不是「PG 做不到」，而是「PG 要**換一套機制（logical repl）＋扛 DDL/sequence/large-object 不複製等 caveat**，才換到 Kafka partition replication **內建就有**的線上滾動升級」。
- **分寸**：minor 版本 PG 本來就能 streaming replica 滾動升級（跟 Kafka 類似）；差別只在 **major**。若講者被追問，補一句「minor 一樣能滾，卡的是 major」。

## 三、other-systems-comparison.md 同步
把該檔 PostgreSQL 段落一併更新為同一結論：physical replication 因 WAL 鎖 major；logical replication（PG17+ `pg_createsubscriber` 更順）可做 blue/green 近零停機大版升級，但 DDL/sequence/large-object 不複製、需 replica identity/PK，屬 bolt-on。

## 四、來源
- PostgreSQL 官方 · Logical Replication：https://www.postgresql.org/docs/current/logical-replication.html
- Cybertec · Major version upgrade using logical replication：https://www.cybertec-postgresql.com/en/upgrading-postgres-major-versions-using-logical-replication/
- AWS Aurora · Major version upgrade via logical replication：https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.MajorVersionUpgrade.html
