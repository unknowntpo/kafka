# Lab：重現經典版本錯誤（tour0）

前置：cluster 已跑（`apache/kafka:4.1.0`，controller ×3 + broker ×3）

```bash
cd study/version-control/lab/tour0
docker compose up -d
# 看目前 feature 上限
docker compose exec broker-1 /opt/kafka/bin/kafka-features.sh \
  --bootstrap-server broker-1:19092 describe
```

---

## ③ INVALID_UPDATE_VERSION —— exec 進去 + 把 feature 升過支援上限　✅ 已實測

`transaction.version` 上限是 2，試著升到 3（沒有節點支援）：

```bash
docker exec kafka-version-tour0-broker-1-1 \
  /opt/kafka/bin/kafka-features.sh --bootstrap-server broker-1:19092 \
  upgrade --feature transaction.version=3
```

**實際輸出（真訊息）**：

```text
Could not upgrade transaction.version to 3. The update failed for all features
since the following feature had an error: Invalid update version 3 for feature
transaction.version. Local controller 3 only supports versions 0-2
1 out of 1 operation(s) failed.
# exit code 1
```

→ 對應投影片 error ③（metadata.version / feature，管理操作時）。
註：升 `metadata.version` 同理，但 `--metadata 4.2` 之類會被工具當 unknown version 擋掉；用 feature 升過上限最乾淨。

---

## ① client wire error —— 老 client 對新 broker（UnsupportedVersionException）

重點：**producer 對 4.1 broker 多半不會錯**，因為 Produce／Metadata 都從 v0 廣播。要觸發得用夠舊的 **consumer**（Fetch < v4，即 0.11 以前）：

```bash
NET=$(docker network ls --format '{{.Name}}' | grep kafka-version-tour0 | head -1)
docker exec kafka-version-tour0-broker-1-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker-1:19092 --create --topic verdemo --if-not-exists

docker run --rm --network "$NET" eclipse-temurin:8-jre bash -c '
  cd /tmp && curl -sL https://archive.apache.org/dist/kafka/0.10.2.2/kafka_2.11-0.10.2.2.tgz | tar xz
  cd kafka_2.11-0.10.2.2
  bin/kafka-console-consumer.sh --bootstrap-server broker-1:19092 --topic verdemo --from-beginning'
```

注意：老 client 下載慢，且遇到版本不合時常是**持續重試 / 印 warning**、不會乾淨退出，現場不好掌握。
**保證版**的 client 錯誤用 Demo 1 單元測試（直接印出 exact message）：

```text
The node does not support PRODUCE with version in range [11,13].
The supported range is [0,10].
```

---

## ④ broker 註冊失敗 —— finalized MV 太高，老 broker 加不進來

需要混版。設計：compose 另加一台舊 binary 的 broker，等 4.1 cluster 的 MV 已 finalize 到 `4.1-IV1` 後再啟動它，它註冊時會丟：

```text
Unable to register because the broker does not support finalized version <MV>
of metadata.version. The broker wants a version between <min> and <max>, inclusive.
```

compose 片段（示意，尚未實測）：

```yaml
  broker-old:
    image: apache/kafka:3.9.1          # 舊到不支援 4.1-IV1
    hostname: broker-old
    environment:
      KAFKA_NODE_ID: 9
      KAFKA_PROCESS_ROLES: broker
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@controller-1:9093,2@controller-2:9093,3@controller-3:9093
      # ...其餘 listener/log dir 比照 broker-1
    # 等主 cluster MV=4.1-IV1 後才 `docker compose up broker-old`，觀察它 register 失敗
```

---

## 對應投影片

| lab 情境 | 投影片 error | 通訊層 |
| --- | --- | --- |
| ③ `upgrade --feature ...=太高` | INVALID_UPDATE_VERSION | metadata.version / feature |
| ④ 老 broker 註冊 | Unable to register … finalized version | metadata.version / feature |
| ① 老 consumer / Demo 1 | UnsupportedVersionException | client ↔ broker（wire）|
| ② 自刻不支援版本 | InvalidRequestException（broker 關線）| client ↔ broker（wire）|
