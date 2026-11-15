# FETCH-PIPELINE-09：在 GCP 上把 broker 與 consumer 分開量測（草稿）

狀態：階段一已在 2026-11-15 執行（12 vCPU 配額內：client `c4-standard-8`、broker `n2-highmem-4`、`us-central1-a`、隨選）。結果在 `FETCH-PIPELINE-06-librdkafka-and-scaling.md` §5b。下面「配置」一節的機型是最初的草稿，已被腳本裡的實際設定取代：專案配額是全專案 12 vCPU、C4 每個 region 8 vCPU；只有 C4 支援硬體計數器；`asia-east1-b` 沒有 C3。階段二需要先提高配額。

## 為什麼需要這個實驗

`kafka-e2e` 的對照組 run（43658324808，trunk `e90d6f42c2`，32 核、62 GB）顯示兩件事：

- 1、2、4、8 個獨立 consumer 的總吞吐量相同（1 KB 約 720 MB/s，100 B 約 420 到 510 MB/s）。
- 單一 consumer 比 morefine 慢（1 KB：722 對 1099 MB/s）。

broker 與 consumer 都在同一台主機的 Docker container 裡。我們分不出瓶頸是 consumer、broker，還是主機本身。這個實驗把兩者放到不同機器，回答下面的問題。

## 要回答的問題

1. consumer 獨占一台機器時，單一 consumer 的上限是多少？（trunk 與 `kip-fetch-pipeline`）
2. 同一台 client 機器上加 consumer，總吞吐量會不會增加？這是「平行化在大機器上有沒有增益」的直接證據（07 文件的缺口 B3）。
3. 100 B 訊息的瓶頸是不是記憶體頻寬？用 `perf stat` 的 cache miss 與 instructions per cycle 佐證。
4. decode 原型（`fp-bench-proto`）在大機器上有沒有增益？

## 配置

| 項目 | 值 | 理由 |
|---|---|---|
| 機器 | 2 台 `c3-standard-22`（22 vCPU、88 GB RAM） | broker 一台，client 一台 |
| zone | `asia-east1-b`（同一個 zone） | 兩台之間延遲最低 |
| 計費 | Spot | 實驗可以重跑，被收回的代價低 |
| 硬體計數器 | `--performance-monitoring-unit standard` | 讓 `perf stat` 看得到 cache miss |
| 自動刪除 | `--max-run-duration 6h` 加 `--instance-termination-action DELETE` | 忘記關機時，機器自己刪除 |
| 磁碟 | 100 GB `pd-ssd` | 資料會在 page cache，磁碟只需要放得下 |

機型、價格、網路頻寬上限是記憶中的數字。建立前要核對：

- 價格：https://cloud.google.com/compute/vm-instance-pricing
- 網路頻寬：https://cloud.google.com/compute/docs/network-bandwidth
- 專案的 C3 CPU 配額是否有 44 vCPU：`gcloud compute regions describe asia-east1`

## 網路是前提

目標吞吐量是 1000 到 1500 MB/s，等於 8 到 12 Gbps。兩台之間的頻寬必須明顯高於這個數字。

- 建立後第一步：`./FETCH-PIPELINE-09-gcp.sh net`（`iperf3`，8 條連線，10 秒）。
- 通過條件：15 Gbps 以上。
- 沒有通過：先刪除機器，再改用更大的機型或加上 Tier 1 networking。不要在頻寬不足的機器上量 consumer。

## 步驟

1. 看計畫，不建立任何東西：`./FETCH-PIPELINE-09-gcp.sh plan`。
2. 建立機器：`./FETCH-PIPELINE-09-gcp.sh up`。
3. 量網路：`./FETCH-PIPELINE-09-gcp.sh net`。
4. 兩台都安裝 JDK 17 與 `linux-tools`（`perf`）。client 機器另外安裝 async-profiler。
5. 在 client 機器 build 三個 revision 的 Kafka：`fp-bench-control`（`0d73ca50b3`）、`kip-fetch-pipeline`（`92a7780ce5`）、`fp-bench-proto`（`92b1f3e88e`）。broker 機器只用 `fp-bench-control` 的版本。
6. broker 機器：啟動單一 KRaft broker（combined mode）。log 目錄放在開機磁碟。
7. 建立 topic（12 partition）並寫入資料：100 B × 10 GB，1 KB × 20 GB。producer 設定與 ducktape 測試相同（`acks=1`、`batch.size` 64 KB、`linger.ms` 5）。
8. 確認資料在 broker 的 page cache（`free -g` 的 buff/cache 大於資料量）。
9. 量測。每個組合跑 3 次，取中位數，並保留逐秒序列：

   | 組合 | revision | consumer 數 |
   |---|---|---|
   | 對照 | `fp-bench-control` | 1、2、4、8 |
   | 本分支 | `kip-fetch-pipeline` | 1、2、4、8 |
   | 原型 | `fp-bench-proto`，`-Dfp.lean=true`、`-Dfp.decode.threads=2`、`=4` | 1 |

10. 每個組合另外各跑一次 `perf stat -e cycles,instructions,cache-misses,LLC-load-misses`，client 與 broker 各一份。
11. 量測期間同時記錄 broker 的 CPU（`mpstat 1`）與網路（`sar -n DEV 1`）。broker 跑滿或網路到頂時，該組數字標成「不是 consumer 的上限」。
12. 把結果複製回本機。
13. 刪除所有資源：`./FETCH-PIPELINE-09-gcp.sh down`。
14. 確認沒有東西還在計費：`./FETCH-PIPELINE-09-gcp.sh status` 的三張表都是空的。

## 怎麼判讀

- 問題 2：8 個 consumer 的總和明顯大於 1 個，而且 broker 與網路都沒有到頂。這代表 `kafka-e2e` 的平坦曲線是共置造成的。
- 總和仍然平坦，而且 broker 跑滿：瓶頸在 broker，要加 broker 再量。
- 總和仍然平坦，broker 與網路都有餘裕：瓶頸在 client 機器的記憶體頻寬。對照 `perf stat` 的 cache miss。

## 成本與安全

- 預估：兩台 Spot 跑 3 到 4 小時，合計約 5 到 10 美元（未核對的估計）。
- 三層防護：`--max-run-duration` 自動刪除；所有資源帶 `fp-bench=1` 標籤；結束時執行 `status` 確認。
- 防火牆規則只允許帶 `fp-bench` tag 的機器互連，不對外開放 Kafka 的 port。
- 每次執行 `up` 之前，先把 `plan` 的輸出與預估費用給使用者看，等使用者同意。

## 分工

- 使用者：GCP 帳號、付款、`gcloud auth login`、同意每次建立。
- Agent：腳本、部署、量測、收結果、刪除資源。

## 還沒決定的事

- broker 要 1 台還是 3 台。先用 1 台。步驟 11 顯示 broker 跑滿時再加。
- 是否同時量 librdkafka。建議第二輪再做。
