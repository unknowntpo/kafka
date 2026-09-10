# End-to-end consumer A/B, rerun per benchmark-method.md §3

2026-09-10。這一輪是 `benchmark-method.md` §4 那張表裡「e2e 依 3.2 重跑」與「`unavailable` 模式改成接受連線但不回應」
兩項的執行結果。所有原始資料在 `results/`（JMH JSON）與 `logs/`（完整 stdout）。

## 1. 環境

- date: Thu Sep 10 18:20:28 CST 2026
- uname: Darwin 25.3.0 arm64
- Apple M1 Pro
- cores: 8 (perf 6 + eff 2)
- mem: 32 GiB
- openjdk version "21.0.1" 2023-10-17 LTS OpenJDK Runtime Environment Zulu21.30+15-CA (build 21.0.1+12-LTS) OpenJDK 64-Bit Server VM Zulu21.30+15-CA (build 21.0.1+12-LTS, mixed mode, sharing) 
- broker: kafka 4.1.0

- branch: `fable/kip-1371-event-loop` @ `fee1f0dceaf0be4e0282ffa20411b757cf816047`（jar 建好後又 commit 了 benchmark 本身，client 程式碼未動）
- trunk baseline: `74fbd50061`，用 `git archive` 展開到 `trunk-src/`，把 branch 的
  `AsyncConsumerBrokerBenchmark.java` 與 `checkstyle/import-control-jmh-benchmarks.xml` 複製進去後建置
  （adaptation 見 `benchmark-trunk-adaptation.diff`：trunk 根本沒有這個 benchmark 檔，等於整檔搬入）
- jar SHA-256：見 `results/jar-sha.txt`
- broker：單節點 KRaft，Homebrew kafka 4.1.0，PLAINTEXT 127.0.0.1:39492、PROXIED 127.0.0.1:39494
  （advertise 成 proxy 的 127.0.0.1:39594）、CONTROLLER 127.0.0.1:39493，資料在 `broker-data/`（跑完刪除）
- topic：`jmh-e2e2-1789034484`，1 partition，2,000,000 筆 × 128 B（約 256 MB），value 前 8 bytes 是
  big-endian offset
- 共用機器：跑的時候還有另一個 session 的 Gradle test JVM、Cursor、IDEA 等在背景。這是雜訊的主要來源。
- JMH 參數：`-f 1 -wi 5 -w 2s -i 5 -r 5s -prof gc`
- session 級預熱（§3.2 rule 4）：正式量測前用一個丟棄的 consumer 連續讀 90 秒，共 102,001,000 筆
  ＝ 整個 topic 讀了 51 遍，page cache 與 broker JVM 都在穩態。

## 2. A/A 雜訊包絡（§3.2 rule 5）

同一個 trunk jar，consume 模式，連跑兩次：



**雜訊包絡：吞吐 ±16%、process CPU ±12%、每次 poll 的配置量 ±0.1%。**

配置量幾乎沒有雜訊，時間有 16%——這正是 `benchmark-method.md` §3.3 rule 10 說的：時間會被雜訊蓋掉，
配置量不會。所以下面每個模式都同時報這兩個。

## 3. 逐對結果（§3.3 rule 9）

順序是交錯的 T,B,B,T,T,…；每個 ratio 是 branch / trunk，>1 代表 branch 的數字比較大。



## consume

| pair | trunk polls/s | branch polls/s | ratio B/T | trunk rec/s | branch rec/s | ratio | trunk cpuMs/s | branch cpuMs/s | ratio | trunk alloc B/op | branch alloc B/op | ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| p1 | 3,775 | 3,197 | 0.847 | 3,576,539 | 3,027,926 | 0.847 | 575 | 503 | 0.875 | 526,276 | 526,249 | 1.000 |
| p2 | 3,750 | 3,364 | 0.897 | 3,553,035 | 3,186,809 | 0.897 | 561 | 550 | 0.981 | 526,646 | 526,560 | 1.000 |
| p3 | 2,582 | 2,189 | 0.848 | 2,444,510 | 2,072,302 | 0.848 | 436 | 455 | 1.043 | 526,101 | 526,974 | 1.002 |
| p4 | 2,430 | 2,467 | 1.015 | 2,300,211 | 2,334,777 | 1.015 | 429 | 406 | 0.945 | 526,504 | 526,457 | 1.000 |
| p5 | 2,393 | 2,928 | 1.224 | 2,264,571 | 2,772,764 | 1.224 | 405 | 480 | 1.184 | 526,626 | 526,419 | 1.000 |

- median branch/trunk polls/s: **0.897** (n=5, min 0.847, max 1.224)
- median branch/trunk records/s: **0.897** (n=5, min 0.847, max 1.224)
- median branch/trunk processCpuMillis/s: **0.981** (n=5, min 0.875, max 1.184)
- median branch/trunk gc.alloc.rate.norm: **1.000** (n=5, min 1.000, max 1.002)


## idle

| pair | trunk polls/s | branch polls/s | ratio B/T | trunk rec/s | branch rec/s | ratio | trunk cpuMs/s | branch cpuMs/s | ratio | trunk alloc B/op | branch alloc B/op | ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| p1 | 0.993 | 0.997 | 1.003 | 0.000 | 0.000 | n/a | 26.533 | 17.263 | 0.651 | 132,961 | 151,226 | 1.137 |
| p2 | 0.997 | 0.996 | 0.999 | 0.000 | 0.000 | n/a | 14.836 | 19.844 | 1.338 | 134,693 | 220,582 | 1.638 |
| p3 | 0.995 | 0.996 | 1.001 | 0.000 | 0.000 | n/a | 19.229 | 15.386 | 0.800 | 135,420 | 246,469 | 1.820 |

- median branch/trunk polls/s: **1.001** (n=3, min 0.999, max 1.003)
- median branch/trunk processCpuMillis/s: **0.800** (n=3, min 0.651, max 1.338)
- median branch/trunk gc.alloc.rate.norm: **1.638** (n=3, min 1.137, max 1.820)


## unavailable

| pair | trunk polls/s | branch polls/s | ratio B/T | trunk rec/s | branch rec/s | ratio | trunk cpuMs/s | branch cpuMs/s | ratio | trunk alloc B/op | branch alloc B/op | ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| p1 | 9.261 | 9.240 | 0.998 | 0.000 | 0.000 | n/a | 24.694 | 25.237 | 1.022 | 24,368 | 24,045 | 0.987 |
| p2 | 9.274 | 9.282 | 1.001 | 0.000 | 0.000 | n/a | 19.384 | 20.921 | 1.079 | 23,996 | 24,074 | 1.003 |
| p3 | 9.479 | 9.272 | 0.978 | 0.000 | 0.000 | n/a | 20.334 | 22.480 | 1.106 | 23,264 | 24,004 | 1.032 |

- median branch/trunk polls/s: **0.998** (n=3, min 0.978, max 1.001)
- median branch/trunk processCpuMillis/s: **1.079** (n=3, min 1.022, max 1.106)
- median branch/trunk gc.alloc.rate.norm: **1.003** (n=3, min 0.987, max 1.032)


## unreachable

| pair | trunk polls/s | branch polls/s | ratio B/T | trunk rec/s | branch rec/s | ratio | trunk cpuMs/s | branch cpuMs/s | ratio | trunk alloc B/op | branch alloc B/op | ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| p1 | 9.261 | 9.233 | 0.997 | 0.000 | 0.000 | n/a | 30.744 | 28.128 | 0.915 | 26,644 | 25,898 | 0.972 |
| p2 | 9.216 | 9.227 | 1.001 | 0.000 | 0.000 | n/a | 28.499 | 31.083 | 1.091 | 25,563 | 26,448 | 1.035 |
| p3 | 9.233 | 9.272 | 1.004 | 0.000 | 0.000 | n/a | 31.584 | 32.857 | 1.040 | 26,650 | 26,981 | 1.012 |

- median branch/trunk polls/s: **1.001** (n=3, min 0.997, max 1.004)
- median branch/trunk processCpuMillis/s: **1.040** (n=3, min 0.915, max 1.091)
- median branch/trunk gc.alloc.rate.norm: **1.012** (n=3, min 0.972, max 1.035)


## heartbeat-blackhole

| pair | trunk polls/s | branch polls/s | ratio B/T | trunk rec/s | branch rec/s | ratio | trunk cpuMs/s | branch cpuMs/s | ratio | trunk alloc B/op | branch alloc B/op | ratio |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| p1 | 10.002 | 9.262 | 0.926 | 0.000 | 0.000 | n/a | 1,725 | 24.738 | 0.014 | 154,646,631 | 15,919 | 0.000 |
| p2 | 10.001 | 9.275 | 0.927 | 0.000 | 0.000 | n/a | 1,713 | 23.479 | 0.014 | 153,127,174 | 16,957 | 0.000 |
| p3 | 9.999 | 9.311 | 0.931 | 0.000 | 0.000 | n/a | 1,699 | 24.367 | 0.014 | 158,268,479 | 18,095 | 0.000 |

- median branch/trunk polls/s: **0.927** (n=3, min 0.926, max 0.931)
- median branch/trunk processCpuMillis/s: **0.014** (n=3, min 0.014, max 0.014)
- median branch/trunk gc.alloc.rate.norm: **0.000** (n=3, min 0.000, max 0.000)


## 4. 偵測下限（§3.3 rule 11）

**A/A 在 consume 模式量到 ±16% 的吞吐漂移，所以這一輪只能偵測「大於 16% 的吞吐回歸」與
「大於 1% 的配置量變化」。小於這個範圍的差異不能宣稱。**

consume 的 median ratio 0.897（branch 慢 10%）落在 ±16% 包絡之內，而且逐對值從 0.847 到 1.224 橫跨包絡兩側，
同時配置量完全一致（526 kB/poll，ratio 1.000）。結論：**consume 路徑沒有量到超過偵測下限的回歸，也不能宣稱
「回歸小於 10%」**。這一輪的漂移方向與上一輪相反（上一輪越跑越快，這一輪越跑越慢），推測是機器上其他負載與
散熱，不是 page cache——預熱已經把 page cache 排除掉了。

## 5. 各模式結論

| mode | 走到哪 | 主指標 | 判讀 |
|---|---|---|---|
| `consume` | 正常讀取 | polls/s、alloc/op | median 0.897，在 ±16% 包絡內；alloc ratio 1.000。無可宣稱的差異。 |
| `idle` | 坐在 log end，poll 1 s | alloc/op | median 1.638，且三對都 > 1（1.137 / 1.638 / 1.820）。trunk 三次自己只差 1.9%（132.9–135.4 kB/op），branch 三次差 63%（151–246 kB/op）。**方向一致，值得追**，但要用 loop-level benchmark 定量，不是靠這裡。 |
| `unreachable` | 連線被拒，一直 reconnect backoff | CPU、alloc | 所有 ratio 都在 0.92–1.09，兩邊都不忙（約 30 cpuMs/s）。與預期一致：這條路 trunk 本來就對。 |
| `unavailable` | TCP 接上但 ApiVersions 永不回應，卡在 `CHECKING_API_VERSIONS` | CPU、alloc | ratio 0.98–1.11，兩邊都不忙（約 20–25 cpuMs/s）。**這個模式走不到 heartbeat-in-flight**，見 §6。 |
| `heartbeat-blackhole` | 第一個 ConsumerGroupHeartbeat 永遠 in-flight | CPU、alloc | **trunk 1,699–1,725 cpuMs/s（約 1.7 顆核心）、154 MB/poll 配置；branch 23–25 cpuMs/s、16–18 kB/poll。CPU ratio 0.014，alloc ratio 0.0001。** |

## 6. busy loop 修好了嗎：看得到了

看得到，而且是這一輪唯一遠遠超出雜訊包絡的訊號。

新的 `heartbeat-blackhole` 模式讓 trunk 燒掉 1.7 顆核心、每次 poll 配置 154 MB；branch 是 24 cpuMs/s、17 kB/poll。
CPU 差 71 倍、配置量差 9,000 倍，偵測下限是 16%——這不需要統計就看得出來。

注意 polls/s 幾乎一樣（trunk 10.0、branch 9.3，ratio 0.927）：**busy loop 發生在 `poll()` 裡面**，
`poll(100ms)` 兩邊都還是每 100 ms 回一次，差別在那 100 ms 是 spin 還是 block。所以這個模式的主指標是 CPU 與配置量，
不是 polls/s；只看 polls/s 會以為沒事。

情境有效性的證據（§3.2 rule 2）：每個 run 的 log 都有 `[hb-blackhole] swallowed ConsumerGroupHeartbeat #1`
（warmup iteration 1）與 `#2`（iteration 5），而 measurement 期間的 `blackholedHeartbeats` counter 是 0——
代表那個 heartbeat 一直卡在 in-flight，沒有被重送。trunk 與 branch 六個 run 的樣子完全一樣，所以兩邊面對的是同一個情境。

### 為什麼舊的 `unavailable`（關閉的 port）測不到，新的「accept 但不回應」也測不到

`NetworkClient.handleInitiateApiVersionRequests` 會把連線標成 `CHECKING_API_VERSIONS`，這個狀態下
`isReady(node)` 是 false，`NetworkClientDelegate.doSend` 送不出任何 request，metadata 拿不到、`FindCoordinator`
送不出去，`AbstractHeartbeatRequestManager.poll` 在 `coordinator().isEmpty()` 就回 `PollResult.EMPTY`，
`makeHeartbeatRequest` 永遠不會被呼叫，`requestInFlight` 永遠是 false。所以 silent peer 只能測到
「連線建立但 ApiVersions 無回應」，測不到本分支修的 busy loop。這兩個模式都保留，但不能互相代替。

要走到 heartbeat-in-flight，peer 必須把 ApiVersions、Metadata、FindCoordinator 都正確回應。這一輪的做法是
**不自己實作 fake broker，而是在真 broker 的一個 listener 前面擺一個 byte-level TCP proxy**：Kafka request 是
「4 bytes 長度 + message」，message 前 2 bytes 就是 api key，所以 proxy 不用懂任何 schema，只要把
`CONSUMER_GROUP_HEARTBEAT` 吞掉、其他原封不動轉發即可。前置條件是那個 listener 必須 advertise proxy 的位址
（否則 client 會繞過 proxy 直連 broker），細節見 benchmark 的 README。

## 7. 這一輪的限制

1. 共用筆電，量測期間有其他 session 的 Gradle test JVM 在跑。consume 的 ±16% 包絡主要來自這裡。
2. consume 只能說「沒有大於 16% 的回歸」。要把下限壓下去需要安靜的機器，或改用 loop-level benchmark。
3. `idle` 的配置量上升方向一致但幅度不穩，這裡不下結論，需要 loop-level 的 IDLE 情境定量。
4. `heartbeat-blackhole` 在第 5 個 measurement iteration 會出現第二個 heartbeat（log 裡的 `#2`），
   代表 in-flight 狀態在約 30 秒後被打斷一次。兩個 variant 一致，不影響比較，但如果要拉長量測時間要先弄清楚原因。
5. 沒有跑 ducktape 系統測試。
