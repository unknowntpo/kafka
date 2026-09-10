# 訂閱成本優化：單獨驗證

Branch: `codex/next-poll-condition-trunk`；base: `820533b870106cc0e0ac60e2076b8644d68bd85f`。所有修改未提交。

## 改動與決策

本次只修改訂閱管理，ready/legacy TreeSet、deadline queue 與 dispatch 語意沿用 profiling 前版本。bitset 草稿保存在 work，尚未推進。條件查詢介面與 anyOf 亦未移除，避免混合不同改動的收益。

- 用直接鏈結的取消 handle 取代 subscriber hash set。
- 發布時切開當代訂閱鏈，不再配置 subscriber array snapshot。
- Waiting 直接實作 callback，省掉 callback lambda。
- 單一訂閱直接保存 cancellation handle；只有多 signal 才需要額外 list。

保留 generation、舊取消隔離、sibling cancellation、穩定執行顺序、去重及有界 pass。取消節點的生命週期邏輯比使用集合更需要測試，因此不將這項 prototype 視為 production-ready。

## 正確性與 memory retention

- 17 個 JUnit 方法通過，包括取消 head/middle/tail、重複取消、發布中取消 sibling、舊 handle 不影響新訂閱、發布中加入的訂閱延至下一次。
- 16,000 次逐輪 request count、wait、coordinator presence、fatal-state presence 在 trunk、舊事件版、新事件版及 scan control 間一致。新增成功 discovery／invalidation 路徑；不代表 wire bytes 或多 manager 真實網路等價。
- 1,000 owners 經 500,000 次通知後，GC 後 Waiting、Entry、Subscription 各保持 1,000；close 後均為 0，signals 與 owners 仍被測試持有。此有限測試未見累積，不是完整 leak 排除證明。
- 已通過隔離編譯與 diff whitespace check。隔離 harness 仍僅替換未使用的 network-client factory body，以相容本地 dependency jar。完整 Gradle、network-thread integration／broker 測試未執行。

## 同輪 JMH：ns/pass ± reported error

| 情境 | trunk A1 | trunk A2 | OLD | NEW |
|---|---:|---:|---:|---:|
| QUIET | 4.14 ± 0.31 | 4.21 ± 0.41 | 5.59 ± 0.10 | 5.56 ± 0.14 |
| BURST | 18.84 ± 0.21 | 18.83 ± 0.28 | 24.12 ± 0.50 | 22.57 ± 0.63 |
| BUSY | 891.02 ± 19.30 | 896.92 ± 11.43 | 1042.33 ± 24.91 | 976.34 ± 12.37 |
| SUCCESS | 6.84 ± 0.47 | 6.84 ± 0.48 | 15.32 ± 3.57 | 12.43 ± 1.15 |

## 同輪 JMH：allocation B/pass

| 情境 | trunk A1 | trunk A2 | OLD | NEW |
|---|---:|---:|---:|---:|
| QUIET | 0.00 | 0.00 | 0.00 | 0.00 |
| BURST | 18.50 | 18.50 | 40.75 | 38.50 |
| BUSY | 1184.02 | 1184.02 | 1600.02 | 1424.02 |
| SUCCESS | 16.88 | 16.88 | 37.63 | 40.94 |

相較 OLD，BUSY 平均耗時約降 6.3%，allocation 約降 11%；BURST 平均耗時與配置量也下降，QUIET 幾乎不變。這是短測的觀察，仍需較長重複測量。所有情境均未證明 NEW 優於原始 trunk。

SUCCESS 模擬 discovery 下一輪成功，等待直到每 64 輪一次的 invalidation，再重新 discovery。成功 response 的建立成本對各組一致納入。SUCCESS 的 OLD/NEW 時間區間重疊，allocation 在本次 JMH 中反而較高；不能宣稱此情境有一致 memory 改善。

四組共 16 個情境固定隨機順序串行執行，同一 benchmark bytecode、同一 JDK 17、128 MB heap、單 worker。各 cell 2 forks、2×300 ms warmup、3×300 ms measurement；使用 GC profiler。所有組有相同 non-inlined per-pass 邊界。未測真實 transport、broker、多 manager、throughput 或使用者 latency。

## 不開 JFR 的 ThreadMXBean 配置量交叉驗證

| 情境 | trunk B/pass | OLD B/pass 範圍 | NEW B/pass 範圍 |
|---|---:|---:|---:|
| QUIET | 0.00 | 0.00–0.00 | 0.00–0.00 |
| BURST | 18.50 | 41.25–44.88 | 41.09–42.63 |
| BUSY | 1184.00 | 1632.00–1632.00 | 1488.00–1488.00 |
| SUCCESS | 16.97 | 47.00–47.00 | 40.88–40.88 |

每情境 warmup 2,048,000 輪，三次各 512,000 輪計數。每版本同一 JVM 依序跑所有情境；這與 JMH 各情境獨立 forks 的編譯狀態不同。只比較同一 harness 內 OLD/NEW，不把不同 harness 的數字差異當成 regression。SUCCESS 在此 counter 驗證下降、JMH 卻上升，說明它的配置收益仍不穩定。

## CPU-only JFR 確認

在 BUSY 的 old/new profiling 中，採 Java/native sampling 1 ms，停用 allocation sampling。OLD 共 262 個 worker 樣本，137 個包含 Object.hashCode → HashSet.add → Signal.subscribe；NEW 共 137 個樣本，該路徑為 0。新版仍有 1 個樣本位於 subscribe，且主要樣本轉到共用的失敗 completion 路徑。這與移除 hash-based 登記一致，但樣本比例不等於精確 CPU 占比。

Profiling 每組只有 1 fork，2×1 s warmup、3×2 s measurement。帶 profiler 的耗時不拿來算加速比。

## 重現

分支內依序執行：

```sh
python3 experiments/next-poll-condition/validate.py
python3 experiments/next-poll-condition/memory.py
python3 experiments/next-poll-condition/benchmark.py
```

不要平行執行 memory 和 benchmark。凍結的舊 scheduler／condition 保存在 Event*.java.in；validation 會編成 OLD 對照。原始 JFR、JSON、histogram、diff 與腳本一併保存在 evidence zip。


## Network-loop follow-up

Six actual runOnce integration tests passed. Full-loop synthetic 32-manager BUSY: EVENT 2840.64 ns/loop versus trunk 288.01–290.66; MIXED: EVENT 1418.13 versus trunk 256.63–326.49. Sparse/idle point estimates can improve, but A/A variability prevents a blanket win claim. See outputs/network-loop-condition-validation.md in the task parent and loop-results raw data. No production scheduler change in this follow-up.
