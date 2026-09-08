# ASYNC-CONSUMER-V2 benchmark 工具

02 文件所有數據的產生腳本。工作目錄（broker 資料、結果）用 `BENCH_DIR` 指定（預設為本目錄，資料量約 20 GB，建議指到 repo 外）。

```bash
export BENCH_DIR=/tmp/kafka-bench KAFKA_HOME=$(pwd)/..   # 先 ./gradlew jar
mkdir -p $BENCH_DIR && sed "s#\${BENCH_DIR}#$BENCH_DIR#" server.properties.template > $BENCH_DIR/server.properties
cp *.sh $BENCH_DIR/ && cp -r verify $BENCH_DIR/
$BENCH_DIR/bench.sh broker-start && $BENCH_DIR/bench.sh topics
$BENCH_DIR/bench.sh produce t1p 3000000 1024 none        # 其餘 topic 見 matrix.sh / exp3.sh
$BENCH_DIR/matrix.sh                                     # 第一輪矩陣 → matrix.csv
KAFKA_OPTS=-Dkafka.exp.prefetch.depth=2 $BENCH_DIR/bench.sh consume consumer big100b 40000000   # 只在 exp 分支有效
ASYNC_PROFILER_LIB=.../libasyncProfiler.dylib $BENCH_DIR/profile.sh consumer big100b 40000000
```

- `bench.sh`：broker 起停、建 topic、produce、consume（`fetch.MB.sec` 為比較欄位）
- `matrix.sh` / `exp2.sh` / `exp3.sh`：02 文件 3.2 / 3.3–3.4 / 3.5 的腳本
- `profile.sh`：async-profiler per-thread 樣本
- `verify/OffsetCheck.java`：offset 連續性 / 重複 / seek 驗證（編譯時 classpath 需 kafka-clients jar + slf4j-api）
- `results-*.csv`：2026-09-07 本機結果
- `results-morefine-4way-run4.csv`：2026-09-08 homelab 四變體（trunk / loop-only 修正前 / loop-only 修正後 / full）交錯 3 輪，**03 §2.1 引用的是這一份**；`results-morefine-3way-run2.csv` 為 R11 修正第一版的三方結果（僅供對照）
- `results-morefine-3way-run1.csv`：2026-09-08 homelab `morefine`（Intel N150 4 核，load ≈ 2）三方交錯 A/B，3 輪；runner 為 Linux 版（`/usr/bin/time -f`），broker 用 trunk 發行包，只換 `kafka-clients` jar
