#!/bin/bash
# One-shot reproduction of every scenario in ASYNC-CONSUMER-V2-02 (§3.2 matrix, §3.5 long runs).
# Prerequisites: ./gradlew jar; BENCH_DIR/server.properties prepared (see README.md).
set -e
S=${BENCH_DIR:-$(cd "$(dirname "$0")" && pwd)}; B=$S/bench.sh
$B broker-start
$B topics
$B produce t1p 3000000 1024 none
$B produce t1p-lz4 3000000 1024 lz4
$B produce t6p 3000000 1024 none
$B produce t1p-100b 5000000 100 none
K=${KAFKA_HOME:-$(cd "$(dirname "$0")/.." && pwd)}
for t in big100b big1k big1k-lz4; do $K/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $t --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true; done
$B produce big100b 40000000 100 none
$B produce big1k 8000000 1024 none
$B produce big1k-lz4 8000000 1024 lz4
$S/matrix.sh
$S/exp3.sh
echo "results: $S/matrix.csv $S/exp3.csv"
