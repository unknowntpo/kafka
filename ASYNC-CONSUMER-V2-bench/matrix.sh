#!/bin/bash
S=${BENCH_DIR:-$(cd "$(dirname "$0")" && pwd)}; B=$S/bench.sh; OUT=$S/matrix.csv
echo "scenario,protocol,rep,MB_sec,msg_sec,rebalance_ms,fetch_ms,fetch_MB_sec,fetch_msg_sec" > $OUT
run() { # name topic records props...
  name=$1; topic=$2; n=$3; shift 3
  for rep in 1 2 3; do for proto in classic consumer; do
    line=$($B consume $proto $topic $n "$@" | tail -1)
    echo "$name,$proto,$rep,$(echo "$line" | awk -F', ' '{print $4","$6","$7","$8","$9","$10}')" >> $OUT
  done; done
}
run 1p-1KB        t1p      3000000
run 1p-1KB-lz4    t1p-lz4  3000000
run 6p-1KB        t6p      3000000
run 1p-100B       t1p-100b 5000000
run 1p-1KB-mpr50  t1p      3000000 max.poll.records=50
run 1p-1KB-pfb256K t1p     3000000 max.partition.fetch.bytes=262144
echo done >> $OUT
