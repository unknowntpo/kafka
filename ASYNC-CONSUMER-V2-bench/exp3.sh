#!/bin/bash
S=${BENCH_DIR:-$(cd "$(dirname "$0")" && pwd)}; B=$S/bench.sh; K=${KAFKA_HOME:-$(cd "$(dirname "$0")/.." && pwd)}
echo "scenario,protocol,rep,fetch_MB_sec,fetch_msg_sec,user_s,sys_s,wall_s" > $S/exp3.csv
timed() { # name proto topic n props...
  name=$1; proto=$2; topic=$3; n=$4; shift 4
  out=$( { /usr/bin/time -l $B consume $proto $topic $n "$@" ; } 2>&1 )
  line=$(echo "$out" | grep -E '^20[0-9][0-9]-' | tail -1)
  read w u s <<<"$(echo "$out" | awk '/ real .* user .* sys/{print $1, $3, $5}')"
  echo "$name,$proto,$rep,$(echo "$line" | awk -F', ' '{print $9","$10}'),$u,$s,$w" >> $S/exp3.csv
}
for rep in 1 2 3; do for proto in classic consumer; do
  timed 1p-100B $proto big100b 40000000
  timed 1p-1KB  $proto big1k   8000000
  timed 1p-1KB-lz4 $proto big1k-lz4 8000000
  timed 1p-100B-mpr50 $proto big100b 40000000 max.poll.records=50
done; done
echo "== profiles (long runs)"
for proto in consumer classic; do echo "=== PROFILE $proto big100b"; $S/profile.sh $proto big100b 40000000; done
echo "=== PROFILE consumer big1k-lz4"; $S/profile.sh consumer big1k-lz4 8000000
echo done >> $S/exp3.csv
