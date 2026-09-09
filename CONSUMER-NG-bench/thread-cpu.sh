#!/bin/bash
# Per-thread CPU (user+sys seconds from /proc/<pid>/task/*/stat, last snapshot before exit) of kafka-consumer-perf-test.
B=$HOME/kafka-bench; K=$B/kafka; LIB=$(ls $K/libs/kafka-clients-*-SNAPSHOT.jar); export KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"
HZ=$(getconf CLK_TCK)
run() { # variant topic records timeout label
  cp $B/jars/kafka-clients-$1.jar $LIB
  $K/bin/kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic $2 --num-records $3 --group tc-$1-$RANDOM --timeout $4 --hide-header --command-property group.protocol=consumer > $B/tc.out 2>&1 &
  sh=$!
  pid=""; for i in $(seq 1 100); do for c in $(pgrep -f "ConsumerPerformance"); do [ "$(cat /proc/$c/comm 2>/dev/null)" = java ] && pid=$c; done; [ -n "$pid" ] && break; sleep 0.1; done
  : > $B/tc.all
  while kill -0 $pid 2>/dev/null; do
    for t in /proc/$pid/task/*; do [ -r $t/stat ] && printf "%s\t%s\t%s\n" "${t##*/}" "$(cat $t/comm 2>/dev/null)" "$(awk '{print $14+$15}' $t/stat 2>/dev/null)"; done >> $B/tc.all 2>/dev/null
    sleep 0.2
  done
  wait $sh
  # per tid: max ticks seen (monotonic); then sum per thread name
  awk -F'\t' '$3!=""{ if ($3>m[$1]) m[$1]=$3; n[$1]=$2 } END{ for (t in m) printf "%s\t%s\n", n[t], m[t] }' $B/tc.all > $B/tc.snap
  line=$(grep -E '^20[0-9][0-9]-' $B/tc.out | tail -1); mb=$(echo "$line" | awk -F', ' '{print $9}')
  bg=$(awk -F'\t' -v hz=$HZ '$1=="consumer_backgr"{s+=$2} END{printf "%.2f", s/hz}' $B/tc.snap)
  main=$(awk -F'\t' -v hz=$HZ '$1=="java"{s+=$2} END{printf "%.2f", s/hz}' $B/tc.snap)
  [ -n "$DUMP" ] && sort -t$'\t' -k2 -nr $B/tc.snap | head -6 | awk -F'\t' -v hz=$HZ '{printf "    %-18s %.2fs\n", $1, $2/hz}'
  gc=$(awk -F'\t' -v hz=$HZ '$1 ~ /^(GC|G1)/{s+=$2} END{printf "%.2f", s/hz}' $B/tc.snap)
  tot=$(awk -F'\t' -v hz=$HZ '{s+=$2} END{printf "%.2f", s/hz}' $B/tc.snap)
  echo "$5,$1,fetchMB/s=$mb,bg_cpu_s=$bg,main_cpu_s=$main,gc_cpu_s=$gc,total_cpu_s=$tot"
}
$K/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic idle1p --partitions 1 --replication-factor 1 >/dev/null 2>&1
if [ "$1" = diag ]; then DUMP=1 run looponly big100b 20000000 60000 diag; DUMP=1 run trunk big100b 20000000 60000 diag; else
for rep in 1 2 3; do for v in trunk looponly; do run $v big100b 40000000 90000 "load-rep$rep"; done; done
for v in trunk looponly; do run $v idle1p 1 60000 "idle60s"; done; fi
cp $B/jars/kafka-clients-dist-orig.jar $LIB
