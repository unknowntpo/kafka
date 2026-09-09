#!/bin/bash
# Long-window steady idle (minutes 3-6) for M1 on JDK 25, with and without the AOT cache. Usage: idle-long.sh
B=$HOME/kafka-bench; cd $B; JDK=$HOME/jdk25; CP="ng/*:kafka/libs/*"; MAIN=org.apache.kafka.clients.consumer.ng.bench.ConsumeBench
HZ=$(getconf CLK_TCK)
run() { # label extra-flags
  $JDK/bin/java -Xmx2G -Xms2G $2 -Dlog4j2.configurationFile=kafka/config/tools-log4j2.yaml -cp "$CP" $MAIN --topic idle1p --partitions 1 --idle-ms 400000 >/dev/null 2>&1 &
  sp=$!; sleep 2; pid=""; for c in $(pgrep -P $sp) $sp; do [ "$(cat /proc/$c/comm 2>/dev/null)" = java ] && pid=$c; done
  snap() { local cpu=0 ctx=0; for t in /proc/$pid/task/*; do cpu=$((cpu + $(awk '{print $14+$15}' $t/stat 2>/dev/null || echo 0))); ctx=$((ctx + $(awk '/^voluntary_ctxt_switches/{print $2}' $t/status 2>/dev/null || echo 0))); done; echo "$cpu $ctx"; }
  sleep 178; read c0 x0 <<<"$(snap)"; sleep 180; read c1 x1 <<<"$(snap)"
  echo "$1: idle minutes 3-6: cpu_s_per_min=$(python3 -c "print(round(($c1-$c0)/$HZ/3,3))") volctx_per_s=$(python3 -c "print(round(($x1-$x0)/180,1))")"
  kill $sp 2>/dev/null; wait $sp 2>/dev/null
}
run "jdk25-no-aot" ""
run "jdk25-aot" "-XX:AOTCache=$B/ng.aot"
