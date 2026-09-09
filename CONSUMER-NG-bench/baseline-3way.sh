#!/bin/bash
# Three-way consumer baseline on one broker: trunk Java (consumer + classic protocol), librdkafka, franz-go.
# Usage: baseline-3way.sh <tier-label> <cpu-list|all> <reps>   e.g.  T2 all 3   |  T1 0 3
set -u
TIER=$1; CPUS=$2; REPS=$3
B=$HOME/kafka-bench; K=$B/kafka; BL=$HOME/baselines; export KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"
PIN=""; [ "$CPUS" != all ] && PIN="taskset -c $CPUS"
OUT=$B/baseline-$TIER.csv
[ -f $OUT ] || echo "tier,rep,variant,topic,mb_s,rec_s,user_s,sys_s,cpu_s_per_gb,vol_ctx,invol_ctx,rss_kb,note" > $OUT
TV=$B/tv.txt
emit() { # variant topic mb_s rec_s gb note
  u=$(awk -F': ' '/User time/{print $2}' $TV); s=$(awk -F': ' '/System time/{print $2}' $TV)
  v=$(awk -F': ' '/Voluntary context/{print $2}' $TV); iv=$(awk -F': ' '/Involuntary context/{print $2}' $TV); r=$(awk -F': ' '/Maximum resident/{print $2}' $TV)
  cpg=$(python3 -c "print(round(($u+$s)/max($5,1e-9),3))")
  echo "$TIER,$REP,$1,$2,$3,$4,$u,$s,$cpg,$v,$iv,$r,$6" | tee -a $OUT
}
java_run() { # proto topic records
  out=$( { /usr/bin/time -v -o $TV $PIN $K/bin/kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic $2 --num-records $3 --group j-$1-$RANDOM --timeout 90000 --hide-header --command-property group.protocol=$1 ; } 2>&1 )
  line=$(echo "$out" | grep -E '^20[0-9][0-9]-' | tail -1)
  mb=$(echo "$line" | awk -F', ' '{print $9}'); rs=$(echo "$line" | awk -F', ' '{print $10}'); gb=$(echo "$line" | awk -F', ' '{print $3/1024}')
  emit java-$1 $2 $mb $rs $gb "fixed-count"
}
rd_run() { # topic records
  out=$( { /usr/bin/time -v -o $TV $PIN $BL/rdkafka_performance -G rd-$RANDOM -t $1 -b localhost:9092 -c $2 -q -X auto.offset.reset=earliest -X fetch.wait.max.ms=500 ; } 2>&1 )
  line=$(echo "$out" | grep -E 'messages \(.*consumed in' | tail -1)
  rs=$(echo "$line" | sed -E 's/.*: ([0-9]+) msgs\/s.*/\1/'); mb=$(echo "$line" | sed -E 's/.*\(([0-9.]+) MB\/s\).*/\1/'); gb=$(echo "$line" | sed -E 's/.*\(([0-9]+) bytes\).*/\1/' | awk '{print $1/1073741824}')
  emit librdkafka $1 $mb $rs $gb "fixed-count"
}
fz_run() { # topic seconds
  out=$( { /usr/bin/time -v -o $TV $PIN timeout -s INT $2 $BL/franz-bench -consume -group fz-$RANDOM -topic $1 -brokers localhost:9092 ; } 2>&1 )
  read mb rs gb <<<"$(echo "$out" | grep -E 'MiB/s;' | awk -F'[ ;k]+' '$1>0{n++; mb+=$1; rs+=$3} END{printf "%.2f %.0f %.4f", mb/n*1.048576, rs/n*1000, mb*1.048576/1024}')"
  emit franz-go $1 $mb $rs $gb "fixed-${2}s"
}
idle_java() { /usr/bin/time -v -o $TV $PIN $K/bin/kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic idle1p --num-records 1 --group ji-$1-$RANDOM --timeout 60000 --hide-header --command-property group.protocol=$1 >/dev/null 2>&1; emit java-$1 idle1p 0 0 0 "idle-60s"; }
idle_rd() { /usr/bin/time -v -o $TV $PIN timeout -s INT 60 $BL/rdkafka_performance -G rdi-$RANDOM -t idle1p -b localhost:9092 -c 1 -q -X fetch.wait.max.ms=500 >/dev/null 2>&1; emit librdkafka idle1p 0 0 0 "idle-60s"; }
idle_fz() { /usr/bin/time -v -o $TV $PIN timeout -s INT 60 $BL/franz-bench -consume -group fzi-$RANDOM -topic idle1p -brokers localhost:9092 >/dev/null 2>&1; emit franz-go idle1p 0 0 0 "idle-60s"; }
for REP in $(seq 1 $REPS); do
  java_run consumer big100b 40000000; rd_run big100b 40000000; fz_run big100b 20; java_run classic big100b 40000000
  java_run consumer t6p 3000000;     rd_run t6p 3000000;     fz_run t6p 12;     java_run classic t6p 3000000
done
REP=1; idle_java consumer; idle_rd; idle_fz
echo "DONE $TIER"
