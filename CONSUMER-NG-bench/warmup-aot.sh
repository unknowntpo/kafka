#!/bin/bash
# Warm-up cost with and without a JDK 25 AOT cache. Reports whole-run CPU and CPU spent before the steady window
# (steady.py's warmup_cpu_s) for the same 12 GB consume.  Usage: warmup-aot.sh <jdk-home>
JDK=$1; B=$HOME/kafka-bench; cd $B
CP="ng/*:kafka/libs/*"; MAIN=org.apache.kafka.clients.consumer.ng.bench.ConsumeBench
ARGS="--topic t6p12 --partitions 6 --records 12000000"
echo "== training run (records the AOT configuration, then creates the cache):"
$JDK/bin/java -Xmx2G -Xms2G -XX:AOTCacheOutput=$B/ng.aot -Dlog4j2.configurationFile=kafka/config/tools-log4j2.yaml -cp "$CP" $MAIN $ARGS >/dev/null 2>$B/aot-train.err; ls -la $B/ng.aot 2>/dev/null | awk '{print "aot cache bytes:", $5}'; tail -2 $B/aot-train.err
for mode in baseline aot; do
  if [ $mode = aot ]; then X="-XX:AOTCache=$B/ng.aot"; else X=""; fi
  echo "== $mode:"
  STEADY_CMD="$JDK/bin/java -Xmx2G -Xms2G $X -Dlog4j2.configurationFile=kafka/config/tools-log4j2.yaml -cp $CP $MAIN --topic {topic} --partitions 6 --records {records}" python3 steady.py warm-$mode 4 t6p12 12000000 consumer 2>&1 | tail -1 | cut -c1-220
  # time to first 1 GB consumed and CPU at that point, from the per-second lines
  $JDK/bin/java -Xmx2G -Xms2G $X -Dlog4j2.configurationFile=kafka/config/tools-log4j2.yaml -cp "$CP" $MAIN $ARGS 2>/dev/null | awk -F', ' 'NR==1{t0=$1} $3>=1024 && !done {print "  first GB consumed after interval", NR, "cumulative MB", $3; done=1}'
done
