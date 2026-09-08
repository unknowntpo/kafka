#!/bin/bash
# Usage: bench.sh <cmd> ...
# cmds: broker-start | broker-stop | topics | produce | consume <protocol> <topic> <records> [extra props...] | matrix <reps>
set -u
S=${BENCH_DIR:-$(cd "$(dirname "$0")" && pwd)}
K=${KAFKA_HOME:-$(cd "$(dirname "$0")/.." && pwd)}
BS=localhost:9092
export KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"
case "$1" in
broker-start)
  rm -rf $S/kraft-logs
  uuid=$($K/bin/kafka-storage.sh random-uuid)
  $K/bin/kafka-storage.sh format --standalone -t $uuid -c $S/server.properties >/dev/null
  nohup $K/bin/kafka-server-start.sh $S/server.properties > $S/broker.log 2>&1 &
  echo $! > $S/broker.pid
  for i in $(seq 1 60); do grep -q "Kafka Server started" $S/broker.log && { echo "broker up pid=$(cat $S/broker.pid)"; exit 0; }; sleep 1; done
  echo "broker failed"; tail -20 $S/broker.log; exit 1 ;;
broker-stop)
  kill $(cat $S/broker.pid) 2>/dev/null; sleep 3; echo stopped ;;
topics)
  for spec in "t1p:1" "t1p-lz4:1" "t6p:6" "t1p-100b:1"; do
    t=${spec%%:*}; p=${spec##*:}
    $K/bin/kafka-topics.sh --bootstrap-server $BS --create --topic $t --partitions $p --replication-factor 1 >/dev/null 2>&1 && echo "created $t p=$p"
  done ;;
produce)
  # topic records size compression
  t=$2; n=$3; sz=$4; comp=${5:-none}
  $K/bin/kafka-producer-perf-test.sh --topic $t --num-records $n --record-size $sz --throughput -1 \
    --producer-props bootstrap.servers=$BS acks=1 batch.size=262144 linger.ms=20 compression.type=$comp buffer.memory=134217728 2>&1 | tail -1 ;;
consume)
  proto=$2; t=$3; n=$4; shift 4
  props=(--command-property group.protocol=$proto --command-property client.id=bench-$proto)
  for p in "$@"; do props+=(--command-property "$p"); done
  $K/bin/kafka-consumer-perf-test.sh --bootstrap-server $BS --topic $t --num-records $n --group g-$proto-$$-$RANDOM --timeout 60000 --hide-header "${props[@]}" 2>&1 | grep -v -e WARN -e SLF4J ;;
*) echo "unknown cmd"; exit 2 ;;
esac
