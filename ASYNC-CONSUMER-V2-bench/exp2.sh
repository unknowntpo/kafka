#!/bin/bash
S=${BENCH_DIR:-$(cd "$(dirname "$0")" && pwd)}; B=$S/bench.sh; K=${KAFKA_HOME:-$(cd "$(dirname "$0")/.." && pwd)}
M='fetch-latency-avg|fetch-rate|records-per-request-avg|fetch-size-avg|select-rate|io-wait-ratio|io-ratio|time-between-network-thread-poll-avg|application-event-queue-time-avg|poll-idle-ratio-avg|time-between-poll-avg|bytes-consumed-rate'
for proto in consumer classic; do for extra in "" "max.poll.records=50"; do
  echo "=== METRICS $proto t1p $extra"
  $K/bin/kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic t1p --num-records 3000000 --group gm-$proto-$$-$RANDOM --timeout 60000 --hide-header --print-metrics --command-property group.protocol=$proto ${extra:+--command-property $extra} 2>&1 | grep -E -e "^20[0-9][0-9]-" -e "($M):" | grep -v -e "node-id" -e "topic=" | sed 's/{client-id=[^}]*}//; s/  */ /g'
done; done
echo "=== CEILING classic x2"; ($B consume classic t1p 3000000 & sleep 0.3; $B consume classic t1p 3000000 & wait)
echo "=== CEILING consumer x2"; ($B consume consumer t1p 3000000 & sleep 0.3; $B consume consumer t1p 3000000 & wait)
echo "=== PIPELINE DEPTH: max.partition.fetch.bytes=8MB"
for rep in 1 2 3; do for proto in classic consumer; do echo -n "$proto rep$rep: "; $B consume $proto t1p 3000000 max.partition.fetch.bytes=8388608 | awk -F', ' '{print "fetchMB/s="$9" msg/s="$10}'; done; done
echo "=== PIPELINE DEPTH: max.partition.fetch.bytes=8MB on 100B"
for rep in 1 2; do for proto in classic consumer; do echo -n "$proto rep$rep: "; $B consume $proto t1p-100b 5000000 max.partition.fetch.bytes=8388608 | awk -F', ' '{print "fetchMB/s="$9" msg/s="$10}'; done; done
