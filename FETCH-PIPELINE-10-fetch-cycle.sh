#!/usr/bin/env bash
# Local, free experiment: is the single-consumer limit at 1 KB the fetch cycle of one connection?
# Same topic (12 partitions, 1 KB records) on 1 broker and on 3 brokers; one consumer.
# Also changes --fetch-size (the tool sets max.partition.fetch.bytes from it), to tell a fixed cost per fetch from a byte-rate limit.
#
#   FETCH-PIPELINE-10-fetch-cycle.sh <tarball dir> <out dir> <brokers: 1 or 3>
#
# <tarball dir> holds fp-bench-control.tgz and kip-fetch-pipeline.tgz.
set -euo pipefail
T=$1; OUT=$2/b$3; N=$3
IMG="${FP_IMG:-ducker-ak-docker.io/library/eclipse-temurin-17-jdk-jammy:latest}"
GB="${FP_GB:-6}"; REPS="${FP_REPS:-3}"; SIZE=1000; PARTS=12; TOPIC=fetch-cycle
mkdir -p "$OUT"

names=(); voters=""
for i in $(seq "$N"); do names+=("fp-b$i"); voters+="${voters:+,}$i@fp-b$i:9093"; done
cleanup() { docker rm -f "${names[@]}" fp-client >/dev/null 2>&1 || true; docker network rm fp-net >/dev/null 2>&1 || true; }
cleanup; trap cleanup EXIT
docker network create fp-net >/dev/null

for c in "${names[@]}" fp-client; do
  docker run -d --user 0 --name "$c" --hostname "$c" --network fp-net --entrypoint sleep -v "$T":/tgz:ro "$IMG" infinity >/dev/null
done
unpack() { docker exec "$1" bash -c "mkdir -p /opt/$3 && tar -xzf /tgz/$2 -C /opt/$3 --strip-components=1"; }
unpack fp-client fp-bench-control.tgz control &
unpack fp-client kip-fetch-pipeline.tgz branch &
for c in "${names[@]}"; do unpack "$c" fp-bench-control.tgz k & done
wait

UUID=$(docker exec fp-client /opt/control/bin/kafka-storage.sh random-uuid)
for i in $(seq "$N"); do
  docker exec -i "fp-b$i" bash -c "cat > /opt/server.properties" <<EOF
process.roles=broker,controller
node.id=$i
controller.quorum.voters=$voters
listeners=PLAINTEXT://fp-b$i:9092,CONTROLLER://fp-b$i:9093
advertised.listeners=PLAINTEXT://fp-b$i:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=/var/fpdata
offsets.topic.replication.factor=1
share.coordinator.state.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
num.network.threads=8
num.io.threads=8
EOF
  docker exec "fp-b$i" /opt/k/bin/kafka-storage.sh format -t "$UUID" -c /opt/server.properties >/dev/null
  docker exec -d -e KAFKA_HEAP_OPTS="-Xms1g -Xmx1g" "fp-b$i" bash -c "/opt/k/bin/kafka-server-start.sh /opt/server.properties > /opt/broker.log 2>&1"
done
B=fp-b1:9092
kc() { docker exec fp-client "$@"; }
for _ in $(seq 60); do kc /opt/control/bin/kafka-topics.sh --bootstrap-server $B --list >/dev/null 2>&1 && break; sleep 2; done

kc /opt/control/bin/kafka-topics.sh --bootstrap-server $B --create --topic $TOPIC --partitions $PARTS --replication-factor 1 >/dev/null
kc /opt/control/bin/kafka-topics.sh --bootstrap-server $B --describe --topic $TOPIC | awk '/Leader:/{print $6}' | sort | uniq -c > "$OUT/leaders.txt"
total=$((GB * 1024 * 1024 * 1024 / SIZE))
for i in 1 2 3; do
  kc env KAFKA_HEAP_OPTS=-Xmx1g /opt/control/bin/kafka-producer-perf-test.sh --topic $TOPIC --num-records $((total / 3)) \
    --record-size $SIZE --throughput -1 --bootstrap-server $B \
    --command-property acks=1 batch.size=65536 linger.ms=5 buffer.memory=134217728 | tail -1 > "$OUT/produce.p$i.out" &
done
wait
records=$((total * 99 / 100))

run() { # <key> <kafka dir> <extra consumer properties>
  local key=$1 k=$2 props=$3 rep f
  for rep in $(seq "$REPS"); do
    f="$OUT/$key.rep$rep"
    # Thread CPU of the consumer and total CPU of every container, 5 s into the run.
    (sleep 5; kc top -H -b -d 3 -n 2 -w 160 2>/dev/null | awk '/^top -/{n++} n==2' | head -14 > "$f.threads"
     docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' > "$f.stats") &
    # shellcheck disable=SC2086
    kc env KAFKA_HEAP_OPTS="-Xms2g -Xmx2g" /opt/$k/bin/kafka-consumer-perf-test.sh --bootstrap-server $B --topic $TOPIC \
      --num-records $records --group "$key-$rep-$RANDOM" --timeout 120000 --print-metrics \
      --command-property group.protocol=consumer $props > "$f.out" 2> "$f.err" || true
    wait
    echo "$(date +%H:%M:%S) b$N $key rep$rep $(awk -F, 'NR==2{print "MB/s=" $4 " fetchMB/s=" $9}' "$f.out")" | tee -a "$OUT/progress.log"
  done
}
run control-default control ""
run branch-default branch ""
run branch-mpfb256k branch "--fetch-size 262144"
run branch-mpfb4m branch "--fetch-size 4194304"
echo done | tee -a "$OUT/progress.log"
