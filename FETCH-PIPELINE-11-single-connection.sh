#!/usr/bin/env bash
# Why does one consumer stop near 1000 MB/s at 1 KB records on GCP? Runs on the client VM.
# Candidates: the per-connection bandwidth of the network, the socket buffers that Kafka sets
# (client receive.buffer.bytes, broker socket.send.buffer.bytes), or the broker.
#
#   FETCH-PIPELINE-11-single-connection.sh produce            create the topic and write the data
#   FETCH-PIPELINE-11-single-connection.sh run <label>        run the combinations; <label> names the broker state
#   FETCH-PIPELINE-11-single-connection.sh extra <label>      combinations that need a larger net.core.rmem_max
#   FETCH-PIPELINE-11-single-connection.sh rcv <label>        client receive.buffer.bytes: 64 KB (the consumer default), 2 MB, -1
#   FETCH-PIPELINE-11-single-connection.sh broker-buffer <v>  on the broker VM: restart with socket.send.buffer.bytes=<v>; "default" removes the line
#
# Expects control.tgz and branch.tgz in the current directory and a broker on fp-broker:9092.
set -euo pipefail
OUT="$PWD/fp11-results"; WORK="$PWD/fp-work"; B="${FP_BROKER:-fp-broker}:9092"
TOPIC=single-connection; SIZE=1000; GB="${FP_GB:-20}"; REPS="${FP_REPS:-3}"
mkdir -p "$OUT" "$WORK"
if [ "${1:-}" = broker-buffer ]; then
  K="$WORK/broker"; P="$WORK/server.properties"
  "$K/bin/kafka-server-stop.sh" >/dev/null 2>&1 || true
  for _ in $(seq 30); do pgrep -f kafka.Kafka >/dev/null || break; sleep 1; done
  sed -i '/^socket.send.buffer.bytes=/d' "$P"
  [ "$2" = default ] || echo "socket.send.buffer.bytes=$2" >> "$P"
  KAFKA_HEAP_OPTS="-Xms4g -Xmx4g" nohup "$K/bin/kafka-server-start.sh" "$P" > "$WORK/broker.log" 2>&1 &
  for _ in $(seq 60); do
    "$K/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && { grep -c '^socket.send' "$P" || true; echo ready; exit 0; }
    sleep 2
  done
  echo "broker did not start"; exit 1
fi
unpack() { local d="$WORK/$1"; [ -d "$d" ] || { mkdir -p "$d"; tar -xzf "$1.tgz" -C "$d" --strip-components=1; }; echo "$d"; }
KC=$(unpack control); KB=$(unpack branch)
total=$((GB * 1024 * 1024 * 1024 / SIZE)); records=$((total * 99 / 100))

run() { # <key> <kafka dir> [extra arguments of kafka-consumer-perf-test]
  local key=$1 k=$2 rep f; shift 2
  for rep in $(seq "$REPS"); do
    f="$OUT/$key.rep$rep"
    [ -f "$f.done" ] && continue
    # 8 s into the run: CPU per thread, and the kernel's view of the fetch connection.
    (sleep 8; ss -tinm '( dport = :9092 )' > "$f.ss" 2>&1 || true
     top -H -b -d 4 -n 2 -w 160 2>/dev/null | awk '/^top -/{n++} n==2' | head -14 > "$f.threads") &
    KAFKA_HEAP_OPTS="-Xms2g -Xmx2g" "$k/bin/kafka-consumer-perf-test.sh" --bootstrap-server "$B" --topic "$TOPIC" \
      --num-records "$records" --group "$key-$rep-$RANDOM" --timeout 120000 --print-metrics \
      --command-property group.protocol=consumer "$@" > "$f.out" 2> "$f.err" && touch "$f.done"
    wait
    echo "$(date +%H:%M:%S) $key rep$rep $(awk -F, 'NR==2{print "MB/s=" $4 " fetchMB/s=" $9}' "$f.out")" | tee -a "$OUT/progress.log"
  done
}

case "${1:-}" in
produce)
  { hostname; nproc; sysctl net.core.rmem_max net.core.wmem_max net.ipv4.tcp_rmem net.ipv4.tcp_wmem; } > "$OUT/client-machine.txt"
  "$KC/bin/kafka-topics.sh" --bootstrap-server "$B" --create --topic "$TOPIC" --partitions 12 --replication-factor 1
  for i in 1 2 3; do
    KAFKA_HEAP_OPTS=-Xmx1g "$KC/bin/kafka-producer-perf-test.sh" --topic "$TOPIC" --num-records $((total / 3)) \
      --record-size "$SIZE" --throughput -1 --bootstrap-server "$B" \
      --command-property acks=1 batch.size=65536 linger.ms=5 buffer.memory=134217728 | tail -1 > "$OUT/produce.p$i.out" &
  done
  wait; cat "$OUT"/produce.p*.out
  ;;
run)
  L=$2
  # rcv2m: the tool's default, receive.buffer.bytes = 2 MB. rcvauto: -1, the kernel sizes the buffer.
  run "$L-branch-rcv2m" "$KB"
  run "$L-branch-rcvauto" "$KB" --socket-buffer-size=-1
  run "$L-control-rcv2m" "$KC"
  run "$L-control-rcvauto" "$KC" --socket-buffer-size=-1
  ;;
rcv)
  L=$2
  # rcv64k: what an application gets when it does not set receive.buffer.bytes (the tool otherwise asks for 2 MB).
  for v in branch control; do
    k=$KB; [ "$v" = control ] && k=$KC
    run "$L-$v-rcv64k" "$k" --socket-buffer-size 65536
    run "$L-$v-rcv2m" "$k"
    run "$L-$v-rcvauto" "$k" --socket-buffer-size=-1
  done
  touch "$OUT/rcv-$L.finished"
  ;;
extra)
  L=$2
  run "$L-branch-rcvauto-fetch4m" "$KB" --socket-buffer-size=-1 --fetch-size 4194304
  # A fixed receive buffer is capped by net.core.rmem_max. Raise the cap and ask for 2 MB and 32 MB again.
  sudo sysctl -qw net.core.rmem_max=67108864
  run "$L-branch-rcv2m-rmemmax" "$KB"
  run "$L-branch-rcv32m-rmemmax" "$KB" --socket-buffer-size 33554432
  ;;
*) sed -n '2,10p' "$0"; exit 1 ;;
esac
