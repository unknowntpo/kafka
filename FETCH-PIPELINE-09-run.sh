#!/usr/bin/env bash
# FetchPipeline benchmark: one script for the broker machine and the client machine.
# Runs the same way on two GCP VMs and on two local containers (rehearsal).
#
#   FETCH-PIPELINE-09-run.sh broker <kafka.tgz>            start one KRaft broker (combined mode)
#   FETCH-PIPELINE-09-run.sh client <broker host> <dir>    run every combination; <dir> holds
#                                                          control.tgz branch.tgz proto.tgz
#   FETCH-PIPELINE-09-run.sh sample <label> <seconds>      record CPU and network (broker side)
#   FETCH-PIPELINE-09-run.sh watch                         broker side, until killed: every 5 s the kernel's view
#                                                          of the client connections and the busy broker threads
#
# Results go to $OUT (default ./fp-results), one file per combination and repetition.
# A combination that already has a result file is skipped, so the script can be re-run
# after a failure without repeating finished work.
#
# Sizes come from the environment so that a rehearsal can use small data:
#   FP_GB_100=10 FP_GB_1000=20 FP_REPS=3 FP_SCALING="1 2 4" FP_PARTITIONS=12 FP_HEAP="-Xms2g -Xmx2g"
# Other settings:
#   FP_BROKER_EXTRA="socket.send.buffer.bytes=-1"   one line added to server.properties
#   FP_CONSUMER_ARGS="--socket-buffer-size=-1"      extra arguments of kafka-consumer-perf-test
#   FP_PLAN=phase5 FP_SIZES="1000 100"              only trunk and the branch, with the consumer counts of FP_SCALING
#   FP_PLAN=phase6                                  one consumer: the prototypes at 100 B (by rounds), trunk, the branch
#                                                   and librdkafka at both sizes. Needs FP_RDK=<rdkafka_performance>.
#   FP_PLAN=phase6b FP_CAP_GB=15                    after "broker-restart" with default buffers: the branch and librdkafka
#   FETCH-PIPELINE-09-run.sh broker-restart         restart the broker and keep its data; server.properties gets
#                                                   socket.send.buffer.bytes from FP_BROKER_EXTRA, or loses it
set -euo pipefail

OUT="${OUT:-$PWD/fp-results}"
WORK="${WORK:-$PWD/fp-work}"
TOPIC=fetch-pipeline
PARTITIONS="${FP_PARTITIONS:-12}"
REPS="${FP_REPS:-3}"
SCALING="${FP_SCALING:-1 2 4}"
HEAP="${FP_HEAP:--Xms2g -Xmx2g}"
GB_100="${FP_GB_100:-10}"
GB_1000="${FP_GB_1000:-20}"
PERF="${FP_PERF:-auto}"   # auto: use "perf stat" when hardware counters work
mkdir -p "$OUT" "$WORK"

unpack() { # <tgz> <name> -> prints the directory
  local d="$WORK/$2"
  if [ ! -d "$d" ]; then mkdir -p "$d"; tar -xzf "$1" -C "$d" --strip-components=1; fi
  echo "$d"
}

perf_ok() {
  [ "$PERF" = off ] && return 1
  command -v perf >/dev/null 2>&1 && perf stat -e cycles true >/dev/null 2>&1
}

machine_info() {
  { echo "host=$(hostname) nproc=$(nproc)"; free -g | sed -n 2p; lscpu | grep -E "Model name|Thread|Core|Socket|L3" || true
    java -version 2>&1 | head -1; } > "$OUT/machine-$(hostname).txt"
}

case "${1:-}" in
broker)
  K=$(unpack "$2" broker)
  machine_info
  ADV="${FP_ADVERTISED:-$(hostname)}"
  cat > "$WORK/server.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://$ADV:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=$WORK/data
num.partitions=1
offsets.topic.replication.factor=1
share.coordinator.state.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
num.network.threads=8
num.io.threads=8
log.retention.hours=24
EOF
  [ -z "${FP_BROKER_EXTRA:-}" ] || echo "$FP_BROKER_EXTRA" >> "$WORK/server.properties"
  rm -rf "$WORK/data"
  "$K/bin/kafka-storage.sh" format -t "$("$K/bin/kafka-storage.sh" random-uuid)" -c "$WORK/server.properties" >/dev/null
  KAFKA_HEAP_OPTS="${FP_BROKER_HEAP:--Xms4g -Xmx4g}" nohup "$K/bin/kafka-server-start.sh" "$WORK/server.properties" > "$WORK/broker.log" 2>&1 &
  for _ in $(seq 60); do
    "$K/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && { echo "broker ready on $ADV:9092"; exit 0; }
    sleep 2
  done
  echo "broker did not start"; tail -20 "$WORK/broker.log"; exit 1
  ;;

broker-restart)
  K="$WORK/broker"
  sed -i '/^socket\.send\.buffer\.bytes/d' "$WORK/server.properties"
  [ -z "${FP_BROKER_EXTRA:-}" ] || echo "$FP_BROKER_EXTRA" >> "$WORK/server.properties"
  "$K/bin/kafka-server-stop.sh" >/dev/null 2>&1 || true
  for _ in $(seq 60); do pgrep -f 'kafka\.Kafka' >/dev/null || break; sleep 1; done
  KAFKA_HEAP_OPTS="${FP_BROKER_HEAP:--Xms4g -Xmx4g}" nohup "$K/bin/kafka-server-start.sh" "$WORK/server.properties" > "$WORK/broker-restart.log" 2>&1 &
  for _ in $(seq 60); do
    "$K/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && { echo "broker restarted: $(grep -c '^socket.send.buffer.bytes' "$WORK/server.properties") buffer line(s)"; exit 0; }
    sleep 2
  done
  echo "broker did not restart"; tail -20 "$WORK/broker-restart.log"; exit 1
  ;;

sample)
  # CPU per second and network per second, to tell whether the broker or the network is the limit.
  (command -v mpstat >/dev/null && mpstat 1 "$3" > "$OUT/broker-cpu-$2.txt" 2>&1) &
  (command -v sar >/dev/null && sar -n DEV 1 "$3" > "$OUT/broker-net-$2.txt" 2>&1) &
  wait
  ;;

watch)
  while true; do
    { date +%H:%M:%S; ss -tinm '( sport = :9092 )' 2>/dev/null || true; } >> "$OUT/broker-ss.txt"
    { date +%H:%M:%S; top -H -b -d 3 -n 2 -w 160 2>/dev/null | awk '/^top -/{n++} n==2 && $9+0>=10 && $1 ~ /^[0-9]+$/ {print "  " $9, $12, $13, $14}'; } >> "$OUT/broker-threads.txt"
    sleep 2
  done
  ;;

client)
  B="$2:9092"; DIR="$3"
  machine_info
  KC=$(unpack "$DIR/control.tgz" control)
  KB=$(unpack "$DIR/branch.tgz" branch)
  KP=$(unpack "$DIR/proto.tgz" proto)
  USE_PERF=0; perf_ok && USE_PERF=1
  PLAN="${FP_PLAN:-full}"
  WHOLE=0; case "$PLAN" in phase5|phase6|phase6b) WHOLE=1 ;; esac
  if [ "$WHOLE" = 1 ] && [ "$PLAN" != phase5 ]; then
    # The whole client machine, for the whole run: CPU per core and network.
    # "disown": the bare "wait" calls below must not wait for these two.
    SAMPLERS=""
    if command -v mpstat >/dev/null; then mpstat -P ALL 5 >> "$OUT/client-cpu-$PLAN.txt" 2>&1 & SAMPLERS="$!"; disown; fi
    if command -v sar >/dev/null; then sar -n DEV 5 >> "$OUT/client-net-$PLAN.txt" 2>&1 & SAMPLERS="$SAMPLERS $!"; disown; fi
    # shellcheck disable=SC2086
    trap '[ -z "$SAMPLERS" ] || kill $SAMPLERS 2>/dev/null; true' EXIT
  fi
  echo "perf hardware counters: $USE_PERF" | tee "$OUT/perf-available.txt"

  ensure_data() { # <record size> <total records>; the topic holds one record size at a time
    local size=$1 total=$2 i
    [ "$(cat "$OUT/dataset" 2>/dev/null)" = "$size" ] && return 0
    "$KC/bin/kafka-topics.sh" --bootstrap-server "$B" --delete --topic "$TOPIC" >/dev/null 2>&1 || true
    for i in $(seq 60); do
      "$KC/bin/kafka-topics.sh" --bootstrap-server "$B" --list 2>/dev/null | grep -qx "$TOPIC" || break
      sleep 2
    done
    sleep 3
    "$KC/bin/kafka-topics.sh" --bootstrap-server "$B" --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1
    for i in 1 2 3; do
      KAFKA_HEAP_OPTS="-Xmx1g" "$KC/bin/kafka-producer-perf-test.sh" --topic "$TOPIC" --num-records $((total / 3)) \
        --record-size "$size" --throughput -1 --bootstrap-server "$B" \
        --command-property acks=1 batch.size=65536 linger.ms=5 buffer.memory=134217728 \
        | tail -1 > "$OUT/produce-$size.p$i.out" &
    done
    wait
    echo "$size" > "$OUT/dataset"
    echo "$(date +%H:%M:%S) produced $((total / 3 * 3)) records of ${size}B" | tee -a "$OUT/progress.log"
    if [ "$WHOLE" = 1 ]; then
      # One read that is not recorded: the first read after a produce was slow in earlier runs.
      # shellcheck disable=SC2086
      KAFKA_HEAP_OPTS="$HEAP" "$KC/bin/kafka-consumer-perf-test.sh" --bootstrap-server "$B" --topic "$TOPIC" \
        --num-records $((total * 99 / 100)) --group "warmup-$size-$RANDOM" --timeout 120000 \
        --command-property group.protocol=consumer ${FP_CONSUMER_ARGS:-} > "$OUT/warmup-$size.out" 2>&1 || true
      echo "$(date +%H:%M:%S) warm-up read done" | tee -a "$OUT/progress.log"
    fi
  }

  consume() { # <key> <kafka dir> <consumers> <records> <jvm opts> [heap]
    local key=$1 k=$2 n=$3 records=$4 opts=$5 heap=${6:-$HEAP} rep i pids
    for rep in $(seq "$REPS"); do
      local done_file="$OUT/$key.rep$rep.done"
      [ -f "$done_file" ] && continue
      ensure_data "$size" "$total"
      pids=()
      for i in $(seq "$n"); do
        local f="$OUT/$key.rep$rep.c$i"
        local cmd=("$k/bin/kafka-consumer-perf-test.sh" --bootstrap-server "$B" --topic "$TOPIC"
          --num-records "$records" --group "$key-$rep-$i-$RANDOM" --timeout 120000
          --command-property group.protocol=consumer ${FP_CONSUMER_ARGS:-})
        # Hardware counters only for the first consumer of the first repetition.
        if [ "$USE_PERF" = 1 ] && [ "$rep" = 1 ] && [ "$i" = 1 ]; then
          cmd=(perf stat -o "$f.perf" -e cycles,instructions,L1-dcache-load-misses,mem_load_retired.l3_miss,slots,topdown-be-bound,topdown-retiring -- "${cmd[@]}")
        fi
        KAFKA_HEAP_OPTS="$heap" KAFKA_OPTS="$opts" "${cmd[@]}" > "$f.out" 2> "$f.err" &
        pids+=($!)
      done
      # Per-thread CPU of every consumer JVM, once the consumers are past start-up (repetition 2 only,
      # so that it never overlaps "perf stat"). Shows which thread is the limit.
      if [ "$WHOLE" = 1 ]; then
        # Every repetition, for the whole run: threads above 10% CPU every 2 s, and the fetch connections 6 s in.
        touch "$OUT/sampling"
        (while [ -f "$OUT/sampling" ]; do
           top -H -b -d 2 -n 2 -w 160 2>/dev/null | awk '/^top -/{n++; if (n==2) print $3} n==2 && $9+0>=10 && $1 ~ /^[0-9]+$/ {print "  " $9, $12, $13, $14}'
         done > "$OUT/$key.rep$rep.threads") &
        (sleep 6; ss -tinm '( dport = :9092 )' > "$OUT/$key.rep$rep.ss" 2>&1 || true) &
      elif [ "$rep" = 2 ]; then
        (sleep "${FP_THREADS_DELAY:-12}"; top -H -b -d 4 -n 2 -w 160 2>/dev/null | awk '/^top -/{n++} n==2' | head -32 > "$OUT/$key.rep$rep.threads") &
      fi
      local ok=1
      for p in "${pids[@]}"; do wait "$p" || ok=0; done
      rm -f "$OUT/sampling"
      wait
      # column 4 is MB/s, column 9 is fetch MB/s; sum over the consumers
      local sum
      sum=$(tail -qn 1 "$OUT/$key.rep$rep".c*.out | awk -F, '{a+=$4; b+=$9} END {printf "%.1f %.1f", a, b}')
      if [ "$ok" = 1 ]; then echo "$sum" > "$done_file"; fi
      echo "$(date +%H:%M:%S) $key rep$rep ok=$ok MB/s=$sum" | tee -a "$OUT/progress.log"
    done
  }

  rdk() { # <key> <records> [-X name=value ...]; librdkafka, same result files as consume()
    local key=$1 records=$2 rep; shift 2
    for rep in $(seq "$REPS"); do
      local done_file="$OUT/$key.rep$rep.done" f="$OUT/$key.rep$rep.c1"
      [ -f "$done_file" ] && continue
      ensure_data "$size" "$total"
      touch "$OUT/sampling"
      (while [ -f "$OUT/sampling" ]; do
         top -H -b -d 2 -n 2 -w 160 2>/dev/null | awk '/^top -/{n++; if (n==2) print $3} n==2 && $9+0>=10 && $1 ~ /^[0-9]+$/ {print "  " $9, $12, $13, $14}'
       done > "$OUT/$key.rep$rep.threads") &
      (sleep 6; ss -tinm '( dport = :9092 )' > "$OUT/$key.rep$rep.ss" 2>&1 || true) &
      local ok=1
      timeout 300 "$FP_RDK" -G "$key-$rep-$RANDOM" -t "$TOPIC" -b "$B" -c "$records" -X auto.offset.reset=earliest "$@" > "$f.out" 2> "$f.err" || ok=0
      rm -f "$OUT/sampling"
      wait
      # Last line: "% N messages (B bytes) consumed in Tms: ...". MB here is 1024 * 1024 bytes, as in the Java tool.
      local sum
      sum=$(grep -a 'consumed in' "$f.out" | tail -1 | sed -E 's/.*\(([0-9]+) bytes\) consumed in ([0-9]+)ms.*/\1 \2/' | awk '{v=$1/1048576/($2/1000); printf "%.1f %.1f", v, v}')
      if [ "$ok" = 1 ] && [ -n "$sum" ]; then echo "$sum" > "$done_file"; fi
      echo "$(date +%H:%M:%S) $key rep$rep ok=$ok MB/s=$sum" | tee -a "$OUT/progress.log"
    done
  }
  RDK_BEST=(-X queued.min.messages=10000000 -X fetch.queue.backoff.ms=0)

  for size in ${FP_SIZES:-100 1000}; do
    gb=$GB_100; [ "$size" = 1000 ] && gb=$GB_1000
    total=$((gb * 1024 * 1024 * 1024 / size))
    records=$((total * 99 / 100))   # stop a little early so that no consumer waits for the poll timeout
    # Most important first: if time runs out, the less important combinations are the ones lost.
    # FP_PLAN=phase2 measures only what phase one could not answer.
    if [ "$PLAN" = phase6 ]; then
      max=$REPS
      if [ "$size" = 100 ]; then
        # By rounds, so that a drift over time touches every variant equally. The branch is the anchor:
        # proto-base is the same code and must give the same number.
        for REPS in $(seq "${FP_PROTO_REPS:-4}"); do
          consume "${size}B-branch-c1" "$KB" 1 "$records" ""
          consume "${size}B-proto-base" "$KP" 1 "$records" ""
          consume "${size}B-proto-lean" "$KP" 1 "$records" "-Dfp.lean=true"
          consume "${size}B-proto-decode2" "$KP" 1 "$records" "-Dfp.decode.threads=2"
          consume "${size}B-proto-lean-decode2" "$KP" 1 "$records" "-Dfp.lean=true -Dfp.decode.threads=2"
        done
        REPS=$max
        rdkrecords=$((${FP_RDK_GB_100:-6} * 1024 * 1024 * 1024 / size))   # librdkafka is slow here: fewer records
        [ "$rdkrecords" -gt "$records" ] && rdkrecords=$records
        REPS=2 consume "${size}B-control-c1" "$KC" 1 "$records" ""
      else
        rdkrecords=$records
        consume "${size}B-control-c1" "$KC" 1 "$records" ""
        consume "${size}B-branch-c1" "$KB" 1 "$records" ""
      fi
      rdk "${size}B-rdk-best" "$rdkrecords" "${RDK_BEST[@]}"
      rdk "${size}B-rdk-default" "$rdkrecords"
      continue
    fi
    if [ "$PLAN" = phase6b ]; then
      # Broker and clients with their default socket buffers. FP_CONSUMER_ARGS must hold "--socket-buffer-size 65536":
      # the Java tool otherwise asks for 2 MB, which is not the consumer default.
      cap=$((${FP_CAP_GB:-15} * 1024 * 1024 * 1024 / size)); [ "$cap" -gt "$records" ] && cap=$records
      rdk "${size}B-rdk-default-defaults" "$cap"
      REPS=2 consume "${size}B-branch-c1-defaults" "$KB" 1 "$cap" ""
      REPS=2 consume "${size}B-control-c1-defaults" "$KC" 1 "$cap" ""
      continue
    fi
    if [ "${FP_PLAN:-full}" = phase2 ] || [ "${FP_PLAN:-full}" = phase5 ]; then
      for n in $SCALING; do
        consume "${size}B-control-c$n" "$KC" "$n" "$records" ""
        consume "${size}B-branch-c$n" "$KB" "$n" "$records" ""
      done
      if [ "$size" = 100 ] && [ "$FP_PLAN" = phase2 ]; then
        consume "${size}B-proto-base" "$KP" 1 "$records" ""
        consume "${size}B-proto-lean" "$KP" 1 "$records" "-Dfp.lean=true"
        consume "${size}B-proto-base-heap512m" "$KP" 1 "$records" "" "-Xms512m -Xmx512m"
        consume "${size}B-proto-lean-heap512m" "$KP" 1 "$records" "-Dfp.lean=true" "-Xms512m -Xmx512m"
        consume "${size}B-proto-decode2" "$KP" 1 "$records" "-Dfp.decode.threads=2"
      fi
      continue
    fi
    consume "${size}B-control-c1" "$KC" 1 "$records" ""
    consume "${size}B-branch-c1" "$KB" 1 "$records" ""
    consume "${size}B-proto-lean" "$KP" 1 "$records" "-Dfp.lean=true"
    consume "${size}B-proto-base" "$KP" 1 "$records" ""
    for n in $SCALING; do
      [ "$n" = 1 ] && continue
      consume "${size}B-control-c$n" "$KC" "$n" "$records" ""
      consume "${size}B-branch-c$n" "$KB" "$n" "$records" ""
    done
    consume "${size}B-proto-decode2" "$KP" 1 "$records" "-Dfp.decode.threads=2"
    consume "${size}B-proto-lean-decode2" "$KP" 1 "$records" "-Dfp.lean=true -Dfp.decode.threads=2"
  done
  # shellcheck disable=SC2086
  [ -z "${SAMPLERS:-}" ] || kill $SAMPLERS 2>/dev/null || true
  echo "all combinations finished" | tee -a "$OUT/progress.log"
  ;;

*)
  sed -n '2,16p' "$0"; exit 1 ;;
esac
