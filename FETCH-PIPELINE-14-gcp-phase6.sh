#!/usr/bin/env bash
# GCP phase six, unattended: prototypes at 100 B, and librdkafka next to trunk and the branch.
#   FETCH-PIPELINE-14-gcp-phase6.sh <dir with control.tgz branch.tgz proto.tgz> <result dir>
# Creates two VMs, measures, copies the results back and deletes the VMs. The VMs are deleted
# when this script ends for any reason, and GCP deletes them itself after FP_MAX_RUN.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
TGZ="$1"; RES="$2"
export FP_BROKER_TYPE="${FP_BROKER_TYPE:-n2-standard-16}" FP_MAX_RUN="${FP_MAX_RUN:-90m}"
G="$HERE/FETCH-PIPELINE-09-gcp.sh"
PROJECT="$(gcloud config get-value project 2>/dev/null)"; ZONE="${FP_ZONE:-us-central1-a}"
on() { local vm=$1; shift; gcloud --project "$PROJECT" compute ssh "$vm" --zone "$ZONE" --quiet --ssh-flag=-oConnectTimeout=20 --command "$*" < /dev/null; }
say() { echo "$(date +%H:%M:%S) $*"; }

finish() {
  say "pull"; "$G" pull "$RES" || say "pull failed"
  say "down"; "$G" down
  say "status after down:"; "$G" status
}
trap finish EXIT

wait_for() { # <number of "all combinations finished" lines> <max minutes>
  local i n
  for i in $(seq $(($2 * 2))); do
    n=$(on fp-client "grep -c 'all combinations finished' fp-results/progress.log 2>/dev/null; tail -1 fp-results/progress.log 2>/dev/null; pgrep -f 'FETCH-PIPELINE-09-run.sh client' >/dev/null || echo CLIENT-NOT-RUNNING" 2>/dev/null)
    say "$(echo "$n" | tr '\n' ' ')"
    [ "$(echo "$n" | head -1)" = "$1" ] && return 0
    echo "$n" | grep -q CLIENT-NOT-RUNNING && { say "client script ended early"; on fp-client "tail -5 client.log"; return 1; }
    sleep 30
  done
  say "timed out"; return 1
}

say "up"; "$G" up || exit 1
sleep 40
say "setup"; "$G" setup || { sleep 20; "$G" setup; } || exit 1
on fp-client "sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install -y build-essential git python3 >/dev/null 2>&1; \
  git clone -q --depth 1 --branch v2.6.1 https://github.com/confluentinc/librdkafka.git && cd librdkafka && \
  ./configure --disable-ssl --disable-gssapi --disable-sasl --disable-curl --disable-zstd --disable-zlib > ../rdk-configure.log 2>&1 && \
  make -j8 libs > ../rdk-make.log 2>&1 && make -C examples rdkafka_performance >> ../rdk-make.log 2>&1; ls -la examples/rdkafka_performance" || exit 1
say "net"; "$G" net
say "push"; "$G" push "$TGZ" || exit 1

on fp-broker "FP_BROKER_EXTRA='socket.send.buffer.bytes=-1' bash FETCH-PIPELINE-09-run.sh broker control.tgz && mkdir -p fp-results && \
  (nohup mpstat 5 > fp-results/broker-cpu.txt 2>&1 &) && (nohup sar -n DEV 5 > fp-results/broker-net.txt 2>&1 &) && \
  (OUT=\$HOME/fp-results nohup bash FETCH-PIPELINE-09-run.sh watch > /dev/null 2>&1 &)" || exit 1
say "stage one: socket buffers at -1"
on fp-client "FP_RDK=\$HOME/librdkafka/examples/rdkafka_performance FP_PLAN=phase6 FP_SIZES='100 1000' FP_REPS=3 FP_PROTO_REPS=4 \
  FP_GB_100=20 FP_GB_1000=30 FP_RDK_GB_100=6 FP_CONSUMER_ARGS='--socket-buffer-size=-1' \
  nohup bash FETCH-PIPELINE-09-run.sh client fp-broker . > client.log 2>&1 &"
sleep 20
wait_for 1 50 || exit 1

say "stage two: default socket buffers"
on fp-broker "bash FETCH-PIPELINE-09-run.sh broker-restart" || exit 1
on fp-client "FP_RDK=\$HOME/librdkafka/examples/rdkafka_performance FP_PLAN=phase6b FP_SIZES=1000 FP_REPS=3 FP_GB_1000=30 FP_CAP_GB=15 \
  FP_CONSUMER_ARGS='--socket-buffer-size 65536' \
  nohup bash FETCH-PIPELINE-09-run.sh client fp-broker . >> client.log 2>&1 &"
sleep 20
wait_for 2 12 || exit 1
say "measurements finished"
