#!/usr/bin/env bash
# FetchPipeline benchmark on two GCP VMs (broker and client on separate machines).
# Read FETCH-PIPELINE-09-gcp-bench.md first.
#
#   ./FETCH-PIPELINE-09-gcp.sh plan            print what "up" would create; creates nothing
#   ./FETCH-PIPELINE-09-gcp.sh up              create the two VMs
#   ./FETCH-PIPELINE-09-gcp.sh setup           install JDK 17, perf, sysstat, iperf3 on both
#   ./FETCH-PIPELINE-09-gcp.sh net             measure broker -> client bandwidth with iperf3
#   ./FETCH-PIPELINE-09-gcp.sh push <dir>      copy control.tgz branch.tgz proto.tgz and the run script
#   ./FETCH-PIPELINE-09-gcp.sh run             start the broker, then run every combination
#   ./FETCH-PIPELINE-09-gcp.sh pull <dir>      copy the results back
#   ./FETCH-PIPELINE-09-gcp.sh status          list everything this script created (billing check)
#   ./FETCH-PIPELINE-09-gcp.sh down            delete everything this script created
#
# Guards against cost: the VMs delete themselves after FP_MAX_RUN; every resource carries the
# label fp-bench=1; the default network already allows internal traffic and SSH, so no
# firewall rule is created.
set -euo pipefail

PROJECT="${FP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
ZONE="${FP_ZONE:-us-central1-a}"
# The broker needs memory for the page cache; the client needs cores.
# C4 quota is 8 vCPU per region in this project, so only the client is C4.
BROKER_TYPE="${FP_BROKER_TYPE:-n2-highmem-4}"
CLIENT_TYPE="${FP_CLIENT_TYPE:-c4-standard-8}"
BROKER_DISK_GB="${FP_BROKER_DISK_GB:-80}"
CLIENT_DISK_GB="${FP_CLIENT_DISK_GB:-30}"
MAX_RUN="${FP_MAX_RUN:-4h}"
HERE="$(cd "$(dirname "$0")" && pwd)"

g() { gcloud --project "$PROJECT" "$@"; }
on() { local vm=$1; shift; g compute ssh "$vm" --zone "$ZONE" --quiet --ssh-flag=-oConnectTimeout=20 --command "$*" < /dev/null; }

create_args() { # <machine type> <disk GB>
  # On-demand, not Spot: a preempted VM would force a paid re-run.
  # C4 is the series with hardware performance counters (PMU) and needs Hyperdisk.
  local extra="--boot-disk-type pd-ssd"
  case "$1" in c4-*) extra="--boot-disk-type hyperdisk-balanced --performance-monitoring-unit standard" ;; esac
  echo --zone "$ZONE" --machine-type "$1" \
    --max-run-duration "$MAX_RUN" --instance-termination-action DELETE \
    --image-family ubuntu-2404-lts-amd64 --image-project ubuntu-os-cloud \
    --boot-disk-size "${2}GB" $extra --labels fp-bench=1
}

case "${1:-}" in
  plan)
    echo "project: $PROJECT   zone: $ZONE   auto-delete after: $MAX_RUN"
    echo "gcloud compute instances create fp-broker $(create_args "$BROKER_TYPE" "$BROKER_DISK_GB")"
    echo "gcloud compute instances create fp-client $(create_args "$CLIENT_TYPE" "$CLIENT_DISK_GB")"
    ;;
  up)
    # shellcheck disable=SC2046
    g compute instances create fp-broker $(create_args "$BROKER_TYPE" "$BROKER_DISK_GB")
    # shellcheck disable=SC2046
    g compute instances create fp-client $(create_args "$CLIENT_TYPE" "$CLIENT_DISK_GB")
    "$0" status
    ;;
  setup)
    for vm in fp-broker fp-client; do
      on "$vm" "sudo apt-get -qq update && sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
        openjdk-17-jdk-headless sysstat iperf3 linux-tools-common linux-tools-\$(uname -r) >/dev/null 2>&1; \
        sudo sysctl -qw kernel.perf_event_paranoid=0; \
        java -version 2>&1 | head -1; nproc; free -g | sed -n 2p; \
        perf stat -e cycles,cache-misses true 2>&1 | grep -E 'cycles|cache-misses' | head -2" &
    done
    wait
    ;;
  net)
    on fp-client "iperf3 -s -D"
    on fp-broker "iperf3 -c fp-client -P 8 -t 10 | grep -E 'SUM.*sender'"
    ;;
  push)
    g compute scp --quiet --zone "$ZONE" "$2/control.tgz" "$HERE/FETCH-PIPELINE-09-run.sh" fp-broker:~ &
    g compute scp --quiet --zone "$ZONE" "$2/control.tgz" "$2/branch.tgz" "$2/proto.tgz" "$HERE/FETCH-PIPELINE-09-run.sh" fp-client:~ &
    wait
    ;;
  run)
    on fp-broker "bash FETCH-PIPELINE-09-run.sh broker control.tgz && mkdir -p fp-results && \
      (nohup mpstat 5 > fp-results/broker-cpu.txt 2>&1 &) && (nohup sar -n DEV 5 > fp-results/broker-net.txt 2>&1 &)"
    # nohup: the run survives a dropped SSH session. Follow it with: ... ssh fp-client 'tail -f fp-results/progress.log'
    on fp-client "nohup bash FETCH-PIPELINE-09-run.sh client fp-broker . > client.log 2>&1 &"
    ;;
  pull)
    mkdir -p "$2/broker" "$2/client"
    on fp-client "tar -czf results.tgz fp-results client.log"
    on fp-broker "tar -czf results.tgz fp-results fp-work/broker.log"
    g compute scp --quiet --zone "$ZONE" fp-client:results.tgz "$2/client/"
    g compute scp --quiet --zone "$ZONE" fp-broker:results.tgz "$2/broker/"
    ;;
  status)
    g compute instances list --filter "labels.fp-bench=1" \
      --format "table(name,zone.basename(),machineType.basename(),status,creationTimestamp)"
    g compute disks list --filter "labels.fp-bench=1 OR name~'^fp-'" --format "table(name,sizeGb,type.basename(),status)"
    ;;
  down)
    g compute instances delete fp-broker fp-client --zone "$ZONE" --quiet || true
    "$0" status
    ;;
  *)
    sed -n '2,14p' "$0"; exit 1 ;;
esac
