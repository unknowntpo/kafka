#!/usr/bin/env bash
# FetchPipeline benchmark on two GCP VMs (broker and client on separate machines).
# Draft. Not run yet. Read FETCH-PIPELINE-09-gcp-bench.md first.
#
#   ./FETCH-PIPELINE-09-gcp.sh plan      print what "up" would create; creates nothing
#   ./FETCH-PIPELINE-09-gcp.sh up        create the two VMs and the firewall rule
#   ./FETCH-PIPELINE-09-gcp.sh net       measure the bandwidth between the VMs with iperf3
#   ./FETCH-PIPELINE-09-gcp.sh status    list everything this script created (billing check)
#   ./FETCH-PIPELINE-09-gcp.sh down      delete everything this script created
#
# Every resource carries the label fp-bench=1, so "status" and "down" find all of it.
set -euo pipefail

PROJECT="${FP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
ZONE="${FP_ZONE:-asia-east1-b}"
MACHINE="${FP_MACHINE:-c3-standard-22}"
# The VMs delete themselves after this time, even if "down" is never run.
MAX_RUN="${FP_MAX_RUN:-6h}"
DISK_GB="${FP_DISK_GB:-100}"
IMAGE_FAMILY="ubuntu-2404-lts-amd64"
IMAGE_PROJECT="ubuntu-os-cloud"
VMS=(fp-broker fp-client)
FW="fp-bench-internal"

g() { gcloud --project "$PROJECT" "$@"; }

create_args() {
  echo --zone "$ZONE" --machine-type "$MACHINE" \
    --provisioning-model SPOT --instance-termination-action DELETE \
    --max-run-duration "$MAX_RUN" \
    --performance-monitoring-unit standard \
    --image-family "$IMAGE_FAMILY" --image-project "$IMAGE_PROJECT" \
    --boot-disk-size "${DISK_GB}GB" --boot-disk-type pd-ssd \
    --tags fp-bench --labels fp-bench=1
}

case "${1:-}" in
  plan)
    echo "project: $PROJECT   zone: $ZONE"
    echo "would create: ${VMS[*]}"
    echo "gcloud compute instances create ${VMS[*]} $(create_args)"
    echo "would create firewall rule $FW: allow tcp between VMs tagged fp-bench"
    ;;
  up)
    # shellcheck disable=SC2046
    g compute instances create "${VMS[@]}" $(create_args)
    g compute firewall-rules create "$FW" --network default \
      --allow tcp --source-tags fp-bench --target-tags fp-bench
    "$0" status
    ;;
  net)
    for vm in "${VMS[@]}"; do
      g compute ssh "$vm" --zone "$ZONE" --command \
        "sudo apt-get -qq update && sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install -y iperf3 >/dev/null"
    done
    g compute ssh fp-broker --zone "$ZONE" --command "iperf3 -s -D"
    g compute ssh fp-client --zone "$ZONE" --command "iperf3 -c fp-broker -P 8 -t 10 | tail -4"
    ;;
  status)
    g compute instances list --filter "labels.fp-bench=1" \
      --format "table(name,zone.basename(),machineType.basename(),status,scheduling.provisioningModel)"
    g compute disks list --filter "labels.fp-bench=1" --format "table(name,sizeGb,status)"
    g compute firewall-rules list --filter "name=$FW" --format "table(name,network.basename())"
    ;;
  down)
    g compute instances delete "${VMS[@]}" --zone "$ZONE" --quiet || true
    g compute firewall-rules delete "$FW" --quiet || true
    "$0" status
    ;;
  *)
    sed -n '2,12p' "$0"; exit 1 ;;
esac
