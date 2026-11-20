#!/bin/bash
# usage: run.sh <variant: base|branch> <topic> <num-records> <protocol: consumer|classic> <tag> [extra KAFKA_OPTS]
set -u
SP="/private/tmp/claude-501/-Users-unknowntpo-repo-unknowntpo-kafka-trunk--claude-worktrees-suspicious-easley-0dd733/55716775-f853-4d86-8e56-82bb355dce63/scratchpad"
W=/Users/unknowntpo/repo/unknowntpo/kafka/consumer-receive-buffer-reuse
variant=$1; topic=$2; n=$3; proto=$4; tag=$5; extra=${6:-}
out="$SP/bench/results/$tag"; mkdir -p "$out"
if [ "$variant" = base ]; then
  export CLASSPATH="$SP/base-5b648ca9cb/clients/build/libs/kafka-clients-4.5.0-SNAPSHOT.jar"
else
  unset CLASSPATH
fi
export KAFKA_HEAP_OPTS="-Xmx2g -Xms2g"
export KAFKA_OPTS="-Xlog:gc:file=$out/gc.log $extra"
cd "$W"
/usr/bin/time -l bin/kafka-consumer-perf-test.sh --bootstrap-server localhost:19092 --topic "$topic" \
  --num-records "$n" --group "g-$tag-$RANDOM" --timeout 60000 \
  --command-property group.protocol="$proto" > "$out/perf.txt" 2> "$out/time.txt"
python3 - "$out" "$variant" "$tag" <<'EOF'
import re,sys
out,variant,tag=sys.argv[1:]
perf=open(out+"/perf.txt").read().strip().splitlines()[-1].split(",")
mb=float(perf[2]); mbs=float(perf[3]); fetch_ms=float(perf[7]); fetch_mbs=float(perf[8])
t=open(out+"/time.txt").read()
user=float(re.search(r'([\d.]+) user',t).group(1)); sys_=float(re.search(r'([\d.]+) sys',t).group(1))
alloc=0; prev=None; n=0
for m in re.finditer(r'Pause Young.*? (\d+)M->(\d+)M',open(out+"/gc.log").read()):
    b,a=int(m.group(1)),int(m.group(2)); alloc+= b-(prev or 0); prev=a; n+=1
print(f"{tag}\t{variant}\tMB={mb:.0f}\tMB/s={mbs:.1f}\tfetchMB/s={fetch_mbs:.1f}\tcpu_s={user+sys_:.2f}\tcpu_s/GB={(user+sys_)/(mb/1024):.3f}\tyoungGCs={n}\talloc_MB/MB={alloc/mb:.2f}")
EOF
