#!/bin/bash
# profile.sh <protocol> <topic> <records> [props...]  -> collapsed stacks with thread names
S=${BENCH_DIR:-$(cd "$(dirname "$0")" && pwd)}; proto=$1; topic=$2; n=$3; shift 3
LIB=${ASYNC_PROFILER_LIB:?set ASYNC_PROFILER_LIB to libasyncProfiler.dylib/.so}
out=$S/prof-$proto-$topic.collapsed
export KAFKA_OPTS="$KAFKA_OPTS -agentpath:$LIB=start,event=cpu,interval=1ms,threads,collapsed,file=$out"
$S/bench.sh consume $proto $topic $n "$@" | tail -1
echo "== samples per thread ($out)"
awk -F';' '{split($NF,a," "); n=a[length(a)]; t=$1; sub(/^\[/,"",t); sub(/ tid=[0-9]+\]$/,"",t); s[t]+=n; tot+=n} END{for(k in s) printf "%8d %5.1f%% %s\n", s[k], 100*s[k]/tot, k; }' $out | sort -rn | head -12
