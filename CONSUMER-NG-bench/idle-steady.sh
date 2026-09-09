#!/bin/bash
# Steady idle cost: start <cmd> in the background, sample process CPU (sum of task utime+stime) and voluntary
# context switches (sum over tasks) at T0=10s and T1=70s, report the per-minute deltas. Usage: idle-steady.sh <label> <cmd...>
label=$1; shift
"$@" >/dev/null 2>&1 &
sp=$!; sleep 1
pid=""; for i in $(seq 1 50); do for c in $(pgrep -P $sp) $sp; do [ "$(cat /proc/$c/comm 2>/dev/null)" = java ] && pid=$c; done; [ -n "$pid" ] && break; sleep 0.2; done
HZ=$(getconf CLK_TCK)
snap() { local cpu=0 ctx=0; for t in /proc/$pid/task/*; do cpu=$((cpu + $(awk '{print $14+$15}' $t/stat 2>/dev/null || echo 0))); ctx=$((ctx + $(awk '/voluntary_ctxt_switches/ && !/nonvoluntary/{print $2}' $t/status 2>/dev/null || echo 0))); done; echo "$cpu $ctx"; }
sleep 9; read c0 x0 <<<"$(snap)"; sleep 60; read c1 x1 <<<"$(snap)"
rss=$(awk '/VmRSS/{print $2}' /proc/$pid/status)
echo "$label: steady idle cpu_s_per_min=$(python3 -c "print(round(($c1-$c0)/$HZ,3))") volctx_per_s=$(python3 -c "print(round(($x1-$x0)/60,1))") rss_kb=$rss"
kill $sp 2>/dev/null; wait $sp 2>/dev/null
