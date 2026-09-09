#!/usr/bin/env python3
"""Steady-state CPU per GB for kafka-consumer-perf-test: sample /proc/<pid>/stat while it prints per-interval
stats, then compute (CPU, bytes) deltas between the first report at or after WARMUP seconds and the last report.
usage: steady.py <label> <warmup_s> <topic> <records> <group_protocol> [extra kafka-consumer-perf args...]"""
import subprocess, sys, time, os, re, threading
label, warm, topic, records, proto = sys.argv[1], float(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]
extra = sys.argv[6:]
K = os.path.expanduser("~/kafka-bench/kafka")
env = dict(os.environ, KAFKA_HEAP_OPTS=os.environ.get("KAFKA_HEAP_OPTS", "-Xmx2G -Xms2G"))
cmd = [f"{K}/bin/kafka-consumer-perf-test.sh", "--bootstrap-server", "localhost:9092", "--topic", topic, "--num-records", records,
       "--group", f"st-{label}-{os.getpid()}", "--timeout", "120000", "--hide-header", "--show-detailed-stats", "--reporting-interval", "1000",
       "--command-property", f"group.protocol={proto}"] + extra
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
# find the java pid (kafka-run-class execs java, so p.pid is usually it; fall back to scanning children)
def java_pid():
    try:
        if open(f"/proc/{p.pid}/comm").read().strip() == "java": return p.pid
    except FileNotFoundError: return None
    for d in os.listdir("/proc"):
        if d.isdigit():
            try:
                if open(f"/proc/{d}/comm").read().strip()=="java" and f"st-{label}-{os.getpid()}" in open(f"/proc/{d}/cmdline").read(): return int(d)
            except Exception: pass
    return None
samples = []  # (t, cpu_seconds)
HZ = os.sysconf("SC_CLK_TCK"); stop = False
def sampler():
    pid = None
    while not stop:
        pid = pid or java_pid()
        if pid:
            try:
                f = open(f"/proc/{pid}/stat").read().rsplit(")",1)[1].split()
                samples.append((time.time(), (int(f[11]) + int(f[12])) / HZ))
            except Exception: pass
        time.sleep(0.25)
threading.Thread(target=sampler, daemon=True).start()
reports = []  # (t, cumulative_MB)
t0 = time.time(); last = ""
for line in p.stdout:
    last = line.rstrip()
    m = re.match(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d:\d{3}), (\d+), ([\d.]+), ", last)
    if m:  # detailed line: time, threadId, data.consumed.in.MB (cumulative), MB.sec, ...
        reports.append((time.time(), float(m.group(3))))
p.wait(); stop = True; time.sleep(0.3)
def cpu_at(t):
    best = min(samples, key=lambda s: abs(s[0]-t)) if samples else (t, float('nan'))
    return best[1]
if len(reports) < 3: print(f"{label},{proto},{topic},ERROR,too few reports ({len(reports)}); last line: {last[:120]}"); sys.exit(1)
start = next((r for r in reports if r[0] - t0 >= warm), reports[len(reports)//2]); end = reports[-1]
dgb = (end[1] - start[1]) / 1024.0; dcpu = cpu_at(end[0]) - cpu_at(start[0]); dt = end[0] - start[0]
total_gb = end[1]/1024.0; total_cpu = samples[-1][1] if samples else float('nan')
print(f"{label},{proto},{topic},steady_window_s={dt:.1f},steady_mb_s={dgb*1024/dt:.1f},steady_cpu_s_per_gb={dcpu/dgb:.3f},"
      f"whole_run_gb={total_gb:.2f},whole_run_cpu_s={total_cpu:.2f},whole_run_cpu_s_per_gb={total_cpu/total_gb:.3f},warmup_cpu_s={cpu_at(start[0]):.2f}")
