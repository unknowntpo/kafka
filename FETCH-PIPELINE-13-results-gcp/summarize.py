#!/usr/bin/env python3
"""Summarize FETCH-PIPELINE-13 (socket buffers at -1). Usage: summarize.py <dir with client/ and broker/ unpacked>"""
import glob, os, re, statistics as st, sys
from collections import defaultdict

root = sys.argv[1]
C = os.path.join(root, 'client', 'fp-results')
B = os.path.join(root, 'broker', 'fp-results')

print('== throughput, MB/s (sum over consumers): median [each repetition]')
runs = defaultdict(list)
for f in sorted(glob.glob(C + '/*.done')):
    key, rep = re.match(r'(.*)\.rep(\d)\.done', os.path.basename(f)).groups()
    runs[key].append(float(open(f).read().split()[0]))
for k, v in runs.items():
    print(f'{k:22s} {st.median(v):7.0f}  {[round(x) for x in v]}')

print('\n== client threads: mean %CPU over samples where the thread was above 10%, per repetition')
for f in sorted(glob.glob(C + '/*.threads')):
    by = defaultdict(list); n = 0
    for line in open(f):
        if not line.startswith(' '):
            n += 1; continue
        p = line.split()
        by[' '.join(p[1:])].append(float(p[0]))
    top = sorted(by.items(), key=lambda kv: -sum(kv[1]))[:4]
    print(os.path.basename(f)[:-8], f'samples={n}', '; '.join(f'{k}: mean {sum(v)/len(v):.0f} max {max(v):.0f} n={len(v)}' for k, v in top))
