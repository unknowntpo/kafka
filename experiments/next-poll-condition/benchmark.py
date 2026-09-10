# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Bounded, randomized component A/A/B/C comparison. Run validate.py first."""
from pathlib import Path
import subprocess, json, random, hashlib, platform
root = Path(__file__).resolve().parents[2]
work = root/'work/next-poll-condition'
cp = (work/'classpath.txt').read_text()
java = '/opt/homebrew/opt/openjdk@17/bin/java'
out = work/'subscription-results'
out.mkdir(exist_ok=True)
cells = [(group, workload) for group in ['A1','A2','OLD','NEW'] for workload in ['QUIET','BURST','BUSY','SUCCESS']]
random.Random(20260907).shuffle(cells)
manifest = {'baseline':'820533b870106cc0e0ac60e2076b8644d68bd85f','order':cells,'platform':platform.platform(),
            'java':subprocess.run([java,'-version'],capture_output=True,text=True).stderr,
            'source_sha256':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [root/f for f in subprocess.check_output(['git','-C',str(root),'diff','--name-only'],text=True).splitlines() if (root/f).is_file()]}}
(out/'manifest.json').write_text(json.dumps(manifest,indent=2))
for group, workload in cells:
    classpath = str(work/'baseline-classes')+':'+cp if group.startswith('A') else cp
    if group == 'OLD': classpath = str(work/'previous-event-classes')+':'+cp
    cmd = [java,'-Xmx128m','-XX:ActiveProcessorCount=1','-cp',classpath,'org.openjdk.jmh.Main',
           'NextPollConditionBenchmark.pollPasses','-p','mode='+('SCHEDULED' if group in ('OLD','NEW') else 'FULL'),
           '-p','workload='+workload,'-f','2','-wi','2','-i','3','-w','300ms','-r','300ms','-t','1',
           '-jvmArgs','-Xms128m -Xmx128m -XX:ActiveProcessorCount=1','-prof','gc',
           '-rf','json','-rff',str(out/(group+'-'+workload+'.json'))]
    with (out/(group+'-'+workload+'.log')).open('w') as log:
        subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=60)
    result=json.loads((out/(group+'-'+workload+'.json')).read_text())
    if len(result)!=1: raise RuntimeError('Missing JMH measurement; inspect log')
    print(group,workload,round(result[0]['primaryMetric']['score'],3),'ns/pass',flush=True)
print('Done. Short component measurements; not end-to-end throughput or latency evidence.')
