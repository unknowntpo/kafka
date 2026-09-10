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

from pathlib import Path
import subprocess, random, json, hashlib
root=Path(__file__).resolve().parents[2];w=root/'work/next-poll-condition';cp=(w/'loop-classpath.txt').read_text();out=w/'loop-results';out.mkdir(exist_ok=True)
java='/opt/homebrew/opt/openjdk@17/bin/java'
cells=[(g,n,p) for g in ['A1','A2','EVENT'] for n in [8,32] for p in ['IDLE','SPARSE','BUSY','MIXED']]
random.Random(20260907).shuffle(cells)
(out/'manifest.json').write_text(json.dumps({'order':cells,'baseline':'820533b870106cc0e0ac60e2076b8644d68bd85f','description':'Actual runOnce, actual metrics/reaper; synthetic managers, nonblocking transport, constructor compatibility adapters; no mock calls in timed region'},indent=2))
for group,count,pattern in cells:
 classpath=cp if group=='EVENT' else str(w/'loop-baseline-classes')+':'+str(w/'baseline-classes')+':'+cp
 name=f'{group}-{count}-{pattern}'
 cmd=[java,'-Xmx128m','-XX:ActiveProcessorCount=1','-cp',classpath,'org.openjdk.jmh.Main','NetworkLoopConditionBenchmark.networkPass',
      '-p','conditional='+str(group=='EVENT').lower(),'-p','managers='+str(count),'-p','pattern='+pattern,
      '-f','2','-wi','2','-i','3','-w','300ms','-r','300ms','-t','1','-jvmArgs','-Xms128m -Xmx128m -XX:ActiveProcessorCount=1',
      '-prof','gc','-rf','json','-rff',str(out/(name+'.json'))]
 with (out/(name+'.log')).open('w') as f: subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=60)
 x=json.loads((out/(name+'.json')).read_text())
 if len(x)!=1: raise RuntimeError('Missing measurement: '+name)
 print(name,round(x[0]['primaryMetric']['score'],2),'ns/loop',flush=True)
for group in ['TRUNK','EVENT']:
 classpath=cp if group=='EVENT' else str(w/'loop-baseline-classes')+':'+str(w/'baseline-classes')+':'+cp
 with (out/(group+'-metrics.csv')).open('w') as f:
  subprocess.run([java,'-Xmx128m','-XX:ActiveProcessorCount=1','-cp',classpath,'org.apache.kafka.clients.consumer.internals.LoopMetricsProbe',str(group=='EVENT').lower()],stdout=f,stderr=subprocess.STDOUT,check=True,timeout=60)
 print(group,'CPU/allocation/latency probe completed',flush=True)
