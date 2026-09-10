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
import subprocess,json,random,hashlib
r=Path(__file__).resolve().parents[4];p=r/'experiments/next-poll-condition/event-only/reuse';w=r/'work/next-poll-condition';paths=json.loads((w/'reuse-classpath.json').read_text());out=w/'reuse-results';out.mkdir(exist_ok=True)
cells=[(g,pattern,32) for g in ['A1','A2','BEFORE','AFTER','ORACLE'] for pattern in ['IDLE','SPARSE','BUSY','TIMER']]+[('FLOOR','IDLE',0)]
random.Random(20260910).shuffle(cells)
manifest={'baseline':'820533b870106cc0e0ac60e2076b8644d68bd85f','order':cells,'source_sha256':{f.name:hashlib.sha256(f.read_bytes()).hexdigest()for f in p.iterdir()if f.is_file()},'description':'Reusable Waiting and Subscription vs frozen one-shot version; same snapshot candidate in both. Oracle uses known deterministic ready schedule, retains manager work/metrics/transport/progress; not a deployable scheduler or universal lower bound. FLOOR has no managers and is not an equal-work BUSY comparison.'}
(out/'manifest.json').write_text(json.dumps(manifest,indent=2))
for group,pattern,count in cells:
    baseline=group in ('A1','A2');conditional=not baseline
    if baseline:prefix=str(w/'loop-baseline-classes')+':'+str(w/'baseline-classes')
    else:prefix=paths['AFTER' if group=='FLOOR' else group]
    cp=prefix+':'+paths['common'];name=group+'-'+pattern
    flags='-Xms128m -Xmx128m -XX:ActiveProcessorCount=1 -Doracle.pattern='+pattern
    cmd=['/opt/homebrew/opt/openjdk@17/bin/java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',cp,'org.openjdk.jmh.Main','UnifiedLoopBenchmark.networkPass','-p','conditional='+str(conditional).lower(),'-p','managers='+str(count),'-p','pattern='+pattern,'-f','2','-wi','3','-i','5','-w','500ms','-r','500ms','-t','1','-jvmArgs',flags,'-prof','gc','-rf','json','-rff',str(out/(name+'.json'))]
    (out/(name+'-command.json')).write_text(json.dumps({'group':group,'conditional':conditional,'command':cmd},indent=2))
    with (out/(name+'.log')).open('w') as f:subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=90)
    x=json.loads((out/(name+'.json')).read_text())
    if len(x)!=1 or x[0]['params']['conditional']!=str(conditional).lower() or x[0]['params']['managers']!=str(count):raise AssertionError('Wrong variant: '+name)
    print(name,round(x[0]['primaryMetric']['score'],2),round(x[0]['secondaryMetrics']['gc.alloc.rate.norm']['score'],2),flush=True)
