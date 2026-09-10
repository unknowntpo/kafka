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
r=Path(__file__).resolve().parents[4];w=r/'work/next-poll-condition';out=w/'lifecycle-results';out.mkdir(exist_ok=True)
gate=json.loads((w/'lifecycle-benchmark-gate.json').read_text())
assert gate['equivalent'] and gate['benchmark_steps']==3000
for path,digest in gate['source_sha256'].items():
 assert hashlib.sha256((r/path).read_bytes()).hexdigest()==digest,'Changed source requires validation: '+path
paths=json.loads((w/'lifecycle-classpath.json').read_text())
cells=[(g,p) for g in ['A1','EVENT','A2'] for p in ['DORMANT','INFLIGHT','BUSY']]
random.Random(20260907).shuffle(cells)
(out/'manifest.json').write_text(json.dumps({'baseline':'820533b870106cc0e0ac60e2076b8644d68bd85f','gate':gate,'order':cells,'description':'Three real managers, fixed virtual clock; nonblocking transport; no Mockito calls in pass; setup uses Mockito agent. Not broker throughput or timer latency.'},indent=2))
for group,pattern in cells:
 c=paths['EVENT' if group=='EVENT' else 'SCAN'];name=group+'-'+pattern
 flags='-Xms128m -Xmx128m -XX:ActiveProcessorCount=1 -javaagent:'+str(w/'byte-buddy-agent-1.14.4.jar')
 cmd=['/opt/homebrew/opt/openjdk@17/bin/java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',c,'org.openjdk.jmh.Main','LifecycleBenchmark.networkPass','-p','pattern='+pattern,'-f','2','-wi','3','-i','5','-w','300ms','-r','300ms','-t','1','-jvmArgs',flags,'-prof','gc','-rf','json','-rff',str(out/(name+'.json'))]
 (out/(name+'-command.json')).write_text(json.dumps({'group':group,'pattern':pattern,'command':cmd},indent=2))
 with (out/(name+'.log')).open('w') as f:subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=120)
 rows=json.loads((out/(name+'.json')).read_text());assert len(rows)==1 and rows[0]['params']['pattern']==pattern
 x=rows[0];print(name,round(x['primaryMetric']['score'],2),'ns/pass',round(x['secondaryMetrics']['gc.alloc.rate.norm']['score'],2),'B/pass',flush=True)
