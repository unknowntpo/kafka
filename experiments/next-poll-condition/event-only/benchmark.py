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
import subprocess,random,json,hashlib
root=Path(__file__).resolve().parents[3];w=root/'work/next-poll-condition';cp=(w/'event-only-classpath.txt').read_text();out=w/'event-only-results';out.mkdir(exist_ok=True)
cells=[(g,p) for g in ['A1','A2','STRICT_SCAN','STRICT'] for p in ['IDLE','SPARSE','BUSY','TIMER']];random.Random(20260908).shuffle(cells)
(out/'manifest.json').write_text(json.dumps({'baseline':'820533b870106cc0e0ac60e2076b8644d68bd85f','order':cells,'managers':32,'description':'Synthetic all-condition managers; actual generated runOnce; strict progress gate, no max-wait scan; nonblocking transport. STRICT_SCAN isolates max-wait scan. Application consumer not migrated.'},indent=2))
for group,pattern in cells:
 c=cp
 if group.startswith('A'): c=str(w/'loop-baseline-classes')+':'+str(w/'baseline-classes')+':'+cp
 elif group=='STRICT_SCAN':c=str(w/'event-only-scan-classes')+':'+cp
 name=f'{group}-{pattern}'
 cmd=['/opt/homebrew/opt/openjdk@17/bin/java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',c,'org.openjdk.jmh.Main','UnifiedLoopBenchmark.networkPass','-p','conditional='+str(not group.startswith('A')).lower(),'-p','managers=32','-p','pattern='+pattern,'-f','2','-wi','3','-i','5','-w','500ms','-r','500ms','-t','1','-jvmArgs','-Xms128m -Xmx128m -XX:ActiveProcessorCount=1','-prof','gc','-rf','json','-rff',str(out/(name+'.json'))]
 with (out/(name+'.log')).open('w') as f:subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=90)
 x=json.loads((out/(name+'.json')).read_text())
 if len(x)!=1:raise AssertionError(name)
 print(name,round(x[0]['primaryMetric']['score'],2),flush=True)
