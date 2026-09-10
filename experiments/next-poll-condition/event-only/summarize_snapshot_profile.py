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
import json,collections,subprocess
r=Path(__file__).resolve().parents[3];out=r/'work/event-only-profiling';j='/opt/homebrew/opt/openjdk@17/bin/jfr';summary={}
for group in ['before','after']:
    summary[group]={}
    for kind,event in [('cpu','jdk.ExecutionSample'),('alloc','jdk.ObjectAllocationSample')]:
        path=out/(group+'-'+kind+'.json')
        if not path.exists():
            with path.open('w') as f:subprocess.run([j,'print','--json','--events',event,str(out/(group+'-'+kind+'.jfr'))],stdout=f,check=True)
        entries=json.loads(path.read_text())['recording']['events'];top=collections.Counter();types=collections.Counter();selected=0
        for e in entries:
            v=e['values'];frames=(v.get('stackTrace') or {}).get('frames',[])
            names=[f["method"]["type"]["name"]+'.'+f["method"]["name"] for f in frames]
            # Exclude profiler/ManagementFactory startup allocation samples.
            if not any('/UnifiedLoopWorkload' in n or '/RequestManagerScheduler' in n or '/NextPollCondition' in n for n in names):continue
            selected+=1;weight=v.get('weight',1)
            if names:top[names[0]]+=weight
            if kind=='alloc':types[v['objectClass']['name']]+=weight
        summary[group][kind]={'events':len(entries),'workload_events':selected,'top_frame':top.most_common(),
                              'allocation_class_weight_bytes':types.most_common()}
(out/'profile-summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
