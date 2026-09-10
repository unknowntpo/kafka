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
import subprocess,json
root=Path(__file__).resolve().parents[3];w=root/'work/next-poll-condition';cp=(w/'event-only-classpath.txt').read_text();out=w/'snapshot-busy-confirm';out.mkdir(exist_ok=True)
order=[('BEFORE1','BEFORE'),('AFTER1','AFTER'),('AFTER2','AFTER'),('BEFORE2','BEFORE')]
(out/'manifest.json').write_text(json.dumps({'order':order,'reason':'Broad before-BUSY confidence interval in corrected main comparison; focused ABBA follow-up, all conditional=true','baseline':'820533b870106cc0e0ac60e2076b8644d68bd85f'},indent=2))
for name,group in order:
    c=str(w/'event-only-before-classes')+':'+cp if group=='BEFORE' else cp
    cmd=['/opt/homebrew/opt/openjdk@17/bin/java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',c,'org.openjdk.jmh.Main','UnifiedLoopBenchmark.networkPass','-p','conditional=true','-p','managers=32','-p','pattern=BUSY','-f','2','-wi','3','-i','5','-w','750ms','-r','750ms','-t','1','-jvmArgs','-Xms128m -Xmx128m -XX:ActiveProcessorCount=1','-prof','gc','-rf','json','-rff',str(out/(name+'.json'))]
    (out/(name+'-command.json')).write_text(json.dumps({'group':group,'command':cmd},indent=2))
    with (out/(name+'.log')).open('w') as f:subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=90)
    x=json.loads((out/(name+'.json')).read_text())
    if len(x)!=1 or x[0]['params']['conditional']!='true':raise AssertionError(name)
    print(name,round(x[0]['primaryMetric']['score'],2),round(x[0]['primaryMetric']['scoreError'],2),flush=True)
