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
import subprocess,json,random
r=Path(__file__).resolve().parents[4];w=r/'work/next-poll-condition';paths=json.loads((w/'reuse-classpath.json').read_text());out=w/'reuse-direct-reference';out.mkdir(exist_ok=True)
# Validate DIRECT separately: conditional=false still uses the oracle network scheduler.
cp=paths['ORACLE']+':'+paths['common']
result=subprocess.check_output(['/opt/homebrew/opt/openjdk@17/bin/java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',cp,'org.apache.kafka.clients.consumer.internals.ReuseTrace','false'],text=True,timeout=45)
(w/'reuse-DIRECT-trace.csv').write_text(result)
baseline=(w/'reuse-TRUNK-trace.csv').read_text().splitlines();candidate=result.splitlines()
if len(candidate)!=4:raise AssertionError('Missing direct trace')
for a,b in zip(baseline,candidate):
    x=a.split(',');y=b.split(',')
    if x[:5]!=y[:5] or x[7]!=y[7] or y[6]!='0':raise AssertionError('Direct control work mismatch')
order=['A1','A2','AFTER','ORACLE','DIRECT'];random.Random(20260911).shuffle(order)
(out/'manifest.json').write_text(json.dumps({'order':order,'pattern':'BUSY','managers':32,'reason':'Separate same-batch optimistic references: ORACLE retains condition construction; DIRECT removes unused signal/condition construction. Both keep oracle scheduler and application progress gate. Not deployable, not physical lower bounds.'},indent=2))
for group in order:
    if group in ('A1','A2'):prefix=str(w/'loop-baseline-classes')+':'+str(w/'baseline-classes')
    else:prefix=paths['ORACLE' if group=='DIRECT' else group]
    conditional={'A1':False,'A2':False,'AFTER':True,'ORACLE':True,'DIRECT':False}[group]
    cp=prefix+':'+paths['common'];flags='-Xms128m -Xmx128m -XX:ActiveProcessorCount=1 -Doracle.pattern=BUSY'
    cmd=['/opt/homebrew/opt/openjdk@17/bin/java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',cp,'org.openjdk.jmh.Main','UnifiedLoopBenchmark.networkPass','-p','conditional='+str(conditional).lower(),'-p','managers=32','-p','pattern=BUSY','-f','2','-wi','3','-i','5','-w','500ms','-r','500ms','-t','1','-jvmArgs',flags,'-prof','gc','-rf','json','-rff',str(out/(group+'.json'))]
    (out/(group+'-command.json')).write_text(json.dumps({'group':group,'conditional':conditional,'command':cmd},indent=2))
    with (out/(group+'.log')).open('w') as f:subprocess.run(cmd,stdout=f,stderr=subprocess.STDOUT,check=True,timeout=90)
    x=json.loads((out/(group+'.json')).read_text())
    if len(x)!=1 or x[0]['params']['conditional']!=str(conditional).lower():raise AssertionError('Wrong direct variant')
    print(group,round(x[0]['primaryMetric']['score'],2),round(x[0]['secondaryMetrics']['gc.alloc.rate.norm']['score'],2),flush=True)
