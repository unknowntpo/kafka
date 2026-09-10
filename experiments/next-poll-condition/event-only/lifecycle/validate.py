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
import subprocess,json,hashlib
r=Path(__file__).resolve().parents[4];p=Path(__file__).resolve().parent;w=r/'work/next-poll-condition';j='/opt/homebrew/opt/openjdk@17/bin/'
cp=(w/'membership-event-classpath.txt').read_text();g=w/'lifecycle-src';g.mkdir(exist_ok=True);out=w/'lifecycle-classes';out.mkdir(exist_ok=True)
base=w/'lifecycle-baseline-classes';base.mkdir(exist_ok=True);bg=w/'lifecycle-baseline-src';bg.mkdir(exist_ok=True)
pkg='clients/src/main/java/org/apache/kafka/clients/consumer/internals/'
for n in ['CoordinatorRequestManager','AbstractHeartbeatRequestManager','ConsumerHeartbeatRequestManager','AbstractMembershipManager']:
 (bg/(n+'.java')).write_text(subprocess.check_output(['git','show','820533b870106cc0e0ac60e2076b8644d68bd85f:'+pkg+n+'.java'],text=True))
subprocess.run([j+'javac','-J-Xmx256m','-cp',cp,'-d',str(base)]+[str(f) for f in bg.glob('*.java')],check=True)
s=(r/'experiments/next-poll-condition/event-only/membership/MembershipEventProbe.java').read_text()
s=s.replace('        int heartbeatPolls;', '        OffsetsRequestManager offsetsBoundary; FetchRequestManager fetchBoundary;\n        int heartbeatPolls;')
s=s.replace('FetchRequestManager fetch=mock(FetchRequestManager.class);','FetchRequestManager fetch=mock(FetchRequestManager.class); offsetsBoundary=offsets;fetchBoundary=fetch;')
s=s.replace('mock(RebalanceMetricsManager.class),false','new ConsumerRebalanceMetricsManager(metrics,subscriptions),false')
s=s.replace('public List<RequestManager> entries() { return Arrays.asList(coordinator,counted,membership); }','private final List<RequestManager> scheduled = Arrays.asList(coordinator,counted,membership); public List<RequestManager> entries() { return scheduled; }')
s=s.replace('throw new AssertionError("No maximumTimeToWait scan");','return heartbeat.maximumTimeToWait(now);')
s=s.replace('if(processingFailure!=null)throw new AssertionError("Application processor failure",processingFailure);','if(processingFailure!=null)throw new AssertionError("Application processor failure",processingFailure);LifecycleTrace.record(this,now);')
(g/'MembershipEventProbe.java').write_text(s)
subprocess.run([j+'javac','-cp',cp,'-d',str(out),str(g/'MembershipEventProbe.java'),str(p/'LifecycleTrace.java'),str(p/'LifecycleWorkload.java'),str(p/'LifecycleBenchmark.java')],check=True)
paths={'EVENT':str(out)+':'+cp,'SCAN':str(base)+':'+str(w/'loop-baseline-classes')+':'+str(out)+':'+cp}
(w/'lifecycle-classpath.json').write_text(json.dumps(paths,indent=2))
for group,c in paths.items():
 result=subprocess.run([j+'java','-javaagent:'+str(w/'byte-buddy-agent-1.14.4.jar'),'-Xmx128m','-XX:ActiveProcessorCount=1','-cp',c,'org.apache.kafka.clients.consumer.internals.LifecycleTrace'],capture_output=True,text=True,timeout=45)
 (w/('lifecycle-'+group+'.log')).write_text(result.stdout+result.stderr)
 print(group,'exit',result.returncode,flush=True)
 if result.returncode: print(result.stderr)
 result.check_returncode()
a=(w/'lifecycle-SCAN.log').read_text().splitlines();b=(w/'lifecycle-EVENT.log').read_text().splitlines()
a=[x for x in a if x.startswith('TRACE|')];b=[x for x in b if x.startswith('TRACE|')]
assert a and b
mismatch=[(i,x,y) for i,(x,y) in enumerate(zip(a,b)) if x!=y]
(w/'lifecycle-comparison.json').write_text(json.dumps({'scan_steps':len(a),'event_steps':len(b),'mismatches':mismatch},indent=2))
print('steps',len(a),len(b),'mismatches',len(mismatch))
for row in mismatch[:5]: print(row)
assert len(a)==len(b) and not mismatch,'Trace differs; benchmark gate CLOSED'
print('PASS identical per-step request/state trace; benchmark gate OPEN for covered traces only')

benchtraces=[]
for group,c in paths.items():
 result=subprocess.run([j+'java','-javaagent:'+str(w/'byte-buddy-agent-1.14.4.jar'),'-Xmx128m','-XX:ActiveProcessorCount=1','-cp',c,'org.apache.kafka.clients.consumer.internals.LifecycleWorkload'],capture_output=True,text=True,timeout=45)
 (w/('lifecycle-bench-'+group+'.log')).write_text(result.stdout+result.stderr)
 if result.returncode:print(result.stderr)
 result.check_returncode()
 benchtraces.append([x for x in result.stdout.splitlines() if x.startswith('BENCHTRACE|')])
assert len(benchtraces[0])==3000 and benchtraces[0]==benchtraces[1],'Benchmark workload trace mismatch'
(w/'lifecycle-benchmark-gate.json').write_text(json.dumps({'lifecycle_steps':len(a),'benchmark_steps':3000,'no_mock_calls_during_pass':True,'equivalent':True,'source_sha256':{str(f.relative_to(r)):hashlib.sha256(f.read_bytes()).hexdigest() for f in list(p.glob('*.java'))+[p/'validate.py',r/'experiments/next-poll-condition/event-only/membership/MembershipEventProbe.java']}}))
print('PASS 3000 benchmark steps match; no Mockito boundary calls in measured path')
