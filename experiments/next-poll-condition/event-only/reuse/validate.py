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
r=Path(__file__).resolve().parents[4];p=r/'experiments/next-poll-condition/event-only/reuse';w=r/'work/next-poll-condition';cp=(w/'event-only-classpath.txt').read_text();j='/opt/homebrew/opt/openjdk@17/bin/';gen=w/'reuse-src';gen.mkdir(exist_ok=True)
paths={}
for group,scheduler,condition in [('BEFORE','BeforeScheduler.java.in','BeforeCondition.java.in'),('AFTER','RequestManagerScheduler.java.in','NextPollCondition.java.in'),('ORACLE','OracleScheduler.java.in','BeforeCondition.java.in')]:
 out=w/('reuse-'+group.lower()+'-classes');out.mkdir(exist_ok=True);paths[group]=str(out)
 (gen/'RequestManagerScheduler.java').write_text((p/scheduler).read_text());(gen/'NextPollCondition.java').write_text((p/condition).read_text())
 subprocess.run([j+'javac','-cp',cp,'-d',str(out),str(gen/'RequestManagerScheduler.java'),str(gen/'NextPollCondition.java')],check=True)
harness=w/'reuse-harness-classes';harness.mkdir(exist_ok=True)
subprocess.run([j+'javac','-cp',paths['AFTER']+':'+cp,'-d',str(harness),str(p/'ReuseTest.java'),str(p/'ReuseTrace.java')],check=True)
cp=str(harness)+':'+cp;(w/'reuse-classpath.json').write_text(json.dumps({'common':cp,**paths},indent=2))
subprocess.run([j+'java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',paths['AFTER']+':'+cp,'org.apache.kafka.clients.consumer.internals.ReuseTest'],check=True,timeout=45)
traces=[]
for group in ['TRUNK','BEFORE','AFTER','ORACLE']:
 c=(str(w/'loop-baseline-classes')+':'+str(w/'baseline-classes') if group=='TRUNK' else paths[group])+':'+cp
 result=subprocess.check_output([j+'java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',c,'org.apache.kafka.clients.consumer.internals.ReuseTrace',str(group!='TRUNK').lower()],text=True,timeout=45)
 (w/('reuse-'+group+'-trace.csv')).write_text(result)
 rows=[line.split(',') for line in result.splitlines() if line.startswith('TRACE,')]
 if len(rows)!=4:raise AssertionError('Missing workload')
 traces.append(rows)
for rows in traces[1:]:
 for a,b in zip(traces[0],rows):
  if a[:5]!=b[:5] or a[7]!=b[7] or b[6]!='0':raise AssertionError('Trace mismatch: '+repr((a,b)))
print('PASS 40,000 cumulative trace steps agree for trunk/before/reuse/oracle; zero event max-wait calls')
