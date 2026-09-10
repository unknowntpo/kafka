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
import subprocess
r=Path(__file__).resolve().parents[3];p=r/'experiments/next-poll-condition/event-only';w=r/'work/next-poll-condition';cp=(w/'loop-classpath.txt').read_text();out=w/'event-only-classes';out.mkdir(exist_ok=True);scan=w/'event-only-scan-classes';scan.mkdir(exist_ok=True)
gen=w/'event-only-src';gen.mkdir(exist_ok=True)
(gen/'RequestManagerScheduler.java').write_text((p/'RequestManagerScheduler.java.in').read_text())
s=(r/'clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerNetworkThread.java').read_text()
s=s.replace('    private volatile long cachedMaximumTimeToWait = MAX_POLL_TIMEOUT_MS;','')
a=s.index('        long maxTimeToWaitMs = Long.MAX_VALUE;');b=s.index('        reapExpiredApplicationEvents',a);s=s[:a]+s[b:]
s=s.replace('return cachedMaximumTimeToWait;', 'throw new UnsupportedOperationException("Event-only application must wait on progress notification");')
s=s.replace('    public long maximumTimeToWait() {','    public ApplicationProgress applicationProgress() {\n        return requestManagerScheduler.applicationProgress();\n    }\n\n    public long maximumTimeToWait() {')
(gen/'ConsumerNetworkThread.java').write_text(s)
java='/opt/homebrew/opt/openjdk@17/bin/'
subprocess.run([java+'javac','-cp',cp,'-d',str(out),str(p/'ApplicationProgress.java'),str(gen/'RequestManagerScheduler.java'),str(gen/'ConsumerNetworkThread.java'),str(p/'UnifiedLoopWorkload.java'),str(p/'UnifiedLoopBenchmark.java'),str(p/'EventOnlyTest.java'),str(p/'UnifiedTrace.java')],check=True)
# Attribution control: same strict scheduler and progress publication, but original max-wait scan retained.
scanSrc=w/'event-only-scan-src';scanSrc.mkdir(exist_ok=True)
(scanSrc/'ConsumerNetworkThread.java').write_text((r/'clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerNetworkThread.java').read_text())
subprocess.run([java+'javac','-cp',str(out)+':'+cp,'-d',str(scan),str(scanSrc/'ConsumerNetworkThread.java')],check=True)
(w/'event-only-classpath.txt').write_text(str(out)+':'+cp)
subprocess.run([java+'java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.EventOnlyTest'],check=True,timeout=45)

# Compare every pass through its cumulative trace checksum and equal completed work.
traces=[]
for group in ['TRUNK', 'STRICT']:
    classpath=str(out)+':'+cp
    if group=='TRUNK':
        classpath=str(w/'loop-baseline-classes')+':'+str(w/'baseline-classes')+':'+classpath
    result=subprocess.check_output([java+'java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',classpath,
        'org.apache.kafka.clients.consumer.internals.UnifiedTrace',str(group=='STRICT').lower()],text=True,timeout=45)
    (w/('event-only-'+group+'-trace.csv')).write_text(result)
    rows=[line.split(',') for line in result.splitlines() if line.startswith('TRACE,')]
    if len(rows)!=4: raise AssertionError('Missing workload')
    traces.append(rows)
for baseline,candidate in zip(*traces):
    if baseline[:5]!=candidate[:5] or baseline[7]!=candidate[7]:
        raise AssertionError('Trace mismatch: '+repr((baseline,candidate)))
    if candidate[6]!='0': raise AssertionError('max wait scan')
print('PASS 40,000 per-pass cumulative checksums; equal input/output/network-pass counts; zero strict max-wait calls')
