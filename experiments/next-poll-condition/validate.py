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
import subprocess, shutil, hashlib, json
root = Path(__file__).resolve().parents[2]
work = root / 'work/next-poll-condition'
work.mkdir(parents=True, exist_ok=True)
cache = Path.home() / '.gradle/caches/modules-2/files-2.1'
libs = []
for group, artifact, version in [('org.apache.kafka','kafka-clients','4.3.1'),('org.slf4j','slf4j-api','2.0.17'),('org.openjdk.jmh','jmh-core','1.37'),('org.openjdk.jmh','jmh-generator-annprocess','1.37'),('net.sf.jopt-simple','jopt-simple','5.0.4'),('org.apache.commons','commons-math3','3.6.1')]:
    source = next((cache/group/artifact/version).rglob(artifact+'-'+version+'.jar'))
    target = work / source.name
    if not target.exists(): shutil.copy2(source,target)
    libs.append(str(target))
(work/'dependencies.json').write_text(json.dumps({p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in libs},indent=2))
cp = ':'.join(libs)
base = root/'clients/src/main/java'
pkg = 'org/apache/kafka/clients/consumer/internals'
sources = [base/pkg/(n+'.java') for n in ['NextPollCondition','RequestManagerScheduler','CoordinatorRequestManager','NetworkClientDelegate','ConsumerNetworkThread','RequestManager','RequestState','GroupCoordinatorNode']]
sources += [base/'org/apache/kafka/common/Node.java', base/'org/apache/kafka/common/errors/BootstrapResolutionException.java', base/'org/apache/kafka/common/annotation/InterfaceAudience.java']
sources += list((base/'org/apache/kafka/common/utils/internals').glob('*.java'))
# Cached dependency jar predates the renamed client factory. Exclude only that unused
# factory body in a generated copy; production sources remain untouched.
network = base/pkg/'NetworkClientDelegate.java'
def fixture(text):
    start = text.index('                KafkaClient client = ClientUtils.createNetworkClient(')
    end = text.index('\n            }', start)
    return text[:start] + '                throw new UnsupportedOperationException("Factory excluded from component harness");' + text[end:]
generated = work/'src'/pkg/'NetworkClientDelegate.java'
generated.parent.mkdir(parents=True, exist_ok=True)
generated.write_text(fixture(network.read_text()))
sources[sources.index(network)] = generated
out = work/'classes'
out.mkdir(exist_ok=True)
subprocess.run(['/opt/homebrew/opt/openjdk@17/bin/javac','-J-Xmx256m','-J-XX:ActiveProcessorCount=2','-cp',cp,'-d',str(out)]+list(map(str,sources)),check=True)
(work/'classpath.txt').write_text(str(out)+':'+cp)
print('Compiled changed classes with only the unused network-client factory excluded; not a full Gradle build.')
# Build the exact original trunk coordinator and PollResult in a separate output directory.
baseline = '820533b870106cc0e0ac60e2076b8644d68bd85f'
base_sources = []
for name in ['CoordinatorRequestManager', 'NetworkClientDelegate']:
    rel = 'clients/src/main/java/'+pkg+'/'+name+'.java'
    text = subprocess.check_output(['git','-C',str(root),'show',baseline+':'+rel],text=True)
    if name == 'NetworkClientDelegate': text = fixture(text)
    dest = work/'baseline-src'/pkg/(name+'.java')
    dest.parent.mkdir(parents=True,exist_ok=True)
    dest.write_text(text)
    base_sources.append(str(dest))
baseline_out = work/'baseline-classes'
baseline_out.mkdir(exist_ok=True)
java = '/opt/homebrew/opt/openjdk@17/bin/'
subprocess.run([java+'javac','-J-Xmx256m','-cp',str(out)+':'+cp,'-d',str(baseline_out)]+base_sources,check=True)
# Run the committed JUnit tests through a tiny launcher, without a Gradle daemon.
test_libs = []
for group,artifact,version in [('org.junit.jupiter','junit-jupiter-api','5.14.4'),('org.junit.platform','junit-platform-commons','1.14.4'),('org.opentest4j','opentest4j','1.3.0'),('org.apiguardian','apiguardian-api','1.1.2')]:
    src = next((cache/group/artifact/version).rglob(artifact+'-'+version+'.jar'))
    dst = work/src.name
    if not dst.exists(): shutil.copy2(src,dst)
    test_libs.append(str(dst))
test_cp = str(out)+':'+cp+':'+':'.join(test_libs)
runner = work/'TestRunner.java'
runner.write_text('''public class TestRunner {
 public static void main(String[] args) throws Exception {
  Class<?> c = Class.forName("org.apache.kafka.clients.consumer.internals.NextPollConditionTest");
  int count = 0;
  for (java.lang.reflect.Method m : c.getDeclaredMethods()) {
   if (m.isAnnotationPresent(org.junit.jupiter.api.Test.class)) {
    try { m.invoke(c.getConstructor().newInstance()); }
    catch (java.lang.reflect.InvocationTargetException e) { throw new AssertionError(m.getName(), e.getCause()); }
    System.out.println("PASS " + m.getName()); count++;
   }
  }
  if (count != 17) throw new AssertionError("Missing tests");
 }
}''')
subprocess.run([java+'javac','-cp',test_cp,'-d',str(out),str(root/'clients/src/test/java'/pkg/'NextPollConditionTest.java'),str(runner)],check=True)
subprocess.run([java+'java','-Xmx128m','-cp',test_cp,'TestRunner'],check=True)
bench = root/'jmh-benchmarks/src/main/java/org/apache/kafka/clients/consumer/internals/NextPollConditionBenchmark.java'
subprocess.run([java+'javac','-cp',str(out)+':'+cp,'-d',str(out),str(bench)],check=True)
print('PASS baseline compiled, 17 tests passed, JMH generated.')
probe = root/'experiments/next-poll-condition/TraceProbe.java'
subprocess.run([java+'javac','-cp',str(out)+':'+cp,'-d',str(out),str(probe)],check=True)
traces = []
for name, classpath, args in [('trunk',str(baseline_out)+':'+str(out)+':'+cp,[]),('condition',str(out)+':'+cp,[]),('scheduled',str(out)+':'+cp,['scheduled'])]:
    result = subprocess.run([java+'java','-Xmx128m','-cp',classpath,'org.apache.kafka.clients.consumer.internals.TraceProbe']+args,capture_output=True,text=True,check=True)
    (work/(name+'-trace.txt')).write_text(result.stdout)
    traces.append(result.stdout)
assert traces[0] == traces[1] == traces[2], 'Request/timing trace divergence'
print('PASS 16000 per-pass request count, wait, coordinator and fatal-state observations across A/B/C; sha256='+hashlib.sha256(traces[0].encode()).hexdigest())
# Previous scan-based scheduler as a control, with only a shared driver entry point added.
scan_source = root/'experiments/next-poll-condition/ScanScheduler.java.in'
scan_dest = work/'scan-src'/pkg/'RequestManagerScheduler.java'
scan_dest.parent.mkdir(parents=True, exist_ok=True)
scan_dest.write_text(scan_source.read_text())
scan_out = work/'scan-classes'
scan_out.mkdir(exist_ok=True)
subprocess.run([java+'javac','-cp',str(out)+':'+cp,'-d',str(scan_out),str(scan_dest)],check=True)

scan_trace = subprocess.run([java+'java','-Xmx128m','-cp',str(scan_out)+':'+str(out)+':'+cp,
    'org.apache.kafka.clients.consumer.internals.TraceProbe','scheduled'],capture_output=True,text=True,check=True)
assert scan_trace.stdout == traces[0], 'Scan-control trace divergence'
(work/'scan-trace.txt').write_text(scan_trace.stdout)
print('PASS scan control also matches all 16000 observations.')

previous_sources = []
for name in ['RequestManagerScheduler', 'NextPollCondition']:
    dest = work/'previous-event-src'/pkg/(name+'.java')
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text((root/'experiments/next-poll-condition'/('Event'+name+'.java.in')).read_text())
    previous_sources.append(str(dest))
previous_out = work/'previous-event-classes'
previous_out.mkdir(exist_ok=True)
subprocess.run([java+'javac','-cp',str(out)+':'+cp,'-d',str(previous_out)]+previous_sources,check=True)
previous_trace = subprocess.run([java+'java','-Xmx128m','-cp',str(previous_out)+':'+str(out)+':'+cp,
    'org.apache.kafka.clients.consumer.internals.TraceProbe','scheduled'],capture_output=True,text=True,check=True)
assert previous_trace.stdout == traces[0], 'Previous event trace divergence'
print('PASS previous event implementation trace parity.')
