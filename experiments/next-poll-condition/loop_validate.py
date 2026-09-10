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
import subprocess, shutil, json
root=Path(__file__).resolve().parents[2];w=root/'work/next-poll-condition';out=w/'loop-classes';out.mkdir(exist_ok=True)
cache=Path.home()/'.gradle/caches/modules-2/files-2.1';cp=(w/'classpath.txt').read_text();java='/opt/homebrew/opt/openjdk@17/bin/'
libs=[]
for group,artifact,version in [('org.mockito','mockito-core','5.3.1'),('net.bytebuddy','byte-buddy','1.14.4'),('net.bytebuddy','byte-buddy-agent','1.14.4'),('org.objenesis','objenesis','3.3')]:
 src=next((cache/group/artifact/version).rglob(artifact+'-'+version+'.jar'));dest=w/src.name
 if not dest.exists():shutil.copy2(src,dest)
 libs.append(str(dest))
cp=':'.join([str(out),cp]+libs+[str(p) for p in w.glob('junit-*.jar')]+[str(p) for p in w.glob('opentest4j-*.jar')]+[str(p) for p in w.glob('apiguardian-*.jar')])
# Subclass mock maker: setup-only mocks, no dynamic attach and no invocation recording in the measured loop.
ext=out/'mockito-extensions/org.mockito.plugins.MockMaker';ext.parent.mkdir(exist_ok=True);ext.write_text('mock-maker-subclass')
pkg='org/apache/kafka/clients/consumer/internals';base=root/'clients/src/main/java'
extra=[base/pkg/'events/CompletableEventReaper.java']+[base/pkg/'metrics'/(n+'.java') for n in ['AsyncConsumerMetrics','AbstractConsumerMetricsManager','MetricsLedger']]
subprocess.run([java+'javac','-J-Xmx256m','-cp',cp,'-d',str(out)]+list(map(str,extra)),check=True)
fixture=root/'clients/src/testFixtures/java'/pkg/'NetworkLoopFixture.java';s=fixture.read_text()
# Adapt only setup constructor calls to the cached RequestManagers / Processor jar signatures.
s=s.replace('super(new LogContext(), mock(ShareConsumeRequestManager.class),','super(new org.apache.kafka.common.utils.LogContext(), mock(ShareConsumeRequestManager.class),')
s=s.replace('super(new LogContext(), managers, mock(Metadata.class), null);','super(new org.apache.kafka.common.utils.LogContext(), managers, mock(Metadata.class), null);')
generated=w/'loop-src'/pkg/'NetworkLoopFixture.java';generated.parent.mkdir(parents=True,exist_ok=True);generated.write_text(s)
subprocess.run([java+'javac','-J-Xmx256m','-cp',cp,'-d',str(out),str(generated)],check=True)
(w/'loop-classpath.txt').write_text(cp)
# Full baseline loop body from pinned trunk, compiled separately against the same fixture boundaries.
old=w/'loop-baseline-src'/pkg/'ConsumerNetworkThread.java';old.parent.mkdir(parents=True,exist_ok=True)
old.write_text(subprocess.check_output(['git','-C',str(root),'show','820533b870106cc0e0ac60e2076b8644d68bd85f:clients/src/main/java/'+pkg+'/ConsumerNetworkThread.java'],text=True))
oldout=w/'loop-baseline-classes';oldout.mkdir(exist_ok=True)
subprocess.run([java+'javac','-J-Xmx256m','-cp',cp,'-d',str(oldout),str(old)],check=True)
print('Compiled real baseline/current loop; real source metrics and reaper; setup-only fixture constructor adaptations.')
test=root/'clients/src/test/java'/pkg/'NetworkLoopConditionTest.java'
runner=w/'LoopTestRunner.java'
runner.write_text('''public class LoopTestRunner {
 public static void main(String[] args) throws Exception {
  Class<?> c=Class.forName("org.apache.kafka.clients.consumer.internals.NetworkLoopConditionTest"); int n=0;
  for(java.lang.reflect.Method m:c.getDeclaredMethods()) if(m.isAnnotationPresent(org.junit.jupiter.api.Test.class)) {
   try {m.invoke(c.getConstructor().newInstance());} catch(java.lang.reflect.InvocationTargetException e) {throw new AssertionError(m.getName(),e.getCause());}
   System.out.println("PASS "+m.getName());n++;
  }
  if(n!=6) throw new AssertionError("Missing tests");
 }
}''')
subprocess.run([java+'javac','-cp',cp,'-d',str(out),str(test),str(runner)],check=True)
subprocess.run([java+'java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',cp,'LoopTestRunner'],check=True,timeout=45)
workload=root/'clients/src/testFixtures/java'/pkg/'NetworkLoopWorkload.java'
benchmark=root/'jmh-benchmarks/src/main/java'/pkg/'NetworkLoopConditionBenchmark.java'
subprocess.run([java+'javac','-cp',cp,'-d',str(out),str(workload),str(benchmark)],check=True)
print('Compiled multi-manager JMH workload with no mock calls in the measured region.')
probe=root/'experiments/next-poll-condition/LoopMetricsProbe.java'
subprocess.run([java+'javac','-cp',cp,'-d',str(out),str(probe)],check=True)
