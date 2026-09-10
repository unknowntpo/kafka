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
r=Path(__file__).resolve().parents[2];w=r/'work/next-poll-condition';out=r/'work/subscription-memory';java='/opt/homebrew/opt/openjdk@17/bin/java';jcmd='/opt/homebrew/opt/openjdk@17/bin/jcmd'
out.mkdir(parents=True,exist_ok=True)
subprocess.run(['/opt/homebrew/opt/openjdk@17/bin/javac','-cp',(w/'classpath.txt').read_text(),'-d',str(out/'classes'),str(r/'experiments/next-poll-condition/MemoryProbe.java')],check=True)
cp=str(out/'classes')+':'+(w/'classpath.txt').read_text()
flags=['-Xms128m','-Xmx128m','-XX:ActiveProcessorCount=1','-XX:CompileCommand=dontinline,org.apache.kafka.clients.consumer.internals.NextPollConditionBenchmark::pollPass']
with (out/'retention-stderr.log').open('w') as err:
 p=subprocess.Popen([java]+flags+['-cp',cp,'org.apache.kafka.clients.consumer.internals.MemoryProbe'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,text=True)
 try:
  for phase in ['initial','after100k','after500k','closed']:
   while True:
    line=p.stdout.readline()
    if not line: raise RuntimeError('Probe ended early')
    if line.startswith(phase+' '): break
   print(line.strip(),flush=True)
   result=subprocess.run([jcmd,str(p.pid),'GC.class_histogram'],capture_output=True,text=True,check=True,timeout=30)
   (out/(phase+'-histogram.txt')).write_text(result.stdout)
   p.stdin.write('\n');p.stdin.flush()
  p.wait(timeout=30)
  if p.returncode: raise RuntimeError('Probe failure')
 finally:
  if p.poll() is None: p.kill();p.wait()
for group in ['trunk','old','new']:
 group_cp=str(w/'baseline-classes')+':'+cp if group=='trunk' else cp
 if group=='old': group_cp=str(w/'previous-event-classes')+':'+cp
 with (out/(group+'-allocation.txt')).open('w') as f:
  subprocess.run([java]+flags+['-cp',group_cp,'org.apache.kafka.clients.consumer.internals.MemoryProbe','FULL' if group=='trunk' else 'SCHEDULED'],stdout=f,stderr=subprocess.STDOUT,check=True,timeout=45)
 print(group,'allocation complete',flush=True)
(out/'manifest.json').write_text(json.dumps({'scheduler':'optimized subscriptions with unchanged TreeSet dispatch','sha256':hashlib.sha256((r/'clients/src/main/java/org/apache/kafka/clients/consumer/internals/RequestManagerScheduler.java').read_bytes()).hexdigest(),'owners':1000,'events':500000,'heap':'128 MB','retention':'GC.class_histogram on only the spawned test JVM at four acknowledged checkpoints','allocation':'ThreadMXBean allocated-byte counters, 2048000 warmup passes/workload, 3 samples of 512000 passes; no JFR'},indent=2))
