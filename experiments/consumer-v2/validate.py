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
import subprocess, hashlib, json
r=Path(__file__).resolve().parents[2]
p=Path(__file__).resolve().parent
w=r/'work/consumer-v2';w.mkdir(exist_ok=True)
out=w/'classes';out.mkdir(exist_ok=True)
j='/opt/homebrew/opt/openjdk@17/bin/'
cp=(r/'work/next-poll-condition/membership-event-classpath.txt').read_text().strip()
b=r/'clients/src/main/java/org/apache/kafka/clients/consumer/internals'
original=b/'CompletedFetch.java'
generated=w/'CompletedFetch.java'
text=original.read_text()
old='import org.apache.kafka.common.utils.internals.BufferSupplier;'
assert text.count(old)==1
text=text.replace(old,'import org.apache.kafka.common.utils.BufferSupplier;')
old='import org.apache.kafka.common.utils.internals.CloseableIterator;'
assert text.count(old)==1
generated.write_text(text.replace(old,'import org.apache.kafka.common.utils.CloseableIterator;'))
sources=[generated,p/'PrefetchWindow.java',p/'PrefetchWindowTest.java']
subprocess.run([j+'javac','-J-Xmx256m','--release','11','-cp',cp,'-d',str(out)]+list(map(str,sources)),check=True,timeout=45)
result=subprocess.run([j+'java','-Xmx128m','-XX:ActiveProcessorCount=2','-cp',str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.PrefetchWindowTest'],capture_output=True,text=True,timeout=45)
(w/'validation.log').write_text(result.stdout+result.stderr)
(w/'sources.json').write_text(json.dumps({str(s.relative_to(r)):hashlib.sha256(s.read_bytes()).hexdigest() for s in sources},indent=2))
print(result.stdout+result.stderr)
result.check_returncode()
# Negative control: an implementation that forgets the seek generation must fail.
negative=w/'negative';negative.mkdir(exist_ok=True)
source=(p/'PrefetchWindow.java').read_text()
needle='generation = Math.incrementExact(generation);'
assert source.count(needle)==1
broken=negative/'PrefetchWindow.java';broken.write_text(source.replace(needle,'// intentionally missing seek fence'))
subprocess.run([j+'javac','--release','11','-cp',str(out)+':'+cp,'-d',str(negative),str(broken)],check=True,timeout=45)
control=subprocess.run([j+'java','-Xmx128m','-XX:ActiveProcessorCount=2','-cp',str(negative)+':'+str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.PrefetchWindowTest'],capture_output=True,text=True,timeout=45)
(w/'negative.log').write_text(control.stdout+control.stderr)
assert control.returncode != 0 and 'seek fences but keeps bytes charged' in control.stderr, control.stdout+control.stderr
print('PASS negative control: missing seek generation detected')
