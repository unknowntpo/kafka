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
import subprocess, json, hashlib, sys
r=Path(__file__).resolve().parents[2];p=Path(__file__).resolve().parent
w=r/'work/consumer-v2';w.mkdir(exist_ok=True)
out=w/'network-classes';out.mkdir(exist_ok=True)
# Use the actual cached Kafka binary, not generated manager adapters, for this wire probe.
lib=r/'work/next-poll-condition'
cp=':'.join(str(lib/n) for n in ['kafka-clients-4.3.1.jar','slf4j-api-2.0.17.jar'])
j='/opt/homebrew/opt/openjdk@17/bin/'
sources=[p/n for n in ['PrefetchWindow.java','FetchBridge.java','NetworkFetchTransport.java','NetworkFetchTest.java','AppFetchDecoder.java']]
subprocess.run([j+'javac','-J-Xmx256m','--release','11','-cp',cp,'-d',str(out)]+list(map(str,sources)),check=True,timeout=45)
if '--compile-only' in sys.argv: sys.exit(0)
result=subprocess.run([j+'java','-Xmx128m','-XX:ActiveProcessorCount=2','-cp',str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.NetworkFetchTest'],capture_output=True,text=True,timeout=45)
(w/'network-validation.log').write_text(result.stdout+result.stderr)
(w/'network-sources.json').write_text(json.dumps({str(s.relative_to(r)):hashlib.sha256(s.read_bytes()).hexdigest() for s in sources},indent=2))
print(result.stdout+result.stderr)
result.check_returncode()

# Regression control: dropping aborted metadata must make app-visible data incorrect.
negative=w/'network-negative';negative.mkdir(exist_ok=True)
original=(p/'NetworkFetchTransport.java').read_text()
needle='completion.accept(data);'
assert original.count(needle)==1
source=negative/'NetworkFetchTransport.java'
source.write_text(original.replace(needle,'completion.accept(data.setAbortedTransactions(null));'))
subprocess.run([j+'javac','--release','11','-cp',str(out)+':'+cp,'-d',str(negative),str(source)],check=True,timeout=45)
control=subprocess.run([j+'java','-Xmx128m','-XX:ActiveProcessorCount=2','-cp',str(negative)+':'+str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.NetworkFetchTest','envelope-only'],capture_output=True,text=True,timeout=45)
(w/'envelope-negative.log').write_text(control.stdout+control.stderr)
assert control.returncode != 0 and 'Aborted transaction metadata lost on wire' in control.stderr, control.stdout+control.stderr
print('PASS negative control: lost aborted transaction metadata detected over TCP')
