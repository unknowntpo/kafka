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
import subprocess, json, hashlib
r=Path(__file__).resolve().parents[2];p=Path(__file__).resolve().parent
w=r/'work/consumer-v2';w.mkdir(exist_ok=True)
out=w/'bridge-classes';out.mkdir(exist_ok=True)
cp=(r/'work/next-poll-condition/membership-event-classpath.txt').read_text().strip()
j='/opt/homebrew/opt/openjdk@17/bin/'
sources=[p/n for n in ['PrefetchWindow.java','FetchBridge.java','FetchBridgeTest.java']]
subprocess.run([j+'javac','-J-Xmx256m','--release','11','-cp',cp,'-d',str(out)]+list(map(str,sources)),check=True,timeout=45)
result=subprocess.run([j+'java','-Xmx128m','-XX:ActiveProcessorCount=2','-cp',str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.FetchBridgeTest'],capture_output=True,text=True,timeout=45)
(w/'bridge-validation.log').write_text(result.stdout+result.stderr)
(w/'bridge-sources.json').write_text(json.dumps({str(s.relative_to(r)):hashlib.sha256(s.read_bytes()).hexdigest() for s in sources},indent=2))
print(result.stdout+result.stderr)
result.check_returncode()

negative=w/'bridge-negative';negative.mkdir(exist_ok=True)
original=(p/'FetchBridge.java').read_text()
needle='appGeneration = Math.incrementExact(appGeneration);'
assert original.count(needle)==1
source=negative/'FetchBridge.java'
source.write_text(original.replace(needle,'// intentionally missing app-side fence'))
subprocess.run([j+'javac','--release','11','-cp',str(out)+':'+cp,'-d',str(negative),str(source)],check=True,timeout=45)
control=subprocess.run([j+'java','-Xmx128m','-XX:ActiveProcessorCount=2','-cp',str(negative)+':'+str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.FetchBridgeTest'],capture_output=True,text=True,timeout=45)
(w/'bridge-negative.log').write_text(control.stdout+control.stderr)
assert control.returncode != 0 and 'App fence must precede owner applying seek' in control.stderr, control.stdout+control.stderr
print('PASS negative control: missing immediate app fence detected')
