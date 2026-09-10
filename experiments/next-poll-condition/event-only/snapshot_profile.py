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
r=Path(__file__).resolve().parents[3];p=r/'experiments/next-poll-condition/event-only';w=r/'work/next-poll-condition'
cp=(w/'event-only-classpath.txt').read_text();j='/opt/homebrew/opt/openjdk@17/bin/';out=r/'work/event-only-profiling';out.mkdir(exist_ok=True)
before=w/'event-only-before-classes';before.mkdir(exist_ok=True);gen=w/'snapshot-before-src';gen.mkdir(exist_ok=True)
(gen/'RequestManagerScheduler.java').write_text((p/'SnapshotBeforeScheduler.java.in').read_text())
subprocess.run([j+'javac','-cp',cp,'-d',str(before),str(gen/'RequestManagerScheduler.java')],check=True)
subprocess.run([j+'javac','-cp',cp,'-d',str(w/'event-only-classes'),str(p/'BusyProfile.java')],check=True)
for group in ['before','after']:
    classpath=str(before)+':'+cp if group=='before' else cp
    for mode,event in [('cpu','jdk.ExecutionSample'),('alloc','jdk.ObjectAllocationSample')]:
        stem=out/(group+'-'+mode)
        with stem.with_suffix('.log').open('w') as log:
            subprocess.run([j+'java','-Xms128m','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',classpath,
                'org.apache.kafka.clients.consumer.internals.BusyProfile',mode,str(stem.with_suffix('.jfr'))],
                stdout=log,stderr=subprocess.STDOUT,check=True,timeout=45)
        with stem.with_suffix('.json').open('w') as f:
            subprocess.run([j+'jfr','print','--json','--events',event,str(stem.with_suffix('.jfr'))],stdout=f,check=True)
        print(group,mode,'done',flush=True)
