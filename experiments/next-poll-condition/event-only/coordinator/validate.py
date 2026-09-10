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
import difflib
import json
import subprocess

root = Path(__file__).resolve().parents[4]
source_dir = Path(__file__).resolve().parent
work = root / 'work/next-poll-condition'
output = work / 'coordinator-dispatch-classes'
output.mkdir(exist_ok=True)
generated = work / 'coordinator-dispatch-src'
generated.mkdir(exist_ok=True)
paths = json.loads((work / 'reuse-classpath.json').read_text())
classpath = paths['AFTER'] + ':' + paths['common']
source = root / 'clients/src/main/java/org/apache/kafka/clients/consumer/internals/CoordinatorRequestManager.java'
before = source.read_text()
anchor = '    public Optional<Node> coordinator() {'
assert before.count(anchor) == 1
addition = """    /**
     * Experimental network-thread-only notification contract.
     * Capture before reading coordinator/fatal state; wake-up requires rechecking state.
     */
    NextPollCondition stateChanged() {
        return inputChanged.await();
    }

"""
after = before.replace(anchor, addition + anchor)
(generated / source.name).write_text(after)
(work / 'coordinator-notification-contract.patch').write_text(''.join(difflib.unified_diff(
    before.splitlines(True), after.splitlines(True), fromfile='a/' + str(source.relative_to(root)),
    tofile='b/' + str(source.relative_to(root)))))
java = '/opt/homebrew/opt/openjdk@17/bin/'
subprocess.run([java + 'javac', '-cp', classpath, '-d', str(output),
    str(generated / source.name), str(source_dir / 'CoordinatorDispatchProbe.java')], check=True)
result = subprocess.run([java + 'java', '-Xmx128m', '-XX:ActiveProcessorCount=1',
    '-cp', str(output) + ':' + classpath,
    'org.apache.kafka.clients.consumer.internals.CoordinatorDispatchProbe'],
    text=True, capture_output=True, timeout=45)
(work / 'coordinator-dispatch-validation.log').write_text(result.stdout + result.stderr)
print(result.stdout)
if result.returncode:
    print(result.stderr)
result.check_returncode()
