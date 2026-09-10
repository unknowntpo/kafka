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
r=Path(__file__).resolve().parents[4];p=Path(__file__).resolve().parent;w=r/'work/next-poll-condition';out=w/'duplex-classes';out.mkdir(exist_ok=True)
j='/opt/homebrew/opt/openjdk@17/bin/'
subprocess.run([j+'javac','--release','11','-J-Xmx128m','-d',str(out),str(p/'DuplexEngine.java'),str(p/'DuplexTest.java')],check=True)
result=subprocess.run([j+'java','-Xms32m','-Xmx128m','-XX:ActiveProcessorCount=2','-cp',str(out),'org.apache.kafka.clients.consumer.internals.duplex.DuplexTest'],capture_output=True,text=True,timeout=45)
(w/'duplex-validation.log').write_text(result.stdout+result.stderr)
print(result.stdout)
if result.returncode:print(result.stderr)
result.check_returncode()
