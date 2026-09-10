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
import subprocess, json, difflib
r=Path(__file__).resolve().parents[4]; p=Path(__file__).resolve().parent; w=r/'work/next-poll-condition'
g=w/'heartbeat-event-src';g.mkdir(exist_ok=True);out=w/'heartbeat-event-classes';out.mkdir(exist_ok=True)
paths=json.loads((w/'reuse-classpath.json').read_text());cp=str(w/'coordinator-dispatch-classes')+':'+paths['AFTER']+':'+paths['common']
b=r/'clients/src/main/java/org/apache/kafka/clients/consumer/internals';patch=[]
for name in ['AbstractHeartbeatRequestManager','ConsumerHeartbeatRequestManager','HeartbeatRequestState','MemberStateListener']:
 before=(b/(name+'.java')).read_text();s=before
 if name=='AbstractHeartbeatRequestManager':
  marker='    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {'
  replacement='''    public NetworkClientDelegate.PollResult poll(long currentTimeMs) {
        NextPollCondition inputs = NextPollCondition.anyOf(
                coordinatorRequestManager.stateChanged(), heartbeatInput.await());
        PollResult result = pollState(currentTimeMs);
        NextPollCondition condition = inputs;
        if (coordinatorRequestManager.coordinator().isPresent() && !membershipManager().shouldSkipHeartbeat()) {
            pollTimer.update(currentTimeMs);
            long delay = membershipManager().isLeavingGroup() ? Long.MAX_VALUE : pollTimer.remainingMs();
            if (!heartbeatRequestState.requestInFlight()) {
                heartbeatRequestState.canSendRequest(currentTimeMs); // Refresh the heartbeat clock.
                delay = Math.min(delay, heartbeatRequestState.timeToNextHeartbeatMs(currentTimeMs));
            }
            condition = NextPollCondition.anyOf(inputs, NextPollCondition.after(currentTimeMs, delay));
        }
        return new PollResult(Long.MAX_VALUE, result.unsentRequests, condition);
    }

    private final NextPollCondition.Signal heartbeatInput = new NextPollCondition.Signal();

    // Network-thread only. Application inputs must enter via the existing event queue.
    void heartbeatInputChanged() {
        heartbeatInput.publish();
    }

    private NetworkClientDelegate.PollResult pollState(long currentTimeMs) {'''
  assert s.count(marker)==1;s=s.replace(marker,replacement)
  s=s.replace('        pollTimer.reset(maxPollIntervalMs);','        pollTimer.reset(maxPollIntervalMs);\n        heartbeatInputChanged();')
  old='''                long completionTimeMs = request.handler().completionTimeMs();'''
  s=s.replace(old,'''                try {
                long completionTimeMs = request.handler().completionTimeMs();''')
  old='''                    onFailure(exception, completionTimeMs);
                }
            });'''
  assert old in s;s=s.replace(old,'''                    onFailure(exception, completionTimeMs);
                }
                } finally {
                    heartbeatInputChanged();
                }
            });''', 2)
 if name=='ConsumerHeartbeatRequestManager':
  # Install after concrete membership field initialization, in both constructors.
  old='        this.membershipManager = membershipManager;'
  s=s.replace(old,old+'''
        membershipManager.registerStateListener(new MemberStateListener() {
            @Override
            public void onMemberEpochUpdated(java.util.Optional<Integer> epoch, String memberId) {
                heartbeatInputChanged();
            }

            @Override
            public void onMemberStateChange(MemberState state) {
                heartbeatInputChanged();
            }
        });''', 2)
 (g/(name+'.java')).write_text(s)
 if s!=before:patch.extend(difflib.unified_diff(before.splitlines(True),s.splitlines(True),fromfile='a/'+str((b/(name+'.java')).relative_to(r)),tofile='b/'+str((b/(name+'.java')).relative_to(r))))
(w/'heartbeat-event-contract.patch').write_text(''.join(patch))
j='/opt/homebrew/opt/openjdk@17/bin/'
subprocess.run([j+'javac','-J-Xmx256m','-cp',cp,'-d',str(out)]+[str(f) for f in g.glob('*.java')]+[str(r/'clients/src/main/java/org/apache/kafka/common/internals/UnsupportedProtocolFieldException.java')],check=True)
(w/'heartbeat-event-classpath.txt').write_text(str(out)+':'+cp)

subprocess.run([j+'javac','-cp',str(out)+':'+cp,'-d',str(out),str(p/'HeartbeatEventProbe.java')],check=True)
result=subprocess.run([j+'java','-Xmx128m','-XX:ActiveProcessorCount=1','-cp',str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.HeartbeatEventProbe'],capture_output=True,text=True,timeout=45)
(w/'heartbeat-event-validation.log').write_text(result.stdout+result.stderr)
print(result.stdout)
if result.returncode: print(result.stderr)
result.check_returncode()
