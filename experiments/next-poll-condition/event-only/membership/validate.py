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
import subprocess, difflib
r=Path(__file__).resolve().parents[4];p=Path(__file__).resolve().parent;w=r/'work/next-poll-condition';j='/opt/homebrew/opt/openjdk@17/bin/'
cp=(w/'heartbeat-event-classpath.txt').read_text();out=w/'membership-event-classes';out.mkdir(exist_ok=True)
b=r/'clients/src/main/java/org/apache/kafka/clients/consumer/internals'
names=['AbstractMembershipManager','ConsumerMembershipManager','events/ApplicationEventProcessor','events/AsyncPollEvent','events/TopicSubscriptionChangeEvent','events/SubscriptionChangeEvent','SubscriptionState','DelegatingRebalanceConsumer','../RebalanceConsumer','../RebalanceListener']
g=w/'membership-event-src';g.mkdir(exist_ok=True)
s=(b/'events/ApplicationEventProcessor.java').read_text()
old='            CompletableFuture<Void> future = requestManagers.streamsMembershipManager.get().leaveGroupOnClose(event.membershipOperation());\n            future.whenComplete(complete(event.future()));'
assert old in s
s=s.replace(old, '            throw new UnsupportedOperationException("Streams close excluded from consumer experiment");')
old='            event.future().complete(requestManagers.offsetsRequestManager.currentLag(\n                event.partition(),\n                event.isolationLevel()\n            ));'
assert old in s
s=s.replace(old,'            throw new UnsupportedOperationException("CurrentLag excluded from consumer experiment");')
(g/'ApplicationEventProcessor.java').write_text(s)
names.remove('events/ApplicationEventProcessor')
before=(b/'AbstractMembershipManager.java').read_text()
s=before.replace('    protected void transitionTo(MemberState nextState) {', '    private final NextPollCondition.Signal membershipInput = new NextPollCondition.Signal();\n\n    protected void transitionTo(MemberState nextState) {')
s=s.replace('        this.autoCommitEnabled = autoCommitEnabled;', '        this.autoCommitEnabled = autoCommitEnabled;\n        metadata.addClusterUpdateListener(resource -> membershipInput.publish());')
s=s.replace('        this.state = nextState;', '        this.state = nextState;\n        membershipInput.publish();')
s=s.replace('    void markReconciliationCompleted() {\n        reconciliationInProgress = false;\n        rejoinedWhileReconciliationInProgress = false;\n    }', '    void markReconciliationCompleted() {\n        reconciliationInProgress = false;\n        rejoinedWhileReconciliationInProgress = false;\n        membershipInput.publish();\n    }')
old="""    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        maybeReconcile(false);
        return NetworkClientDelegate.PollResult.EMPTY;
    }"""
assert old in s
s=s.replace(old,"""    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        NextPollCondition input = membershipInput.await();
        maybeReconcile(false);
        return new NetworkClientDelegate.PollResult(Long.MAX_VALUE, Collections.emptyList(), input);
    }""")
(g/'AbstractMembershipManager.java').write_text(s)
(w/'membership-notification-contract.patch').write_text(''.join(difflib.unified_diff(before.splitlines(True),s.splitlines(True),fromfile='a/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractMembershipManager.java',tofile='b/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractMembershipManager.java')))
names.remove('AbstractMembershipManager')

subprocess.run([j+'javac' ,'-J-Xmx256m','-cp',cp,'-d',str(out)]+[str(b/(n+'.java')) for n in names]+[str(g/'ApplicationEventProcessor.java'),str(g/'AbstractMembershipManager.java')],check=True)
(w/'membership-event-classpath.txt').write_text(str(out)+':'+cp)

subprocess.run([j+'javac','-cp',str(out)+':'+cp,'-d',str(out),str(p/'MembershipEventProbe.java')],check=True)
ext=out/'mockito-extensions/org.mockito.plugins.MockMaker';ext.parent.mkdir(exist_ok=True);ext.write_text('mock-maker-inline')
result=subprocess.run([j+'java','-javaagent:'+str(w/'byte-buddy-agent-1.14.4.jar'),'-Xmx128m','-XX:ActiveProcessorCount=1','-cp',str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.MembershipEventProbe'],capture_output=True,text=True,timeout=45)
(w/'membership-event-validation.log').write_text(result.stdout+result.stderr)
print(result.stdout)
if result.returncode:print(result.stderr)
result.check_returncode()

# A deliberately broken notification control must be rejected by the integration test.
control=w/'membership-missing-metadata-classes';control.mkdir(exist_ok=True)
controlsrc=w/'membership-missing-metadata-src';controlsrc.mkdir(exist_ok=True)
s=(g/'AbstractMembershipManager.java').read_text()
old='metadata.addClusterUpdateListener(resource -> membershipInput.publish());'
assert s.count(old)==1
(controlsrc/'AbstractMembershipManager.java').write_text(s.replace(old,'metadata.addClusterUpdateListener(resource -> { });'))
subprocess.run([j+'javac','-J-Xmx256m','-cp',str(out)+':'+cp,'-d',str(control),str(controlsrc/'AbstractMembershipManager.java')],check=True)
negative=subprocess.run([j+'java','-javaagent:'+str(w/'byte-buddy-agent-1.14.4.jar'),'-Xmx128m','-XX:ActiveProcessorCount=1','-cp',str(control)+':'+str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.MembershipEventProbe'],capture_output=True,text=True,timeout=45)
(w/'membership-missing-metadata-control.log').write_text(negative.stdout+negative.stderr)
assert negative.returncode != 0 and 'Metadata completion must resume reconciliation without another application poll' in negative.stderr, negative.stdout+negative.stderr
print('PASS negative control: missing metadata notification is detected')
