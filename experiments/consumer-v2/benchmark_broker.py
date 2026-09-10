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
import subprocess, socket, base64, uuid, os, signal, time, json, random, hashlib, statistics
r=Path(__file__).resolve().parents[2];p=Path(__file__).resolve().parent
w=r/'work/consumer-v2';out=w/'broker-bench-classes';out.mkdir(exist_ok=True)
run=w/('broker-bench-'+time.strftime('%Y%m%d-%H%M%S'));run.mkdir()
j='/opt/homebrew/opt/openjdk@17/bin/java';jc='/opt/homebrew/opt/openjdk@17/bin/javac'
lib=r/'work/next-poll-condition'
cp=':'.join(str(lib/n) for n in ['kafka-clients-4.3.1.jar','slf4j-api-2.0.17.jar'])
sources=[p/n for n in ['PrefetchWindow.java','FetchBridge.java','AppFetchDecoder.java','NetworkFetchTransport.java','BrokerBench.java']]
subprocess.run([jc,'-J-Xmx256m','--release','11','-cp',cp,'-d',str(out)]+list(map(str,sources)),check=True,timeout=60)
def port():
 with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
bp,controller=port(),port()
assert bp!=controller
config=run/'server.properties'
config.write_text(f'''process.roles=broker,controller
node.id=0
controller.quorum.voters=0@127.0.0.1:{controller}
listeners=BROKER://127.0.0.1:{bp},CONTROLLER://127.0.0.1:{controller}
advertised.listeners=BROKER://127.0.0.1:{bp}
listener.security.protocol.map=BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=BROKER
log.dirs={run/'data'}
num.network.threads=2
num.io.threads=2
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
log.segment.bytes=134217728
''')
cluster=base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=')
broker_cp='/opt/homebrew/opt/kafka/libexec/libs/*'
logging=run/'log4j2.properties';logging.write_text('status=error\nname=Bench\nrootLogger.level=warn\nrootLogger.appenderRef.stdout.ref=STDOUT\nappender.stdout.type=Console\nappender.stdout.name=STDOUT\nappender.stdout.layout.type=PatternLayout\nappender.stdout.layout.pattern=%d %p %c %m%n\n')
brokerjava=[j,'-Xms256m','-Xmx512m','-XX:ActiveProcessorCount=2','-Dlog4j2.configurationFile='+str(logging),'-cp',broker_cp]
with (run/'format.log').open('w') as log:
 subprocess.run(brokerjava+['kafka.tools.StorageTool','format','--cluster-id='+cluster,'-c',str(config)],stdout=log,stderr=subprocess.STDOUT,check=True,timeout=60)
count=200000;measure=20;blocks=5
order=[];rng=random.Random(9017)
for size in [100,1024]:
 for block in range(blocks):
  modes=['original','1','2'];rng.shuffle(modes)
  for mode in modes:order.append({'size':size,'block':block,'mode':mode})
manifest={'cluster_id':cluster,'client_version':'4.3.1','broker_version':'4.1.0','count':count,'warm_seconds':3,'fetch_max_wait_ms':1,'measurement_seconds':measure,'blocks':blocks,'order':order,'scope':'single partition manual assignment; repeated backlog with seek included; fetch.max.wait.ms=1 limits tail long-poll interference; no group/rebalance/commit; exploratory reference, not full consumer equivalence','source_hashes':{str(s.relative_to(r)):hashlib.sha256(s.read_bytes()).hexdigest() for s in sources}}
(run/'manifest.json').write_text(json.dumps(manifest,indent=2));(w/'latest-broker-bench.txt').write_text(str(run))
base=[j,'-Xms128m','-Xmx256m','-XX:ActiveProcessorCount=2','-cp',str(out)+':'+cp,'org.apache.kafka.clients.consumer.internals.BrokerBench']
broker=None;results=[]
try:
 log=(run/'broker.log').open('w');broker=subprocess.Popen(brokerjava+['kafka.Kafka',str(config)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 deadline=time.monotonic()+45
 while True:
  if broker.poll() is not None:raise RuntimeError('Owned broker exited; see broker.log')
  try:
   with socket.create_connection(('127.0.0.1',bp),timeout=.5):break
  except OSError:
   if time.monotonic()>deadline:raise TimeoutError('broker startup')
   time.sleep(.2)
 print('BROKER_READY '+str(run),flush=True)
 for size in [100,1024]:
  topic='bench-'+str(size)
  with (run/('setup-'+str(size)+'.log')).open('w') as output:
   subprocess.run(base+['setup',str(bp),topic,str(count),str(size)],stdout=output,stderr=subprocess.STDOUT,check=True,timeout=120)
  print('PRELOADED '+str(size),flush=True)
 # Three short correctness smokes finish before timed samples.
 for mode in ['original','1','2']:
  with (run/('smoke-'+mode+'.log')).open('w') as output:
   subprocess.run(base+[mode,str(bp),'bench-100',str(count),'100','1'],stdout=output,stderr=subprocess.STDOUT,check=True,timeout=25)
 print('SMOKES_PASS',flush=True)
 for i,cell in enumerate(order):
  if broker.poll() is not None:raise RuntimeError('Owned broker exited during measurement')
  filename=run/(f'{i:02d}-{cell["size"]}-{cell["mode"]}.log')
  with filename.open('w') as output:
   subprocess.run(base+[cell['mode'],str(bp),'bench-'+str(cell['size']),str(count),str(cell['size']),str(measure)],stdout=output,stderr=subprocess.STDOUT,check=True,timeout=measure+30)
  lines=[line[len('RESULT '):] for line in filename.read_text().splitlines() if line.startswith('RESULT ')]
  assert len(lines)==1
  row=json.loads(lines[0]);row.update({'block':cell['block'],'index':i});assert row['records']>0 and row['seconds']>=measure
  results.append(row);(run/'results.json').write_text(json.dumps(results,indent=2))
  print('CELL '+json.dumps(row),flush=True)
 print('COMPLETE '+str(run),flush=True)
finally:
 if broker is not None and broker.poll() is None:
  os.killpg(broker.pid,signal.SIGTERM)
  try:broker.wait(timeout=20)
  except subprocess.TimeoutExpired:os.killpg(broker.pid,signal.SIGKILL);broker.wait(timeout=5)
 if broker is not None:(run/'cleanup.json').write_text(json.dumps({'broker_exit':broker.returncode,'task_owned_broker_stopped':broker.poll() is not None}))
