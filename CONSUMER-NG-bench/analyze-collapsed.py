import sys, re, collections
path=sys.argv[1]
cats=[("socket read (JDK NIO)", r"sun\.nio\.ch\.|SocketDispatcher|IOUtil\.read|Net\.poll|EPoll|Selector\.poll|KafkaChannel\.read|NetworkReceive\.readFrom|TransportLayer\.read"),
      ("kernel/libc (itimer sees only user)", r"^(\[k\]|libc|__)"),
      ("CRC32C / checksum", r"Crc32C|CRC32C|Checksums|checksum|isValid|ensureValid|validateBatch"),
      ("record parse (DefaultRecord/Batch iter)", r"DefaultRecord\.readFrom|DefaultRecordBatch|RecordIterator|readVarint|ByteUtils|LegacyRecord|MemoryRecords"),
      ("deserialize + value copy", r"Deserializer|Utils\.toArray|toNullableArray|deserialize"),
      ("ConsumerRecord/ConsumerRecords build", r"ConsumerRecord|FetchCollector|CompletedFetch\.fetchRecords|CompletedFetch\.fetchRecord|Fetch\.add|Fetch\.records"),
      ("FetchResponse parse (bg)", r"FetchResponse|FetchResponseData|handleFetchSuccess|FetchRequestManager|AbstractFetch|CompletedFetch\.<init>|FetchBuffer\.add"),
      ("metrics/sensors", r"Sensor\.record|Metrics\.|SampledStat|KafkaMetric|Meter\.|Rate\.|FetchMetrics|recordAggregatedMetrics|Frequencies|Histogram"),
      ("locks/park/handoff", r"LockSupport|ReentrantLock|AbstractQueuedSynchronizer|Condition|park|unpark|FetchBuffer\.awaitWakeup|FetchBuffer\.wakeup|ConcurrentLinkedQueue|LinkedBlockingQueue|BlockingQueue|Object\.wait|Object\.notify"),
      ("network-thread loop overhead", r"ConsumerNetworkThread|ConsumerEventLoop|RequestManagers|ApplicationEventProcessor|maximumTimeToWait|HeartbeatRequestManager|CommitRequestManager|OffsetsRequestManager|TopicMetadataRequestManager|CoordinatorRequestManager|MembershipManager|NetworkClientDelegate\.poll|NetworkClient\.poll|ClientUtils|MetadataUpdater|InFlightRequests|Selector\.(select|pollSelectionKeys|clear|maybeCloseOldest)"),
      ("harness (ConsumerPerformance)", r"ConsumerPerformance|kafka\.tools"),
      ("GC (threads)", r"^\[GC|G1|Parallel GC|ZWorker|VM Thread"),
      ("JIT compiler (threads)", r"^\[C[12] Compiler|Compiler"),
      ("allocation slow path / TLAB", r"TLAB|allocate|MemAllocator|OutOfMemory|SharedRuntime::new|slow_path|Interpreter"),
     ]
tot=0; threads=collections.Counter(); catcount=collections.defaultdict(collections.Counter); selfc=collections.defaultdict(collections.Counter)
for line in open(path):
    line=line.rstrip("\n"); 
    if not line: continue
    stack, n = line.rsplit(" ",1); n=int(n); tot+=n
    frames=stack.split(";"); thread=frames[0] if frames[0].startswith("[") else "?"
    tname = "app(main)" if thread.startswith("[main") else ("bg-thread" if "consumer_background" in thread else ("GC" if re.search(r"GC|G1|VM Thread", thread) else ("JIT" if "Compiler" in thread else thread)))
    threads[tname]+=n
    selfc[tname][frames[-1]]+=n
    joined=";".join(frames[1:])
    for name,rx in cats:
        if re.search(rx, joined) or (name.startswith("GC") and tname=="GC") or (name.startswith("JIT") and tname=="JIT"):
            catcount[tname][name]+=n
print("TOTAL samples", tot)
for t,n in threads.most_common():
    print("\n== thread %s: %d samples (%.1f%% of total)"%(t,n,100*n/tot))
    for name,c in sorted(catcount[t].items(), key=lambda x:-x[1])[:10]:
        print("   inclusive %-42s %6.1f%% of thread"%(name, 100*c/n))
    print("   top self frames:")
    for f,c in selfc[t].most_common(12):
        print("     %5.1f%%  %s"%(100*c/n, f[:110]))
