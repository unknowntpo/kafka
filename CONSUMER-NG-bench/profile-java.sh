#!/bin/bash
# CPU profile (async-profiler, itimer) of the trunk consumer on one topic. Usage: profile-java.sh <topic> <records> <label>
B=$HOME/kafka-bench; K=$B/kafka; AP=$HOME/baselines/async-profiler/lib/libasyncProfiler.so; export KAFKA_HEAP_OPTS="-Xmx2G -Xms2G"
mkdir -p $B/profiles
export KAFKA_OPTS="-agentpath:$AP=start,event=itimer,interval=1ms,file=$B/profiles/$3.collapsed,collapsed,threads"
$K/bin/kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic $1 --num-records $2 --group prof-$RANDOM --timeout 90000 --hide-header --command-property group.protocol=consumer 2>&1 | grep -E '^20[0-9][0-9]-' | cut -c1-160
export KAFKA_OPTS="-agentpath:$AP=start,event=itimer,interval=1ms,file=$B/profiles/$3.html,threads"
$K/bin/kafka-consumer-perf-test.sh --bootstrap-server localhost:9092 --topic $1 --num-records $2 --group prof-$RANDOM --timeout 90000 --hide-header --command-property group.protocol=consumer >/dev/null 2>&1
ls -la $B/profiles/$3.*
