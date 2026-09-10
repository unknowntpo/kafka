/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.jmh.consumer;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.sun.management.OperatingSystemMXBean;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * End-to-end benchmark of the async consumer ({@code group.protocol=consumer}, auto-commit on) against a
 * running broker. Each benchmark invocation is one {@code poll()} call, so the primary result is polls per
 * second; the {@code records} auxiliary counter gives records per second.
 *
 * <p>Configuration is read from system properties (pass them with {@code -jvmArgs} or {@code -D} on the
 * {@code java -jar} command line):
 * <ul>
 *     <li>{@code kafka.bench.bootstrap} (default {@value #DEFAULT_BOOTSTRAP}): the broker.</li>
 *     <li>{@code kafka.bench.topic} (default {@value #DEFAULT_TOPIC}): the topic to subscribe to. Its records
 *     must start with an 8-byte big-endian sequence number equal to the record offset.</li>
 *     <li>{@code kafka.bench.records} (default {@value #DEFAULT_RECORDS}): records per pass in
 *     {@code consume} mode; the consumer seeks back to the beginning once a pass has read this many.</li>
 *     <li>{@code kafka.bench.mode} (default {@code consume}), one of:
 *     <ul>
 *         <li>{@code consume}: reads and validates records.</li>
 *         <li>{@code idle}: sits at the log end and polls for {@value #IDLE_POLL_MS} ms.</li>
 *         <li>{@code unreachable}: points the consumer at {@code kafka.bench.unreachable} (default
 *         {@value #DEFAULT_UNREACHABLE_BOOTSTRAP}, a closed port). Every connect is refused, so the client
 *         stays in reconnect backoff and never opens a connection.</li>
 *         <li>{@code unavailable}: points the consumer at a {@link ServerSocket} opened by this benchmark
 *         that accepts connections and then never reads or writes. The connection is established but the
 *         {@code ApiVersions} request is never answered, so the client stays in
 *         {@code CHECKING_API_VERSIONS} until {@code request.timeout.ms} tears the connection down.
 *         <b>This mode does not reach the heartbeat-in-flight state</b> (see {@code heartbeat-blackhole}).</li>
 *         <li>{@code heartbeat-blackhole}: puts a byte-level TCP proxy in front of a broker listener. The
 *         proxy forwards every frame except {@link ApiKeys#CONSUMER_GROUP_HEARTBEAT} requests, which it
 *         swallows. {@code ApiVersions}, {@code Metadata} and {@code FindCoordinator} therefore succeed
 *         normally and the first {@code ConsumerGroupHeartbeat} stays in flight forever, which is the state
 *         the KAFKA-21031 busy loop needs. The proxy listens on {@code kafka.bench.blackhole.port} and
 *         forwards to {@code kafka.bench.blackhole.target}; the broker listener behind it must
 *         <em>advertise</em> the proxy's address, otherwise the client reconnects straight to the broker and
 *         the heartbeat is answered. {@code request.timeout.ms} is raised to
 *         {@value #BLACKHOLE_REQUEST_TIMEOUT_MS} ms so the parked heartbeat is not timed out mid-run.</li>
 *     </ul></li>
 * </ul>
 *
 * <p>Auxiliary counters ({@link AuxCounters.Type#OPERATIONS}, reported per second): {@code records},
 * {@code emptyPolls}, {@code seeksToBeginning}, {@code peerConnections} (connections accepted by the
 * {@code unavailable} listener), {@code blackholedHeartbeats} (heartbeat requests swallowed by the
 * {@code heartbeat-blackhole} proxy; a non-zero value is the proof that the mode reached the intended
 * state), {@code processCpuMillis} (process CPU time,
 * {@link OperatingSystemMXBean#getProcessCpuTime()}; 1000 = one core busy) and {@code networkThreadCpuMillis}
 * (CPU time of the threads whose name contains {@value #NETWORK_THREAD_NAME_FRAGMENT}, via
 * {@link ThreadMXBean#getThreadCpuTime(long)}; {@code 0} when thread CPU time is unsupported).
 */
@State(Scope.Thread)
@Fork(1)
@Threads(1)
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 5, time = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class AsyncConsumerBrokerBenchmark {

    public enum BenchMode {
        CONSUME,
        IDLE,
        /** Peer accepts the connection and then stays silent. Does not reach heartbeat-in-flight. */
        UNAVAILABLE,
        /** Closed port: every connect is refused. */
        UNREACHABLE,
        /** Proxy in front of a broker that swallows {@code ConsumerGroupHeartbeat} requests. */
        HEARTBEAT_BLACKHOLE
    }

    private static final String BOOTSTRAP_PROPERTY = "kafka.bench.bootstrap";
    private static final String UNREACHABLE_PROPERTY = "kafka.bench.unreachable";
    private static final String BLACKHOLE_PORT_PROPERTY = "kafka.bench.blackhole.port";
    private static final String BLACKHOLE_TARGET_PROPERTY = "kafka.bench.blackhole.target";
    private static final String TOPIC_PROPERTY = "kafka.bench.topic";
    private static final String RECORDS_PROPERTY = "kafka.bench.records";
    private static final String MODE_PROPERTY = "kafka.bench.mode";
    private static final String DEFAULT_BOOTSTRAP = "127.0.0.1:9092";
    private static final String DEFAULT_UNREACHABLE_BOOTSTRAP = "127.0.0.1:1";
    private static final int BLACKHOLE_REQUEST_TIMEOUT_MS = 300_000;
    private static final String DEFAULT_TOPIC = "jmh-input";
    private static final long DEFAULT_RECORDS = 2_000_000L;
    private static final long CONSUME_POLL_MS = 100;
    private static final long IDLE_POLL_MS = 1000;
    private static final long ASSIGNMENT_TIMEOUT_MS = 60_000;
    private static final String NETWORK_THREAD_NAME_FRAGMENT = "consumer_background_thread";
    private static final int SEQUENCE_PREFIX_BYTES = Long.BYTES;
    private static final long UNKNOWN_OFFSET = -1L;

    /** Accepted by the {@code unavailable} listener; read by {@link Counters} across state objects. */
    private static final AtomicLong PEER_CONNECTIONS = new AtomicLong();
    /** Swallowed by the {@code heartbeat-blackhole} proxy; read by {@link Counters}. */
    private static final AtomicLong BLACKHOLED_HEARTBEATS = new AtomicLong();

    private BenchMode mode;
    private SilentPeer silentPeer;
    private HeartbeatBlackholeProxy blackholeProxy;
    private long recordCount;
    private Duration pollTimeout;
    private KafkaConsumer<byte[], byte[]> consumer;
    private Set<TopicPartition> assignment;
    /** Next expected offset per partition index; {@link #UNKNOWN_OFFSET} until the first record is seen. */
    private long[] nextOffsets;
    private long consumedInPass;

    /**
     * Per-second counters. JMH reports {@link AuxCounters.Type#OPERATIONS} counters as {@code counter/s},
     * so the CPU counters are CPU milliseconds per wall-clock second (1000 = one core).
     */
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.OPERATIONS)
    public static class Counters {
        public long records;
        public long emptyPolls;
        public long seeksToBeginning;
        public long peerConnections;
        public long blackholedHeartbeats;
        public long processCpuMillis;
        public long networkThreadCpuMillis;

        private final OperatingSystemMXBean os = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        private long processCpuStartNs;
        private long networkThreadCpuStartNs;
        private long peerConnectionsStart;
        private long blackholedHeartbeatsStart;

        @Setup(Level.Iteration)
        public void start() {
            processCpuStartNs = os.getProcessCpuTime();
            networkThreadCpuStartNs = networkThreadCpuNs();
            peerConnectionsStart = PEER_CONNECTIONS.get();
            blackholedHeartbeatsStart = BLACKHOLED_HEARTBEATS.get();
        }

        @TearDown(Level.Iteration)
        public void stop() {
            processCpuMillis = TimeUnit.NANOSECONDS.toMillis(os.getProcessCpuTime() - processCpuStartNs);
            networkThreadCpuMillis = TimeUnit.NANOSECONDS.toMillis(networkThreadCpuNs() - networkThreadCpuStartNs);
            peerConnections = PEER_CONNECTIONS.get() - peerConnectionsStart;
            blackholedHeartbeats = BLACKHOLED_HEARTBEATS.get() - blackholedHeartbeatsStart;
        }

        private long networkThreadCpuNs() {
            if (!threads.isThreadCpuTimeSupported())
                return 0L;
            long total = 0L;
            for (ThreadInfo info : threads.getThreadInfo(threads.getAllThreadIds())) {
                if (info == null || !info.getThreadName().contains(NETWORK_THREAD_NAME_FRAGMENT))
                    continue;
                long cpuNs = threads.getThreadCpuTime(info.getThreadId());
                if (cpuNs > 0)
                    total += cpuNs;
            }
            return total;
        }
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        mode = BenchMode.valueOf(System.getProperty(MODE_PROPERTY, "consume")
            .toUpperCase(Locale.ROOT).replace('-', '_'));
        recordCount = Long.getLong(RECORDS_PROPERTY, DEFAULT_RECORDS);
        String topic = System.getProperty(TOPIC_PROPERTY, DEFAULT_TOPIC);
        pollTimeout = Duration.ofMillis(mode == BenchMode.IDLE ? IDLE_POLL_MS : CONSUME_POLL_MS);

        Map<String, Object> props = consumerProps(bootstrap());
        if (mode == BenchMode.HEARTBEAT_BLACKHOLE)
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, BLACKHOLE_REQUEST_TIMEOUT_MS);
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
        if (mode != BenchMode.CONSUME && mode != BenchMode.IDLE)
            return;

        assignment = waitForAssignment();
        int maxPartition = assignment.stream().mapToInt(TopicPartition::partition).max().orElse(0);
        nextOffsets = new long[maxPartition + 1];
        if (mode == BenchMode.CONSUME) {
            consumer.seekToBeginning(assignment);
            resetPass();
        } else {
            consumer.seekToEnd(assignment);
        }
    }

    /** Resolves the bootstrap address for the mode, starting the fake peer if the mode needs one. */
    private String bootstrap() throws IOException {
        switch (mode) {
            case UNREACHABLE:
                return System.getProperty(UNREACHABLE_PROPERTY, DEFAULT_UNREACHABLE_BOOTSTRAP);
            case UNAVAILABLE:
                silentPeer = new SilentPeer();
                return silentPeer.bootstrap();
            case HEARTBEAT_BLACKHOLE:
                String target = System.getProperty(BLACKHOLE_TARGET_PROPERTY);
                if (target == null)
                    throw new IllegalStateException(BLACKHOLE_TARGET_PROPERTY + " must be set in "
                        + mode + " mode: the broker listener the proxy forwards to");
                blackholeProxy = new HeartbeatBlackholeProxy(Integer.getInteger(BLACKHOLE_PORT_PROPERTY, 0), target);
                return blackholeProxy.bootstrap();
            default:
                return System.getProperty(BOOTSTRAP_PROPERTY, DEFAULT_BOOTSTRAP);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (consumer != null)
            consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)));
        closeQuietly(silentPeer);
        closeQuietly(blackholeProxy);
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable == null)
            return;
        try {
            closeable.close();
        } catch (IOException e) {
            // benchmark teardown; nothing useful to do
        }
    }

    @Benchmark
    public long poll(Counters counters) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
        int count = records.count();
        if (count == 0) {
            counters.emptyPolls++;
            return 0L;
        }
        if (mode != BenchMode.CONSUME)
            throw new IllegalStateException("Unexpected " + count + " records in " + mode + " mode");

        for (ConsumerRecord<byte[], byte[]> record : records)
            validate(record);
        counters.records += count;
        consumedInPass += count;
        if (consumedInPass >= recordCount) {
            consumer.seekToBeginning(assignment);
            resetPass();
            counters.seeksToBeginning++;
        }
        return count;
    }

    private void validate(ConsumerRecord<byte[], byte[]> record) {
        int partition = record.partition();
        long expected = nextOffsets[partition];
        if (expected != UNKNOWN_OFFSET && record.offset() != expected)
            throw new IllegalStateException("Offset gap on partition " + partition + ": expected " + expected
                + " but got " + record.offset());
        byte[] value = record.value();
        if (value == null || value.length < SEQUENCE_PREFIX_BYTES)
            throw new IllegalStateException("Record at offset " + record.offset() + " has no sequence prefix");
        long sequence = ByteBuffer.wrap(value).getLong();
        if (sequence != record.offset())
            throw new IllegalStateException("Sequence mismatch at offset " + record.offset() + ": " + sequence);
        nextOffsets[partition] = record.offset() + 1;
    }

    private void resetPass() {
        consumedInPass = 0;
        Arrays.fill(nextOffsets, UNKNOWN_OFFSET);
    }

    private Set<TopicPartition> waitForAssignment() {
        long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ASSIGNMENT_TIMEOUT_MS);
        while (consumer.assignment().isEmpty()) {
            if (System.nanoTime() >= deadlineNs)
                throw new IllegalStateException("No partitions assigned after " + ASSIGNMENT_TIMEOUT_MS + " ms");
            consumer.poll(Duration.ofMillis(250));
        }
        return Set.copyOf(consumer.assignment());
    }

    private static Map<String, Object> consumerProps(String bootstrap) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "jmh-" + UUID.randomUUID());
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return props;
    }

    /**
     * Accepts TCP connections on an ephemeral loopback port and then does nothing with them: no read, no
     * write, no close. Accepted sockets are held in a list so they are not garbage collected. The client's
     * {@code ApiVersions} request is therefore never answered.
     */
    private static final class SilentPeer implements Closeable {
        private final ServerSocket server;
        private final List<Socket> accepted = Collections.synchronizedList(new ArrayList<>());

        SilentPeer() throws IOException {
            server = new ServerSocket(0, 64, InetAddress.getLoopbackAddress());
            Thread acceptor = new Thread(this::acceptLoop, "bench-silent-peer");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        String bootstrap() {
            return "127.0.0.1:" + server.getLocalPort();
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    accepted.add(server.accept());
                    PEER_CONNECTIONS.incrementAndGet();
                } catch (IOException e) {
                    return;
                }
            }
        }

        @Override
        public void close() throws IOException {
            server.close();
            synchronized (accepted) {
                for (Socket socket : accepted)
                    closeQuietly(socket);
                accepted.clear();
            }
        }
    }

    /**
     * Byte-level TCP proxy in front of a broker listener. Requests are Kafka frames (4-byte big-endian size
     * then the message, whose first two bytes are the API key), so the proxy can drop
     * {@link ApiKeys#CONSUMER_GROUP_HEARTBEAT} requests without understanding any schema. Everything else is
     * forwarded verbatim, so {@code ApiVersions}, {@code Metadata} and {@code FindCoordinator} behave
     * exactly as against the real broker and the first heartbeat stays in flight forever.
     */
    private static final class HeartbeatBlackholeProxy implements Closeable {
        private final ServerSocket server;
        private final String targetHost;
        private final int targetPort;
        private final List<Closeable> open = Collections.synchronizedList(new ArrayList<>());

        HeartbeatBlackholeProxy(int listenPort, String target) throws IOException {
            int colon = target.lastIndexOf(':');
            if (colon < 0)
                throw new IllegalArgumentException("Expected host:port but got " + target);
            targetHost = target.substring(0, colon);
            targetPort = Integer.parseInt(target.substring(colon + 1));
            server = new ServerSocket(listenPort, 64, InetAddress.getLoopbackAddress());
            Thread acceptor = new Thread(this::acceptLoop, "bench-hb-blackhole");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        String bootstrap() {
            return "127.0.0.1:" + server.getLocalPort();
        }

        private void acceptLoop() {
            while (!server.isClosed()) {
                try {
                    Socket client = server.accept();
                    Socket broker = new Socket();
                    broker.connect(new InetSocketAddress(targetHost, targetPort));
                    client.setTcpNoDelay(true);
                    broker.setTcpNoDelay(true);
                    open.add(client);
                    open.add(broker);
                    PEER_CONNECTIONS.incrementAndGet();
                    start("bench-hb-blackhole-req", () -> pumpRequests(client, broker));
                    start("bench-hb-blackhole-resp", () -> pumpResponses(broker, client));
                } catch (IOException e) {
                    return;
                }
            }
        }

        private static void start(String name, Runnable body) {
            Thread thread = new Thread(body, name);
            thread.setDaemon(true);
            thread.start();
        }

        /** Forwards every request frame except heartbeats, which are swallowed. */
        private void pumpRequests(Socket from, Socket to) {
            try {
                DataInputStream in = new DataInputStream(from.getInputStream());
                OutputStream out = to.getOutputStream();
                while (true) {
                    int size = in.readInt();
                    if (size < 2 || size > 100 * 1024 * 1024)
                        return;
                    byte[] frame = new byte[size];
                    in.readFully(frame);
                    short apiKey = (short) (((frame[0] & 0xff) << 8) | (frame[1] & 0xff));
                    if (apiKey == ApiKeys.CONSUMER_GROUP_HEARTBEAT.id) {
                        // Logged as well as counted: the aux counter only covers one measurement iteration,
                        // and in the intended state the single parked heartbeat is dropped before it starts.
                        System.err.println("[hb-blackhole] swallowed ConsumerGroupHeartbeat #"
                            + BLACKHOLED_HEARTBEATS.incrementAndGet());
                        continue;
                    }
                    out.write(ByteBuffer.allocate(Integer.BYTES).putInt(size).array());
                    out.write(frame);
                    out.flush();
                }
            } catch (IOException e) {
                // connection closed; the pump is done
            }
        }

        private void pumpResponses(Socket from, Socket to) {
            try {
                InputStream in = from.getInputStream();
                OutputStream out = to.getOutputStream();
                byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = in.read(buffer)) >= 0) {
                    out.write(buffer, 0, read);
                    out.flush();
                }
            } catch (IOException e) {
                // connection closed; the pump is done
            }
        }

        @Override
        public void close() throws IOException {
            server.close();
            synchronized (open) {
                for (Closeable closeable : open)
                    closeQuietly(closeable);
                open.clear();
            }
        }
    }
}
