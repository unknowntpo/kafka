# KIP-1034 DLQ Demo

Demonstrates error handling in Kafka Streams **before** and **after** KIP-1034 Dead Letter Queue.

## Directory Structure

```
demo/
├── before-dlq/           # Kafka 4.0.0 (no DLQ)
│   ├── kafka_2.13-4.0.0/ # Downloaded tarball
│   ├── src/main/java/demo/BeforeDlqDemo.java
│   └── build.gradle
│
├── after-dlq/            # Kafka 4.3.0-SNAPSHOT (with DLQ)
│   ├── kafka_2.13-4.3.0-SNAPSHOT/  # Built from source
│   ├── src/main/java/demo/AfterDlqDemo.java
│   └── build.gradle
│
├── monitoring/           # Prometheus + Grafana stack
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   └── provisioning/
│   │       ├── dashboards/
│   │       │   ├── dashboard.yml
│   │       │   └── kafka-streams.json
│   │       └── datasources/
│   │           └── datasource.yml
│   └── jmx-exporter/
│       └── config.yml
│
├── docker-compose.yml    # Kafka + Prometheus + Grafana
└── README.md
```

## Prerequisites

- Java 17+
- Docker & Docker Compose

## Setup

### 1. Start Kafka Broker

```bash
cd demo
docker-compose up -d
```

### 2. Create Topics

```bash
docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh \
  --create --topic input-topic --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh \
  --create --topic output-topic --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh \
  --create --topic error-topic --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

docker exec kafka-dlq-demo /opt/kafka/bin/kafka-topics.sh \
  --create --topic dlq-topic --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092
```

### 3. Setup Before-DLQ Demo (Kafka 4.0.0)

```bash
cd demo/before-dlq

# Download Kafka 4.0.0
curl -O https://archive.apache.org/dist/kafka/4.0.0/kafka_2.13-4.0.0.tgz
tar -xzf kafka_2.13-4.0.0.tgz
rm kafka_2.13-4.0.0.tgz
```

### 4. Setup After-DLQ Demo (Kafka with DLQ)

Option A: Copy existing tarball (if available)
```bash
# If you already have kafka_2.13-4.3.0-SNAPSHOT in the parent dir
cp -r ../kafka_2.13-4.3.0-SNAPSHOT demo/after-dlq/
```

Option B: Build from source
```bash
cd /Users/unknowntpo/repo/unknowntpo/kafka
./gradlew streams:releaseTarGz -x test

# Find and extract tarball
cp streams/build/distributions/kafka-streams-*.tgz kip1034-presentation/demo/after-dlq/
cd kip1034-presentation/demo/after-dlq
tar -xzf kafka-streams-*.tgz
rm kafka-streams-*.tgz
```

## Running the Demos

### Before DLQ Demo

```bash
cd demo/before-dlq
./gradlew run
```

**What it shows:**
- Manual try/catch in processor
- Manual KafkaProducer for error routing
- Verbose boilerplate code

### After DLQ Demo

```bash
cd demo/after-dlq
./gradlew run
```

**What it shows:**
- Clean processing code (no try/catch)
- ProcessingExceptionHandler with DLQ support
- Automatic error metadata in headers

## Key Differences

| Aspect | Before (4.0.0) | After (4.3.0+) |
|--------|----------------|----------------|
| Error handling | Manual try/catch | ProcessingExceptionHandler |
| DLQ routing | Manual KafkaProducer | Response.resume(dlqRecords) |
| Error metadata | Must capture manually | Built-in context (topic, partition, offset) |
| Code complexity | High (boilerplate) | Low (clean separation) |

## Topics Used

- `input-topic` - Source messages
- `output-topic` - Successfully processed messages
- `error-topic` - Failed messages (before-dlq)
- `dlq-topic` - Dead Letter Queue (after-dlq)

## Consume Messages

```bash
# Output
docker exec kafka-dlq-demo /opt/kafka/bin/kafka-console-consumer.sh \
  --topic output-topic --from-beginning --bootstrap-server localhost:9092

# Error topic (before-dlq)
docker exec kafka-dlq-demo /opt/kafka/bin/kafka-console-consumer.sh \
  --topic error-topic --from-beginning --bootstrap-server localhost:9092

# DLQ topic (after-dlq)
docker exec kafka-dlq-demo /opt/kafka/bin/kafka-console-consumer.sh \
  --topic dlq-topic --from-beginning --bootstrap-server localhost:9092
```

## Monitoring (Prometheus + Grafana)

The demo includes a pre-configured monitoring stack to visualize Kafka Streams metrics.

### Start Monitoring Stack

```bash
cd demo
docker-compose up -d
```

This starts:
- **Kafka** at `localhost:9092`
- **Prometheus** at `localhost:9090`
- **Grafana** at `localhost:3000` (login: admin/admin)

### Access Dashboards

1. Open Grafana: http://localhost:3000
2. Login with `admin` / `admin`
3. Go to Dashboards → "Kafka Streams DLQ Demo"

### Exposed Metrics Ports

| App | JMX Port | Prometheus Metrics Port |
|-----|----------|------------------------|
| before-dlq | 9991 | 9401 |
| after-dlq | 9992 | 9402 |

### Key Metrics Shown

- **App Status** - UP/DOWN for each demo
- **Records Processed Rate** - Throughput comparison
- **Processing Latency** - avg/max latency
- **Commit Rate** - Commits per second
- **Thread Activity Ratio** - Poll/Process/Commit breakdown
- **DLQ Topic Activity** - Messages sent to DLQ
- **Failed Stream Threads** - Error indicator

### Check Prometheus Targets

Visit http://localhost:9090/targets to verify both apps are being scraped.

## Cleanup

```bash
# Stop Kafka + Prometheus + Grafana
cd demo && docker-compose down -v

# Remove downloaded tarballs (optional)
rm -rf before-dlq/kafka_2.13-4.0.0
rm -rf after-dlq/kafka_2.13-4.3.0-SNAPSHOT

# Remove JMX agent JARs (optional)
rm -f before-dlq/jmx_prometheus_javaagent.jar before-dlq/jmx-exporter-config.yml
rm -f after-dlq/jmx_prometheus_javaagent.jar after-dlq/jmx-exporter-config.yml
```
