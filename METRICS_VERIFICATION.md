# Kafka Streams Metrics Application-ID Tag Verification

## Overview

This document describes the verification of PR #20766 which adds the `application-id` tag to Kafka Streams metrics as part of KAFKA-19734.

## Background

**PR**: https://github.com/apache/kafka/pull/20766
**Discussion**: https://github.com/apache/kafka/pull/20766#discussion_r2484872956

### What Changed

The PR adds `APPLICATION_ID_TAG` to all client-level metrics in Kafka Streams by modifying `StreamsMetricsImpl.java`:

```java
public Map<String, String> clientLevelTagMap() {
    final Map<String, String> tagMap = new LinkedHashMap<>();
    tagMap.put(CLIENT_ID_TAG, clientId);
    tagMap.put(PROCESS_ID_TAG, processId);
    tagMap.put(APPLICATION_ID_TAG, applicationId);  // ← NEW
    return tagMap;
}
```

**Important Note**: The KIP originally specified only the `client-state` metric should receive this tag, but the implementation applies it to ALL client-level metrics. This scope difference is being discussed on the mailing list.

## Files Created

- **METRICS_VERIFICATION.md** - This documentation
- **MetricsTestApp.java** - Integration test with JMX support
- **run_metrics_test.sh** - Script to run full test with JMX
- **run_metrics_test_quick.sh** - Script for quick verification (no JMX)

## Verification Methods

### 1. Unit Test

**Location**: `streams/src/test/java/org/apache/kafka/streams/processor/internals/metrics/StreamsMetricsImplTest.java`

**Test**: `shouldIncludeApplicationIdTagInAllClientLevelMetrics()`

**What it does**:
- Creates both mutable and immutable client-level metrics
- Verifies each metric has all three tags: `client-id`, `process-id`, `application-id`
- Validates tag values match the expected values

**Run it**:
```bash
./gradlew :streams:test --tests StreamsMetricsImplTest.shouldIncludeApplicationIdTagInAllClientLevelMetrics
```

**Result**: ✅ PASSED

### 2. Integration Test App with JMX

**Location**: `MetricsTestApp.java` (in repo root)

**What it does**:
- Starts a simple Kafka Streams application
- Enumerates all metrics and checks for `application-id` tag
- Lists JMX MBeans containing the application-id attribute
- Keeps running for manual JMX inspection via JConsole/VisualVM

## Running the Integration Test App

### Option 1: Using Helper Scripts (Easiest)

**Quick test** (prints results and exits):
```bash
./run_metrics_test_quick.sh
```

**Full test with JMX** (keeps running for inspection):
```bash
./run_metrics_test.sh
# Then connect with JConsole: jconsole localhost:9999
```

### Option 2: Using Gradle

```bash
# Build Kafka first
./gradlew :streams:jar :clients:jar

# Run with proper classpath
./gradlew --quiet execute -PmainClass=MetricsTestApp \
  -PcustomClasspath="$(pwd)/streams/build/libs/*:$(pwd)/clients/build/libs/*:$(pwd)/MetricsTestApp.java"
```

### Option 3: Manual Compilation and Execution

```bash
# 1. Build Kafka jars and dependencies
./gradlew :streams:jar :clients:jar :streams:copyDependantLibs :tools:copyDependantLibs

# 2. Compile the test app
javac -cp "streams/build/libs/*:clients/build/libs/*:streams/build/dependant-libs-2.13.17/*" \
  MetricsTestApp.java

# 3. Set up classpath
CLASSPATH="."
CLASSPATH="$CLASSPATH:streams/build/libs/*"
CLASSPATH="$CLASSPATH:clients/build/libs/*"
CLASSPATH="$CLASSPATH:streams/build/dependant-libs-2.13.17/*"
CLASSPATH="$CLASSPATH:tools/build/dependant-libs-2.13.17/*"

# 4. Run without JMX (quick check)
java -cp "$CLASSPATH" MetricsTestApp

# 5. Run WITH JMX for visual inspection
java -Dcom.sun.management.jmxremote \
     -Dcom.sun.management.jmxremote.port=9999 \
     -Dcom.sun.management.jmxremote.authenticate=false \
     -Dcom.sun.management.jmxremote.ssl=false \
     -cp "$CLASSPATH" \
     MetricsTestApp
```


## Required Dependencies

The test app needs these jars:

1. **kafka-streams** - Main Kafka Streams library
   - Location: `streams/build/libs/kafka-streams-*.jar`
   - Contains: `KafkaStreams`, `StreamsBuilder`, `StreamsConfig`

2. **kafka-clients** - Kafka client library (dependency of streams)
   - Location: `clients/build/libs/kafka-clients-*.jar`
   - Contains: `Metric`, `MetricName`, `ConsumerConfig`, `Serdes`

3. **Standard Java libraries** (included in JDK):
   - `javax.management.*` - For JMX inspection
   - `java.lang.management.*` - For MBeanServer

### Understanding the Classpath

The classpath must include:
1. **Current directory** (`.`) - Where compiled `MetricsTestApp.class` is
2. **Streams jars** (`streams/build/libs/*`) - Kafka Streams library
3. **Client jars** (`clients/build/libs/*`) - Kafka client dependencies
4. **Streams dependencies** (`streams/build/dependant-libs-2.13.17/*`) - SLF4J, Jackson, RocksDB
5. **Logging implementation** (`tools/build/dependant-libs-2.13.17/*`) - Log4j jars

**Correct format**:
```bash
# Unix/Linux/macOS (colon separator)
CLASSPATH="."
CLASSPATH="$CLASSPATH:streams/build/libs/*"
CLASSPATH="$CLASSPATH:clients/build/libs/*"
CLASSPATH="$CLASSPATH:streams/build/dependant-libs-2.13.17/*"
CLASSPATH="$CLASSPATH:tools/build/dependant-libs-2.13.17/*"
java -cp "$CLASSPATH" MetricsTestApp

# Windows (semicolon separator)
set CLASSPATH=.;streams/build/libs/*;clients/build/libs/*;streams/build/dependant-libs-2.13.17/*;tools/build/dependant-libs-2.13.17/*
java -cp "%CLASSPATH%" MetricsTestApp
```

**Why wildcards work**: The `*` in classpath expands to all `.jar` files in the directory. This is handled by the JVM, not the shell.

**Build output locations**:
```bash
# After running:
# ./gradlew :streams:jar :clients:jar :streams:copyDependantLibs :tools:copyDependantLibs

streams/build/libs/
├── kafka-streams-4.2.0-SNAPSHOT.jar       # Main jar
├── kafka-streams-4.2.0-SNAPSHOT-javadoc.jar
├── kafka-streams-4.2.0-SNAPSHOT-sources.jar
└── kafka-streams-4.2.0-SNAPSHOT-test.jar

clients/build/libs/
├── kafka-clients-4.2.0-SNAPSHOT.jar       # Main jar
├── kafka-clients-4.2.0-SNAPSHOT-javadoc.jar
├── kafka-clients-4.2.0-SNAPSHOT-sources.jar
└── kafka-clients-4.2.0-SNAPSHOT-test.jar

streams/build/dependant-libs-2.13.17/
├── slf4j-api-1.7.36.jar                   # Logging API
├── jackson-annotations-2.19.2.jar         # JSON processing
├── jackson-core-2.19.2.jar
├── jackson-databind-2.19.2.jar
└── rocksdbjni-10.1.3.jar                  # RocksDB state store

tools/build/dependant-libs-2.13.17/
├── log4j-api-2.25.1.jar                   # Log4j API
├── log4j-core-2.25.1.jar                  # Log4j implementation
└── log4j-slf4j-impl-2.25.1.jar            # SLF4J to Log4j bridge
```

## Inspecting Metrics via JMX

Once the app is running with JMX enabled:

### Using Azul Mission Control (Recommended)

**Step 1: Start your app with JMX**
```bash
./run_metrics_test.sh
# App will be available at localhost:9999
```

**Step 2: Connect Mission Control**
1. Launch Azul Mission Control (or Zulu Mission Control)
2. In the **JVM Browser** panel (left side):
   - Look for "Local" section
   - Find process: `MetricsTestApp` or PID with port 9999
   - Right-click → **Connect** (or double-click)

**Step 3: Navigate to MBean Browser**
1. Once connected, click the **MBean Browser** tab
2. In the left tree panel, expand:
   ```
   kafka.streams
   └── stream-metrics
       └── (application-id=test-metrics-app,client-id=...,process-id=...)
   ```

**Step 4: View Metrics**
- Each node under `stream-metrics` represents a metric
- The node name format: `type=stream-metrics,application-id=...,client-id=...,process-id=...`
- Click on any metric to see its attributes in the right panel
- Look for attributes like:
  - `state` - Current state (RUNNING, etc.)
  - `version` - Kafka version
  - `client-state` - Client state (the main metric from the KIP)
  - `alive-stream-threads` - Number of active threads
  - etc.

**Step 5: Verify application-id Tag**
- Check the MBean **ObjectName** in the right panel
- It should contain: `application-id=test-metrics-app`
- **All** stream-metrics should have this tag

**Screenshot locations in Azul Mission Control:**
```
┌─────────────────────────────────────────────────────────────┐
│ File  Window  Help                                           │
├─────────────────┬───────────────────────────────────────────┤
│ JVM Browser     │  MBean Browser Tab                         │
│                 │                                            │
│ ▼ Local         │  MBean Tree:                              │
│   ▼ MetricsTest │  ▼ kafka.streams                          │
│     • Overview  │    ▼ stream-metrics                       │
│     • MBean Br..│      • type=stream-metrics,application... │
│     • Threads   │      • type=stream-metrics,application... │
│                 │                                            │
│                 │  Selected MBean Details:                   │
│                 │  ObjectName: kafka.streams:type=stream-... │
│                 │  Attributes:                               │
│                 │    • state: RUNNING                        │
│                 │    • version: 4.2.0-SNAPSHOT              │
└─────────────────┴───────────────────────────────────────────┘
```

### Using JConsole

```bash
# Connect to the app
jconsole localhost:9999

# Navigate to:
# MBeans → kafka.streams → stream-metrics
#
# Look for attributes containing "application-id"
```

### Using VisualVM

```bash
# Install MBeans plugin if not already installed
# Tools → Plugins → Available Plugins → VisualVM-MBeans

# Connect to localhost:9999
# Go to MBeans tab
# Expand: kafka.streams → stream-metrics
```

### Using JMX CLI Tools

```bash
# List all Kafka Streams MBeans with application-id
echo "beans kafka.streams:*application-id=*" | \
  java -jar jmxterm.jar -l localhost:9999
```

## Expected Output

When running `MetricsTestApp`, you should see:

```
=== Kafka Streams Metrics (via streams.metrics()) ===

Metric: version
  Group: stream-metrics
  Tags: {client-id=test-metrics-app-..., process-id=..., application-id=test-metrics-app}
  Value: 4.2.0-SNAPSHOT
  Has application-id? true

Metric: state
  Group: stream-metrics
  Tags: {client-id=test-metrics-app-..., process-id=..., application-id=test-metrics-app}
  Value: RUNNING
  Has application-id? true

... (more metrics)

=== Summary ===
Total metrics: 150+
Metrics with application-id tag: 10+
Client-level metrics: 10
Client-level metrics with application-id: 10

✓ ALL client-level metrics have application-id tag

=== JMX MBeans for Kafka Streams ===
Found 10+ Kafka Streams JMX MBeans

Sample MBeans with application-id attribute:
  kafka.streams:type=stream-metrics,client-id=...,process-id=...,application-id=test-metrics-app
```

## Verification Checklist

- [x] Unit test passes
- [x] Integration test app compiles
- [ ] Integration test app runs successfully
- [ ] All client-level metrics show `application-id` tag
- [ ] JMX MBeans contain `application-id` attribute
- [ ] Tag value matches configured `APPLICATION_ID_CONFIG`

## Troubleshooting

### ClassNotFoundException / NoClassDefFoundError

**Error**: `java.lang.ClassNotFoundException: org.apache.kafka.streams.KafkaStreams`
**Error**: `java.lang.NoClassDefFoundError: org/slf4j/LoggerFactory`

**Cause**: Missing jars in classpath

**Fix**: Build all required jars and dependencies:
```bash
./gradlew :streams:jar :clients:jar :streams:copyDependantLibs :tools:copyDependantLibs
```

Then ensure your classpath includes:
- `streams/build/libs/*` - Kafka Streams
- `clients/build/libs/*` - Kafka Clients
- `streams/build/dependant-libs-2.13.17/*` - SLF4J, Jackson, RocksDB
- `tools/build/dependant-libs-2.13.17/*` - Log4j logging implementation

### Debugging Classpath Issues

**Step 1**: Check what jars exist:
```bash
ls -la streams/build/libs/*.jar
ls -la clients/build/libs/*.jar
ls -la streams/build/dependant-libs-2.13.17/
ls -la tools/build/dependant-libs-2.13.17/
```

**Step 2**: Verify your classpath includes all directories:
```bash
echo $CLASSPATH
# Should show: .:streams/build/libs/*:clients/build/libs/*:streams/build/dependant-libs-2.13.17/*:tools/build/dependant-libs-2.13.17/*
```

**Step 3**: Test which classes are available:
```bash
java -cp "$CLASSPATH" -XshowSettings:class -version 2>&1 | grep -i kafka
```

### Connection Refused (Kafka)

**Error**: `Connection to node -1 could not be established`

**Note**: This is expected if Kafka broker is not running. The metrics are still created and visible, just the app won't process data.

### JMX Connection Failed

**Error**: Cannot connect via JConsole/VisualVM

**Fix**: Ensure JMX parameters are set:
```bash
-Dcom.sun.management.jmxremote.port=9999 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false
```

## References

- **PR**: https://github.com/apache/kafka/pull/20766
- **JIRA**: KAFKA-19734
- **Code**: `streams/src/main/java/org/apache/kafka/streams/processor/internals/metrics/StreamsMetricsImpl.java:287-293`
- **Test**: `streams/src/test/java/org/apache/kafka/streams/processor/internals/metrics/StreamsMetricsImplTest.java:1430-1488`
