# Using Azul Mission Control to View Kafka Streams Metrics

## Quick Start

```bash
# 1. Start the test app with JMX
./run_metrics_test.sh

# 2. Open Azul Mission Control

# 3. Connect to the app and view metrics (see below)
```

## Step-by-Step Guide

### Step 1: Start the Application

```bash
./run_metrics_test.sh
```

You should see:
```
[4/4] Running MetricsTestApp with JMX enabled...

Note: Kafka broker doesn't need to be running for metrics verification
JMX will be available on port 9999 for inspection via JConsole/VisualVM

Press Ctrl+C to stop
```

**Leave this terminal running!**

### Step 2: Launch Azul Mission Control

- macOS: Open from Applications or use Spotlight
- Linux: Run `zmc` or `azul-mission-control`
- Windows: Launch from Start Menu

### Step 3: Connect to the Application

1. In Azul Mission Control, look at the **JVM Browser** panel on the left
2. Under **"Local"**, find:
   - Process name: `MetricsTestApp`
   - Or: Process with `localhost:9999`
3. **Right-click** on the process → Select **"Connect"**
   - Or just **double-click** the process

### Step 4: Open MBean Browser

Once connected, you'll see several tabs at the top:
- Overview
- **MBean Browser** ← Click this
- Memory
- Threads
- etc.

Click the **MBean Browser** tab.

### Step 5: Navigate to Kafka Streams Metrics

In the MBean Browser, you'll see a tree on the left side:

```
▼ Domains
  ▼ java.lang
  ▼ java.nio
  ▼ jdk.management
  ▼ kafka.consumer
  ▼ kafka.streams          ← Expand this
    ▼ stream-metrics       ← Expand this
      ▼ type=stream-metrics,application-id=test-metrics-app,client-id=...,process-id=...
        • alive-stream-threads
        • application-id
        • client-state       ← The metric mentioned in the KIP
        • commit-id
        • failed-stream-threads
        • recording-level
        • state
        • topology-description
        • version
```

### Step 6: Inspect a Metric

Click on any metric (e.g., `client-state`).

In the **right panel**, you'll see:

#### Attributes Tab
```
Name                Value
─────────────────────────────────────
client-state        RUNNING
```

#### Info Tab (or Details)
```
ObjectName:    kafka.streams:type=stream-metrics,
               application-id=test-metrics-app,
               client-id=test-metrics-app-bf586c12-...,
               process-id=bf586c12-bf8b-4a0e-bf4a-98ef8abf00ac

Description:   The state of the client
```

### Step 7: Verify application-id Tag

**Key thing to check**: Look at the **ObjectName** in the right panel.

It should contain:
```
application-id=test-metrics-app
```

✅ **Success**: ALL metrics under `stream-metrics` have the `application-id` tag!

## What You're Looking For

### Expected Metrics with application-id

All these metrics should have `application-id=test-metrics-app` in their ObjectName:

1. **client-state** (RUNNING) - The main metric from KIP
2. **state** (RUNNING) - Application state
3. **version** (4.2.0-SNAPSHOT) - Kafka version
4. **application-id** (test-metrics-app) - The app ID itself
5. **alive-stream-threads** (1) - Number of active threads
6. **failed-stream-threads** (0) - Number of failed threads
7. **commit-id** (hash) - Git commit ID
8. **topology-description** (text) - Topology description
9. **recording-level** (INFO) - Recording level

### ObjectName Format

Each metric's ObjectName follows this pattern:
```
kafka.streams:type=stream-metrics,
              application-id=test-metrics-app,
              client-id=test-metrics-app-<UUID>,
              process-id=<UUID>
```

The three tags:
- **application-id** ← NEW (added by PR #20766)
- **client-id** ← Existing
- **process-id** ← Existing

## Tips for Azul Mission Control

### Searching for Metrics

1. In the MBean tree, use **Ctrl+F** (Cmd+F on macOS) to search
2. Type: `application-id`
3. It will highlight all MBeans containing this tag

### Filtering

Some versions have a filter box at the top of the MBean tree:
```
[Filter: ] 🔍
```

Type: `stream-metrics` to show only Kafka Streams metrics.

### Refreshing Values

- Metric values auto-refresh every few seconds
- To force refresh: Click the **refresh icon** 🔄 in the toolbar
- Or: Right-click on metric → **Refresh**

### Exporting Data

To save metric data:
1. Right-click on the domain (e.g., `kafka.streams`)
2. Select **"Export to CSV"** or similar option
3. This creates a snapshot of all metrics and their values

## Troubleshooting

### Can't See MetricsTestApp in JVM Browser

**Problem**: The app doesn't appear in the Local section.

**Solutions**:
1. Check the app is running: Look for terminal output
2. Try **File → Connect → New Connection**:
   - Host: `localhost`
   - Port: `9999`
   - Click **OK**

### "Connection Refused" Error

**Problem**: Cannot connect to localhost:9999

**Solutions**:
1. Verify JMX is enabled: Check the app startup command has:
   ```bash
   -Dcom.sun.management.jmxremote.port=9999
   ```
2. Check port not in use:
   ```bash
   lsof -i :9999
   ```
3. Firewall: Ensure localhost connections are allowed

### Can't Find kafka.streams Domain

**Problem**: No `kafka.streams` in the MBean tree

**Solutions**:
1. Wait 5-10 seconds: Metrics take time to register
2. Refresh the MBean Browser: Click refresh icon 🔄
3. Check the app is actually running (not crashed)
4. Look at the terminal - should show "Kafka Streams Metrics" output

### ObjectName Doesn't Show application-id

**Problem**: Metrics don't have `application-id` tag

**This means the PR changes aren't applied!**

Check:
1. You pulled the latest trunk: `git pull origin trunk`
2. You reset to latest: `git reset --hard origin/trunk`
3. You rebuilt: `./gradlew clean :streams:jar`

## Next Steps

Once you've verified the metrics in Azul Mission Control:

### For PR Review
- Take screenshots showing the ObjectName with `application-id`
- Document which metrics you verified
- Note any metrics that DON'T have the tag (there shouldn't be any)

### For Testing in Your App
Apply the same verification to your own Kafka Streams application:
1. Enable JMX on your app
2. Connect Azul Mission Control
3. Navigate to kafka.streams → stream-metrics
4. Verify all metrics have `application-id` tag matching your app

## Screenshots to Take (for PR review)

Recommended screenshots:
1. **MBean tree** showing `kafka.streams → stream-metrics` expanded
2. **ObjectName** of `client-state` metric showing `application-id` tag
3. **List of all metrics** under stream-metrics
4. **Attributes panel** showing metric values

Save these to document the verification!

## Reference

- **PR**: https://github.com/apache/kafka/pull/20766
- **Discussion**: https://github.com/apache/kafka/pull/20766#discussion_r2484872956
- **Full Documentation**: See `METRICS_VERIFICATION.md`
