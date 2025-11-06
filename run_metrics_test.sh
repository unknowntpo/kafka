#!/bin/bash
set -e

# Script to build and run the MetricsTestApp with proper classpath
# This verifies that application-id tag is added to all client-level metrics

echo "======================================"
echo "Kafka Streams Metrics Verification"
echo "======================================"
echo ""

# Check if we're in the right directory
if [ ! -f "gradlew" ]; then
    echo "Error: Must run from Kafka repository root"
    exit 1
fi

echo "[1/4] Building Kafka jars..."
./gradlew :streams:jar :clients:jar :streams:copyDependantLibs :tools:copyDependantLibs --quiet

echo "[2/4] Compiling MetricsTestApp..."
javac -cp "streams/build/libs/*:clients/build/libs/*:streams/build/dependant-libs-2.13.17/*" MetricsTestApp.java

echo "[3/4] Setting up classpath..."
# Build classpath with all necessary jars
CLASSPATH="."
CLASSPATH="$CLASSPATH:streams/build/libs/*"
CLASSPATH="$CLASSPATH:clients/build/libs/*"
CLASSPATH="$CLASSPATH:streams/build/dependant-libs-2.13.17/*"
CLASSPATH="$CLASSPATH:tools/build/dependant-libs-2.13.17/*"

echo "[4/4] Running MetricsTestApp with JMX enabled..."
echo ""
echo "Note: Kafka broker doesn't need to be running for metrics verification"
echo "JMX will be available on port 9999 for inspection via JConsole/VisualVM"
echo ""
echo "Press Ctrl+C to stop"
echo ""

java -Dcom.sun.management.jmxremote \
     -Dcom.sun.management.jmxremote.port=9999 \
     -Dcom.sun.management.jmxremote.authenticate=false \
     -Dcom.sun.management.jmxremote.ssl=false \
     -cp "$CLASSPATH" \
     MetricsTestApp
