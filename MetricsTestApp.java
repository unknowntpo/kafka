import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Simple test app to verify if application-id tag is applied to all metrics.
 *
 * Run with JMX enabled:
 * java -Dcom.sun.management.jmxremote \
 *      -Dcom.sun.management.jmxremote.port=9999 \
 *      -Dcom.sun.management.jmxremote.authenticate=false \
 *      -Dcom.sun.management.jmxremote.ssl=false \
 *      -cp <classpath> MetricsTestApp
 *
 * Then use JConsole or VisualVM to connect to localhost:9999
 */
public class MetricsTestApp {

    public static void main(final String[] args) throws Exception {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-metrics-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("test-input").to("test-output");

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Start the streams app
        System.out.println("Starting Kafka Streams application...");
        streams.start();

        // Wait a bit for metrics to be registered
        Thread.sleep(5000);

        System.out.println("\n=== Kafka Streams Metrics (via streams.metrics()) ===");
        final Map<MetricName, ? extends Metric> metrics = streams.metrics();

        int totalMetrics = 0;
        int metricsWithAppId = 0;
        int clientLevelMetrics = 0;
        int clientLevelMetricsWithAppId = 0;

        for (final Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            final MetricName metricName = entry.getKey();
            totalMetrics++;

            final boolean hasAppIdTag = metricName.tags().containsKey("application-id");
            final boolean isClientLevel = "stream-metrics".equals(metricName.group());

            if (isClientLevel) {
                clientLevelMetrics++;
                if (hasAppIdTag) {
                    clientLevelMetricsWithAppId++;
                }
            }

            if (hasAppIdTag) {
                metricsWithAppId++;
            }

            // Print detailed info for client-level metrics
            if (isClientLevel) {
                System.out.println("\nMetric: " + metricName.name());
                System.out.println("  Group: " + metricName.group());
                System.out.println("  Tags: " + metricName.tags());
                System.out.println("  Value: " + entry.getValue().metricValue());
                System.out.println("  Has application-id? " + hasAppIdTag);
            }
        }

        System.out.println("\n=== Summary ===");
        System.out.println("Total metrics: " + totalMetrics);
        System.out.println("Metrics with application-id tag: " + metricsWithAppId);
        System.out.println("Client-level metrics: " + clientLevelMetrics);
        System.out.println("Client-level metrics with application-id: " + clientLevelMetricsWithAppId);

        if (clientLevelMetrics > 0 && clientLevelMetrics == clientLevelMetricsWithAppId) {
            System.out.println("\n✓ ALL client-level metrics have application-id tag");
        } else if (clientLevelMetricsWithAppId > 0) {
            System.out.println("\n⚠ Only SOME client-level metrics have application-id tag");
        } else {
            System.out.println("\n✗ NO client-level metrics have application-id tag");
        }

        // List JMX MBeans for Kafka Streams metrics
        System.out.println("\n=== JMX MBeans for Kafka Streams ===");
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Set<ObjectName> kafkaMetrics = mBeanServer.queryNames(
            new ObjectName("kafka.streams:*"), null);

        System.out.println("Found " + kafkaMetrics.size() + " Kafka Streams JMX MBeans");
        System.out.println("\nSample MBeans with application-id attribute:");
        int count = 0;
        for (final ObjectName objectName : kafkaMetrics) {
            final String objNameStr = objectName.toString();
            if (objNameStr.contains("application-id") && objNameStr.contains("type=stream-metrics")) {
                System.out.println("  " + objectName);
                if (++count >= 5) {
                    break;
                }
            }
        }

        System.out.println("\n=== Application will keep running for JMX inspection ===");
        System.out.println("Connect with JConsole/VisualVM to inspect metrics via JMX");
        System.out.println("Press Ctrl+C to stop...\n");

        // Keep running for JMX inspection
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down...");
            streams.close();
        }));

        // Keep alive
        Thread.currentThread().join();
    }
}
