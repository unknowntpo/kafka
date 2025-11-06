import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Map;
import java.util.Properties;

public class MetricsTestAppQuick {
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

        System.out.println("Starting Kafka Streams application...");
        streams.start();
        Thread.sleep(3000);

        System.out.println("\n=== Client-Level Metrics ===");
        final Map<MetricName, ? extends Metric> metrics = streams.metrics();

        int clientLevelMetrics = 0;
        int clientLevelMetricsWithAppId = 0;

        for (final Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            final MetricName metricName = entry.getKey();
            if ("stream-metrics".equals(metricName.group())) {
                clientLevelMetrics++;
                final boolean hasAppIdTag = metricName.tags().containsKey("application-id");
                if (hasAppIdTag) {
                    clientLevelMetricsWithAppId++;
                }
                System.out.println("\nMetric: " + metricName.name());
                System.out.println("  Tags: " + metricName.tags());
                System.out.println("  Has application-id? " + hasAppIdTag);
            }
        }

        System.out.println("\n=== Summary ===");
        System.out.println("Client-level metrics: " + clientLevelMetrics);
        System.out.println("With application-id tag: " + clientLevelMetricsWithAppId);

        if (clientLevelMetrics > 0 && clientLevelMetrics == clientLevelMetricsWithAppId) {
            System.out.println("\n✅ SUCCESS: ALL client-level metrics have application-id tag");
        } else if (clientLevelMetricsWithAppId > 0) {
            System.out.println("\n⚠️  PARTIAL: Only SOME metrics have application-id tag");
        } else {
            System.out.println("\n❌ FAILED: NO metrics have application-id tag");
        }

        streams.close();
        System.exit(clientLevelMetrics == clientLevelMetricsWithAppId ? 0 : 1);
    }
}
